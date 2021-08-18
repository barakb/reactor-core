/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

/**
 * @author Barak Bar Orion
 * <p>
 * A lazy version of FluxBufferTimeoutLazy tha perfar to respec the demand of the downstream
 * subscriber over the timespan
 * In case the timespan pass and there are items in the buffer and the demand from downstream is 0
 * this publisher will not send onNext event to the downstream subscriber, instead it will collect every
 * next emited by the upstream publiser into the list and wait for request() from downlstream subscriber.
 * For that to work FluxBufferTimeoutLazy never request more than batchSize items from the upstream subscriber,
 * So it can be somewhat less efficient from the eager version.
 */
final class FluxBufferTimeoutLazy<T, C extends Collection<? super T>> extends InternalFluxOperator<T, C> {

  final int batchSize;
  final Supplier<C> bufferSupplier;
  final Scheduler timer;
  final long timespan;
  final TimeUnit unit;

  FluxBufferTimeoutLazy(Flux<T> source,
                        int maxSize,
                        long timespan,
                        TimeUnit unit,
                        Scheduler timer,
                        Supplier<C> bufferSupplier) {
    super(source);
    if (timespan <= 0) {
      throw new IllegalArgumentException("Timeout period must be strictly positive");
    }
    if (maxSize <= 0) {
      throw new IllegalArgumentException("maxSize must be strictly positive");
    }
    this.timer = Objects.requireNonNull(timer, "Timer");
    this.timespan = timespan;
    this.unit = Objects.requireNonNull(unit, "unit");
    this.batchSize = maxSize;
    this.bufferSupplier = Objects.requireNonNull(bufferSupplier, "bufferSupplier");
  }

  @Override
  public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super C> actual) {
    return new BufferTimeoutSubscriber<>(
        Operators.serialize(actual),
        batchSize,
        timespan,
        unit,
        timer.createWorker(),
        bufferSupplier
    );
  }

  @Override
  public Object scanUnsafe(Attr key) {
    if (key == Attr.RUN_ON) return timer;
    if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

    return super.scanUnsafe(key);
  }

  final static class BufferTimeoutSubscriber<T, C extends Collection<? super T>>
      implements InnerOperator<T, C> {

    final CoreSubscriber<? super C> actual;

    final static int NOT_TERMINATED = 0;
    final static int TERMINATED_WITH_SUCCESS = 1;
    final static int TERMINATED_WITH_ERROR = 2;
    final static int TERMINATED_WITH_CANCEL = 3;

    final int batchSize;
    final long timespan;
    final TimeUnit unit;
    final Scheduler.Worker timer;
    final Runnable flushTask;

    private Subscription subscription;

    volatile int terminated =
        NOT_TERMINATED;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<BufferTimeoutSubscriber> TERMINATED =
        AtomicIntegerFieldUpdater.newUpdater(BufferTimeoutSubscriber.class, "terminated");


    volatile long requested;

    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<BufferTimeoutSubscriber> REQUESTED =
        AtomicLongFieldUpdater.newUpdater(BufferTimeoutSubscriber.class, "requested");

    volatile long outstanding;

    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<BufferTimeoutSubscriber> OUTSTANDING =
        AtomicLongFieldUpdater.newUpdater(BufferTimeoutSubscriber.class, "outstanding");

    volatile int index = 0;

    static final AtomicIntegerFieldUpdater<BufferTimeoutSubscriber> INDEX =
        AtomicIntegerFieldUpdater.newUpdater(BufferTimeoutSubscriber.class, "index");

    static final AtomicReferenceFieldUpdater<BufferTimeoutSubscriber, Disposable> TIME_SPAN_REGISTRATION =
        AtomicReferenceFieldUpdater.newUpdater(BufferTimeoutSubscriber.class, Disposable.class, "timespanRegistration");


    volatile Disposable timespanRegistration;

    final Supplier<C> bufferSupplier;

    volatile C values;

    BufferTimeoutSubscriber(CoreSubscriber<? super C> actual,
                            int maxSize,
                            long timespan,
                            TimeUnit unit,
                            Scheduler.Worker timer,
                            Supplier<C> bufferSupplier) {
      this.actual = actual;
      this.timespan = timespan;
      this.unit = unit;
      this.timer = timer;
      this.flushTask = () -> {
        if (terminated == NOT_TERMINATED) {
          int index;
          for (; ; ) {
            index = this.index;
            Disposable timespanRegistration_ = timespanRegistration;
            if (index == 0 || requested == 0) {
              if (timespanRegistration_ != null) {
                timespanRegistration = null;
                timespanRegistration_.dispose();
              }
              return;
            }
            if (INDEX.compareAndSet(this, index, 0)) {
              break;
            }
          }
          flushCallback();
          // in case request(n) was called during the flushCallback()
          Disposable timespanRegistration_ = timespanRegistration;
          if (0 < requested && 0 < index && timespanRegistration_ != null) {
            timespanRegistration = null;
            timespanRegistration_.dispose();
            subscribeTimer(null);
          }
        }
      };

      this.batchSize = maxSize;
      this.bufferSupplier = bufferSupplier;
    }

    private void doOnSubscribe() {
      values = bufferSupplier.get();
    }

    void nextCallback(T value) {
      synchronized (this) {
        if (OUTSTANDING.decrementAndGet(this) < 0) {
          actual.onError(Exceptions.failWithOverflow("Unrequested element received"));
          Context ctx = actual.currentContext();
          Operators.onDiscard(value, ctx);
          Operators.onDiscardMultiple(values, ctx);
          return;
        }

        C v = values;
        if (v == null) {
          v = Objects.requireNonNull(bufferSupplier.get(),
              "The bufferSupplier returned a null buffer");
          values = v;
        }
        v.add(value);
      }
    }

    void flushCallback() {
      final C v;
      boolean flush = false;
      synchronized (this) {
        if (0 == requested) {
          return;
        }
        v = values;
        if (v != null && !v.isEmpty()) {
          values = bufferSupplier.get();
          flush = true;
        }
      }

      if (flush) {
        long r = requested;
        if (r != 0L) {
          if (r != Long.MAX_VALUE) {
            long next;
            for (; ; ) {
              next = r - 1;
              if (REQUESTED.compareAndSet(this, r, next)) {
                actual.onNext(v);
                if (0 < next) {
                  requestMore();
                }
                return;
              }

              r = requested;
              if (r <= 0L) {
                break;
              }
            }
          } else {
            actual.onNext(v);
          }
        }
      }
    }

    @Override
    @Nullable
    public Object scanUnsafe(Attr key) {
      if (key == Attr.PARENT) return subscription;
      if (key == Attr.CANCELLED) return terminated == TERMINATED_WITH_CANCEL;
      if (key == Attr.TERMINATED) return terminated == TERMINATED_WITH_ERROR || terminated == TERMINATED_WITH_SUCCESS;
      if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
      if (key == Attr.CAPACITY) return batchSize;
      if (key == Attr.BUFFERED) return batchSize - index;
      if (key == Attr.RUN_ON) return timer;
      if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

      return InnerOperator.super.scanUnsafe(key);
    }

    @Override
    public void onNext(final T value) {
      int index;
      for (; ; ) {
        index = this.index + 1;
        if (INDEX.compareAndSet(this, index - 1, index)) {
          break;
        }
      }

      if (index == 1) {
        if (!subscribeTimer(value)) {
          return;
        }
      }

      nextCallback(value);

      if (this.index % batchSize == 0) {
        this.index = 0;
        synchronized (this) {
          Disposable timespanRegistration_ = timespanRegistration;
          if (timespanRegistration_ != null) {
            timespanRegistration = null;
            timespanRegistration_.dispose();
          }
        }
        flushCallback();
      }
    }

    private boolean subscribeTimer(@Nullable final T value) {
      try {
        timespanRegistration = timer.schedule(flushTask, timespan, unit);
        return true;
      } catch (RejectedExecutionException ree) {
        Context ctx = actual.currentContext();
        onError(Operators.onRejectedExecution(ree, subscription, null, value, ctx));
        Operators.onDiscard(value, ctx);
        return false;
      }
    }

    void checkedComplete() {
      try {
        flushCallback();
      } finally {
        actual.onComplete();
      }
    }

    /**
     * @return has this {@link Subscriber} terminated with success ?
     */
    boolean isCompleted() {
      return terminated == TERMINATED_WITH_SUCCESS;
    }

    /**
     * @return has this {@link Subscriber} terminated with an error ?
     */
    boolean isFailed() {
      return terminated == TERMINATED_WITH_ERROR;
    }

    @Override
    public void request(long n) {
      if (Operators.validate(n)) {
        long prev = Operators.addCap(REQUESTED, this, n);
        if (terminated != NOT_TERMINATED) {
          return;
        }
        if (batchSize == Integer.MAX_VALUE || n == Long.MAX_VALUE) {
          requestAll();
        } else {
          requestMore();
        }

        if (prev == 0 && 0 < index) {

          // should activate timer if not already activated
          synchronized (this) {
            if (timespanRegistration == null) {
              subscribeTimer(null);
            }
          }
        }
      }
    }

    void requestAll() {
      Subscription s = this.subscription;
      if (s != null) {
        Operators.addCap(OUTSTANDING, this, Long.MAX_VALUE);
        s.request(Long.MAX_VALUE);
      }
    }

    void requestMore() {
      Subscription s = this.subscription;
      if (s != null) {
        long requestedAlready = outstanding;
        long missing = batchSize - outstanding;
        while (true) {
          if (requestedAlready == Long.MAX_VALUE) {
            return;
          }
          if (OUTSTANDING.compareAndSet(this, requestedAlready, batchSize)) {
            break;
          }
          requestedAlready = outstanding;
          missing = batchSize - outstanding;
        }
        if (0 < missing) {
          s.request(missing);
        }
      }
    }

    @Override
    public CoreSubscriber<? super C> actual() {
      return actual;
    }

    @Override
    public void onComplete() {
      if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_SUCCESS)) {
        timer.dispose();
        checkedComplete();
      }
    }

    @Override
    public void onError(Throwable throwable) {
      if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_ERROR)) {
        timer.dispose();
        Context ctx = actual.currentContext();
        synchronized (this) {
          C v = values;
          if (v != null) {
            Operators.onDiscardMultiple(v, ctx);
            v.clear();
            values = null;
          }
        }
        actual.onError(throwable);
      }
    }

    @Override
    public void onSubscribe(Subscription s) {
      if (Operators.validate(this.subscription, s)) {
        this.subscription = s;
        doOnSubscribe();
        actual.onSubscribe(this);
      }
    }

    @Override
    public void cancel() {
      if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_CANCEL)) {
        timer.dispose();
        Subscription s = this.subscription;
        if (s != null) {
          this.subscription = null;
          s.cancel();
        }
        C v = values;
        if (v != null) {
          Operators.onDiscardMultiple(v, actual.currentContext());
          v.clear();
        }
      }
    }
  }
}

