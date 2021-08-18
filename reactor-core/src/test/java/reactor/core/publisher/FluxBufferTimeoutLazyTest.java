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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;
import reactor.test.publisher.TestPublisher;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.test.util.RaceTestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.Scannable.from;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxBufferTimeoutLazyTest {

  @AfterEach
  public void tearDown() {
    VirtualTimeScheduler.reset();
  }

  Flux<List<Integer>> scenario_bufferWithTimeoutAccumulateOnSize() {
    return Flux.range(1, 6)
        .delayElements(Duration.ofMillis(300))
        .lazyBufferTimeout(5, Duration.ofMillis(2000));
  }

  @Test
  public void bufferWithTimeoutAccumulateOnSize() {
    StepVerifier.withVirtualTime(this::scenario_bufferWithTimeoutAccumulateOnSize)
        .thenAwait(Duration.ofMillis(1500))
        .assertNext(s -> assertThat(s).containsExactly(1, 2, 3, 4, 5))
        .thenAwait(Duration.ofMillis(2000))
        .assertNext(s -> assertThat(s).containsExactly(6))
        .verifyComplete();
  }

	Flux<List<Integer>> scenario_bufferWithTimeoutAccumulateOnTime() {
		return Flux.range(1, 6)
		           .delayElements(Duration.ofNanos(300)).log("delayed")
		           .lazyBufferTimeout(15, Duration.ofNanos(1500)).log("buffered");
	}

  @Test
  public void bufferWithTimeoutAccumulateOnTime() {
    StepVerifier.withVirtualTime(this::scenario_bufferWithTimeoutAccumulateOnTime)
        //.create(this.scenario_bufferWithTimeoutAccumulateOnTime())
        .thenAwait(Duration.ofNanos(1800))
        .assertNext(s -> assertThat(s).containsExactly(1, 2, 3, 4, 5))
        .thenAwait(Duration.ofNanos(2000))
        .assertNext(s -> assertThat(s).containsExactly(6))
        .verifyComplete();
  }

  @Test
  public void scanSubscriber() {
    CoreSubscriber<List<String>> actual = new LambdaSubscriber<>(null, e -> {
    }, null, null);

    final Scheduler.Worker worker = Schedulers.boundedElastic()
        .createWorker();
    FluxBufferTimeoutLazy.BufferTimeoutSubscriber<String, List<String>> test = new FluxBufferTimeoutLazy.BufferTimeoutSubscriber<String, List<String>>(
        actual, 123, 1000, TimeUnit.MILLISECONDS,
        worker, ArrayList::new);

    try {
      Subscription subscription = Operators.emptySubscription();
      test.onSubscribe(subscription);

      test.requested = 3L;
      test.index = 100;

      assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(worker);
      assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(subscription);
      assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

      assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(3L);
      assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(123);
      assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(23);
      assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);

      assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
      assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();

      test.onError(new IllegalStateException("boom"));
      assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
      assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    } finally {
      worker.dispose();
    }
  }

  @Test
  public void scanOperator() {
    final Flux<List<Integer>> flux = Flux.just(1).lazyBufferTimeout(3, Duration.ofSeconds(1));

    assertThat(flux).isInstanceOf(Scannable.class);
    assertThat(from(flux).scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.parallel());
    assertThat(from(flux).scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
  }

  @Test
  public void shouldShowActualSubscriberDemand() {
    Subscription[] subscriptionsHolder = new Subscription[1];
    CoreSubscriber<List<String>> actual = new LambdaSubscriber<>(null, e -> {
    }, null, s -> subscriptionsHolder[0] = s);

    FluxBufferTimeoutLazy.BufferTimeoutSubscriber<String, List<String>> test = new FluxBufferTimeoutLazy.BufferTimeoutSubscriber<String, List<String>>(
        actual, 123, 1000, TimeUnit.MILLISECONDS, Schedulers.boundedElastic().createWorker(), ArrayList::new);

    Subscription subscription = Operators.emptySubscription();
    test.onSubscribe(subscription);
    subscriptionsHolder[0].request(10);
    assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(10L);
    subscriptionsHolder[0].request(5);
    assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(15L);
  }

  @Test
  public void downstreamDemandShouldBeAbleToDecreaseOnFullBuffer() {
    Subscription[] subscriptionsHolder = new Subscription[1];
    CoreSubscriber<List<String>> actual = new LambdaSubscriber<>(null, e -> {
    }, null, s -> subscriptionsHolder[0] = s);

    FluxBufferTimeoutLazy.BufferTimeoutSubscriber<String, List<String>> test = new FluxBufferTimeoutLazy.BufferTimeoutSubscriber<String, List<String>>(
        actual, 5, 1000, TimeUnit.MILLISECONDS, Schedulers.boundedElastic().createWorker(), ArrayList::new);
    Subscription subscription = Operators.emptySubscription();
    test.onSubscribe(subscription);
    subscriptionsHolder[0].request(1);
    assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(1L);

    for (int i = 0; i < 5; i++) {
      test.onNext(String.valueOf(i));
    }

    assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0L);
  }

  @Test
  public void downstreamDemandShouldBeAbleToDecreaseOnTimeSpan() {
    Subscription[] subscriptionsHolder = new Subscription[1];
    CoreSubscriber<List<String>> actual = new LambdaSubscriber<>(null, e -> {
    }, null, s -> subscriptionsHolder[0] = s);

    VirtualTimeScheduler timeScheduler = VirtualTimeScheduler.getOrSet();
    FluxBufferTimeoutLazy.BufferTimeoutSubscriber<String, List<String>> test = new FluxBufferTimeoutLazy.BufferTimeoutSubscriber<String, List<String>>(
        actual, 5, 100, TimeUnit.MILLISECONDS, timeScheduler.createWorker(), ArrayList::new);

    Subscription subscription = Operators.emptySubscription();
    test.onSubscribe(subscription);
    subscriptionsHolder[0].request(1);
    assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(1L);
    timeScheduler.advanceTimeBy(Duration.ofMillis(100));
    assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(1L);
    test.onNext("0");
    timeScheduler.advanceTimeBy(Duration.ofMillis(100));
    assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0L);
  }

  @Test
  public void requestedFromUpstreamShouldNotExceedDownstreamDemand() {
    Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();
    Flux<String> emitter = sink.asFlux();

    AtomicLong requestedOutstanding = new AtomicLong(0);

    VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();

    //noinspection PointlessArithmeticExpression
    Flux<List<String>> flux = emitter.doOnRequest(requestedOutstanding::addAndGet)
        .lazyBufferTimeout(5, Duration.ofMillis(100), scheduler)
        .doOnNext(list -> requestedOutstanding.addAndGet(0 - list.size()));

    StepVerifier.withVirtualTime(() -> flux, () -> scheduler, 0)
        .expectSubscription()
        .then(() -> assertThat(requestedOutstanding).hasValue(0))
        .thenRequest(2)
        .then(() -> assertThat(requestedOutstanding.get()).isEqualTo(5))
        .then(() -> sink.emitNext("a", FAIL_FAST))
        .thenAwait(Duration.ofMillis(100))
        .assertNext(s -> assertThat(s).containsExactly("a"))
        .then(() -> assertThat(requestedOutstanding).hasValue(5))
        .thenRequest(1)
        .then(() -> assertThat(requestedOutstanding).hasValue(5))
        .thenCancel()
        .verify();
  }


  @Test
  public void requestedFromUpstreamShouldNotExceedMaxSize() {
    Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();
    Flux<String> emitter = sink.asFlux();

    AtomicLong requestedOutstanding = new AtomicLong(0);

    VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();

    //noinspection PointlessArithmeticExpression
    Flux<List<String>> flux = emitter.doOnRequest(requestedOutstanding::addAndGet)
        .lazyBufferTimeout(5, Duration.ofMillis(100), scheduler)
        .doOnNext(list -> requestedOutstanding.addAndGet(0 - list.size()));

    StepVerifier.withVirtualTime(() -> flux, () -> scheduler, 0)
        .expectSubscription()
        .then(() -> assertThat(requestedOutstanding).hasValue(0))
        .thenRequest(2)
        .then(() -> assertThat(requestedOutstanding.get()).isEqualTo(5))
        .then(() -> sink.emitNext("a", FAIL_FAST))
        .thenAwait(Duration.ofMillis(100))
        .assertNext(s -> assertThat(s).containsExactly("a"))
        .then(() -> assertThat(requestedOutstanding).hasValue(5))
        .thenRequest(1)
        .then(() -> assertThat(requestedOutstanding).hasValue(5))
        .then(() -> sink.emitNext("a", FAIL_FAST))
        .thenAwait(Duration.ofMillis(100))
        .assertNext(s -> assertThat(s).containsExactly("a"))
        .then(() -> assertThat(requestedOutstanding).hasValue(5))
        .then(() -> sink.emitNext("a", FAIL_FAST))
        .thenAwait(Duration.ofMillis(100))
        .assertNext(s -> assertThat(s).containsExactly("a"))
        .then(() -> assertThat(requestedOutstanding).hasValue(4))
        .thenCancel()
        .verify();
  }



  @Test
  public void shouldRespectBackpressure() {
		Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();
		Flux<String> emitter = sink.asFlux();

		AtomicLong requestedOutstanding = new AtomicLong(0);

		VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();

		//noinspection PointlessArithmeticExpression
		Flux<List<String>> flux = emitter.doOnRequest(requestedOutstanding::addAndGet)
				.lazyBufferTimeout(5, Duration.ofSeconds(1), scheduler)
				.doOnNext(list -> requestedOutstanding.addAndGet(0 - list.size()));

		StepVerifier.withVirtualTime(() -> flux, () -> scheduler, 0)
				.expectSubscription()
				.then(() -> assertThat(requestedOutstanding).hasValue(0))
				.thenRequest(1)
				.then(() -> assertThat(requestedOutstanding.get()).isEqualTo(5))
				.then(() -> sink.emitNext("a", FAIL_FAST))
				.thenAwait(Duration.ofSeconds(1))
				.assertNext(s -> assertThat(s).containsExactly("a"))
				.then(() -> assertThat(requestedOutstanding).hasValue(4))
				.then(() -> sink.emitNext("b", FAIL_FAST))
        .then(() -> sink.emitNext("c", FAIL_FAST))
				.expectNoEvent(Duration.ofSeconds(2))
				.thenAwait(Duration.ofSeconds(2))
        .thenRequest(1)
        .thenAwait(Duration.ofSeconds(2))
        .assertNext(s -> assertThat(s).containsExactly("b", "c"))
				.thenCancel()
				.verify();
	}

  @Test
  public void exceedingUpstreamDemandResultsInError() {
    Subscription[] subscriptionsHolder = new Subscription[1];

    AtomicReference<Throwable> capturedException = new AtomicReference<>();

    CoreSubscriber<List<String>> actual = new LambdaSubscriber<>(null, capturedException::set, null, s -> subscriptionsHolder[0] = s);

    VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
    FluxBufferTimeoutLazy.BufferTimeoutSubscriber<String, List<String>> test = new FluxBufferTimeoutLazy.BufferTimeoutSubscriber<String, List<String>>(
        actual, 5, 1000, TimeUnit.MILLISECONDS, scheduler.createWorker(), ArrayList::new);

    Subscription subscription = Operators.emptySubscription();
    test.onSubscribe(subscription);
    subscriptionsHolder[0].request(1);

    for (int i = 0; i < 5; i++) {
      test.onNext(String.valueOf(i));
    }

    assertThat(capturedException.get()).isNull();

    test.onNext(String.valueOf(123));

    assertThat(capturedException.get()).isInstanceOf(IllegalStateException.class)
        .hasMessage("Unrequested element received");
  }

  @Test
  public void scanSubscriberCancelled() {
    CoreSubscriber<List<String>>
        actual = new LambdaSubscriber<>(null, e -> {
    }, null, null);

    FluxBufferTimeoutLazy.BufferTimeoutSubscriber<String, List<String>> test = new FluxBufferTimeoutLazy.BufferTimeoutSubscriber<String, List<String>>(
        actual, 123, 1000, TimeUnit.MILLISECONDS, Schedulers.boundedElastic().createWorker(), ArrayList::new);

    assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
    assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();

    test.cancel();
    assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
  }

  @Test
  public void flushShouldNotRaceWithNext() {
    Set<Integer> seen = new HashSet<>();
    Consumer<List<Integer>> consumer = integers -> {
      for (Integer i : integers) {
        if (!seen.add(i)) {
          throw new IllegalStateException("Duplicate! " + i);
        }
      }
    };
    CoreSubscriber<List<Integer>> actual = new LambdaSubscriber<>(consumer, null, null, null);

    FluxBufferTimeoutLazy.BufferTimeoutSubscriber<Integer, List<Integer>> test = new FluxBufferTimeoutLazy.BufferTimeoutSubscriber<Integer, List<Integer>>(
        actual, 3, 1000, TimeUnit.MILLISECONDS, Schedulers.boundedElastic().createWorker(), ArrayList::new);
    test.onSubscribe(Operators.emptySubscription());

    AtomicInteger counter = new AtomicInteger();
    for (int i = 0; i < 500; i++) {
      RaceTestUtils.race(
          () -> test.onNext(counter.getAndIncrement()),
          test::flushCallback
      );
    }
  }

  //see https://github.com/reactor/reactor-core/issues/1247
  @Test
  public void rejectedOnNextLeadsToOnError() {
    Scheduler scheduler = Schedulers.newSingle("rejectedOnNextLeadsToOnError");
    scheduler.dispose();

    StepVerifier.create(Flux.just(1, 2, 3)
            .lazyBufferTimeout(4, Duration.ofMillis(500), scheduler))
        .expectError(RejectedExecutionException.class)
        .verify(Duration.ofSeconds(1));
  }

  @Test
  public void discardOnCancel() {
    StepVerifier.create(Flux.just(1, 2, 3)
            .concatWith(Mono.never())
            .lazyBufferTimeout(10, Duration.ofMillis(100)))
        .thenAwait(Duration.ofMillis(10))
        .thenCancel()
        .verifyThenAssertThat()
        .hasDiscardedExactly(1, 2, 3);
  }

  @Test
  public void discardOnFlushWithoutRequest() {
    TestPublisher<Integer> testPublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

    StepVerifier.create(testPublisher
                .flux()
                .lazyBufferTimeout(10, Duration.ofMillis(200)),
            StepVerifierOptions.create().initialRequest(0))
        .then(() -> testPublisher.emit(1, 2, 3))
        .thenAwait(Duration.ofMillis(250))
        .expectErrorMatches(Exceptions::isOverflow)
        .verifyThenAssertThat()
        .hasDiscardedExactly(1, 2, 3);
  }

  @Test
  public void discardOnTimerRejected() {
    Scheduler scheduler = Schedulers.newSingle("discardOnTimerRejected");

    StepVerifier.create(Flux.just(1, 2, 3)
            .doOnNext(n -> scheduler.dispose())
            .lazyBufferTimeout(10, Duration.ofMillis(100), scheduler))
        .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(RejectedExecutionException.class))
        .verifyThenAssertThat()
        .hasDiscardedExactly(1);
  }

  @Test
  public void discardOnError() {
    StepVerifier.create(Flux.just(1, 2, 3)
            .concatWith(Mono.error(new IllegalStateException("boom")))
            .lazyBufferTimeout(10, Duration.ofMillis(100)))
        .expectErrorMessage("boom")
        .verifyThenAssertThat()
        .hasDiscardedExactly(1, 2, 3);
  }
}
