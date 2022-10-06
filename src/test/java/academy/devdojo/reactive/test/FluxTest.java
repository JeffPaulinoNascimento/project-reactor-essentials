package academy.devdojo.reactive.test;

import java.time.Duration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@Slf4j
class FluxTest {

    @Test
    void fluxSubscriber() {

         Flux<String> fluxString = Flux.just("Jeff", "William", "DevDojo", "Academy")
             .log();

         StepVerifier.create(fluxString)
             .expectNext("Jeff", "William", "DevDojo", "Academy")
             .verifyComplete();
     }

    @Test
    void fluxSubscriberNumbers() {

        Flux<Integer> fluxNumbers = Flux.range(1, 5)
            .log();

        fluxNumbers.subscribe(integer -> log.info("Number {}", integer));

        log.info("-----------------------------------------------");

        StepVerifier.create(fluxNumbers)
            .expectNext(1,2,3,4,5)
            .verifyComplete();
    }

    @Test
    void fluxSubscriberNumbersFromList() {

        Flux<Integer> fluxNumbers = Flux.fromIterable(List.of(1, 2, 3, 4, 5))
            .log();

        fluxNumbers.subscribe(integer -> log.info("Number {}", integer));

        log.info("-----------------------------------------------");

        StepVerifier.create(fluxNumbers)
            .expectNext(1,2,3,4,5)
            .verifyComplete();
    }

    @Test
    void fluxSubscriberNumbersErrors() {

        Flux<Integer> fluxNumbers = Flux.range(1, 5)
            .log()
            .map(integer -> {
                if (integer == 4) {
                    throw new IndexOutOfBoundsException("Index error when number " + integer + ".");
                }
                return integer;
            });

        fluxNumbers.subscribe(integer -> log.info("Number {}", integer),
            Throwable::printStackTrace,
            () -> log.info("DONE!"));

        log.info("-----------------------------------------------");

        StepVerifier.create(fluxNumbers)
            .expectNext(1,2,3)
            .expectError(IndexOutOfBoundsException.class)
            .verify();
    }

    @Test //elementos retornados de 2 em 2
    // quando publicar tudo que ele tem o onComplete é chamado
    void fluxSubscriberNumbersUglyBackpressure() {

        Flux<Integer> fluxNumbers = Flux.range(1, 10)
            .log();

        fluxNumbers.subscribe(new Subscriber<Integer>() {
            private int count = 0;
            private Subscription subscription;
            private int requestCount = 2;

            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                subscription.request(requestCount);

            }

            @Override
            public void onNext(Integer integer) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    subscription.request(2);
                }

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {

            }
        });

        log.info("-----------------------------------------------");

        StepVerifier.create(fluxNumbers)
            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .verifyComplete();
    }

    @Test //elementos retornados de 2 em 2
        // quando publicar tudo que ele tem o onComplete é chamado
    void fluxSubscriberNumbersNotSoUglyBackpressure() {

        Flux<Integer> fluxNumbers = Flux.range(1, 10)
            .log();

        fluxNumbers.subscribe(new BaseSubscriber<>() {

            private int count = 0;
            private final int requestCount = 2;

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(requestCount);
            }

            @Override
            protected void hookOnNext(Integer value) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    request(requestCount);
                }
            }

        });

        log.info("-----------------------------------------------");

        StepVerifier.create(fluxNumbers)
            .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .verifyComplete();
    }

    @Test
    void fluxSubscriberNumbersWithoutErrorWhenLimitRequestFor3() {

        Flux<Integer> fluxNumbers = Flux.range(1, 5)
            .log()
            .map(integer -> {
                if (integer == 4) {
                    throw new IndexOutOfBoundsException("Index error when number " + integer + ".");
                }
                return integer;
            });

        fluxNumbers.subscribe(integer -> log.info("Number {}", integer),
            Throwable::printStackTrace,
            () -> log.info("DONE!"), subscription -> subscription.request(3));

        log.info("-----------------------------------------------");

        StepVerifier.create(fluxNumbers)
            .expectNext(1,2,3)
            .expectError(IndexOutOfBoundsException.class)
            .verify();
    }

    @Test
    void fluxSubscriberIntervalOne() throws InterruptedException {

        Flux<Long> interval = Flux.interval(Duration.ofMillis(100))
            .take(10)
            .log();

        interval.subscribe(i -> log.info("Number {}", i));

        Thread.sleep(3000);

        log.info("DONE");
    }

    @Test
    void fluxSubscriberIntervalTwo() {

        StepVerifier.withVirtualTime(this::createInterval)
            .expectSubscription()
            .expectNoEvent(Duration.ofDays(1))
            .thenAwait(Duration.ofDays(1))
            .expectNext(0L)
            .thenAwait(Duration.ofDays(1))
            .expectNext(1L)
            .thenCancel()
            .verify();
    }

    private Flux<Long> createInterval() {
        return Flux.interval(Duration.ofDays(1))
            .log();
    }

}
