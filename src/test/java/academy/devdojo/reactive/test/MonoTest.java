package academy.devdojo.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Slf4j
class MonoTest {

    @Test
    void monoSubscriber() {
        String name = "Jeff";
        Mono<String> mono = Mono.just(name)
            .log();

        mono.subscribe();

        log.info("--------------------------------");

        StepVerifier.create(mono)
            .expectNext(name)
            .verifyComplete();

    }

    @Test
    void monoSubscriberConsumerError() {
        String name = "Jeff";
        Mono<String> mono = Mono.just(name)
            .log()
            .map(s -> {throw new RuntimeException("Testing mono with error");});

        mono.subscribe(s -> log.info("Name {}", s), s -> log.error("Something bad happened"));
        mono.subscribe(s -> log.info("Name {}", s), Throwable::printStackTrace);

        log.info("--------------------------------");

        StepVerifier.create(mono)
            .expectError(RuntimeException.class)
            .verify();

    }

    @Test
    void monoSubscriberConsumerComplete() {
        String name = "Jeff";
        Mono<String> mono = Mono.just(name)
            .log()
            .map(String::toUpperCase);

        mono.subscribe(s -> log.info("Value {}", s),
            Throwable::printStackTrace,
            () -> log.info("FINISHED"));

        log.info("--------------------------------");

        StepVerifier.create(mono)
            .expectNext(name.toUpperCase())
            .verifyComplete();

    }

    @Test // cancela tudo que foi feito no subscribe na linha 75
    void monoSubscriberConsumerSubscriptionCancel() {
        String name = "Jeff";
        Mono<String> mono = Mono.just(name)
            .log()
            .map(String::toUpperCase);

        mono.subscribe(s -> log.info("Value {}", s),
            Throwable::printStackTrace,
            () -> log.info("FINISHED"),
             Subscription::cancel);

        log.info("--------------------------------");

        StepVerifier.create(mono)
            .expectNext(name.toUpperCase())
            .verifyComplete();

    }

    @Test
    void monoDoOnMethods() {
        String name = "Jeff";
        Mono<String> mono = Mono.just(name)
            .log()
            .map(String::toUpperCase)
            .doOnSubscribe(subscription -> log.info("Subscribed"))
            .doOnRequest(longNumber -> log.info("Request received, starting doing something..."))
            .doOnNext(s-> log.info("Value is here. Executing on doNext {} ", s))
            .doOnSuccess(s -> log.info("doOnSucess executed"));

        mono.subscribe(s -> log.info("Value {}", s),
            Throwable::printStackTrace,
            () -> log.info("FINISHED"));

        log.info("--------------------------------");

    }

    @Test
    void monoDoOnError() {

        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal argument exception"))
            .doOnError(e -> MonoTest.log.error("Error message: {}", e.getMessage()))
            .doOnNext(s -> log.info("Executing")) // essa linha nao executa porque deu erro
            .log();

        StepVerifier.create(error)
            .expectError(IllegalArgumentException.class)
            .verify();

    }

    @Test // try catch
    void monoDoOnErrorResume() {

        String name = "Jeff test error";

        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal argument exception"))
            .doOnError(e -> MonoTest.log.error("Error message: {}", e.getMessage()))
            .onErrorResume(s -> {
                log.info("Inside on error resume");
                return Mono.just(name);
            })
            .log();

        StepVerifier.create(error)
            .expectNext(name)
            .verifyComplete();

    }

    @Test
    void monoDoOnErrorReturn() {

        String message = "EMPTY - Ignore the last lines";

        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal argument exception"))
            .doOnError(e -> MonoTest.log.error("Error message: {}", e.getMessage()))
            .onErrorReturn("EMPTY - Ignore the last lines")
            .onErrorResume(s -> {
                log.info("Inside on error resume");
                return Mono.just("Jeff");
            })
            .log();

        StepVerifier.create(error)
            .expectNext(message) // nao falha no test porque nao executou a linha 145 em diante
            .verifyComplete();

    }

}
