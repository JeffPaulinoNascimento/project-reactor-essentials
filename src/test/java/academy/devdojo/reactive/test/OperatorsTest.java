package academy.devdojo.reactive.test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

@Slf4j
class OperatorsTest {

    @Test //roda na mesma thread
    void subscriberOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
            .map(integer -> {
                log.info("Map 1 - Number {} on Thread {}", integer,
                    Thread.currentThread().getName());
                return integer;
            })
            .subscribeOn(Schedulers.boundedElastic())
            .map(integer -> {
                log.info("Map 2 - Number {} on Thread {}", integer,
                    Thread.currentThread().getName());
                return integer;
            });

        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1,2,3,4)
            .verifyComplete();
    }

    @Test // roda em threads separadas paralelas
    void publishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
            .map(integer -> {
                log.info("Map 1 - Number {} on Thread {}", integer,
                    Thread.currentThread().getName());
                return integer;
            })
            .publishOn(Schedulers.boundedElastic()) // ele muda a thread se colocar antes do primeiro map todos os numeros serao executados na mesma thread
            .map(integer -> {
                log.info("Map 2 - Number {} on Thread {}", integer,
                    Thread.currentThread().getName());
                return integer;
            });

//        flux.subscribe();
//        flux.subscribe();

        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext(1,2,3,4)
            .verifyComplete();
    }

    @Test
    void subscribeOnIO() throws InterruptedException {
        // thread in background
        Mono<List<String>> list = Mono
            .fromCallable(() -> Files.readAllLines(Path.of("text-file")))
            .log()
            .subscribeOn(Schedulers.boundedElastic());

//        list.subscribe(s -> log.info("{}", s));
//
//        // pq o fromCallable vai rodar muito rapido e a main vai finalizar sem printar nada, por isso o thread.sleep
//        Thread.sleep(2000);

        StepVerifier.create(list)
            .expectSubscription()
            .thenConsumeWhile(l -> {
                Assertions.assertFalse(l.isEmpty());
                log.info("Size {}", l.size());
                return true;
            })
            .verifyComplete();

    }

    @Test // um exemplo quando consulta um banco ou uma api externa e é retornado uma lista vazia e precisa retornar um valor padrão
    void switchIfEmptyOperator() {
        Flux<Object> flux = emptyFlux()
            .switchIfEmpty(Flux.just("Not empty anymore"))
            .log();

        StepVerifier.create(flux)
            .expectSubscription()
            .expectNext("Not empty anymore")
            .expectComplete()
            .verify();

    }

    @Test
    void deferOperator() throws InterruptedException {
//        quando instancia ele o Mono.just é executado e mantem na memoria por isso o mesmo tempo para todos os prints abaixo
        Mono<Long> just = Mono.just(System.currentTimeMillis());

        //   com o Mono.defer toda vez que se inscreve é um time diferente
        Mono<Long> defer = Mono.defer(() -> Mono.just(System.currentTimeMillis()) );

        log.info("********* JUST SUBSCRIBRE ***************** ");

        just.subscribe(l -> log.info("mono time {}", l));
        Thread.sleep(300);
        just.subscribe(l -> log.info("mono time {}", l));
        Thread.sleep(300);
        just.subscribe(l -> log.info("mono time {}", l));
        Thread.sleep(300);
        just.subscribe(l -> log.info("mono time {}", l));

        log.info("********* DEFER SUBSCRIBRE ***************** ");

        defer.subscribe(l -> log.info("defer time {}", l));
        Thread.sleep(300);
        defer.subscribe(l -> log.info("defer time {}", l));
        Thread.sleep(300);
        defer.subscribe(l -> log.info("defer time {}", l));
        Thread.sleep(300);
        defer.subscribe(l -> log.info("defer time {}", l));

        AtomicLong atomicLong = new AtomicLong();
        defer.subscribe(atomicLong::set);
        Assertions.assertTrue(atomicLong.get() > 0);

    }

    @Test
    void concatOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concatFlux = Flux.concat(flux1, flux2)
            .log();

        StepVerifier
            .create(concatFlux)
            .expectSubscription()
            .expectNext("a","b","c","d")
            .expectComplete()
            .verify();


    }

    @Test
    void concatWithOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concatFlux = flux1.concatWith(flux2)
            .log();

        StepVerifier
            .create(concatFlux)
            .expectSubscription()
            .expectNext("a","b","c","d")
            .expectComplete()
            .verify();

    }

    @Test
    void combineLatestOperator() {

        // pega o ultimo dado emitido do primeiro flux com o ultimo dado emitido do segundo flux
        // porem se o dado do primeiro flux emitir mais rapido que o segundo acaba combinando no exemplo abaixo 'b' com 'c' e 'b' com 'd'
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> combineLatest = Flux
            .combineLatest(flux1, flux2, (s1, s2) -> s1.toUpperCase() + s2.toUpperCase())
            .log();

        combineLatest.subscribe();

        StepVerifier
            .create(combineLatest)
            .expectSubscription()
            .expectNext("BC","BD")
            .expectComplete()
            .verify();

    }

    private Flux<Object> emptyFlux() {
        return Flux.empty();
    }



}
