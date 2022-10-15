package academy.devdojo.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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
    void concatOperatorError() {

        Flux<String> flux1 = Flux.just("a", "b")
            .map(s -> {
                if (s.equals("b")) {
                    throw new IllegalArgumentException();
                }
                return s;
            });
        Flux<String> flux2 = Flux.just("c", "d");

        // para adiar o erro
        Flux<String> concatFlux = Flux.concatDelayError(flux1, flux2)
            .log();

        StepVerifier
            .create(concatFlux)
            .expectSubscription()
            .expectNext("a","c", "d")
            .expectError()
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

    @Test
    void mergeOperator() {

        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> mergeFlux = Flux.merge(flux1, flux2).log();

        StepVerifier
            .create(mergeFlux)
            .expectSubscription()
            .expectNext("a", "b", "c", "d")
            .expectComplete()
            .verify();

    }

    @Test
    void mergeWithOperator() {

        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> mergeFlux = flux1.mergeWith(flux2).log();

        StepVerifier
            .create(mergeFlux)
            .expectSubscription()
            .expectNext("a", "b", "c", "d")
            .expectComplete()
            .verify();

    }


    @Test // faz o merge da string em ordem sequencial mesmo com o delay colocado na variavel flux1
    void mergeSequentialOperator() {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> mergeFlux = Flux.mergeSequential(flux1,flux2, flux1)
            .delayElements(Duration.ofMillis(200))
            .log();

        StepVerifier
            .create(mergeFlux)
            .expectSubscription()
            .expectNext("a", "b", "c", "d", "a", "b")
            .expectComplete()
            .verify();

    }

    @Test
    void mergeDelayErrorOperator() {

        Flux<String> flux1 = Flux.just("a", "b")
            .map(s -> {
                if (s.equals("b")) {
                    throw new IllegalArgumentException();
                }
                return s;
            })
            .doOnError(throwable -> log.error("We could do something with this"));

        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> mergeFlux = Flux.mergeDelayError(1, flux1,flux2, flux1)
            .log();

        StepVerifier
            .create(mergeFlux)
            .expectSubscription()
            .expectNext("a", "c", "d", "a")
            .expectError()
            .verify();

    }

    @Test
    void flatMapOperator() throws InterruptedException {

        Flux<String> flux1 = Flux.just("a", "b");

        log.info("********** map ****************");

        // para obter o de dentro ou seja um Flux<String> dever usar o flatmap
        Flux<Flux<String>> fluxFlux = flux1.map(String::toUpperCase)
                .map(this::findByName)
                .log();

        fluxFlux.subscribe(o -> log.info(o.toString()));

        log.info("********** map ****************");

        log.info("********** flatMap ****************");

        Flux<String> flatMap =
                flux1.map(String::toUpperCase)
                        .flatMap(this::findByName)
                        .log();

        flatMap.subscribe(o -> log.info("strings: {} ", o));

        log.info("********** flatMap ****************");

        log.info("********** flatMap with delay ****************");
        // flatmap é assincrono entao ele faz a busca do primeiro 'a' e ja busca o 'b',
        // porem se demorar pra trazer o 'a' ele pode trazer o 'b' antes do 'a'
        Flux<String> flatMapWithDelay =
                flux1.map(String::toUpperCase)
                        .flatMap(s -> findByNameWithDelayInElements(s, 100))
                        .log();

        flatMapWithDelay.subscribe(o -> log.info("strings: {} ", o));

        Thread.sleep(5000);

        log.info("********** flatMap with delay ****************");

        StepVerifier
                .create(flatMap)
                .expectSubscription()
                .expectNext("nameA1", "nameA2", "nameB1", "nameB2")
                .verifyComplete();

    }

    @Test
    void flatMapSequentialOperator() {

        Flux<String> flux1 = Flux.just("a", "b");

        log.info("********** flatMap with delay ****************");
        // flatmap é assincrono entao ele faz a busca do primeiro 'a' e ja busca o 'b',
        // porem se demorar pra trazer o 'a' ele pode trazer o 'b' antes do 'a'
        // para trazer sequencial pode usar o flatMapSequential para manter a ordem de retorno
        Flux<String> flatMapWithDelay =
                flux1.map(String::toUpperCase)
                        .flatMapSequential(s -> findByNameWithDelayInElements(s, 500))
                        .log();

        flatMapWithDelay.subscribe(o -> log.info("strings: {} ", o));

        log.info("********** flatMap with delay ****************");

        StepVerifier
                .create(flatMapWithDelay)
                .expectSubscription()
                .expectNext("nameA1", "nameA2", "nameB1", "nameB2")
                .verifyComplete();

    }

    public Flux<String> findByName(String name) {
        return name.equals("A") ? Flux.just("nameA1", "nameA2") : Flux.just("nameB1", "nameB2");
    }

    public Flux<String> findByNameWithDelayInElements(String name, int delay) {
        return name.equals("A") ? Flux.just("nameA1", "nameA2").delayElements(Duration.ofMillis(delay))  : Flux.just("nameB1", "nameB2");
    }

    private Flux<Object> emptyFlux() {
        return Flux.empty();
    }

}
