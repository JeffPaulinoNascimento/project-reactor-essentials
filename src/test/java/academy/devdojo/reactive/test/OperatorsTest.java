package academy.devdojo.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
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

}
