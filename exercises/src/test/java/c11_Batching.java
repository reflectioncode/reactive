import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

/**
 * Another way of controlling amount of data flowing is batching.
 * Reactor provides three batching strategies: grouping, windowing, and buffering.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#advanced-three-sorts-batching
 * https://projectreactor.io/docs/core/release/reference/#which.window
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c11_Batching extends BatchingBase {

    /**
     * To optimize disk writing, write data in batches of max 10 items, per batch.
     */
    @Test
    public void batch_writer() {
        // Создаем поток данных и разбиваем его на чанки по 10 элементов
        Flux<Void> dataStream = dataStream()
                .buffer(10) // Разбиваем на чанки по 10 элементов
                .flatMap(this::writeToDisk); // Записываем каждый чанк на диск

        // do not change the code below
        StepVerifier.create(dataStream)
                .verifyComplete();

        Assertions.assertEquals(10, diskCounter.get());
    }

    /**
     * You are implementing a command gateway in CQRS based system. Each command belongs to an aggregate and has `aggregateId`.
     * All commands that belong to the same aggregate needs to be sent sequentially, after previous command was sent, to
     * prevent aggregate concurrency issue.
     * But commands that belong to different aggregates can and should be sent in parallel.
     * Implement this behaviour by using `GroupedFlux`, and knowledge gained from the previous exercises.
     */
    @Test
    public void command_gateway() {
        // Группируем команды по aggregateId и обрабатываем их последовательно
        Mono<Void> processCommands = inputCommandStream()
                .groupBy(Command::getAggregateId) // Группируем команды по aggregateId
                .flatMap(groupedFlux -> groupedFlux.concatMap(this::sendCommand)) // Обрабатываем команды последовательно в каждой группе
                .then(); // Возвращаем Mono<Void> после завершения всех команд

        // do not change the code below
        Duration duration = StepVerifier.create(processCommands)
                .verifyComplete();

        Assertions.assertTrue(duration.getSeconds() <= 3, "Expected to complete in less than 3 seconds");
    }


    /**
     * You are implementing time-series database. You need to implement `sum over time` operator. Calculate sum of all
     * metric readings that have been published during one second.
     */
    @Test
    public void sum_over_time() {
        Flux<Long> metrics = metrics()
                .window(Duration.ofSeconds(1)) // Разделяем поток на окна по 1 секунде
                .flatMap(window -> window.reduce(0L, Long::sum)) // Считаем сумму в каждом окне
                .take(10); // Берем первые 10 значений

        StepVerifier.create(metrics)
                .expectNext(45L, 165L, 255L, 396L, 465L, 627L, 675L, 858L, 885L, 1089L)
                .verifyComplete();
    }
}
