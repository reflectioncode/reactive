import org.junit.jupiter.api.*;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Backpressure is a mechanism that allows a consumer to signal to a producer that it is ready receive data.
 * This is important because the producer may be sending data faster than the consumer can process it, and can overwhelm consumer.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#reactive.backpressure
 * https://projectreactor.io/docs/core/release/reference/#_on_backpressure_and_ways_to_reshape_requests
 * https://projectreactor.io/docs/core/release/reference/#_operators_that_change_the_demand_from_downstream
 * https://projectreactor.io/docs/core/release/reference/#producing
 * https://projectreactor.io/docs/core/release/reference/#_asynchronous_but_single_threaded_push
 * https://projectreactor.io/docs/core/release/reference/#_a_hybrid_pushpull_model
 * https://projectreactor.io/docs/core/release/reference/#_an_alternative_to_lambdas_basesubscriber
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c10_Backpressure extends BackpressureBase {

    /**
     * In this exercise subscriber (test) will request several messages from the message stream.
     * Hook to the requests and record them to the `requests` list.
     */
    @Test
    public void request_and_demand() {
        CopyOnWriteArrayList<Long> requests = new CopyOnWriteArrayList<>();
        Flux<String> messageStream = messageStream1()
                .doOnRequest(requests::add); // Записываем запросы в список

        StepVerifier.create(messageStream, StepVerifierOptions.create().initialRequest(0))
                .expectSubscription()
                .thenRequest(1)
                .then(() -> pub1.next("msg#1"))
                .thenRequest(3)
                .then(() -> pub1.next("msg#2", "msg#3"))
                .then(pub1::complete)
                .expectNext("msg#1", "msg#2", "msg#3")
                .verifyComplete();

        Assertions.assertEquals(List.of(1L, 3L), requests);
    }

    /**
     * Adjust previous solution in such a way that you limit rate of requests. Number of requested messages stays the
     * same, but each request should be limited to 1 message.
     */
    @Test
    public void limited_demand() {
        CopyOnWriteArrayList<Long> requests = new CopyOnWriteArrayList<>();
        Flux<String> messageStream = messageStream2()
                .doOnRequest(request -> {
                    for (long i = 0; i < request; i++) {
                        requests.add(1L); // Записываем 1 для каждого запроса
                    }
                });

        StepVerifier.create(messageStream, StepVerifierOptions.create().initialRequest(0))
                .expectSubscription()
                .thenRequest(1)
                .then(() -> pub2.next("msg#1"))
                .thenRequest(1) // Запрашиваем 1 сообщение
                .then(() -> pub2.next("msg#2"))
                .thenRequest(1) // Запрашиваем 1 сообщение
                .then(() -> pub2.next("msg#3"))
                .then(pub2::complete)
                .expectNext("msg#1", "msg#2", "msg#3")
                .verifyComplete();

        Assertions.assertEquals(List.of(1L, 1L, 1L), requests);
    }

    /**
     * Finish the implementation of the `uuidGenerator` so it exactly requested amount of UUIDs. Or better said, it
     * should respect the backpressure of the consumer.
     */
    @Test
    public void uuid_generator() {
        AtomicInteger count = new AtomicInteger(0); // Счетчик для отслеживания сгенерированных UUID
        Flux<UUID> uuidGenerator = Flux.create(sink -> {
            // Подписываемся на запросы
            sink.onRequest(n -> {
                for (int i = 0; i < n; i++) {
                    if (count.get() < 10) { // Ограничиваем до 10 UUID
                        sink.next(UUID.randomUUID());
                        count.incrementAndGet();
                    } else {
                        sink.complete(); // Завершаем, если достигли лимита
                        break;
                    }
                }
            });
        });

        StepVerifier.create(uuidGenerator
                                .doOnNext(System.out::println)
                                .timeout(Duration.ofSeconds(1))
                                .onErrorResume(TimeoutException.class, e -> Flux.empty()),
                        StepVerifierOptions.create().initialRequest(0))
                .expectSubscription()
                .thenRequest(10)
                .expectNextCount(10)
                .thenCancel()
                .verify();
    }

    /**
     * You are receiving messages from malformed publisher that may not respect backpressure.
     * In case that publisher produces more messages than subscriber is able to consume, raise an error.
     */
    @Test
    public void pressure_is_too_much() {
        Flux<String> messageStream = messageStream3()
                .onBackpressureError(); // Изменяем эту строку, чтобы обрабатывать переполнение

        StepVerifier.create(messageStream, StepVerifierOptions.create()
                        .initialRequest(0))
                .expectSubscription()
                .thenRequest(3)
                .then(() -> pub3.next("A", "B", "C", "D")) // Публикуем 4 сообщения
                .expectNext("A", "B", "C") // Ожидаем 3 сообщения
                .expectErrorMatches(Exceptions::isOverflow) // Ожидаем ошибку переполнения
                .verify();
    }

    /**
     * You are receiving messages from malformed publisher that may not respect backpressure. In case that publisher
     * produces more messages than subscriber is able to consume, buffer them for later consumption without raising an
     * error.
     */
    @Test
    public void u_wont_brake_me() {
        Flux<String> messageStream = messageStream4()
                .onBackpressureBuffer(); // Изменяем эту строку, чтобы буферизовать сообщения

        StepVerifier.create(messageStream, StepVerifierOptions.create()
                        .initialRequest(0))
                .expectSubscription()
                .thenRequest(3)
                .then(() -> pub4.next("A", "B", "C", "D")) // Публикуем 4 сообщения
                .expectNext("A", "B", "C") // Ожидаем 3 сообщения
                .then(() -> pub4.complete()) // Завершаем публикацию
                .thenAwait() // Ждем завершения
                .thenRequest(1) // Запрашиваем 1 сообщение
                .expectNext("D") // Ожидаем 4-е сообщение
                .verifyComplete(); // Проверяем завершение
    }

    /**
     * We saw how to react to request demand from producer side. In this part we are going to control demand from
     * consumer side by implementing BaseSubscriber directly.
     * Finish implementation of base subscriber (consumer of messages) with following objectives:
     * - once there is subscription, you should request exactly 10 messages from publisher
     * - once you received 10 messages, you should cancel any further requests from publisher.
     * Producer respects backpressure.
     */
    @Test
    public void subscriber() throws InterruptedException {
        AtomicReference<CountDownLatch> lockRef = new AtomicReference<>(new CountDownLatch(1));
        AtomicInteger count = new AtomicInteger(0);
        AtomicReference<Subscription> sub = new AtomicReference<>();

        remoteMessageProducer()
                .doOnCancel(() -> lockRef.get().countDown())
                .subscribeWith(new BaseSubscriber<String>() {
                    // Изменения в реализации BaseSubscriber
                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        sub.set(subscription);
                        // Запрашиваем 10 сообщений
                        request(10);
                    }

                    @Override
                    protected void hookOnNext(String s) {
                        System.out.println(s);
                        count.incrementAndGet();
                        // Если получили 10 сообщений, отменяем подписку
                        if (count.get() >= 10) {
                            cancel();
                        }
                    }
                    //-----------------------------------------------------
                });

        lockRef.get().await();
        Assertions.assertEquals(10, count.get());
    }
}
