import org.junit.jupiter.api.*;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * In this important chapter we are going to cover different ways of combining publishers.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#which.values
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c6_CombiningPublishers extends CombiningPublishersBase {

    /**
     * Goal of this exercise is to retrieve e-mail of currently logged-in user.
     * `getCurrentUser()` method retrieves currently logged-in user
     * and `getUserEmail()` will return e-mail for given user.
     *
     * No blocking operators, no subscribe operator!
     * You may only use `flatMap()` operator.
     */
    @Test
    public void behold_flatmap() {
        Hooks.enableContextLossTracking(); //used for testing - detects if you are cheating!

        // Получаем текущего пользователя и его email
        Mono<String> currentUserEmail = getCurrentUser()
                .flatMap(this::getUserEmail); // Используем flatMap для получения email

        //don't change below this line
        StepVerifier.create(currentUserEmail)
                .expectNext("user123@gmail.com")
                .verifyComplete();
    }

    /**
     * `taskExecutor()` returns tasks that should execute important work.
     * Get all the tasks and execute them.
     *
     * Answer:
     * - Is there a difference between Mono.flatMap() and Flux.flatMap()?
     */
    @Test
    public void task_executor() {
        // Получаем задачи и выполняем их
        Flux<Void> tasks = taskExecutor()
                .flatMap(task -> {
                    // Здесь предполагается, что task - это Mono<Void>
                    return task; // Возвращаем задачу для выполнения
                });

        //don't change below this line
        StepVerifier.create(tasks)
                .verifyComplete();

        Assertions.assertEquals(taskCounter.get(), 10);
    }

    /**
     * `streamingService()` opens a connection to the data provider.
     * Once connection is established you will be able to collect messages from stream.
     *
     * Establish connection and get all messages from data provider stream!
     */
    @Test
    public void streaming_service() {
        // Извлекаем Flux<Message> из Mono<Flux<Message>>
        Flux<Message> messageFlux = streamingService()
                .flatMapMany(flux -> flux); // Используем flatMapMany для извлечения Flux из Mono

        //don't change below this line
        StepVerifier.create(messageFlux)
                .expectNextCount(10)
                .verifyComplete();
    }


    /**
     * Join results from services `numberService1()` and `numberService2()` end-to-end.
     * First `numberService1` emits elements and then `numberService2`. (no interleaving)
     *
     * Bonus: There are two ways to do this, check out both!
     */
    @Test
    public void i_am_rubber_you_are_glue() {
        // Объединяем результаты из двух сервисов
        Flux<Integer> numbers = numberService1()
                .concatWith(numberService2()); // Используем concatWith для последовательного объединения

        //don't change below this line
        StepVerifier.create(numbers)
                .expectNext(1, 2, 3, 4, 5, 6, 7)
                .verifyComplete();
    }

    /**
     * Similar to previous task:
     *
     * `taskExecutor()` returns tasks that should execute important work.
     * Get all the tasks and execute each of them.
     *
     * Instead of flatMap() use concatMap() operator.
     *
     * Answer:
     * - What is difference between concatMap() and flatMap()?
     * - What is difference between concatMap() and flatMapSequential()?
     * - Why doesn't Mono have concatMap() operator?
     */
    @Test
    public void task_executor_again() {
        // Получаем задачи и выполняем их с использованием concatMap
        Flux<Void> tasks = taskExecutor()
                .concatMap(task -> {
                    // Здесь предполагается, что task - это Mono<Void>
                    return task; // Возвращаем задачу для выполнения
                });

        //don't change below this line
        StepVerifier.create(tasks)
                .verifyComplete();

        Assertions.assertEquals(taskCounter.get(), 10);
    }

    /**
     * You are writing software for broker house. You can retrieve current stock prices by calling either
     * `getStocksGrpc()` or `getStocksRest()`.
     * Since goal is the best response time, invoke both services but use result only from the one that responds first.
     */
    @Test
    public void need_for_speed() {
        // Используем merge для объединения результатов обоих вызовов
        Flux<String> stonks = Flux.merge(getStocksGrpc(), getStocksRest())
                .take(5); // Берем только первые 5 значений

        //don't change below this line
        StepVerifier.create(stonks)
                .expectNextCount(5)
                .verifyComplete();
    }

    /**
     * As part of your job as software engineer for broker house, you have also introduced quick local cache to retrieve
     * stocks from. But cache may not be formed yet or is empty. If cache is empty, switch to a live source:
     * `getStocksRest()`.
     */
    @Test
    public void plan_b() {
        // Используем switchIfEmpty для переключения на getStocksRest, если кэш пуст
        Flux<String> stonks = getStocksLocalCache()
                .switchIfEmpty(getStocksRest());

        //don't change below this line
        StepVerifier.create(stonks)
                .expectNextCount(6)
                .verifyComplete();

        Assertions.assertTrue(localCacheCalled.get());
    }

    /**
     * You are checking mail in your mailboxes. Check first mailbox, and if first message contains spam immediately
     * switch to a second mailbox. Otherwise, read all messages from first mailbox.
     */
    @Test
    public void mail_box_switcher() {
        // Получаем сообщения из первого почтового ящика
        Flux<Message> primaryMail = mailBoxPrimary();
        // Получаем сообщения из второго почтового ящика
        Flux<Message> secondaryMail = mailBoxSecondary();

        // Объединяем оба потока, но переключаемся на второй, если первое сообщение - спам
        Flux<Message> myMail = primaryMail
                .take(1) // Берем только первое сообщение
                .flatMap(message -> {
                    if (message.metaData.equals("spam")) {
                        // Если сообщение спам, переключаемся на второй почтовый ящик
                        return secondaryMail;
                    } else {
                        // Иначе возвращаем сообщение из первого почтового ящика
                        return Flux.concat(Flux.just(message), primaryMail.skip(1));
                    }
                });

        // Проверяем, что в потоке нет спама
        StepVerifier.create(myMail)
                .expectNextMatches(m -> !m.metaData.equals("spam")) // Ожидаем, что первое сообщение не спам
                .expectNextMatches(m -> !m.metaData.equals("spam")) // Ожидаем, что второе сообщение не спам
                .verifyComplete();

        Assertions.assertEquals(1, consumedSpamCounter.get());
    }

    /**
     * You are implementing instant search for software company.
     * When user types in a text box results should appear in near real-time with each keystroke.
     *
     * Call `autoComplete()` function for each user input
     * but if newer input arrives, cancel previous `autoComplete()` call and call it for latest input.
     */
    @Test
    public void instant_search() {
        // Используем switchMap для отмены предыдущих вызовов
        Flux<String> suggestions = userSearchInput()
                .switchMap(input -> autoComplete(input)); // Отменяем предыдущий вызов при новом вводе

        //don't change below this line
        StepVerifier.create(suggestions)
                .expectNext("reactor project", "reactive project")
                .verifyComplete();
    }


    /**
     * Code should work, but it should also be easy to read and understand.
     * Orchestrate file writing operations in a self-explanatory way using operators like `when`,`and`,`then`...
     * If all operations have been executed successfully return boolean value `true`.
     */
    @Test
    public void prettify() {
        // Используем when, and, then для организации операций
        Mono<Boolean> successful = Mono.when(openFile(), writeToFile("0x3522285912341"), closeFile())
                .then(Mono.just(true)); // Возвращаем true, если все операции успешны

        //don't change below this line
        StepVerifier.create(successful)
                .expectNext(true)
                .verifyComplete();

        Assertions.assertTrue(fileOpened.get());
        Assertions.assertTrue(writtenToFile.get());
        Assertions.assertTrue(fileClosed.get());
    }

    /**
     * Before reading from a file we need to open file first.
     */
    @Test
    public void one_to_n() {
        // Открываем файл перед чтением
        openFile().block(); // Блокируем, чтобы дождаться открытия файла

        Flux<String> fileLines = readFile();

        StepVerifier.create(fileLines)
                .expectNext("0x1", "0x2", "0x3")
                .verifyComplete();
    }


    /**
     * Execute all tasks sequentially and after each task have been executed, commit task changes. Don't lose id's of
     * committed tasks, they are needed to further processing!
     */
    @Test
    public void acid_durability() {
        List<String> committedTasksIds = new ArrayList<>();

        // Выполняем задачи последовательно
        Flux<Mono<String>> taskFlux = tasksToExecute();

        // Обрабатываем каждую задачу и фиксируем её
        taskFlux.concatMap(task -> task.flatMap(taskId -> {
            committedTasksIds.add(taskId); // Сохраняем идентификатор задачи
            return commitTask(taskId); // Фиксируем задачу
        })).blockLast(); // Блокируем, чтобы дождаться завершения всех задач

        //don't change below this line
        StepVerifier.create(Flux.fromIterable(committedTasksIds))
                .expectNext("task#1", "task#2", "task#3")
                .verifyComplete();

        Assertions.assertEquals(3, committedTasksCounter.get());
    }


    /**
     * News have come that Microsoft is buying Blizzard and there will be a major merger.
     * Merge two companies, so they may still produce titles in individual pace but as a single company.
     */
    @Test
    public void major_merger() {
        // Объединяем потоки microsoftTitles и blizzardTitles
        Flux<String> microsoftBlizzardCorp = Flux.merge(microsoftTitles(), blizzardTitles());

        //don't change below this line
        StepVerifier.create(microsoftBlizzardCorp)
                .expectNext("windows12",
                        "wow2",
                        "bing2",
                        "overwatch3",
                        "office366",
                        "warcraft4")
                .verifyComplete();
    }



    /**
     * Your job is to produce cars. To produce car you need chassis and engine that are produced by a different
     * manufacturer. You need both parts before you can produce a car.
     * Also, engine factory is located further away and engines are more complicated to produce, therefore it will be
     * slower part producer.
     * After both parts arrive connect them to a car.
     */
    @Test
    public void car_factory() {
        // Объединяем шасси и двигатели в автомобили
        Flux<Car> producedCars = Flux.zip(carChassisProducer(), carEngineProducer(), Car::new);

        //don't change below this line
        StepVerifier.create(producedCars)
                .recordWith(ConcurrentLinkedDeque::new)
                .expectNextCount(3)
                .expectRecordedMatches(cars -> cars.stream()
                        .allMatch(car -> Objects.equals(car.chassis.getSeqNum(),
                                car.engine.getSeqNum())))
                .verifyComplete();
    }

    /**
     * When `chooseSource()` method is used, based on current value of sourceRef, decide which source should be used.
     */

    //only read from sourceRef
    AtomicReference<String> sourceRef = new AtomicReference<>("X");

    //todo: implement this method based on instructions
    public Mono<String> chooseSource() {
        sourceA(); //<- choose if sourceRef == "A"
        sourceB(); //<- choose if sourceRef == "B"
        return Mono.empty(); //otherwise, return empty
    }

    @Test
    public void deterministic() {
        //don't change below this line
        Mono<String> source = chooseSource();

        sourceRef.set("A");
        StepVerifier.create(source)
                    .expectNext("A")
                    .verifyComplete();

        sourceRef.set("B");
        StepVerifier.create(source)
                    .expectNext("B")
                    .verifyComplete();
    }

    /**
     * Sometimes you need to clean up after your self.
     * Open a connection to a streaming service and after all elements have been consumed,
     * close connection (invoke closeConnection()), without blocking.
     *
     * This may look easy...
     */
    @Test
    public void cleanup() {
        BlockHound.install(); //don't change this line, blocking = cheating!

        // Open a connection to a streaming service
        Flux<String> stream = StreamingConnection.startStreaming()
                .flatMapMany(Function.identity())
                .doFinally(signalType -> {
                    // Close the connection after all elements have been consumed
                    StreamingConnection.closeConnection().subscribe();
                });

        //don't change below this line
        StepVerifier.create(stream)
                .then(() -> Assertions.assertTrue(StreamingConnection.isOpen.get()))
                .expectNextCount(20)
                .verifyComplete();
        Assertions.assertTrue(StreamingConnection.cleanedUp.get());
    }
}
