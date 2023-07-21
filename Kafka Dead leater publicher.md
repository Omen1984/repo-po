# Kafka Dead Letter Publishing
Оригинал: https://blog.jdriven.com/2022/01/kafka-dead-letter-publishing/

Дата публикации: 18 января 2022 года  
Автор: Tim te Beek  

При потреблении потоков событий в Apache Kafka есть различные способы обработки исключений. В этом блоге будет представлен детальный пример публикации dead-letter записей с использованием Spring Kafka. Будут выделены области, где мы отклоняемся от значений по умолчанию, а также рассмотрены соображения, и будут представлены тесты.

## Приложение KafkaListener

Сначала нам нужно небольшое приложение Kafka Consumer. Наше приложение будет обрабатывать заказы, регистрируя их при получении. Так как мы используем Java 17, мы можем использовать запись Java для нашего Payload.

```java
@Component
class OrderListener {

  private static final Logger log = LoggerFactory.getLogger(OrderListener.class);

  @KafkaListener(topics = KafkaConfiguration.ORDERS)
  void listen(@Payload @Validated Order order) {
    log.info("Received: {}", order);
  }

}

record Order(
  @NotNull UUID orderId,
  @NotNull UUID articleId,
  @Positive int amount) {
}
```

Обратите внимание, что мы используем валидацию `@KafkaListener @Payload` с помощью аннотации нашего заказа как `@Validated`. Мы нуждаемся только в следующей части конфигурации, чтобы включить валидацию.

```java
@Configuration
@EnableKafka
public class KafkaConfiguration implements KafkaListenerConfigurer {

  @Autowired
  private LocalValidatorFactoryBean validator;

  @Override
  public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
    registrar.setValidator(this.validator);
  }

  ...
}
```

Мы вызовем исключение в наших тестах, отправив заказ с отрицательной суммой. Затем вы ожидаете увидеть `ListenerExecutionFailedException` с основной причиной `MethodArgumentNotValidException`.

## Настройка топиков

Затем нам нужны два топика: один обычный для наших входящих заказов и другой исходящий dead letter топик (DLT) для любых заказов, которые мы не сможем успешно обработать. Обе создаются одинаково с помощью `@Bean NewTopic`, который в свою очередь использует `TopicBuilder`.

```java
public static final String ORDERS = "orders";

@Bean
public NewTopic ordersTopic() {
  return TopicBuilder.name(ORDERS)
    // Используйте более одного раздела для часто используемой входной темы
    .partitions(6)
    .build();
}

@Bean
public NewTopic deadLetterTopic(AppKafkaProperties properties) {
  // https://docs.spring.io/spring-kafka/docs/2.8.2/reference/html/#configuring-topics
  return TopicBuilder.name(ORDERS + properties.deadletter().suffix())
    // Используйте только один раздел для редко используемой Dead Letter Topic
    .partitions(1)
    // Используйте более длительное хранение для Dead Letter Topic, позволяющее больше времени на устранение неполадок
    .config(TopicConfig.RETENTION_MS_CONFIG, "" + properties.deadletter().retention().toMillis())
    .build();
}
```

##### Вы заметите два отличия между темами:
- Dead Letter Topic имеет только один раздел. В документации указано, что по умолчанию на Dead Letter Topic следует использовать столько же разделов, сколько доступно на исходной теме. Таким образом, запись DLT может прибыть на тот же раздел, что и исходная запись. Однако с небольшим изменением, который указан ниже, вы можете использовать один раздел для редко используемого DLT, так как исходный раздел уже доступен как заголовок. Таким образом, ваши брокеры не должны управлять множеством неиспользуемых разделов.

- Dead Letter Topic имеет явно заданное время хранения. Таким образом, мы можем хранить записи DLT дольше, чем записи исходной темы, для отладки и восстановления позже, если это необходимо.

## Обработка ошибок

Используя `DeadLetterPublishingRecoverer`, мы можем опубликовать dead-letter запись при возникновении исключения. Мы передаем нашу dead-letter тему в `DeadLetterPublishingRecoverer`, а затем передаем его в `SeekToCurrentErrorHandler`, который обрабатывает исключения.

```java
@Bean
public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<String, Object> kafkaTemplate) {
  return new DeadLetterPublishingRecoverer(kafkaTemplate, (r, e) -> new TopicPartition(ORDERS + ".dlt", r.partition()));
}

@Bean
public SeekToCurrentErrorHandler errorHandler(DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {
  return new SeekToCurrentErrorHandler(deadLetterPublishingRecoverer);
}
```

## Настройка публикации Dead Letter

Следующим шагом мы настроим наш обработчик ошибок, в первую очередь рассмотрев `ConsumerRecordRecoverer`. Мы будем использовать [стандартный DeadLetterPublishingRecoverer](https://docs.spring.io/spring-kafka/docs/2.8.2/reference/html/#dead-letters) с пользовательским аргументом `destinationResolver`, чтобы маршрутизировать записи на первый и единственный раздел нашей темы dead letter:

```java
@Bean
public DefaultErrorHandler defaultErrorHandler(
    KafkaOperations<Object, Object> operations,
    AppKafkaProperties properties) {

  // Опубликуем в теме Dead Letter все сообщения, которые были отброшены после нескольких попыток с задержкой
  var recoverer = new DeadLetterPublishingRecoverer(operations,
    // Отправляем на первый и единственный раздел темы DLT
    (cr, e) -> new TopicPartition(cr.topic() + properties.deadletter().suffix(), 0));

  // Распределяем попытки восстановления во времени, увеличивая время между каждой попыткой
  // Установим максимальное количество попыток ниже max.poll.interval.ms; по умолчанию: 5 минут, иначе мы вызываем перебалансировку потребителя
  Backoff backoff = properties.backoff();
  var exponentialBackOff = new ExponentialBackOffWithMaxRetries(backoff.maxRetries());
  exponentialBackOff.setInitialInterval(backoff.initialInterval().toMillis());
  exponentialBackOff.setMultiplier(backoff.multiplier());
  exponentialBackOff.setMaxInterval(backoff.maxInterval().toMillis());

  // Не пытаемся восстановиться от исключений валидации, когда проверка заказов не удалась
  var errorHandler = new DefaultErrorHandler(recoverer, exponentialBackOff);
  errorHandler.addNotRetryableExceptions(javax.validation.ValidationException.class);
  return errorHandler;
}
```

Мы хотим дать нашему сервису некоторое время для попытки восстановления перед публикацией записей в тему dead letter. Для этого мы используем [ExponentialBackOffWithMaxRetries](https://docs.spring.io/spring-kafka/docs/2.8.2/reference/html/#exp-backoff), который является реализацией `BackOff` и может увеличивать время ожидания между максимальным количеством попыток перед публикацией записи в DLT. Таким образом, если, например, база данных перезапускается, вы не получите немедленно записи DLT, при условии, что перезапуск завершится достаточно скоро. В любом случае вы захотите оставаться ниже значения `max.poll.interval.ms` (по умолчанию: 5 минут), чтобы избежать запуска перебалансировки потребителя.

Наконец, мы объединяем `ConsumerRecordRecoverer` и `BackOff` в `DefaultErrorHandler`. Обратите внимание, как мы явно сообщаем обработчику ошибок не повторять никакие исключения `ValidationException`, так как любые проблемы с валидацией являются постоянными. Это также позволяет нам быстро тестировать, так как нам не нужно исчерпывать все попытки до получения записи DLT. И с этим мы завершаем наш основной код приложения.

## Тестирование 

Наши тесты стремятся максимально приблизиться к реальному сценарию, при этом оставаясь практичными в настройке наших тестов. [TestContainers для Kafka](https://www.testcontainers.org/modules/kafka/) позволяет быстро запустить контейнер Kafka, который мы связываем с нашим приложением Spring Boot, используя аннотацию [@DynamicPropertySource](https://docs.spring.io/spring-boot/docs/2.6.2/reference/htmlsingle/#howto.testing.testcontainers).

Мы используем `@Autowired`, чтобы получить экземпляр `KafkaOperations`, с помощью которого мы производим наши входные записи. И мы создаем `KafkaConsumer` [используя KafkaTestUtils](https://docs.spring.io/spring-kafka/docs/2.8.2/reference/html/#ktu), чтобы прочитать любые записи из темы мертвых писем.

```java
@SpringBootTest
@Testcontainers
class KafkaDeadLetterPublishingApplicationTests {

  private static final String ORDERS_DLT = "orders.DLT";

  private static final Logger log = LoggerFactory.getLogger(KafkaDeadLetterPublishingApplicationTests.class);

  @Container // https://www.testcontainers.org/modules/kafka/
  static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.1"));

  @DynamicPropertySource
  static void setProperties(DynamicPropertyRegistry registry) {
    // Connect our Spring application to our Testcontainers Kafka instance
    registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
  }

  @Autowired
  private KafkaOperations<String, Order> operations;

  private static KafkaConsumer<String, String> kafkaConsumer;

  @BeforeAll
  static void setup() {
    // Create a test consumer that handles <String, String> records, listening to orders.DLT
    // https://docs.spring.io/spring-kafka/docs/2.8.2/reference/html/#testing
    var consumerProps = KafkaTestUtils.consumerProps(kafka.getBootstrapServers(), "test-consumer", "true");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    kafkaConsumer = new KafkaConsumer<>(consumerProps);
    kafkaConsumer.subscribe(List.of(ORDERS_DLT));
  }

  @AfterAll
  static void close() {
    // Close the consumer before shutting down Testcontainers Kafka instance
    kafkaConsumer.close();
  }

  ...
}
```

Теперь мы сначала захотим убедиться, что мы можем обработать действительный заказ, не производя ничего на нашу тему мертвых писем. Следующий тест производит запись в тему ввода и утверждает, что за определенный период времени не поступают записи в тему мертвых писем.

```java
@Test
void should_not_produce_onto_dlt_for_ok_message() throws Exception {
  // Send in valid order
  Order order = new Order(randomUUID(), randomUUID(), 1);
  operations.send("orders", order.orderId().toString(), order)
    .addCallback(
      success -> log.info("Success: {}", success),
      failure -> log.info("Failure: {}", failure));

  // Verify no message was produced onto Dead Letter Topic
  assertThrows(
    IllegalStateException.class,
    () -> KafkaTestUtils.getSingleRecord(kafkaConsumer, ORDERS_DLT, 5000),
    "No records found for topic");
}
```

Во-вторых, мы захотим убедиться, что любые недействительные заказы немедленно поступают на нашу тему мертвых писем. Следующий тест производит заказ с отрицательной суммой, что должно вызвать `ValidationException` в нашем потребителе. Мы утверждаем, что запись производится на нашу тему мертвых писем и что запись имеет ожидаемые значения заголовков и нагрузки.

```java
@Test
void should_produce_onto_dlt_for_bad_message() throws Exception {
  // Amount can not be negative, validation will fail
  Order order = new Order(randomUUID(), randomUUID(), -2);
  operations.send("orders", order.orderId().toString(), order)
    .addCallback(
      success -> log.info("Success: {}", success),
      failure -> log.info("Failure: {}", failure));

  // Verify message produced onto Dead Letter Topic
  ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(kafkaConsumer, ORDERS_DLT, 2000);

  // Verify headers present, and single header value
  ...
}
```

## Заключение

Мы увидели, что довольно легко добавить повторы с обратным отсчетом, публикацию и восстановление темы мертвых писем в Spring Kafka. Это позволяет вам проверять любые неудачные записи в отдельной теме, с диагностическими данными, доступными в заголовках и полезной нагрузке. Затем можно использовать инструменты, такие как AKHQ, чтобы [опубликовать записи из темы мертвых писем на входную тему снова](https://github.com/tchiotludo/akhq/issues/579) после применения исправления.

Конечно, как было сказано в обзоре, это только один из [различных способов обработки исключений](https://www.confluent.io/blog/error-handling-patterns-in-kafka/). Следует отметить, что этот метод не предоставляет автоматический способ обработки записей, опубликованных в теме мертвых писем. Его можно использовать для редких случаев публикации темы мертвых писем, где полностью автоматизированное восстановление не требуется. Также учитывайте, что гарантии порядка обработки не поддерживаются для последующих записей с использованием того же ключа. Вы можете изучить темы повторов и перенаправленные события, если вам нужны более продвинутые гарантии упорядоченной обработки.

Разработано с использованием Spring Kafka версии 2.8.2 и Confluent Platform для Apache Kafka версии 7.0.1. [Полное приложение доступно на GitHub](https://github.com/timtebeek/kafka-dead-letter-publishing).
