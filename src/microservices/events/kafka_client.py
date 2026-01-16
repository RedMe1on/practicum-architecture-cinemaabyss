from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
from typing import Any, Dict, Optional, Callable
import logging
from conf import Settings
import threading

conf = Settings.read_or_update()
logger = logging.getLogger(__name__)


class KafkaClient:
    def __init__(self):
        self.producer = None
        self.consumer = None
        self.running = False
        self.consumer_thread = None

    def _create_producer(self) -> KafkaProducer:
        """Создание Kafka Producer"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=[conf.KAFKA_BROKERS],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            logger.info("Kafka producer created successfully")
            return producer
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise

    def _create_consumer(
        self, topic: str | None = None, group_id: Optional[str] = None
    ) -> KafkaConsumer:
        """Создание Kafka Consumer"""
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[conf.KAFKA_BROKERS],
                group_id=group_id or conf.KAFKA_GROUP_ID,
                auto_offset_reset=conf.KAFKA_AUTO_OFFSET_RESET,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
            )
            logger.info("Kafka consumer created successfully")
            return consumer
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            raise

    def send_message(
        self,
        message: Dict[str, Any],
        key: Optional[str] = None,
        topic: Optional[str] = None,
    ) -> bool:
        """Отправка сообщения в Kafka"""
        try:
            if not self.producer:
                self.producer = self._create_producer()

            future = self.producer.send(
                topic or conf.KAFKA_TOPIC, value=message, key=key
            )

            # Ожидаем подтверждения
            record_metadata = future.get(timeout=10)
            logger.info(
                f"Message sent to topic {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}"
            )
            return record_metadata

        except KafkaError as e:
            logger.error(f"Failed to send message: {e}")
            return False

    def consume_messages_sync(
        self, topic: str | None = None, timeout_ms: int = 1000, max_messages: int = 10
    ) -> list:
        """Синхронное получение сообщений"""
        messages = []
        try:
            if not self.consumer:
                self.consumer = self._create_consumer(topic)

            # Получаем сообщения
            raw_messages = self.consumer.poll(
                timeout_ms=timeout_ms, max_records=max_messages
            )

            for tp, msg_list in raw_messages.items():
                for msg in msg_list:
                    messages.append(
                        {
                            "topic": tp.topic,
                            "partition": tp.partition,
                            "offset": msg.offset,
                            "key": msg.key.decode("utf-8") if msg.key else None,
                            "value": msg.value,
                            "timestamp": msg.timestamp,
                        }
                    )

            return messages

        except Exception as e:
            logger.error(f"Failed to consume messages: {e}")
            return messages

    def start_async_consumer(self, callback: Callable, group_id: Optional[str] = None):
        """Запуск асинхронного потребителя в отдельном потоке"""

        def consumer_loop():
            consumer = self._create_consumer(group_id)
            self.running = True

            try:
                for message in consumer:
                    if not self.running:
                        break

                    callback(
                        {
                            "topic": message.topic,
                            "partition": message.partition,
                            "offset": message.offset,
                            "key": message.key.decode("utf-8") if message.key else None,
                            "value": message.value,
                            "timestamp": message.timestamp,
                        }
                    )
            except Exception as e:
                logger.error(f"Consumer loop error: {e}")
            finally:
                consumer.close()

        self.consumer_thread = threading.Thread(target=consumer_loop, daemon=True)
        self.consumer_thread.start()
        logger.info("Async consumer started")

    def stop_async_consumer(self):
        """Остановка асинхронного потребителя"""
        self.running = False
        if self.consumer_thread:
            self.consumer_thread.join(timeout=5)
        logger.info("Async consumer stopped")

    def check_connection(self) -> bool:
        """Проверка подключения к Kafka"""
        try:
            # Пробуем создать временного потребителя для проверки соединения
            test_consumer = KafkaConsumer(
                bootstrap_servers=[conf.KAFKA_BROKERS], consumer_timeout_ms=1000
            )
            test_consumer.topics()
            test_consumer.close()

            logger.info("Kafka connection check successful")
            return True
        except Exception as e:
            logger.error(f"Kafka connection check failed: {e}")
            return False

    def close(self):
        """Закрытие соединений"""
        if self.producer:
            self.producer.flush()
            self.producer.close()

        if self.consumer:
            self.consumer.close()

        logger.info("Kafka connections closed")


# Создаем глобальный экземпляр клиента
kafka_client = KafkaClient()
