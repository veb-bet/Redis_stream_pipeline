import time
import redis
from pydantic import BaseModel
import json
import threading
import logging
from typing import Optional, List, Dict, Any
import argparse
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("redis-pipeline")

# Настройка Redis
class RedisConfig:
    def __init__(self, host="localhost", port=6379, db=0):
        self.host = host
        self.port = port
        self.db = db
    
    def get_connection(self):
        return redis.Redis(host=self.host, port=self.port, db=self.db)

# Конфигурация
class PipelineConfig:
    def __init__(self):
        self.stream_name = "events"
        self.dlq_stream = "dlq"
        self.consumer_group = "event_processing_group"
        self.consumer_prefix = "consumer"
        self.max_retries = 3
        self.processing_timeout = 30000  # 30 seconds
        self.batch_size = 10

# Событие
class Event(BaseModel):
    id: int
    type: str
    payload: dict
    timestamp: Optional[float] = None
    retry_count: Optional[int] = 0

    def __init__(self, **data):
        if 'timestamp' not in data:
            data['timestamp'] = time.time()
        if 'retry_count' not in data:
            data['retry_count'] = 0
        super().__init__(**data)

# Базовый класс для обработки событий
class EventProcessor:
    def process(self, event: Event) -> bool:
        raise NotImplementedError("Subclasses must implement process method")

# Простой процессор для демонстрации
class DemoEventProcessor(EventProcessor):
    def process(self, event: Event) -> bool:
        try:
            logger.info(f"Processing event {event.id} of type {event.type}")
            
            # Эмуляция обработки
            time.sleep(0.1)
            
            # Имитация ошибок для определенных типов событий
            if event.type == "fail":
                raise ValueError("Simulated processing error")
            elif event.type == "retry" and event.retry_count < 2:
                raise Exception("Temporary error, should retry")
                
            logger.info(f"Successfully processed event {event.id}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing event {event.id}: {e}")
            return False

# --- Продьюсер ---
class Producer:
    def __init__(self, redis_config: RedisConfig, pipeline_config: PipelineConfig):
        self.redis = redis_config.get_connection()
        self.config = pipeline_config
        self.event_counter = 1
    
    def produce_event(self, event: Event):
        try:
            data = {k: json.dumps(v) for k, v in event.model_dump().items()}
            message_id = self.redis.xadd(self.config.stream_name, data)
            logger.info(f"Produced event {event.id} with message ID: {message_id}")
            return message_id
        except Exception as e:
            logger.error(f"Error producing event {event.id}: {e}")
            raise
    
    def produce_demo_events(self, interval: float = 1.0):
        """Генерация демо-событий"""
        event_types = ["test", "fail", "retry", "success"]
        
        while True:
            event_type = event_types[self.event_counter % len(event_types)]
            event = Event(
                id=self.event_counter,
                type=event_type,
                payload={
                    "data": f"value {self.event_counter}",
                    "source": "demo_producer",
                    "created_at": datetime.now().isoformat()
                }
            )
            
            self.produce_event(event)
            self.event_counter += 1
            time.sleep(interval)

# --- Консьюмер ---
class Consumer:
    def __init__(self, redis_config: RedisConfig, pipeline_config: PipelineConfig, 
                 processor: EventProcessor, consumer_id: str):
        self.redis = redis_config.get_connection()
        self.config = pipeline_config
        self.processor = processor
        self.consumer_id = consumer_id
        self.running = False
    
    def setup_consumer_group(self):
        """Создание consumer group если не существует"""
        try:
            self.redis.xgroup_create(
                self.config.stream_name, 
                self.config.consumer_group, 
                id="0", 
                mkstream=True
            )
            logger.info(f"Created consumer group {self.config.consumer_group}")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info(f"Consumer group {self.config.consumer_group} already exists")
            else:
                logger.error(f"Error creating consumer group: {e}")
                raise
    
    def process_message(self, message_id: str, message_data: Dict[bytes, bytes]) -> bool:
        """Обработка одного сообщения"""
        try:
            # Парсинг данных события
            event_data = {}
            for key, value in message_data.items():
                if key != b'redis-msg-id':
                    event_data[key.decode()] = json.loads(value)
            
            event = Event(**event_data)
            
            # Обработка события
            success = self.processor.process(event)
            
            if success:
                # Подтверждение обработки
                self.redis.xack(
                    self.config.stream_name, 
                    self.config.consumer_group, 
                    message_id
                )
                logger.info(f"Successfully processed and acknowledged message {message_id}")
                return True
            else:
                # Отправка в DLQ
                self.redis.xadd(self.config.dlq_stream, message_data)
                self.redis.xack(
                    self.config.stream_name, 
                    self.config.consumer_group, 
                    message_id
                )
                logger.warning(f"Message {message_id} moved to DLQ after processing failure")
                return False
                
        except Exception as e:
            logger.error(f"Error processing message {message_id}: {e}")
            # В случае ошибки парсинга тоже отправляем в DLQ
            self.redis.xadd(self.config.dlq_stream, message_data)
            self.redis.xack(
                self.config.stream_name, 
                self.config.consumer_group, 
                message_id
            )
            return False
    
    def consume_events(self):
        """Основной цикл потребления событий"""
        self.setup_consumer_group()
        self.running = True
        
        logger.info(f"Consumer {self.consumer_id} started")
        
        while self.running:
            try:
                # Чтение событий из потока
                entries = self.redis.xreadgroup(
                    groupname=self.config.consumer_group,
                    consumername=self.consumer_id,
                    streams={self.config.stream_name: ">"},
                    count=self.config.batch_size,
                    block=self.config.processing_timeout
                )
                
                if not entries:
                    continue
                
                # Обработка пакета сообщений
                for stream_name, messages in entries:
                    for message_id, message_data in messages:
                        self.process_message(message_id, message_data)
                        
            except redis.exceptions.ConnectionError as e:
                logger.error(f"Redis connection error: {e}")
                time.sleep(5)  # Пауза перед повторной попыткой
            except Exception as e:
                logger.error(f"Unexpected error in consumer loop: {e}")
                time.sleep(1)
    
    def stop(self):
        """Остановка консьюмера"""
        self.running = False
        logger.info(f"Consumer {self.consumer_id} stopped")

# Менеджер для запуска multiple consumers
class ConsumerManager:
    def __init__(self, redis_config: RedisConfig, pipeline_config: PipelineConfig, 
                 processor: EventProcessor, num_consumers: int = 3):
        self.redis_config = redis_config
        self.pipeline_config = pipeline_config
        self.processor = processor
        self.num_consumers = num_consumers
        self.consumers = []
        self.threads = []
    
    def start(self):
        """Запуск multiple consumers в отдельных потоках"""
        for i in range(self.num_consumers):
            consumer_id = f"{self.pipeline_config.consumer_prefix}-{i+1}"
            consumer = Consumer(
                self.redis_config, 
                self.pipeline_config, 
                self.processor, 
                consumer_id
            )
            
            thread = threading.Thread(
                target=consumer.consume_events,
                name=f"consumer-thread-{i+1}",
                daemon=True
            )
            
            self.consumers.append(consumer)
            self.threads.append(thread)
            thread.start()
            logger.info(f"Started consumer {consumer_id}")
    
    def stop(self):
        """Остановка всех consumers"""
        for consumer in self.consumers:
            consumer.stop()
        
        for thread in self.threads:
            thread.join(timeout=5)
        
        logger.info("All consumers stopped")

# Мониторинг
class PipelineMonitor:
    def __init__(self, redis_config: RedisConfig, pipeline_config: PipelineConfig):
        self.redis = redis_config.get_connection()
        self.config = pipeline_config
    
    def get_stream_info(self, stream_name: str) -> Dict[str, Any]:
        """Получение информации о потоке"""
        try:
            info = self.redis.xlen(stream_name)
            return {"length": info}
        except Exception as e:
            logger.error(f"Error getting stream info for {stream_name}: {e}")
            return {"length": 0, "error": str(e)}
    
    def get_consumer_group_info(self) -> Dict[str, Any]:
        """Получение информации о consumer group"""
        try:
            info = self.redis.xinfo_groups(self.config.stream_name)
            group_info = {}
            for group in info:
                if group[b'name'].decode() == self.config.consumer_group:
                    group_info = {
                        "consumers": group[b'consumers'],
                        "pending": group[b'pending'],
                        "last_delivered_id": group[b'last-delivered-id'].decode()
                    }
                    break
            return group_info
        except Exception as e:
            logger.error(f"Error getting consumer group info: {e}")
            return {}
    
    def get_pending_messages(self) -> List[Dict[str, Any]]:
        """Получение pending messages"""
        try:
            pending = self.redis.xpending_range(
                self.config.stream_name, 
                self.config.consumer_group,
                min="-", max="+", count=100
            )
            return pending
        except Exception as e:
            logger.error(f"Error getting pending messages: {e}")
            return []
    
    def monitor_pipeline(self, interval: int = 10):
        """Мониторинг пайплайна с заданным интервалом"""
        logger.info("Starting pipeline monitoring...")
        
        while True:
            try:
                # Основная статистика
                main_stream = self.get_stream_info(self.config.stream_name)
                dlq_stream = self.get_stream_info(self.config.dlq_stream)
                group_info = self.get_consumer_group_info()
                pending_messages = self.get_pending_messages()
                
                # Логирование статистики
                logger.info(
                    f"Pipeline Stats - "
                    f"Main Stream: {main_stream.get('length', 0)} messages, "
                    f"DLQ: {dlq_stream.get('length', 0)} messages, "
                    f"Pending: {group_info.get('pending', 0)}, "
                    f"Consumers: {group_info.get('consumers', 0)}"
                )
                
                # Детальная информация о pending messages
                if pending_messages:
                    logger.warning(f"Found {len(pending_messages)} pending messages")
                    for msg in pending_messages[:5]:  # Логируем первые 5
                        logger.warning(
                            f"Pending message: {msg[b'message_id'].decode()}, "
                            f"Consumer: {msg[b'consumer'].decode()}, "
                            f"Idle time: {msg[b'idle']}ms"
                        )
                
                time.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error in monitoring: {e}")
                time.sleep(interval)

# Главная функция
def main():
    parser = argparse.ArgumentParser(description="Redis Streams Pipeline")
    parser.add_argument("--mode", choices=["producer", "consumer", "monitor"], 
                       required=True, help="Operation mode")
    parser.add_argument("--consumers", type=int, default=3, 
                       help="Number of consumer threads (consumer mode only)")
    parser.add_argument("--monitor-interval", type=int, default=10,
                       help="Monitoring interval in seconds (monitor mode only)")
    parser.add_argument("--produce-interval", type=float, default=0.5,
                       help="Event production interval in seconds (producer mode only)")
    
    args = parser.parse_args()
    
    # Конфигурация
    redis_config = RedisConfig()
    pipeline_config = PipelineConfig()
    processor = DemoEventProcessor()
    
    if args.mode == "producer":
        # Режим продьюсера
        producer = Producer(redis_config, pipeline_config)
        logger.info("Starting producer...")
        try:
            producer.produce_demo_events(args.produce_interval)
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
    
    elif args.mode == "consumer":
        # Режим консьюмера
        consumer_manager = ConsumerManager(
            redis_config, pipeline_config, processor, args.consumers
        )
        logger.info(f"Starting {args.consumers} consumers...")
        
        try:
            consumer_manager.start()
            # Бесконечное ожидание (можно прервать Ctrl+C)
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping consumers...")
            consumer_manager.stop()
    
    elif args.mode == "monitor":
        # Режим мониторинга
        monitor = PipelineMonitor(redis_config, pipeline_config)
        monitor.monitor_pipeline(args.monitor_interval)

if __name__ == "__main__":
    main()