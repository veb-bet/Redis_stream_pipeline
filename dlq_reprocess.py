import redis
import json
import logging
import time
from typing import Dict, Any, List
import argparse
from app import RedisConfig, PipelineConfig, Event, DemoEventProcessor

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("dlq-reprocessor")

class DLQReprocessor:
    def __init__(self, redis_config: RedisConfig, pipeline_config: PipelineConfig):
        self.redis = redis_config.get_connection()
        self.config = pipeline_config
        self.processor = DemoEventProcessor()
    
    def get_dlq_messages(self, count: int = 100) -> List[Dict[str, Any]]:
        """Получение сообщений из DLQ"""
        try:
            messages = self.redis.xrange(self.config.dlq_stream, count=count)
            return messages
        except Exception as e:
            logger.error(f"Error reading DLQ messages: {e}")
            return []
    
    def reprocess_message(self, message_id: str, message_data: Dict[bytes, bytes]) -> bool:
        """Повторная обработка сообщения из DLQ"""
        try:
            # Парсинг данных события
            event_data = {}
            for key, value in message_data.items():
                event_data[key.decode()] = json.loads(value)
            
            # Увеличиваем счетчик попыток
            if 'retry_count' in event_data:
                event_data['retry_count'] += 1
            else:
                event_data['retry_count'] = 1
            
            event = Event(**event_data)
            
            # Попытка обработки
            success = self.processor.process(event)
            
            if success:
                # Удаляем из DLQ при успешной обработке
                self.redis.xdel(self.config.dlq_stream, message_id)
                logger.info(f"Successfully reprocessed and removed message {message_id} from DLQ")
                return True
            else:
                # Если снова неудача, оставляем в DLQ
                logger.warning(f"Reprocessing failed for message {message_id}, keeping in DLQ")
                return False
                
        except Exception as e:
            logger.error(f"Error reprocessing message {message_id}: {e}")
            return False
    
    def reprocess_dlq(self, batch_size: int = 10, max_retries: int = 3):
        """Основная функция повторной обработки DLQ"""
        logger.info(f"Starting DLQ reprocessing with batch size {batch_size}")
        
        processed_count = 0
        failed_count = 0
        
        while True:
            try:
                # Получаем сообщения из DLQ
                messages = self.get_dlq_messages(batch_size)
                
                if not messages:
                    logger.info("No messages in DLQ")
                    break
                
                logger.info(f"Found {len(messages)} messages in DLQ")
                
                # Обрабатываем каждое сообщение
                for message_id, message_data in messages:
                    message_id_str = message_id.decode()
                    
                    # Проверяем количество попыток
                    retry_count = 0
                    try:
                        retry_count = json.loads(message_data[b'retry_count']) if b'retry_count' in message_data else 0
                    except:
                        pass
                    
                    if retry_count >= max_retries:
                        logger.warning(f"Message {message_id_str} exceeded max retries ({max_retries}), skipping")
                        continue
                    
                    # Пытаемся обработать
                    if self.reprocess_message(message_id_str, message_data):
                        processed_count += 1
                    else:
                        failed_count += 1
                
                # Если обработали все сообщения, выходим
                if len(messages) < batch_size:
                    break
                    
            except Exception as e:
                logger.error(f"Error in reprocessing loop: {e}")
                time.sleep(1)
        
        logger.info(f"DLQ reprocessing completed: {processed_count} processed, {failed_count} failed")
        return processed_count, failed_count

def main():
    parser = argparse.ArgumentParser(description="DLQ Reprocessing Tool")
    parser.add_argument("--batch-size", type=int, default=10, 
                       help="Number of messages to process in one batch")
    parser.add_argument("--max-retries", type=int, default=3,
                       help="Maximum number of reprocessing attempts")
    parser.add_argument("--continuous", action="store_true",
                       help="Run in continuous mode, monitoring DLQ periodically")
    parser.add_argument("--interval", type=int, default=60,
                       help="Interval between checks in continuous mode (seconds)")
    
    args = parser.parse_args()
    
    # Конфигурация
    redis_config = RedisConfig()
    pipeline_config = PipelineConfig()
    reprocessor = DLQReprocessor(redis_config, pipeline_config)
    
    if args.continuous:
        logger.info(f"Starting continuous DLQ monitoring with {args.interval}s interval")
        try:
            while True:
                reprocessor.reprocess_dlq(args.batch_size, args.max_retries)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            logger.info("Continuous DLQ monitoring stopped by user")
    else:
        reprocessor.reprocess_dlq(args.batch_size, args.max_retries)

if __name__ == "__main__":
    main()