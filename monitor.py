import redis
import time
import logging
from typing import Dict, Any
import argparse
from app import RedisConfig, PipelineConfig

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("advanced-monitor")

class AdvancedMonitor:
    def __init__(self, redis_config: RedisConfig, pipeline_config: PipelineConfig):
        
        self.redis = redis_config.get_connection()
        self.config = pipeline_config
    
    def get_detailed_stats(self) -> Dict[str, Any]:
        """Получение детальной статистики"""
        stats = {}
        
        try:
            # Основные потоки
            stats['main_stream_length'] = self.redis.xlen(self.config.stream_name)
            stats['dlq_stream_length'] = self.redis.xlen(self.config.dlq_stream)
            
            group_info = self.redis.xinfo_groups(self.config.stream_name)
            for group in group_info:
                if group[b'name'].decode() == self.config.consumer_group:
                    stats['consumers'] = group[b'consumers']
                    stats['pending'] = group[b'pending']
                    stats['last_delivered_id'] = group[b'last-delivered-id'].decode()
                    break

            pending = self.redis.xpending_range(
                self.config.stream_name, 
                self.config.consumer_group,
                min="-", max="+", count=50
            )
            stats['pending_details'] = pending
            
            stats['memory_info'] = self.redis.info('memory')
            
        except Exception as e:
            logger.error(f"Error getting detailed stats: {e}")
            stats['error'] = str(e)
        
        return stats
    
    def format_stats(self, stats: Dict[str, Any]) -> str:
        """Форматирование статистики для вывода"""
        output = []
        output.append("=" * 50)
        output.append("REDIS STREAMS PIPELINE MONITOR")
        output.append("=" * 50)
        
        output.append(f"Main Stream ({self.config.stream_name}): {stats.get('main_stream_length', 0)} messages")
        output.append(f"DLQ Stream ({self.config.dlq_stream}): {stats.get('dlq_stream_length', 0)} messages")
        output.append(f"Consumer Group: {self.config.consumer_group}")
        output.append(f"Consumers: {stats.get('consumers', 0)}")
        output.append(f"Pending Messages: {stats.get('pending', 0)}")
        
        if 'pending_details' in stats and stats['pending_details']:
            output.append("\nPending Messages Details:")
            for i, msg in enumerate(stats['pending_details'][:5]):
                output.append(
                    f"  {i+1}. ID: {msg[b'message_id'].decode()}, "
                    f"Consumer: {msg[b'consumer'].decode()}, "
                    f"Idle: {msg[b'idle']}ms, "
                    f"Deliveries: {msg[b'delivered']}"
                )
        
        output.append("=" * 50)
        return "\n".join(output)
    
    def monitor_continuously(self, interval: int = 10):
        """Непрерывный мониторинг"""
        logger.info(f"Starting advanced monitoring with {interval}s interval")
        
        try:
            while True:
                stats = self.get_detailed_stats()
                print(self.format_stats(stats))
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        except Exception as e:
            logger.error(f"Monitoring error: {e}")

def main():
    parser = argparse.ArgumentParser(description="Advanced Pipeline Monitor")
    parser.add_argument("--interval", type=int, default=10,
                       help="Monitoring interval in seconds")
    
    args = parser.parse_args()
    
    # Конфигурация
    redis_config = RedisConfig()
    pipeline_config = PipelineConfig()
    monitor = AdvancedMonitor(redis_config, pipeline_config)
    
    monitor.monitor_continuously(args.interval)

if __name__ == "__main__":
    main()