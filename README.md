# Redis_stream_pipeline
Полнофункциональная система обработки событий на основе Redis Streams с поддержкой многопоточности, мониторинга и повторной обработки.

## Возможности

- **Многопоточные консьюмеры**: Запуск нескольких consumer'ов для параллельной обработки
- **Dead Letter Queue (DLQ)**: Автоматическое перемещение неудачных сообщений
- **Reprocessing DLQ**: Повторная обработка сообщений из DLQ с лимитом попыток
- **Продвинутый мониторинг**: Детальная статистика и отслеживание lag'а
- **Конфигурируемость**: Гибкая настройка через классы конфигурации
- **Логирование**: Детальное логирование всех операций

## Структура проекта
```yaml
redis-stream-pipeline/
├── app.py # Основное приложение (Producer/Consumer)
├── dlq_reprocess.py # Инструмент повторной обработки DLQ
├── monitor.py # Продвинутый мониторинг
├── requirements.txt # Зависимости
├── docker-compose.yml # Docker конфигурация
└── README.md
```
## Установка и запуск

1. **Запуск Redis**
```bash
docker-compose up -d
```

2. **Установка зависимостей**
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
```
или
```bash
venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

3. **Запуск в разных режимах**

- Производитель событий:

```bash
python app.py --mode producer --produce-interval 0.1
```

- Потребители (3 потока):

```bash
python app.py --mode consumer --consumers 3
```

- Мониторинг:

```bash
python app.py --mode monitor --monitor-interval 5
```

- Повторная обработка DLQ:

```bash
python dlq_reprocess.py --batch-size 20 --max-retries 3
```

- Непрерывный мониторинг DLQ:

```bash
python dlq_reprocess.py --continuous --interval 30
```

- Продвинутый мониторинг:

```bash
python monitor.py --interval 10
```

## Конфигурация
Основные параметры настройки в app.py:

```python
class PipelineConfig:
    stream_name = "events"           # Основной поток
    dlq_stream = "dlq"               # DLQ поток
    consumer_group = "event_processing_group"  # Consumer group
    consumer_prefix = "consumer"     # Префикс consumer'ов
    max_retries = 3                  # Максимальное количество попыток
    processing_timeout = 30000       # Таймаут обработки (ms)
    batch_size = 10                  # Размер батча
```
## Мониторинг
Система предоставляет несколько уровней мониторинга:

- Базовый мониторинг: Статистика потоков и consumer group

- Детальный мониторинг: Информация о pending messages и использовании памяти

- DLQ мониторинг: Отслеживание сообщений в dead letter queue

## Расширение
Для создания собственных обработчиков событий:

```python
class CustomEventProcessor(EventProcessor):
    def process(self, event: Event) -> bool:
        # Ваша логика обработки
        try:
            # Обработка события
            return True
        except Exception:
            return False
```
## Пример использования в production
```bash
# Запуск multiple consumers
python app.py --mode consumer --consumers 10 &

# Мониторинг в реальном времени
python monitor.py --interval 5 &

# Автоматическая обработка DLQ
python dlq_reprocess.py --continuous --interval 60 &
```
## Производительность
- Поддерживает тысячи сообщений в секунду

- Масштабируется горизонтально добавлением consumer'ов

- Минимальная задержка обработки

- Автоматическое восстановление при ошибках

---

Эта версия предоставляет полноценную промышленную реализацию с:

1. **Многопоточными консьюмерами** для параллельной обработки
2. **Расширенным мониторингом** lag'а и статистики
3. **Автоматической обработкой DLQ** с лимитом попыток
4. **Гибкой конфигурацией** и логированием
5. **Docker композом** с Redis Commander для визуализации
6. **Производственными best practices** (обработка ошибок, retry логика)

---

### Если возникают проблемы с Docker:
Запустите Redis

```bash
docker-compose up -d
```

Проверьте, что Redis запустился:

```bash
docker-compose ps
```

Должно быть примерно так:

```text
NAME                COMMAND                  SERVICE             STATUS              PORTS
redis-streams       "docker-entrypoint.s…"   redis               running             0.0.0.0:6379->6379/tcp
redis-commander     "/bin/sh -c 'node /a…"   redis-commander     running             0.0.0.0:8081->8081/tcp
```

Проверьте подключение к Redis

```bash
redis-cli ping
```

Если redis-cli не установлен, можно использовать:

```bash
docker-compose exec redis redis-cli ping
```

Должен вернуться ответ PONG.

### Если есть ошибки прав доступа к Docker:

```bash
# Добавьте пользователя в группу docker
sudo usermod -aG docker $USER

# Перезагрузите сессию или выполните:
newgrp docker

# Проверьте статус Docker
sudo systemctl status docker

# Если Docker не запущен:
sudo systemctl start docker
sudo systemctl enable docker
```

Альтернатива: установите Redis локально

```bash
sudo apt update
sudo apt install redis-server
sudo systemctl start redis-server
sudo systemctl enable redis-server
```

## Интерпретация вывода системы
События типа "test" и "success" - обрабатываются успешно

События типа "fail" - всегда попадают в DLQ (имитация неустранимых ошибок)

События типа "retry" - временные ошибки, остаются в DLQ для повторных попыток