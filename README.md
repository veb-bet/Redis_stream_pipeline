# redis-stream-pipeline
Пет-проект на Python с использованием Redis Streams.  

Сценарий: продьюсер публикует события, консьюмер их обрабатывает, а «падения» событий уходят в DLQ (Dead Letter Queue) с возможностью повторной обработки.  

---

## Структура проекта
```yaml
redis-stream-pipeline/
├─ app.py # главный файл с продьюсером и консьюмером
├─ dlq_reprocess.py # скрипт для повторной обработки DLQ
├─ requirements.txt # зависимости
├─ docker-compose.yml # Redis для локального запуска
└─ README.md
```

---

## Функциональность

1. **Продьюсер**: публикует события каждую секунду.  
2. **Консьюмер**: читает события из потока и обрабатывает их.  
3. **DLQ**: при ошибке события отправляются в `dlq` поток.  
4. **Reprocessing DLQ**: отдельный скрипт для повторной обработки событий из DLQ.  
5. **Мониторинг lag-а**: скрипт считает, сколько сообщений осталось необработанными.

---

## Быстрый старт

1. **Запустить Redis**

```bash
docker-compose up -d
```

2. **Создать виртуальное окружение и установить зависимости**

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
3. **Запустить продьюсер + консьюмер**

```bash
python app.py
```

4. **Обработка DLQ (если есть падения)**

```bash
python dlq_reprocess.py
```

5. **Мониторинг lag-а**

```bash
python dlq_reprocess.py --monitor
```

## Настройка
- STREAM_NAME — основной поток событий (по умолчанию events)
- DLQ_STREAM — Dead Letter Queue (по умолчанию dlq)
- Можно менять group_name и consumer_name для добавления нескольких консьюмеров.

## Технологии
- Python 3.10+
- Redis Streams
- Pydantic (валидация событий)
- Docker Compose

