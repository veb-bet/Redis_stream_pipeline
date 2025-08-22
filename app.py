import time
import redis
from pydantic import BaseModel
import json

# Настройка Redis
r = redis.Redis(host="localhost", port=6379, db=0)

STREAM_NAME = "events"
DLQ_STREAM = "dlq"

# Событие
class Event(BaseModel):
    id: int
    type: str
    payload: dict

# --- Продьюсер ---
def produce_event(event: Event):
    data = {k: json.dumps(v) for k, v in event.model_dump().items()}
    r.xadd(STREAM_NAME, data)
    print(f"Produced event {event.id}")


# --- Консьюмер ---
def consume_events(group_name="event_group", consumer_name="consumer1"):
    try:
        # создаем группу, если её нет
        r.xgroup_create(STREAM_NAME, group_name, id="0", mkstream=True)
    except redis.exceptions.ResponseError:
        # группа уже существует
        pass

    while True:
        # читаем события
        entries = r.xreadgroup(group_name, consumer_name, {STREAM_NAME: ">"}, count=5, block=1000)
        if not entries:
            continue

        for stream, messages in entries:
            for message_id, message_data in messages:
                try:
                    # эмуляция обработки
                    print(f"Processing {message_data}")
                    if json.loads(message_data[b"type"]) == "fail":
                        raise ValueError("Simulated processing error")


                    # подтверждаем обработку
                    r.xack(STREAM_NAME, group_name, message_id)

                except Exception as e:
                    print(f"Error processing {message_id}: {e}")
                    # отправляем в DLQ
                    r.xadd(DLQ_STREAM, message_data)
                    r.xack(STREAM_NAME, group_name, message_id)


if __name__ == "__main__":
    import threading

    # старт продьюсера
    def produce_loop():
        i = 1
        while True:
            event = Event(id=i, type="test", payload={"data": f"value {i}"})
            produce_event(event)
            i += 1
            time.sleep(1)

    threading.Thread(target=produce_loop, daemon=True).start()

    # старт консьюмера
    consume_events()