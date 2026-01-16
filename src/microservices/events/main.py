import datetime
from fastapi import FastAPI, HTTPException, Query, Request, Response
import random
import os
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import requests
from conf import Settings
from kafka_client import kafka_client
from schemas import MovieEvent, PaymentEvent, UserEvent

app = FastAPI(
    title="Events API",
    version="1.0.0",
)

conf = Settings.read_or_update()


@app.get("/api/events/health")
def health():
    kafka_status = kafka_client.check_connection()
    if not kafka_status:
        raise HTTPException(status_code=404, detail={"status": kafka_status})
    return {"status": kafka_status}


@app.post("/api/events/movie")
def movie(data: MovieEvent, key: str = None):
    return JSONResponse(
        content=send_and_consume(data, key, "movie-events"), status_code=201
    )


@app.post("/api/events/user")
def user(data: UserEvent, key: str = None):
    return JSONResponse(
        content=send_and_consume(data, key, "user-events"), status_code=201
    )


@app.post("/api/events/payment")
def payment(data: PaymentEvent, key: str = None):
    return JSONResponse(
        content=send_and_consume(data, key, "payment-events"), status_code=201
    )


def send_and_consume(data: BaseModel, key: str | None = None, topic: str | None = None):
    metadata = send(data, key, topic)

    kafka_data = consume(topic=topic)
    return metadata | kafka_data


def send(data: BaseModel, key: str | None = None, topic: str | None = None):
    try:
        # Добавляем метаданные
        enriched_message = {
            **data.model_dump(exclude_none=True),
            "timestamp": "auto",  # Будет добавлено Kafka
            "source": "events",
        }

        record_metadata = kafka_client.send_message(enriched_message, key, topic)

        if record_metadata:
            return {
                "partition": record_metadata.partition,
                "offset": record_metadata.offset,
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to send message")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def consume(topic: str | None = None, timeout_ms: int = 1000, max_messages: int = 10):
    try:
        messages = kafka_client.consume_messages_sync(topic, timeout_ms, max_messages)

        return {"status": "success", "count": len(messages), "messages": messages}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ["PORT"])
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
