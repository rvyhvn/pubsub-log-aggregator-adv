from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import func, distinct
import redis
import json
import logging
import os
import time
from datetime import datetime
from typing import List

from database import get_db, init_db, health_check
from models import (
    Event,
    EventBatch,
    EventResponse,
    StatsResponse,
    PublishResponse,
    ProcessedEvent,
    EventStats,
)
from consumer import start_consumer_async

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Distributed Pub-Sub Log Aggregator",
    description="Idempotent consumer with deduplication and ACID transactions",
    version="1.0.0",
)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CHANNEL = os.getenv("REDIS_CHANNEL", "events")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

START_TIME = time.time()


@app.on_event("startup")
async def startup_event():
    logger.info("Starting application!")

    init_db()

    logger.info("Starting Redis consumer")

    logger.info("Application started successfully")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down application")
    redis_client.close()


@app.get("/")
async def root():
    return {
        "service": "Distributed Pub-Sub Log Aggregator",
        "version": "1.0.0",
        "status": "running",
    }


@app.get("/health")
async def health():
    db_healthy = health_check()

    try:
        redis_client.ping()
        redis_healthy = True
    except:
        redis_healthy = False

    if db_healthy and redis_healthy:
        return {"status": "healthy", "database": "ok", "redis": "ok"}
    else:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "unhealthy",
                "database": "ok" if db_healthy else "failed",
                "redis": "ok" if redis_healthy else "failed",
            },
        )


@app.post("/publish", response_model=PublishResponse)
async def publish_events(
    event_or_batch: Event | EventBatch, db: Session = Depends(get_db)
):
    try:
        events = []

        if isinstance(event_or_batch, Event):
            events = [event_or_batch]
        else:
            events = event_or_batch.events

        # Publish to redis
        published_count = 0
        for event in events:
            message = event.model_dump_json()
            redis_client.publish(REDIS_CHANNEL, message)
            published_count += 1

        logger.info(f"Published {published_count} events to Redis")

        return PublishResponse(
            status="accepted",
            accepted=published_count,
            message=f"Published {published_count} event(s) to queue",
        )

    except Exception as e:
        logger.error(f"Error publishing events: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/events", response_model=List[EventResponse])
async def get_events(
    topic: str = None, limit: int = 100, offset: int = 0, db: Session = Depends(get_db)
):
    try:
        query = db.query(ProcessedEvent)

        if topic:
            query = query.filter(ProcessedEvent.topic == topic)

        query = query.order_by(ProcessedEvent.processed_at.desc())
        query = query.limit(limit).offset(offset)

        events = query.all()

        return [
            EventResponse(
                topic=e.topic,
                event_id=e.event_id,
                timestamp=e.timestamp.isoformat(),
                processed_at=e.processed_at.isoformat(),
                source=e.source,
                payload=e.payload,
            )
            for e in events
        ]

    except Exception as e:
        logger.error(f"Error retrieving events: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats", response_model=StatsResponse)
async def get_stats(db: Session = Depends(get_db)):
    try:
        stats = db.query(EventStats).filter(EventStats.id == 1).first()

        topics_count = (
            db.query(func.count(distinct(ProcessedEvent.topic))).scalar() or 0
        )

        uptime = time.time() - START_TIME

        return StatsResponse(
            received=stats.received if stats else 0,
            unique_processed=stats.unique_processed if stats else 0,
            duplicate_dropped=stats.duplicate_dropped if stats else 0,
            topics=topics_count,
            uptime_seconds=round(uptime, 2),
            last_updated=(
                stats.last_updated.isoformat() if stats and stats.last_updated else None
            ),
        )

    except Exception as e:
        logger.error(f"Error retrieving stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/topics")
async def get_topics(db: Session = Depends(get_db)):
    try:
        topics = db.query(distinct(ProcessedEvent.topic)).all()
        return {"topics": [t[0] for t in topics]}
    except Exception as e:
        logger.error(f"Error retrieving topics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
