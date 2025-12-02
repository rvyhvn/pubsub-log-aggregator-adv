import redis
import json
import logging
import asyncio
import signal
import os
from concurrent.futures import ThreadPoolExecutor
from database import get_db_context
from dedup import DedupProcessor
from models import Event
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class RedisConsumer:

    def __init__(self, redis_url: str, channel: str, num_workers: int = 3):
        self.redis_url = redis_url
        self.channel = channel
        self.num_workers = num_workers
        self.running = False
        self.redis_client = None
        self.pubsub = None
        self.executor = ThreadPoolExecutor(max_workers=num_workers)

    def start(self):
        logger.info(f"Starting Redis consumer with {self.num_workers} workers...")
        self.running = True

        self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
        self.pubsub = self.redis_client.pubsub()
        self.pubsub.subscribe(self.channel)

        logger.info(f"Subscribed to channel: {self.channel}")

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self._consume_loop()

    def _signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False

    def _consume_loop(self):
        try:
            for message in self.pubsub.listen():
                if not self.running:
                    break

                if message["type"] == "message":
                    self.executor.submit(self._process_message, message["data"])

        except Exception as e:
            logger.error(f"Error in consume loop: {e}")
        finally:
            self._cleanup()

    def _process_message(self, data: str):
        try:
            event_data = json.loads(data)

            # Validate
            event = Event(**event_data)

            with get_db_context() as db:
                processor = DedupProcessor(db)
                success, result = processor.process_event(event)

                if not success:
                    logger.warning(f"Failed to process event: {result}")

        except ValidationError as ve:
            logger.error(f"Invalid event schema: {ve}")
        except json.JSONDecodeError as je:
            logger.error(f"Invalid JSON: {je}")
        except Exception as e:
            logger.error(f"Unexpected error processing message: {e}")

    def _cleanup(self):
        logger.info("Cleaning up consumer resources...")

        if self.pubsub:
            self.pubsub.unsubscribe()
            self.pubsub.close()

        if self.redis_client:
            self.redis_client.close()

        self.executor.shutdown(wait=True)
        logger.info("Consumer stopped")


async def start_consumer_async():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    channel = os.getenv("REDIS_CHANNEL", "events")
    num_workers = int(os.getenv("NUM_WORKERS", "3"))

    consumer = RedisConsumer(redis_url, channel, num_workers)

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, consumer.start)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    channel = os.getenv("REDIS_CHANNEL", "events")
    num_workers = int(os.getenv("NUM_WORKERS", "3"))

    consumer = RedisConsumer(redis_url, channel, num_workers)
    consumer.start()
