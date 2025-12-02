import redis
import json
import time
import random
import uuid
import logging
import os
from datetime import datetime, timezone
from typing import List, Dict, Any

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class EventPublisher:

    def __init__(
        self,
        redis_url: str,
        channel: str,
        total_events: int = 25000,
        duplication_rate: float = 0.35,
        batch_size: int = 100,
    ):
        self.redis_url = redis_url
        self.channel = channel
        self.total_events = total_events
        self.duplication_rate = duplication_rate
        self.batch_size = batch_size
        self.redis_client = redis.from_url(redis_url, decode_responses=True)

        self.topics = [
            "user.login",
            "user.logout",
            "user.register",
            "order.created",
            "order.completed",
            "order.cancelled",
            "payment.processed",
            "payment.failed",
            "inventory.updated",
            "notification.sent",
        ]

        self.generated_events: List[Dict[str, Any]] = []

    def generate_event(self, event_id: str = None) -> Dict[str, Any]:
        if event_id is None:
            event_id = f"evt_{uuid.uuid4().hex[:16]}"

        topic = random.choice(self.topics)

        payload = self._generate_payload(topic)

        event = {
            "topic": topic,
            "event_id": event_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": f"publisher-{random.randint(1, 5)}",
            "payload": payload,
        }

        return event

    def _generate_payload(self, topic: str) -> Dict[str, Any]:
        if topic.startswith("user."):
            return {
                "user_id": f"user_{random.randint(1000, 9999)}",
                "ip_address": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
                "user_agent": "Mozilla/5.0",
            }
        elif topic.startswith("order."):
            return {
                "order_id": f"ord_{uuid.uuid4().hex[:12]}",
                "user_id": f"user_{random.randint(1000, 9999)}",
                "amount": round(random.uniform(10.0, 1000.0), 2),
                "items": random.randint(1, 10),
            }
        elif topic.startswith("payment."):
            return {
                "payment_id": f"pay_{uuid.uuid4().hex[:12]}",
                "order_id": f"ord_{uuid.uuid4().hex[:12]}",
                "amount": round(random.uniform(10.0, 1000.0), 2),
                "method": random.choice(
                    ["credit_card", "debit_card", "paypal", "bank_transfer"]
                ),
            }
        elif topic.startswith("inventory."):
            return {
                "product_id": f"prod_{random.randint(100, 999)}",
                "quantity": random.randint(-50, 100),
                "warehouse": f"WH-{random.randint(1, 5)}",
            }
        else:
            return {
                "message": f"Event data for {topic}",
                "priority": random.choice(["low", "medium", "high"]),
            }

    def publish_event(self, event: Dict[str, Any]):
        message = json.dumps(event)
        self.redis_client.publish(self.channel, message)

    def run(self):
        logger.info("Starting publisher!")
        logger.info(f"Total events: {self.total_events}")
        logger.info(f"Duplication rate: {self.duplication_rate * 100}%")
        logger.info(f"Batch size: {self.batch_size}")

        start_time = time.time()
        published_count = 0
        unique_count = 0
        duplicate_count = 0

        logger.info("Generating unique events..")
        target_unique = int(self.total_events / (1 + self.duplication_rate))

        for i in range(target_unique):
            event = self.generate_event()
            self.generated_events.append(event)
            self.publish_event(event)

            published_count += 1
            unique_count += 1

            if (i + 1) % self.batch_size == 0:
                logger.info(f"Generated {i + 1}/{target_unique} unique events")
                time.sleep(0.1)

        logger.info(f"{unique_count} unique events published!")

        logger.info("Generating duplicate events..")
        target_duplicates = self.total_events - target_unique

        for i in range(target_duplicates):
            original_event = random.choice(self.generated_events)

            duplicate_event = self.generate_event(event_id=original_event["event_id"])
            duplicate_event["topic"] = original_event["topic"]  # Ensure same topic

            self.publish_event(duplicate_event)

            published_count += 1
            duplicate_count += 1

            if (i + 1) % self.batch_size == 0:
                logger.info(f"Generated {i + 1}/{target_duplicates} duplicate events")
                time.sleep(0.1)

        logger.info(f"{duplicate_count} duplicate events published")

        # Summary
        elapsed_time = time.time() - start_time
        # logger.info("=" * 60)
        logger.info("Publishing Summary")
        logger.info(f"Total events published: {published_count}")
        logger.info(f"Unique events: {unique_count}")
        logger.info(f"Duplicate events: {duplicate_count}")
        logger.info(
            f"Actual duplication rate: {(duplicate_count / published_count * 100):.2f}%"
        )
        logger.info(f"Time elapsed: {elapsed_time:.2f} seconds")
        logger.info(f"Throughput: {(published_count / elapsed_time):.2f} events/sec")
        # logger.info("=" * 60)

        logger.info("Publishing complete. Container will exit in 10 seconds")
        time.sleep(10)


if __name__ == "__main__":
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    channel = os.getenv("REDIS_CHANNEL", "events")
    total_events = int(os.getenv("TOTAL_EVENTS", "25000"))
    duplication_rate = float(os.getenv("DUPLICATION_RATE", "0.35"))
    batch_size = int(os.getenv("BATCH_SIZE", "100"))

    logger.info("Waiting for services to be ready")
    time.sleep(5)

    publisher = EventPublisher(
        redis_url=redis_url,
        channel=channel,
        total_events=total_events,
        duplication_rate=duplication_rate,
        batch_size=batch_size,
    )

    publisher.run()
