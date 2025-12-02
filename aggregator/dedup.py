from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select
from models import ProcessedEvent, EventStats, AuditLog, Event
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DedupProcessor:
    def __init__(self, db: Session):
        self.db = db

    def process_event(self, event: Event) -> tuple[bool, str]:
        try:
            ts = datetime.fromisoformat(event.timestamp.replace("Z", "+00:00"))

            processed_event = ProcessedEvent(
                topic=event.topic,
                event_id=event.event_id,
                timestamp=ts,
                source=event.source,
                payload=event.payload,
            )

            self.db.add(processed_event)

            try:
                self.db.flush()

                # If succeed, make new event and update statistics
                self._update_stats(received=1, unique=1)

                self._log_audit(
                    event_topic=event.topic,
                    event_id=event.event_id,
                    action="processed",
                    details={"source": event.source},
                )

                self.db.commit()

                logger.info(f"Processed new event: {event.topic}/{event.event_id}")
                return True, "processed"

            except IntegrityError as ie:
                # Duplicate detected via unique constraint violation
                self.db.rollback()

                self._update_stats_duplicate()

                self._log_audit(
                    event_topic=event.topic,
                    event_id=event.event_id,
                    action="duplicate",
                    details={"reason": "unique_constraint_violation"},
                )

                logger.info(
                    f"Duplicate detected (idempotent): {event.topic}/{event.event_id}"
                )
                return True, "duplicate"

        except Exception as e:
            self.db.rollback()
            logger.error(f"Error processing event {event.topic}/{event.event_id}: {e}")

            try:
                self._log_audit(
                    event_topic=event.topic,
                    event_id=event.event_id,
                    action="error",
                    details={"error": str(e)},
                )
                self.db.commit()
            except:
                pass

            return False, f"Error: {str(e)}"

    def _update_stats(self, received: int = 0, unique: int = 0):
        stmt = select(EventStats).where(EventStats.id == 1).with_for_update()
        stats = self.db.execute(stmt).scalar_one()

        stats.received += received
        stats.unique_processed += unique

    def _update_stats_duplicate(self):
        try:
            stmt = select(EventStats).where(EventStats.id == 1).with_for_update()
            stats = self.db.execute(stmt).scalar_one()

            stats.received += 1
            stats.duplicate_dropped += 1

            self.db.commit()
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error updating duplicate stats: {e}")

    def _log_audit(self, event_topic: str, event_id: str, action: str, details: dict):
        audit = AuditLog(
            event_topic=event_topic, event_id=event_id, action=action, details=details
        )
        self.db.add(audit)


# Utils
def is_duplicate(db: Session, topic: str, event_id: str) -> bool:
    stmt = (
        select(ProcessedEvent)
        .where(ProcessedEvent.topic == topic, ProcessedEvent.event_id == event_id)
        .limit(1)
    )

    result = db.execute(stmt).first()
    return result is not None
