import pytest
import sys
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "aggregator"))

from models import Base, ProcessedEvent, EventStats, Event
from dedup import DedupProcessor


def test_persistence_after_session_close():
    db_path = "./test_persistence.db"
    db_url = f"sqlite:///{db_path}"

    # Session 1: Insert data
    engine1 = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine1)
    SessionLocal1 = sessionmaker(autocommit=False, autoflush=False, bind=engine1)

    db1 = SessionLocal1()

    # Initialize stats
    stats = EventStats(id=1, received=0, unique_processed=0, duplicate_dropped=0)
    db1.add(stats)
    db1.commit()

    # Process event
    processor = DedupProcessor(db1)
    event_data = {
        "topic": "persist.test",
        "event_id": "persist_evt_123",
        "timestamp": "2025-12-02T10:30:00Z",
        "source": "test",
        "payload": {"data": "persistent"},
    }
    event = Event(**event_data)
    processor.process_event(event)

    db1.close()
    engine1.dispose()

    # Verify data persisted (simulate restart)
    engine2 = create_engine(db_url, connect_args={"check_same_thread": False})
    SessionLocal2 = sessionmaker(autocommit=False, autoflush=False, bind=engine2)

    db2 = SessionLocal2()

    # Check event still exists
    persisted_event = (
        db2.query(ProcessedEvent)
        .filter(
            ProcessedEvent.topic == event_data["topic"],
            ProcessedEvent.event_id == event_data["event_id"],
        )
        .first()
    )

    assert persisted_event is not None
    assert persisted_event.topic == event_data["topic"]
    assert persisted_event.event_id == event_data["event_id"]

    # Check stats persisted
    persisted_stats = db2.query(EventStats).filter(EventStats.id == 1).first()
    assert persisted_stats is not None
    assert persisted_stats.unique_processed == 1

    db2.close()
    engine2.dispose()

    # Cleanup
    try:
        os.remove(db_path)
    except:
        pass


def test_dedup_survives_restart():
    db_path = "./test_dedup_restart.db"
    db_url = f"sqlite:///{db_path}"

    event_data = {
        "topic": "restart.test",
        "event_id": "restart_evt_123",
        "timestamp": "2025-12-02T10:30:00Z",
        "source": "test",
        "payload": {"data": "test"},
    }

    # Session 1: Process event
    engine1 = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine1)
    SessionLocal1 = sessionmaker(autocommit=False, autoflush=False, bind=engine1)

    db1 = SessionLocal1()
    stats = EventStats(id=1, received=0, unique_processed=0, duplicate_dropped=0)
    db1.add(stats)
    db1.commit()

    processor1 = DedupProcessor(db1)
    event = Event(**event_data)
    success1, result1 = processor1.process_event(event)

    assert result1 == "processed"

    db1.close()
    engine1.dispose()

    # Session 2: Try to process same event (simulate restart)
    engine2 = create_engine(db_url, connect_args={"check_same_thread": False})
    SessionLocal2 = sessionmaker(autocommit=False, autoflush=False, bind=engine2)

    db2 = SessionLocal2()
    processor2 = DedupProcessor(db2)
    event2 = Event(**event_data)
    success2, result2 = processor2.process_event(event2)

    # Should detect as duplicate
    assert result2 == "duplicate"

    # Verify only 1 entry exists
    count = (
        db2.query(ProcessedEvent)
        .filter(
            ProcessedEvent.topic == event_data["topic"],
            ProcessedEvent.event_id == event_data["event_id"],
        )
        .count()
    )

    assert count == 1

    db2.close()
    engine2.dispose()

    # Cleanup
    try:
        os.remove(db_path)
    except:
        pass


def test_stats_accumulation_across_sessions():
    db_path = "./test_stats_accum.db"
    db_url = f"sqlite:///{db_path}"

    # Session 1: Process some events
    engine1 = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine1)
    SessionLocal1 = sessionmaker(autocommit=False, autoflush=False, bind=engine1)

    db1 = SessionLocal1()
    stats = EventStats(id=1, received=0, unique_processed=0, duplicate_dropped=0)
    db1.add(stats)
    db1.commit()

    processor1 = DedupProcessor(db1)

    for i in range(5):
        event = Event(
            topic="accum.test",
            event_id=f"accum_evt_{i}",
            timestamp="2025-12-02T10:30:00Z",
            source="test",
            payload={"index": i},
        )
        processor1.process_event(event)

    # Check stats after session 1
    stats1 = db1.query(EventStats).filter(EventStats.id == 1).first()
    session1_unique = stats1.unique_processed

    db1.close()
    engine1.dispose()

    # Session 2: Process more events
    engine2 = create_engine(db_url, connect_args={"check_same_thread": False})
    SessionLocal2 = sessionmaker(autocommit=False, autoflush=False, bind=engine2)

    db2 = SessionLocal2()
    processor2 = DedupProcessor(db2)

    for i in range(5, 10):
        event = Event(
            topic="accum.test",
            event_id=f"accum_evt_{i}",
            timestamp="2025-12-02T10:30:00Z",
            source="test",
            payload={"index": i},
        )
        processor2.process_event(event)

    # Check accumulated stats
    stats2 = db2.query(EventStats).filter(EventStats.id == 1).first()
    assert stats2.unique_processed == 10
    assert stats2.unique_processed > session1_unique

    db2.close()
    engine2.dispose()

    # Cleanup
    try:
        os.remove(db_path)
    except:
        pass


def test_transaction_rollback_on_error():
    db_path = "./test_rollback.db"
    db_url = f"sqlite:///{db_path}"

    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    db = SessionLocal()
    stats = EventStats(id=1, received=0, unique_processed=0, duplicate_dropped=0)
    db.add(stats)
    db.commit()

    # Process valid event
    processor = DedupProcessor(db)
    valid_event = Event(
        topic="rollback.test",
        event_id="valid_evt",
        timestamp="2025-12-02T10:30:00Z",
        source="test",
        payload={},
    )
    processor.process_event(valid_event)

    initial_count = db.query(ProcessedEvent).count()
    initial_stats = db.query(EventStats).filter(EventStats.id == 1).first()
    initial_unique = initial_stats.unique_processed

    # Try to process event dengan invalid timestamp (will cause error)
    invalid_event_data = {
        "topic": "rollback.test",
        "event_id": "invalid_evt",
        "timestamp": "invalid-timestamp-format",
        "source": "test",
        "payload": {},
    }

    try:
        invalid_event = Event(**invalid_event_data)
        processor.process_event(invalid_event)
    except:
        pass  # Expected to fail

    # Verify no data corruption
    final_count = db.query(ProcessedEvent).count()
    final_stats = db.query(EventStats).filter(EventStats.id == 1).first()

    # Count should not change (rollback happened)
    assert final_count == initial_count
    assert final_stats.unique_processed == initial_unique

    db.close()
    engine.dispose()

    # Cleanup
    try:
        os.remove(db_path)
    except:
        pass
