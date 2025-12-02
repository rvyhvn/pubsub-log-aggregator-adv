import pytest
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "aggregator"))

from models import Event, ProcessedEvent, EventStats, Base
from dedup import DedupProcessor


def worker_process_event(db_url: str, event_data: dict) -> tuple[bool, str]:
    """
    Worker function for concurrent processing.
    Each worker use its own session
    """
    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()

    try:
        processor = DedupProcessor(db)
        event = Event(**event_data)
        return processor.process_event(event)
    finally:
        db.close()


def test_concurrent_same_event(test_engine):
    """
    Test race condition.
    Only one that must succesfully processed, the rest are duplicates
    """
    db_url = "sqlite:///./test_concurrent.db"
    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)

    # Initialize stats
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    stats = EventStats(id=1, received=0, unique_processed=0, duplicate_dropped=0)
    db.add(stats)
    db.commit()
    db.close()

    event_data = {
        "topic": "concurrent.test",
        "event_id": "concurrent_evt_123",
        "timestamp": "2025-12-02T10:30:00Z",
        "source": "test",
        "payload": {"test": "concurrent"},
    }

    # Launch 10 workers processing same event simultaneously
    num_workers = 10
    results = []

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(worker_process_event, db_url, event_data)
            for _ in range(num_workers)
        ]

        for future in as_completed(futures):
            try:
                success, result = future.result()
                results.append((success, result))
            except Exception as e:
                results.append((False, str(e)))

    # Analyze results
    processed_count = sum(1 for _, result in results if result == "processed")
    duplicate_count = sum(1 for _, result in results if result == "duplicate")

    assert processed_count == 1, f"Expected 1 processed, got {processed_count}"
    assert duplicate_count == num_workers - 1

    # Verify database
    db = SessionLocal()
    count = (
        db.query(ProcessedEvent)
        .filter(
            ProcessedEvent.topic == event_data["topic"],
            ProcessedEvent.event_id == event_data["event_id"],
        )
        .count()
    )
    db.close()

    assert count == 1, f"Expected 1 entry in DB, got {count}"

    Base.metadata.drop_all(bind=engine)
    try:
        os.remove("./test_concurrent.db")
    except:
        pass


def test_concurrent_different_events(test_engine):
    """
    Test concurrent processing on all events
    """
    db_url = "sqlite:///./test_concurrent_diff.db"
    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)

    # Initialize stats
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    stats = EventStats(id=1, received=0, unique_processed=0, duplicate_dropped=0)
    db.add(stats)
    db.commit()
    db.close()

    # Generate different events
    events = [
        {
            "topic": "concurrent.test",
            "event_id": f"concurrent_evt_{i}",
            "timestamp": "2025-12-02T10:30:00Z",
            "source": "test",
            "payload": {"index": i},
        }
        for i in range(20)
    ]

    # Process concurrently
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(worker_process_event, db_url, event_data)
            for event_data in events
        ]

        for future in as_completed(futures):
            success, result = future.result()
            results.append((success, result))

    # All should be processed successfully
    processed_count = sum(1 for _, result in results if result == "processed")
    assert processed_count == len(events)

    # Verify all in db
    db = SessionLocal()
    total_count = db.query(ProcessedEvent).count()
    db.close()

    assert total_count == len(events)

    # Cleanup
    Base.metadata.drop_all(bind=engine)
    try:
        os.remove("./test_concurrent_diff.db")
    except:
        pass


def test_concurrent_stats_consistency(test_engine):
    """
    Test that stats updates still consistent under concurrent load
    """
    db_url = "sqlite:///./test_stats_concurrent.db"
    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    stats = EventStats(id=1, received=0, unique_processed=0, duplicate_dropped=0)
    db.add(stats)
    db.commit()
    db.close()

    # Mix of unique and duplicate events
    events = []
    for i in range(10):
        events.append(
            {
                "topic": "stats.test",
                "event_id": f"stats_evt_{i}",
                "timestamp": "2025-12-02T10:30:00Z",
                "source": "test",
                "payload": {"index": i},
            }
        )

    # Add duplicates
    for i in range(5):
        events.append(
            {
                "topic": "stats.test",
                "event_id": f"stats_evt_{i}",  # Duplicate
                "timestamp": "2025-12-02T10:31:00Z",
                "source": "test",
                "payload": {"index": i},
            }
        )

    # Process concurrently
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(worker_process_event, db_url, event_data)
            for event_data in events
        ]

        for future in as_completed(futures):
            future.result()

    # Verify stats consistency
    db = SessionLocal()
    final_stats = db.query(EventStats).filter(EventStats.id == 1).first()

    assert final_stats.received == 15
    assert final_stats.unique_processed == 10
    assert final_stats.duplicate_dropped == 5

    db.close()

    # Cleanup
    Base.metadata.drop_all(bind=engine)
    try:
        os.remove("./test_stats_concurrent.db")
    except:
        pass
