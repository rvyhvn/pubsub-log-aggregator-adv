import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "aggregator"))

from models import Event, ProcessedEvent, EventStats
from dedup import DedupProcessor, is_duplicate


def test_process_new_event(test_db, sample_event):
    """Test processing new event (not duplocate)"""
    processor = DedupProcessor(test_db)
    event = Event(**sample_event)

    success, result = processor.process_event(event)

    assert success is True
    assert result == "processed"

    # Verify event stored in db
    saved_event = (
        test_db.query(ProcessedEvent)
        .filter(
            ProcessedEvent.topic == event.topic,
            ProcessedEvent.event_id == event.event_id,
        )
        .first()
    )

    assert saved_event is not None
    assert saved_event.topic == event.topic
    assert saved_event.event_id == event.event_id


def test_process_duplicate_event(test_db, sample_event):
    """Test idempotency"""
    processor = DedupProcessor(test_db)
    event = Event(**sample_event)

    success1, result1 = processor.process_event(event)
    assert success1 is True
    assert result1 == "processed"

    success2, result2 = processor.process_event(event)
    assert success2 is True
    assert result2 == "duplicate"

    # Verify
    count = (
        test_db.query(ProcessedEvent)
        .filter(
            ProcessedEvent.topic == event.topic,
            ProcessedEvent.event_id == event.event_id,
        )
        .count()
    )

    assert count == 1


def test_statistics_update_on_new_event(test_db, sample_event):
    """Test states update when new event processed"""
    processor = DedupProcessor(test_db)
    event = Event(**sample_event)

    initial_stats = test_db.query(EventStats).filter(EventStats.id == 1).first()
    initial_received = initial_stats.received
    initial_unique = initial_stats.unique_processed

    processor.process_event(event)
    test_db.commit()

    updated_stats = test_db.query(EventStats).filter(EventStats.id == 1).first()

    assert updated_stats.received == initial_received + 1
    assert updated_stats.unique_processed == initial_unique + 1


def test_statistics_update_on_duplicate(test_db, sample_event):
    """Test stats update when there's duplicate"""
    processor = DedupProcessor(test_db)
    event = Event(**sample_event)

    processor.process_event(event)
    test_db.commit()

    stats_after_first = test_db.query(EventStats).filter(EventStats.id == 1).first()
    received_after_first = stats_after_first.received
    duplicate_after_first = stats_after_first.duplicate_dropped

    # Process duplicate
    processor.process_event(event)
    test_db.commit()

    final_stats = test_db.query(EventStats).filter(EventStats.id == 1).first()

    assert final_stats.received == received_after_first + 1
    assert final_stats.duplicate_dropped == duplicate_after_first + 1
    assert final_stats.unique_processed == 1


def test_is_duplicate_function(test_db, sample_event):
    """Test utility function is_duplicate."""
    processor = DedupProcessor(test_db)
    event = Event(**sample_event)

    # Should not be duplicate initially
    assert is_duplicate(test_db, event.topic, event.event_id) is False

    processor.process_event(event)
    test_db.commit()

    # Should be duplicate now
    assert is_duplicate(test_db, event.topic, event.event_id) is True


def test_different_topics_same_event_id(test_db):
    """Test that same event_id in different topic treated differently"""
    processor = DedupProcessor(test_db)

    event1 = Event(
        topic="topic.a",
        event_id="same_id",
        timestamp="2025-12-02T10:30:00Z",
        source="test",
        payload={"data": "1"},
    )

    event2 = Event(
        topic="topic.b",
        event_id="same_id",
        timestamp="2025-12-02T10:30:00Z",
        source="test",
        payload={"data": "2"},
    )

    # Both should be processed (different topics)
    success1, result1 = processor.process_event(event1)
    success2, result2 = processor.process_event(event2)

    assert success1 is True
    assert result1 == "processed"
    assert success2 is True
    assert result2 == "processed"

    # Verify both exist in db
    count = (
        test_db.query(ProcessedEvent)
        .filter(ProcessedEvent.event_id == "same_id")
        .count()
    )

    assert count == 2


def test_multiple_duplicates(test_db, sample_event):
    """Test all multiple duplicates detected"""
    processor = DedupProcessor(test_db)
    event = Event(**sample_event)

    success, result = processor.process_event(event)
    assert result == "processed"

    # Process 5 duplicates
    duplicate_count = 0
    for i in range(5):
        success, result = processor.process_event(event)
        if result == "duplicate":
            duplicate_count += 1

    assert duplicate_count == 5

    # Verify stats
    stats = test_db.query(EventStats).filter(EventStats.id == 1).first()
    assert stats.unique_processed == 1
    assert stats.duplicate_dropped >= 5
