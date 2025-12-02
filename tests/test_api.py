import pytest
from unittest.mock import Mock, patch


def test_root_endpoint(client):
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "service" in data
    assert "version" in data


def test_health_endpoint_healthy(client):
    """Test health endpoint while services healthy."""
    with patch("app.health_check", return_value=True):
        with patch("app.redis_client.ping", return_value=True):
            response = client.get("/health")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "healthy"


def test_publish_single_event(client, sample_event):
    """Test publish single event via API."""
    with patch("app.redis_client.publish") as mock_publish:
        response = client.post("/publish", json=sample_event)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "accepted"
        assert data["accepted"] == 1
        assert mock_publish.called


def test_publish_batch_events(client, sample_events_batch):
    """Test publish batch events via API."""
    with patch("app.redis_client.publish") as mock_publish:
        response = client.post("/publish", json=sample_events_batch)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "accepted"
        assert data["accepted"] == len(sample_events_batch["events"])
        assert mock_publish.call_count == len(sample_events_batch["events"])


def test_publish_invalid_event_schema(client):
    """Test publish with invalid schema"""
    invalid_event = {
        "topic": "test",
        # missing required fields
    }
    response = client.post("/publish", json=invalid_event)
    assert response.status_code == 422


def test_publish_invalid_topic_format(client):
    """Test topic with invalid format"""
    invalid_event = {
        "topic": "invalid topic with spaces!",
        "event_id": "test_123",
        "timestamp": "2025-12-02T10:30:00Z",
        "source": "test",
        "payload": {},
    }
    response = client.post("/publish", json=invalid_event)
    assert response.status_code == 422


def test_get_events_empty(client):
    """Test GET /events when there are no events."""
    response = client.get("/events")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 0


def test_get_events_with_data(client, test_db, sample_event):
    """Test GET /events returns processed events."""
    from models import ProcessedEvent
    from datetime import datetime

    # Insert test data
    event = ProcessedEvent(
        topic=sample_event["topic"],
        event_id=sample_event["event_id"],
        timestamp=datetime.fromisoformat(
            sample_event["timestamp"].replace("Z", "+00:00")
        ),
        source=sample_event["source"],
        payload=sample_event["payload"],
    )
    test_db.add(event)
    test_db.commit()

    response = client.get("/events")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["topic"] == sample_event["topic"]
    assert data[0]["event_id"] == sample_event["event_id"]


def test_get_events_filter_by_topic(client, test_db):
    """Test GET /events with topic filter"""
    from models import ProcessedEvent
    from datetime import datetime

    # Insert events with different topics
    events = [
        ProcessedEvent(
            topic="topic.a",
            event_id=f"evt_{i}",
            timestamp=datetime.now(),
            source="test",
            payload={},
        )
        for i in range(3)
    ]
    events.extend(
        [
            ProcessedEvent(
                topic="topic.b",
                event_id=f"evt_{i+3}",
                timestamp=datetime.now(),
                source="test",
                payload={},
            )
            for i in range(2)
        ]
    )

    for event in events:
        test_db.add(event)
    test_db.commit()

    # Filter by topic.a
    response = client.get("/events?topic=topic.a")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3
    assert all(e["topic"] == "topic.a" for e in data)


def test_get_events_pagination(client, test_db):
    """Test GET /events pagination."""
    from models import ProcessedEvent
    from datetime import datetime

    # Insert 15 events
    for i in range(15):
        event = ProcessedEvent(
            topic="test.pagination",
            event_id=f"page_evt_{i}",
            timestamp=datetime.now(),
            source="test",
            payload={"index": i},
        )
        test_db.add(event)
    test_db.commit()

    # Get first page
    response = client.get("/events?limit=5&offset=0")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 5

    # Get second page
    response = client.get("/events?limit=5&offset=5")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 5


def test_get_stats(client, test_db):
    """Test GET /stats endpoint."""
    response = client.get("/stats")
    assert response.status_code == 200
    data = response.json()

    assert "received" in data
    assert "unique_processed" in data
    assert "duplicate_dropped" in data
    assert "topics" in data
    assert "uptime_seconds" in data
    assert isinstance(data["received"], int)
    assert isinstance(data["uptime_seconds"], float)


def test_get_topics_empty(client):
    """Test GET /topics when thre are no topics."""
    response = client.get("/topics")
    assert response.status_code == 200
    data = response.json()
    assert "topics" in data
    assert len(data["topics"]) == 0


def test_get_topics_with_data(client, test_db):
    """Test GET /topics returns unique topics."""
    from models import ProcessedEvent
    from datetime import datetime

    # Insert events with different topics
    topics = ["topic.a", "topic.b", "topic.c"]
    for topic in topics:
        for i in range(3):  # Multiple events per topic
            event = ProcessedEvent(
                topic=topic,
                event_id=f"{topic}_evt_{i}",
                timestamp=datetime.now(),
                source="test",
                payload={},
            )
            test_db.add(event)
    test_db.commit()

    response = client.get("/topics")
    assert response.status_code == 200
    data = response.json()
    assert len(data["topics"]) == 3
    assert set(data["topics"]) == set(topics)
