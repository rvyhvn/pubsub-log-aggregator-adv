import pytest
import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "aggregator"))

from models import Base, EventStats
from database import get_db
from app import app

# Test database URL (SQLite untuk testing)
TEST_DATABASE_URL = "sqlite:///./test_aggregator.db"


@pytest.fixture(scope="function")
def test_engine():
    """Create test database engine."""
    engine = create_engine(TEST_DATABASE_URL, connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    yield engine
    Base.metadata.drop_all(bind=engine)
    # Cleanup test database file
    try:
        os.remove("./test_aggregator.db")
    except:
        pass


@pytest.fixture(scope="function")
def test_db(test_engine):
    """Create test database session."""
    TestSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)
    db = TestSessionLocal()

    # Initialize stats
    stats = EventStats(id=1, received=0, unique_processed=0, duplicate_dropped=0)
    db.add(stats)
    db.commit()

    try:
        yield db
    finally:
        db.close()


@pytest.fixture(scope="function")
def client(test_db):
    """Create test client with test database."""

    def override_get_db():
        try:
            yield test_db
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db

    with TestClient(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()


@pytest.fixture
def sample_event():
    """Sample event untuk testing."""
    return {
        "topic": "test.event",
        "event_id": "test_evt_123",
        "timestamp": "2025-12-02T10:30:00Z",
        "source": "test-source",
        "payload": {"test": "data"},
    }


@pytest.fixture
def sample_events_batch():
    """Batch of sample events."""
    return {
        "events": [
            {
                "topic": "test.event",
                "event_id": f"test_evt_{i}",
                "timestamp": "2025-12-02T10:30:00Z",
                "source": "test-source",
                "payload": {"test": "data", "index": i},
            }
            for i in range(10)
        ]
    }
