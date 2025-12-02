from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
import logging
import os

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://agguser:aggpass123@localhost:5432/aggregator_db"
)

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    isolation_level="SERIALIZABLE",
    echo=False,
)


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_db_context():
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        db.close()


def init_db():
    from models import Base, EventStats

    logger.info("Creating database tables...")
    Base.metadata.create_all(bind=engine)

    # Initialize stats row jika belum ada
    with get_db_context() as db:
        stats = db.query(EventStats).filter(EventStats.id == 1).first()
        if not stats:
            stats = EventStats(
                id=1, received=0, unique_processed=0, duplicate_dropped=0
            )
            db.add(stats)
            db.commit()
            logger.info("Initialized event_stats table")

    logger.info("Database initialized successfully")


def health_check() -> bool:
    try:
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False
