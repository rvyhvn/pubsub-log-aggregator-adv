import os


class Config:
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", "postgresql://agguser:aggpass123@localhost:5432/aggregator_db"
    )

    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379")
    REDIS_CHANNEL: str = os.getenv("REDIS_CHANNEL", "events")

    NUM_WORKERS: int = int(os.getenv("NUM_WORKERS", "3"))

    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8080"))

    DB_POOL_SIZE: int = int(os.getenv("DB_POOL_SIZE", "10"))
    DB_MAX_OVERFLOW: int = int(os.getenv("DB_MAX_OVERFLOW", "20"))

    DB_ISOLATION_LEVEL: str = os.getenv("DB_ISOLATION_LEVEL", "SERIALIZABLE")

    @classmethod
    def validate(cls):
        assert cls.NUM_WORKERS > 0, "NUM_WORKERS must be positive"
        assert cls.DB_POOL_SIZE > 0, "DB_POOL_SIZE must be positive"
        assert cls.API_PORT > 0, "API_PORT must be positive"
        assert cls.DB_ISOLATION_LEVEL in [
            "READ UNCOMMITTED",
            "READ COMMITTED",
            "REPEATABLE READ",
            "SERIALIZABLE",
        ], "Invalid isolation level"

    @classmethod
    def print_config(cls):
        print(f"DATABASE_URL: {cls.DATABASE_URL}")
        print(f"REDIS_URL: {cls.REDIS_URL}")
        print(f"REDIS_CHANNEL: {cls.REDIS_CHANNEL}")
        print(f"NUM_WORKERS: {cls.NUM_WORKERS}")
        print(f"LOG_LEVEL: {cls.LOG_LEVEL}")
        print(f"DB_ISOLATION_LEVEL: {cls.DB_ISOLATION_LEVEL}")
        print("=" * 21)


Config.validate()
