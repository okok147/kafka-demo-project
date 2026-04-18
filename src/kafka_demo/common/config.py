from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    postgres_host: str = os.environ.get("POSTGRES_HOST", "postgres")
    postgres_port: int = int(os.environ.get("POSTGRES_PORT", "5432"))
    postgres_db: str = os.environ.get("POSTGRES_DB", "kafkademo")
    postgres_user: str = os.environ.get("POSTGRES_USER", "kafka")
    postgres_password: str = os.environ.get("POSTGRES_PASSWORD", "kafka")
    order_api_port: int = int(os.environ.get("ORDER_API_PORT", "8000"))
    query_api_port: int = int(os.environ.get("QUERY_API_PORT", "8080"))


settings = Settings()
