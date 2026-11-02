"""Background workers for Kafka ingestion and Slack HITL resumption."""

from src.workers.kafka_consumer import (
    is_stock_low,
    process_inventory_event,
    run_kafka_consumer,
)
from src.workers.slack_listener import (
    create_slack_app,
    start_slack_socket_listener,
)
from src.producer import produce_inventory_event

__all__ = [
    "is_stock_low",
    "process_inventory_event",
    "run_kafka_consumer",
    "create_slack_app",
    "start_slack_socket_listener",
    "produce_inventory_event",
]
