"""Inventory Event Kafka Producer for E-Commerce Stock Changes."""

import argparse
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional
import confluent_kafka
from src.config import get_settings

logger = logging.getLogger(__name__)


def produce_inventory_event(
    sku: str,
    current_stock: int,
    reorder_threshold: int = 10,
    event_id: Optional[str] = None,
    timestamp: Optional[str] = None,
    bootstrap_servers: Optional[str] = None,
    topic: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Produce an immutable inventory event message to the Kafka cluster on inventory-events topic.

    Respects INVENTORY_AGENT_ENABLED feature flag.
    Partitions message by SKU as key, guaranteeing sequential delivery per product.

    Args:
        sku: Stock Keeping Unit identifier (e.g. 'TSHIRT-BLK-M').
        current_stock: Updated available stock level.
        reorder_threshold: Safety stock threshold.
        event_id: Unique event ID (defaults to 'evt-<uuid4>').
        timestamp: ISO-8601 UTC timestamp (defaults to current time).
        bootstrap_servers: Optional Kafka bootstrap servers override.
        topic: Optional Kafka topic override.

    Returns:
        The published payload dictionary, or None if the inventory agent is disabled.
    """
    settings = get_settings()

    # Check environment variable feature flag
    if not settings.is_inventory_agent_enabled:
        logger.info(
            "Inventory Agent is DISABLED (INVENTORY_AGENT_ENABLED=%s, INVENTORY_AGENT=%s). Skipping event dispatch for SKU=%s.",
            settings.INVENTORY_AGENT_ENABLED,
            settings.INVENTORY_AGENT,
            sku,
        )
        return None

    evt_id = event_id or f"evt-{uuid.uuid4().hex[:8]}"
    evt_time = timestamp or datetime.now(timezone.utc).isoformat()
    target_topic = topic or settings.KAFKA_TOPIC_INVENTORY_EVENTS
    servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS

    payload = {
        "event_id": evt_id,
        "timestamp": evt_time,
        "sku": sku,
        "current_stock": int(current_stock),
        "reorder_threshold": int(reorder_threshold),
    }

    raw_json = json.dumps(payload)
    logger.info(
        "Publishing inventory event to Kafka topic='%s' on '%s' (key=%s): %s",
        target_topic,
        servers,
        sku,
        raw_json,
    )

    try:
        producer = confluent_kafka.Producer({"bootstrap.servers": servers})
        # Partition by SKU as key
        producer.produce(
            topic=target_topic,
            key=sku.encode("utf-8"),
            value=raw_json.encode("utf-8"),
        )
        producer.flush(timeout=5.0)
        logger.info("Successfully produced inventory event for SKU=%s to topic=%s", sku, target_topic)
        return payload
    except Exception as exc:
        logger.error("Failed to produce Kafka message for SKU=%s: %s", sku, exc)
        raise


def main():
    """CLI utility for publishing test inventory events."""
    parser = argparse.ArgumentParser(description="Publish an inventory event to Apache Kafka")
    parser.add_argument("--sku", default="TSHIRT-BLK-M", help="Stock Keeping Unit (default: TSHIRT-BLK-M)")
    parser.add_argument("--stock", type=int, default=4, help="Current available stock (default: 4)")
    parser.add_argument("--threshold", type=int, default=10, help="Reorder threshold (default: 10)")
    parser.add_argument("--topic", default=None, help="Kafka topic override")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    print(f"Producing event: SKU={args.sku}, Current Stock={args.stock}, Threshold={args.threshold}")
    try:
        result = produce_inventory_event(
            sku=args.sku,
            current_stock=args.stock,
            reorder_threshold=args.threshold,
            topic=args.topic,
        )
        if result:
            print("Dispatched event successfully:")
            print(json.dumps(result, indent=2))
        else:
            print("Event was skipped because INVENTORY_AGENT_ENABLED is false.")
    except Exception as e:
        print(f"Error publishing event: {e}")


if __name__ == "__main__":
    main()
