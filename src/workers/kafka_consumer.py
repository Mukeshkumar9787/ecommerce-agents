"""Kafka event ingestion worker with deterministic pre-filtering."""

import json
import logging
import signal
import sys
import time
from typing import Any, Dict, Optional
import confluent_kafka
from src.config import get_settings
from src.graph import create_procurement_app, get_langfuse_callback
from src.tools.erp import list_purchase_orders, log_agent_activity

logger = logging.getLogger(__name__)


def is_stock_low(current_stock: int, reorder_threshold: int) -> bool:
    """
    Deterministic pre-filter logic:
    Returns True if stock requires replenishment (current_stock <= reorder_threshold).
    Returns False if stock is healthy (current_stock > reorder_threshold).
    """
    return current_stock <= reorder_threshold


def process_inventory_event(
    payload: Dict[str, Any],
    app: Optional[Any] = None,
) -> Optional[Dict[str, Any]]:
    """
    Process an incoming inventory event.

    1. Checks deterministic pre-filter: if current_stock > reorder_threshold, skips processing.
    2. If stock is low, generates unique thread_id: reorder-{sku}-{timestamp}.
    3. Triggers the LangGraph multi-agent workflow with initial state.

    Args:
        payload: Event dictionary with 'sku', 'current_stock', 'reorder_threshold'.
        app: Optional compiled LangGraph application.

    Returns:
        Graph output state if triggered, or None if pre-filtered / invalid.
    """
    # Skip restock / replenishment events directed at the store backend
    if payload.get("event_type") == "STOCK_RESTOCK" or payload.get("action") == "RESTOCK":
        logger.debug("Skipping STOCK_RESTOCK event in procurement consumer: %s", payload)
        return None

    sku = payload.get("sku")
    current_stock = payload.get("current_stock")
    reorder_threshold = payload.get("reorder_threshold")

    if sku is None or current_stock is None or reorder_threshold is None:
        logger.error("Malformed inventory event payload missing required keys: %s", payload)
        return None

    # Deterministic Pre-Filter (zero LLM token spend on healthy inventory)
    if not is_stock_low(current_stock=current_stock, reorder_threshold=reorder_threshold):
        logger.info(
            "PRE-FILTER SKIPPED: SKU=%s has healthy stock (%d > threshold %d). Committing offset immediately.",
            sku,
            current_stock,
            reorder_threshold,
        )
        log_agent_activity(
            agent="Deterministic Pre-Filter",
            event_type="PRE_FILTER_SKIPPED",
            title=f"Pre-Filter: {sku} Stock Healthy",
            description=f"Stock level ({current_stock}) > threshold ({reorder_threshold}). Zero LLM tokens spent.",
            details={"sku": sku, "stock": current_stock, "threshold": reorder_threshold},
            sku=sku,
        )
        return None

    # Deterministic Pre-Filter: Prevent duplicate POs if an active DRAFT PO is already pending for this SKU
    try:
        existing_pos = list_purchase_orders()
        cleaned_sku = str(sku).strip().upper()
        for po in existing_pos:
            if (
                po.get("sku", "").strip().upper() == cleaned_sku
                and po.get("status", "").upper() == "DRAFT"
            ):
                po_num = po.get("po_number", "UNKNOWN")
                logger.info(
                    "PRE-FILTER SKIPPED: SKU=%s already has active draft PO %s awaiting approval. Skipping duplicate workflow.",
                    sku,
                    po_num,
                )
                log_agent_activity(
                    agent="Deterministic Pre-Filter",
                    event_type="PRE_FILTER_SKIPPED",
                    title=f"Pre-Filter: {sku} Reorder In-Flight",
                    description=f"Draft PO '{po_num}' is already awaiting approval. Zero LLM tokens spent. Skipping duplicate PO generation.",
                    details={"sku": sku, "existing_po": po_num, "status": "DRAFT_PENDING"},
                    sku=sku,
                )
                return po
    except Exception as check_err:
        logger.debug("Active PO check skipped: %s", check_err)

    # Stock is low -> instantiate workflow
    timestamp = int(time.time())
    thread_id = f"reorder-{sku}-{timestamp}"
    logger.info(
        "PRE-FILTER PASSED: SKU=%s requires reorder (%d <= threshold %d). Spawning workflow thread='%s'",
        sku,
        current_stock,
        reorder_threshold,
        thread_id,
    )
    log_agent_activity(
        agent="Kafka Ingestion",
        event_type="INVENTORY_EVENT",
        title=f"Kafka Ingestion: {sku} Depleted",
        description=f"Low inventory event received. Current stock {current_stock} <= threshold {reorder_threshold}. Thread '{thread_id}' created.",
        details={"sku": sku, "stock": current_stock, "threshold": threshold_val if (threshold_val := reorder_threshold) else 0, "thread_id": thread_id},
        sku=sku,
    )

    if app is None:
        app = create_procurement_app()

    initial_state = {
        "sku": sku,
        "current_stock": int(current_stock),
        "reorder_threshold": int(reorder_threshold),
        "status": "PENDING_ANALYSIS",
        "audit_log": [
            f"Kafka Ingestion: Received inventory event for {sku} "
            f"(current_stock={current_stock}, threshold={reorder_threshold})."
        ],
    }

    callbacks = []
    lf_callback = get_langfuse_callback()
    if lf_callback:
        callbacks.append(lf_callback)

    config = {
        "configurable": {"thread_id": thread_id},
        "callbacks": callbacks,
    }

    result = app.invoke(initial_state, config=config)
    try:
        from src.graph import flush_langfuse
        flush_langfuse()
    except Exception:
        pass
    logger.info(
        "Workflow thread '%s' reached interrupt. Status: %s, PO: %s",
        thread_id,
        result.get("status"),
        result.get("po_number"),
    )
    return result


def _apply_security_config(conf: dict, settings: Any):
    """Apply SASL, SSL, and CA certificate configurations to librdkafka config dict."""
    import os
    from pathlib import Path

    if settings.KAFKA_SECURITY_PROTOCOL:
        conf["security.protocol"] = settings.KAFKA_SECURITY_PROTOCOL
    elif settings.KAFKA_USERNAME and settings.KAFKA_PASSWORD:
        conf["security.protocol"] = "SASL_SSL"
    elif settings.KAFKA_CA_LOCATION or settings.KAFKA_CA_CERT:
        conf["security.protocol"] = "SSL"

    if settings.KAFKA_USERNAME and settings.KAFKA_PASSWORD:
        conf["sasl.mechanism"] = settings.KAFKA_SASL_MECHANISM or "PLAIN"
        conf["sasl.username"] = settings.KAFKA_USERNAME
        conf["sasl.password"] = settings.KAFKA_PASSWORD

    if settings.KAFKA_CA_LOCATION:
        ca_path = settings.KAFKA_CA_LOCATION
        if not os.path.isabs(ca_path):
            base_dir = Path(__file__).resolve().parent.parent.parent
            ca_path = str(base_dir / ca_path)
        conf["ssl.ca.location"] = ca_path
    elif settings.KAFKA_CA_CERT:
        conf["ssl.ca.pem"] = settings.KAFKA_CA_CERT

    if settings.KAFKA_CLIENT_CERT_LOCATION:
        cert_path = settings.KAFKA_CLIENT_CERT_LOCATION
        if not os.path.isabs(cert_path):
            base_dir = Path(__file__).resolve().parent.parent.parent
            cert_path = str(base_dir / cert_path)
        conf["ssl.certificate.location"] = cert_path
    if settings.KAFKA_CLIENT_KEY_LOCATION:
        key_path = settings.KAFKA_CLIENT_KEY_LOCATION
        if not os.path.isabs(key_path):
            base_dir = Path(__file__).resolve().parent.parent.parent
            key_path = str(base_dir / key_path)
        conf["ssl.key.location"] = key_path


def ensure_topic_exists(bootstrap_servers: str, topic: str):
    """Ensure topic exists on Kafka broker before polling."""
    settings = get_settings()
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
        admin_conf = {"bootstrap.servers": bootstrap_servers}
        _apply_security_config(admin_conf, settings)
        admin = AdminClient(admin_conf)
        fs = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
        for t, f in fs.items():
            try:
                f.result()
                logger.info("Kafka topic '%s' created successfully.", t)
            except Exception:
                pass
    except Exception as exc:
        logger.debug("Topic creation check skipped: %s", exc)


def run_kafka_consumer():
    """Run continuous Kafka consumer loop polling inventory-events topic."""
    settings = get_settings()

    ensure_topic_exists(settings.KAFKA_BOOTSTRAP_SERVERS, settings.KAFKA_TOPIC_INVENTORY_EVENTS)

    conf = {
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": settings.KAFKA_CONSUMER_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    _apply_security_config(conf, settings)

    consumer = confluent_kafka.Consumer(conf)
    consumer.subscribe([settings.KAFKA_TOPIC_INVENTORY_EVENTS])
    logger.info(
        "Kafka consumer subscribed to topic '%s' on '%s' (group: '%s')",
        settings.KAFKA_TOPIC_INVENTORY_EVENTS,
        settings.KAFKA_BOOTSTRAP_SERVERS,
        settings.KAFKA_CONSUMER_GROUP_ID,
    )

    procurement_app = create_procurement_app()
    running = True

    def handle_exit(sig, frame):
        nonlocal running
        logger.info("Received termination signal %s. Shutting down Kafka consumer...", sig)
        running = False

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                err_code = msg.error().code()
                if err_code == confluent_kafka.KafkaError._PARTITION_EOF:
                    continue
                elif err_code in (
                    confluent_kafka.KafkaError.UNKNOWN_TOPIC_OR_PART,
                    getattr(confluent_kafka.KafkaError, "_UNKNOWN_TOPIC", -168),
                ):
                    logger.warning(
                        "Topic '%s' not yet available or awaiting partition assignment. Waiting...",
                        settings.KAFKA_TOPIC_INVENTORY_EVENTS,
                    )
                    time.sleep(1.0)
                    continue
                else:
                    logger.error("Kafka consumer error: %s", msg.error())
                    time.sleep(1.0)
                    continue

            try:
                raw_payload = msg.value().decode("utf-8")
                payload = json.loads(raw_payload)
                process_inventory_event(payload=payload, app=procurement_app)
                # Commit offset deterministically after processing or filtering
                consumer.commit(message=msg, asynchronous=False)
            except Exception as exc:
                logger.exception("Failed to process Kafka message: %s", exc)
                # Commit or dead-letter in production; commit to advance
                consumer.commit(message=msg, asynchronous=False)
    finally:
        logger.info("Closing Kafka consumer connection...")
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    run_kafka_consumer()
