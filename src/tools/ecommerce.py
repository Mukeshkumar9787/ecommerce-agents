"""E-Commerce Store client interfacing with SMK backend API in real time."""

import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import httpx
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


def get_smk_base_urls() -> List[str]:
    """Return candidates for SMK server URL (host or docker)."""
    env_url = os.getenv("SMK_SERVER_URL")
    urls = []
    if env_url:
        urls.append(env_url.rstrip("/"))
    urls.extend([
        "http://localhost:4000/api",
        "http://host.docker.internal:4000/api",
        "http://127.0.0.1:4000/api",
    ])
    # Remove duplicates preserving order
    seen = set()
    result = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            result.append(u)
    return result


def fetch_product_by_slug(slug: str) -> Optional[Dict[str, Any]]:
    """
    Query SMK e-commerce backend in real-time to fetch product details by slug.
    GET /api/products/{slug}
    """
    if not slug:
        return None

    cleaned_slug = str(slug).strip()
    slug_candidates = [cleaned_slug]

    # Normalized candidate (e.g. "Classic Heavyweight Cotton" -> "classic-heavyweight-cotton")
    normalized = re.sub(r"[^a-zA-Z0-9]+", "-", cleaned_slug).strip("-").lower()
    if normalized and normalized not in slug_candidates:
        slug_candidates.append(normalized)

    for target_slug in slug_candidates:
        for base in get_smk_base_urls():
            url = f"{base}/products/{target_slug}"
            try:
                with httpx.Client(timeout=3.0) as client:
                    resp = client.get(url)
                    if resp.status_code == 200:
                        data = resp.json()
                        if data.get("success") and "data" in data and data["data"]:
                            logger.info("Successfully fetched real-time product details for slug '%s' from %s", target_slug, url)
                            return data["data"]
            except Exception as exc:
                logger.debug("Failed connecting to %s: %s", url, exc)

    # Secondary lookup: if slug is an ID or PROD-XX, inspect active products list
    if cleaned_slug.isdigit() or cleaned_slug.upper().startswith("PROD-"):
        prod_id = int(cleaned_slug.replace("PROD-", "").replace("prod-", "")) if any(char.isdigit() for char in cleaned_slug) else None
        if prod_id is not None:
            active_list = fetch_active_products()
            for p in active_list:
                if p.get("id") == prod_id:
                    logger.info("Matched active product ID %d with slug '%s'", prod_id, p.get("slug"))
                    return p

    return None


def fetch_active_products() -> List[Dict[str, Any]]:
    """
    Fetch all active products from SMK e-commerce backend.
    GET /api/products/active
    """
    for base in get_smk_base_urls():
        url = f"{base}/products/active"
        try:
            with httpx.Client(timeout=3.0) as client:
                resp = client.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("success") and isinstance(data.get("data"), list):
                        return data["data"]
        except Exception as exc:
            logger.debug("Failed connecting to %s: %s", url, exc)

    return []


class ProductSlugInput(BaseModel):
    slug: str = Field(..., description="Store product slug (e.g. 'classic-heavyweight-organic-cotton-t-shirt')")


def get_ecommerce_product_details(slug: str) -> Dict[str, Any]:
    """Retrieve live real-time product specifications, inventory, and retail pricing from SMK store."""
    product = fetch_product_by_slug(slug)
    if not product:
        return {"found": False, "slug": slug, "message": f"Product slug '{slug}' not found on store backend."}

    return {
        "found": True,
        "id": product.get("id"),
        "title": product.get("title"),
        "slug": product.get("slug"),
        "stock": product.get("stock", 0),
        "retail_price": float(product.get("price", 0.0)),
        "mrp": float(product.get("mrp", 0.0)),
        "variant": product.get("variant"),
        "size": product.get("sizeTitle") or product.get("sizeName"),
        "color": product.get("colorCode"),
    }


def produce_restock_event(
    slug: str,
    quantity: int,
    po_number: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Produce a Kafka replenishment event for SMK e-commerce backend upon PO approval.
    Topic: KAFKA_TOPIC_RESTOCK_EVENTS (default 'inventory-restock')
    """
    if not slug or quantity <= 0:
        return None

    # Policy: Quantity of products to be added should be less than 20
    effective_quantity = int(quantity)
    if effective_quantity >= 20:
        logger.info(
            "Capping restock quantity from %d to 19 (quantity of products to be added should be less than 20)",
            effective_quantity,
        )
        effective_quantity = 19

    clean_slug = str(slug).strip()
    evt_id = f"restock-{uuid.uuid4().hex[:8]}"
    evt_time = datetime.now(timezone.utc).isoformat()

    payload: Dict[str, Any] = {
        "event_id": evt_id,
        "event_type": "STOCK_RESTOCK",
        "action": "RESTOCK",
        "timestamp": evt_time,
        "slug": clean_slug,
        "sku": clean_slug,
        "quantity": effective_quantity,
        "po_number": po_number or "",
        "poNumber": po_number or "",
    }
    if clean_slug.isdigit():
        payload["productId"] = int(clean_slug)

    try:
        from src.config import get_settings
        from src.workers.kafka_consumer import _apply_security_config
        import confluent_kafka

        settings = get_settings()
        topic = (
            os.getenv("KAFKA_TOPIC_RESTOCK_EVENTS")
            or getattr(settings, "KAFKA_TOPIC_RESTOCK_EVENTS", "inventory-restock")
            or "inventory-restock"
        )
        servers = settings.KAFKA_BOOTSTRAP_SERVERS

        conf = {"bootstrap.servers": servers}
        _apply_security_config(conf, settings)

        producer = confluent_kafka.Producer(conf)
        raw_json = json.dumps(payload)
        producer.produce(
            topic=topic,
            key=clean_slug.encode("utf-8"),
            value=raw_json.encode("utf-8"),
        )
        producer.flush(timeout=5.0)

        logger.info(
            "🚀 Dispatched STOCK_RESTOCK event to Kafka topic='%s' for '%s' (+%d units, PO: %s)",
            topic,
            clean_slug,
            effective_quantity,
            po_number,
        )
        return {
            "success": True,
            "addedQuantity": effective_quantity,
            "slug": clean_slug,
            "poNumber": po_number,
            "eventId": evt_id,
        }
    except Exception as exc:
        logger.error("Failed to publish restock Kafka event for '%s': %s", clean_slug, exc)
        return None


def restock_ecommerce_product(
    slug: str,
    quantity: int,
    po_number: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Backward-compatible wrapper dispatching stock replenishment via Kafka event."""
    return produce_restock_event(slug=slug, quantity=quantity, po_number=po_number)

