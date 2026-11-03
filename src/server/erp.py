"""Embedded Mock ERP Service for Purchase Order Lifecycle and Traceability."""

import logging
import os
import random
import string
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from src.server.traceability import broadcast_event, record_activity, subscribers

logger = logging.getLogger("agent_erp")

# In-memory Purchase Orders Datastore (shared across agent system)
purchase_orders_db: Dict[str, dict] = {}


# ==============================================================================
# Pydantic Schemas
# ==============================================================================

class CreatePurchaseOrderRequest(BaseModel):
    sku: str = Field(..., description="Product identifier, SKU, or slug")
    quantity: int = Field(..., gt=0, description="Quantity to order")
    vendor_id: str = Field(..., description="Vendor/Supplier identifier")
    total_cost: float = Field(..., gt=0, description="Total estimated cost")
    product_name: Optional[str] = Field(None, description="Human-readable product item name")
    vendor_name: Optional[str] = Field(None, description="Optional vendor name")
    formula_breakdown: Optional[str] = Field(None, description="Calculation formula")


class IssuePurchaseOrderRequest(BaseModel):
    approver: str = Field(..., description="Username or ID of the approver")


class RejectPurchaseOrderRequest(BaseModel):
    rejector: Optional[str] = Field("approver", description="Username or ID of the person rejecting")


class PurchaseOrderResponse(BaseModel):
    po_number: str
    sku: str
    quantity: int
    vendor_id: str
    total_cost: float
    status: str
    product_name: Optional[str] = None
    approver: Optional[str] = None
    vendor_name: Optional[str] = None
    formula_breakdown: Optional[str] = None
    audit_trail: Optional[List[str]] = None
    store_stock: Optional[int] = None
    created_at: str
    updated_at: str


# ==============================================================================
# Store Product Catalog & Live Resolver
# ==============================================================================

STORE_CATALOG = {
    "classic-heavyweight-organic-cotton-t-shirt": {
        "name": "Classic Heavyweight Organic Cotton T-Shirt",
        "vendor_id": "VEND-102",
        "vendor_name": "Apex Textiles Inc.",
        "moq": 50,
        "unit_price": 12.50,
        "lead_time": 5,
        "burn_rate": 4.0,
    },
    "urban-oversized-streetwear-fleece-hoodie": {
        "name": "Urban Oversized Streetwear Fleece Hoodie",
        "vendor_id": "VEND-NORDIC-04",
        "vendor_name": "Nordic Apparel Corp",
        "moq": 40,
        "unit_price": 28.00,
        "lead_time": 6,
        "burn_rate": 3.5,
    },
    "floral-print-pure-silk-wrap-summer-dress": {
        "name": "Floral Print Pure Silk Wrap Summer Dress",
        "vendor_id": "VEND-SILK-05",
        "vendor_name": "SilkCraft Studio",
        "moq": 25,
        "unit_price": 36.00,
        "lead_time": 8,
        "burn_rate": 2.0,
    },
    "tailored-high-waisted-linen-blend-trousers": {
        "name": "Tailored High-Waisted Linen Blend Trousers",
        "vendor_id": "VEND-ELITE-06",
        "vendor_name": "Elite Tailoring Ltd.",
        "moq": 30,
        "unit_price": 32.00,
        "lead_time": 7,
        "burn_rate": 3.0,
    },
    "PROD-101": {
        "name": "Classic Heavyweight Organic Cotton T-Shirt",
        "vendor_id": "VEND-102",
        "vendor_name": "Apex Textiles Inc.",
        "moq": 50,
        "unit_price": 12.50,
        "lead_time": 5,
        "burn_rate": 4.0,
    },
    "PROD-102": {
        "name": "Urban Oversized Streetwear Fleece Hoodie",
        "vendor_id": "VEND-NORDIC-04",
        "vendor_name": "Nordic Apparel Corp",
        "moq": 40,
        "unit_price": 28.00,
        "lead_time": 6,
        "burn_rate": 3.5,
    },
    "PROD-103": {
        "name": "Floral Print Pure Silk Wrap Summer Dress",
        "vendor_id": "VEND-SILK-05",
        "vendor_name": "SilkCraft Studio",
        "moq": 25,
        "unit_price": 36.00,
        "lead_time": 8,
        "burn_rate": 2.0,
    },
    "PROD-104": {
        "name": "Tailored High-Waisted Linen Blend Trousers",
        "vendor_id": "VEND-ELITE-06",
        "vendor_name": "Elite Tailoring Ltd.",
        "moq": 30,
        "unit_price": 32.00,
        "lead_time": 7,
        "burn_rate": 3.0,
    },
    "TSHIRT-BLK-M": {
        "name": "Apex Classic Black Cotton T-Shirt",
        "slug": "classic-heavyweight-organic-cotton-t-shirt",
        "vendor_id": "VEND-102",
        "vendor_name": "Apex Textiles Inc.",
        "moq": 100,
        "unit_price": 8.50,
        "lead_time": 6,
        "burn_rate": 5.0,
    },
    "SKU-WIDGET-01": {
        "name": "Acme Industrial Gear Widget",
        "vendor_id": "VEND-ACME-001",
        "vendor_name": "Acme Industrial Supplies",
        "moq": 50,
        "unit_price": 12.50,
        "lead_time": 7,
        "burn_rate": 5.0,
    },
    "SKU-CABLE-02": {
        "name": "Nexus High-Speed Braided Cable",
        "vendor_id": "VEND-NEXUS-002",
        "vendor_name": "Nexus Wire & Harness",
        "moq": 100,
        "unit_price": 4.25,
        "lead_time": 5,
        "burn_rate": 10.0,
    },
    "SKU-SENSOR-03": {
        "name": "OpticSensors Precision Module",
        "vendor_id": "VEND-OPTIC-003",
        "vendor_name": "OpticSensors Corp",
        "moq": 25,
        "unit_price": 48.00,
        "lead_time": 14,
        "burn_rate": 2.0,
    },
}


def resolve_product_info(sku: str, custom_name: Optional[str] = None) -> dict:
    """Resolve product title, supplier terms, and burn rates dynamically from slug or SKU via SMK backend or catalog."""
    cleaned_sku = sku.strip()

    try:
        from src.tools.ecommerce import fetch_product_by_slug
        live_prod = fetch_product_by_slug(cleaned_sku)
        if live_prod:
            title = custom_name or live_prod.get("title") or cleaned_sku.replace("-", " ").title()
            retail_price = float(live_prod.get("price") or live_prod.get("mrp") or 30.0)
            catalog_info = STORE_CATALOG.get(cleaned_sku, {})

            vendor_id = catalog_info.get("vendor_id", "VEND-PRIMARY-SUPPLY")
            vendor_name = catalog_info.get("vendor_name", "Primary Goods Supplier")
            moq = catalog_info.get("moq", 30)
            unit_price = catalog_info.get("unit_price", round(retail_price * 0.45, 2))
            lead_time = catalog_info.get("lead_time", 5)
            burn_rate = catalog_info.get("burn_rate", 3.5)

            return {
                "name": title,
                "vendor_id": vendor_id,
                "vendor_name": vendor_name,
                "moq": moq,
                "unit_price": unit_price,
                "lead_time": lead_time,
                "burn_rate": burn_rate,
                "current_stock": live_prod.get("stock", 0),
                "retail_price": retail_price,
                "slug": live_prod.get("slug", cleaned_sku),
            }
    except Exception:
        pass

    if cleaned_sku in STORE_CATALOG:
        info = dict(STORE_CATALOG[cleaned_sku])
        if custom_name:
            info["name"] = custom_name
        return info

    if f"PROD-{cleaned_sku}" in STORE_CATALOG:
        return STORE_CATALOG[f"PROD-{cleaned_sku}"]

    formatted_title = cleaned_sku.replace("-", " ").title() if "-" in cleaned_sku else f"Store Product ({cleaned_sku})"
    return {
        "name": custom_name or formatted_title,
        "vendor_id": "VEND-PRIMARY-SUPPLY",
        "vendor_name": "Primary Goods Supplier",
        "moq": 30,
        "unit_price": 15.00,
        "lead_time": 5,
        "burn_rate": 3.0,
    }


def generate_po_number() -> str:
    """Generate a unique PO-XXXX format number."""
    while True:
        suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
        po_number = f"PO-{suffix}"
        if po_number not in purchase_orders_db:
            return po_number


def send_slack_channel_message(text: str, blocks: Optional[List[dict]] = None):
    """Post real-time procurement notification message to Slack channel."""
    token = os.getenv("SLACK_BOT_TOKEN")
    channel = os.getenv("SLACK_CHANNEL_ID")
    if not token or token.startswith("xoxb-placeholder") or not channel:
        return

    try:
        import httpx
        url = "https://slack.com/api/chat.postMessage"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        payload = {"channel": channel, "text": text}
        if blocks:
            payload["blocks"] = blocks
        with httpx.Client(timeout=4.0) as client:
            resp = client.post(url, json=payload, headers=headers)
            if resp.status_code == 200:
                d = resp.json()
                if not d.get("ok"):
                    logger.warning("Slack notification API error: %s", d.get("error"))
                else:
                    logger.info("Dispatched Slack message to %s (ts=%s)", channel, d.get("ts"))
    except Exception as exc:
        logger.warning("Failed sending Slack notification: %s", exc)


def create_po_record_sync(payload: CreatePurchaseOrderRequest) -> dict:
    """Create a draft PO synchronously in ERP datastore and emit agent traceability events."""
    # Deduplication / Idempotency: Reuse existing DRAFT PO if one is already pending for this SKU
    cleaned_req_sku = payload.sku.strip().upper()
    for existing_po in purchase_orders_db.values():
        if (
            existing_po.get("sku", "").strip().upper() == cleaned_req_sku
            and existing_po.get("status", "").upper() == "DRAFT"
        ):
            logger.info(
                "Existing draft PO '%s' already pending for SKU '%s'. Reusing to eliminate duplicate PO generation.",
                existing_po.get("po_number"),
                payload.sku,
            )
            return existing_po

    now = datetime.now(timezone.utc).isoformat()
    po_number = generate_po_number()

    prod_info = resolve_product_info(payload.sku, custom_name=payload.product_name)
    product_name = payload.product_name or prod_info["name"]
    vendor_name = payload.vendor_name or prod_info["vendor_name"]

    po_record = {
        "po_number": po_number,
        "sku": payload.sku,
        "slug": prod_info.get("slug") or payload.sku,
        "product_name": product_name,
        "quantity": payload.quantity,
        "vendor_id": payload.vendor_id,
        "vendor_name": vendor_name,
        "total_cost": round(payload.total_cost, 2),
        "status": "DRAFT",
        "approver": None,
        "formula_breakdown": payload.formula_breakdown or f"Calculated order of {payload.quantity} units for {product_name}",
        "audit_trail": [
            f"Draft Purchase Order {po_number} created in ERP at {now} for {payload.quantity} units of '{product_name}' (${round(payload.total_cost, 2):,.2f})."
        ],
        "created_at": now,
        "updated_at": now,
    }
    purchase_orders_db[po_number] = po_record

    act = record_activity(
        agent="PO Manager",
        event_type="DRAFT_CREATED",
        title=f"Draft {po_number}: {product_name}",
        description=f"Generated draft purchase order for {payload.quantity} units ({vendor_name}). Awaiting approval.",
        details={"po": po_number, "product": product_name, "qty": payload.quantity, "cost": f"${round(payload.total_cost, 2):,.2f}"},
        sku=payload.sku,
        po_number=po_number,
    )
    for queue in list(subscribers):
        try:
            queue.put_nowait({"type": "PO_CREATED", "data": po_record})
            queue.put_nowait({"type": "ACTIVITY", "data": act})
        except Exception:
            pass
    return po_record


async def create_po_record(payload: CreatePurchaseOrderRequest) -> dict:
    """Async wrapper for create_po_record_sync."""
    return create_po_record_sync(payload)


def issue_po_record_sync(po_id: str, approver: str) -> Optional[dict]:
    """Synchronously issue a purchase order in ERP datastore and announce on Slack & activity stream."""
    if po_id not in purchase_orders_db:
        return None

    po = purchase_orders_db[po_id]
    now = datetime.now(timezone.utc).isoformat()
    po["status"] = "ISSUED"
    po["approver"] = approver
    po["updated_at"] = now
    po.setdefault("audit_trail", []).append(
        f"Approved by @{approver} at {now}. Purchase Order issued to supplier."
    )

    # Replenish stock in e-commerce platform upon PO approval
    sku_val = po.get("slug") or po.get("sku") or ""
    qty_val = po.get("quantity", 0)
    restock_res = None
    try:
        from src.tools.ecommerce import restock_ecommerce_product
        restock_res = restock_ecommerce_product(
            slug=sku_val,
            quantity=qty_val,
            po_number=po_id,
        )
    except Exception as exc:
        logger.warning("Failed to replenish store stock for PO %s: %s", po_id, exc)

    act_restock = None
    if restock_res:
        added_qty = restock_res.get("addedQuantity", min(qty_val, 19) if qty_val >= 20 else qty_val)
        po.setdefault("audit_trail", []).append(
            f"Kafka stock replenishment event dispatched: +{added_qty} units for store replenishment (PO: {po_id})."
        )
        act_restock = record_activity(
            agent="Inventory Restock Agent",
            event_type="STOCK_RESTOCKED",
            title=f"Stock Restock Event: +{added_qty} units",
            description=f"Purchase order {po_id} approved. Dispatched Kafka replenishment event for +{added_qty} units of {po.get('product_name', sku_val)}.",
            details={
                "po": po_id,
                "sku": sku_val,
                "quantity_added": added_qty,
                "topic": "inventory-restock",
            },
            sku=sku_val,
            po_number=po_id,
        )

    act = record_activity(
        agent="Human Approver",
        event_type="APPROVED",
        title=f"{po_id} Approved & Issued",
        description=f"Order {po_id} for {po.get('product_name', po.get('sku'))} approved by @{approver}. Transmitted to {po.get('vendor_name', po.get('vendor_id'))}.",
        details={"po": po_id, "approver": approver, "product": po.get("product_name"), "status": "ISSUED"},
        sku=po.get("sku"),
        po_number=po_id,
    )
    for queue in list(subscribers):
        try:
            queue.put_nowait({"type": "PO_UPDATED", "data": po})
            queue.put_nowait({"type": "ACTIVITY", "data": act})
            if act_restock:
                queue.put_nowait({"type": "ACTIVITY", "data": act_restock})
        except Exception:
            pass

    # Dispatch official Slack announcement card
    prod_name = po.get("product_name") or po.get("sku")
    vendor_label = po.get("vendor_name") or po.get("vendor_id")
    cost_val = po.get("total_cost", 0.0)

    slack_fields = [
        {"type": "mrkdwn", "text": f"*PO Number:*\n`{po_id}`"},
        {"type": "mrkdwn", "text": f"*Status:*\n*ISSUED*"},
        {"type": "mrkdwn", "text": f"*Product Item:*\n*{prod_name}* (`{po.get('sku')}`)"},
        {"type": "mrkdwn", "text": f"*Order Quantity:*\n{qty_val:,} units"},
        {"type": "mrkdwn", "text": f"*Total Value:*\n${cost_val:,.2f}"},
        {"type": "mrkdwn", "text": f"*Supplier:*\n{vendor_label}"},
        {"type": "mrkdwn", "text": f"*Approved By:*\n@{approver}"},
    ]
    if po.get("store_stock") is not None:
        slack_fields.append({"type": "mrkdwn", "text": f"*Store Stock:*\n{po['store_stock']:,} units"})

    send_slack_channel_message(
        text=f"✅ Purchase Order {po_id} APPROVED by @{approver}: {qty_val} units of '{prod_name}' (${cost_val:,.2f}) issued to {vendor_label}.",
        blocks=[
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "✅ Purchase Order Approved & Issued",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "fields": slack_fields,
            },
        ],
    )
    return po


async def issue_po_record(po_id: str, approver: str) -> Optional[dict]:
    """Async wrapper for issue_po_record_sync."""
    return issue_po_record_sync(po_id, approver)


def reject_po_record_sync(po_id: str, rejector: str) -> Optional[dict]:
    """Synchronously reject a purchase order in ERP datastore and announce on Slack & activity stream."""
    if po_id not in purchase_orders_db:
        return None

    po = purchase_orders_db[po_id]
    now = datetime.now(timezone.utc).isoformat()
    po["status"] = "REJECTED"
    po["approver"] = rejector
    po["updated_at"] = now
    po.setdefault("audit_trail", []).append(
        f"Order rejected by @{rejector} at {now}. Order cancelled."
    )

    act = record_activity(
        agent="Human Approver",
        event_type="REJECTED",
        title=f"{po_id} Rejected",
        description=f"Purchase order {po_id} for {po.get('product_name', po.get('sku'))} was declined by @{rejector}.",
        details={"po": po_id, "rejector": rejector, "status": "REJECTED"},
        sku=po.get("sku"),
        po_number=po_id,
    )
    for queue in list(subscribers):
        try:
            queue.put_nowait({"type": "PO_UPDATED", "data": po})
            queue.put_nowait({"type": "ACTIVITY", "data": act})
        except Exception:
            pass

    # Dispatch official Slack announcement card
    prod_name = po.get("product_name") or po.get("sku")
    send_slack_channel_message(
        text=f"❌ Purchase Order {po_id} REJECTED by @{rejector} for item '{prod_name}'.",
        blocks=[
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "❌ Purchase Order Declined",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*PO Number:*\n`{po_id}`"},
                    {"type": "mrkdwn", "text": f"*Status:*\n*REJECTED*"},
                    {"type": "mrkdwn", "text": f"*Product Item:*\n*{prod_name}* (`{po.get('sku')}`)"},
                    {"type": "mrkdwn", "text": f"*Rejected By:*\n@{rejector}"},
                    {"type": "mrkdwn", "text": f"*Decision:*\nDeclined / Cancelled. No order placed with vendor."},
                ],
            },
        ],
    )
    return po


async def reject_po_record(po_id: str, rejector: str) -> Optional[dict]:
    """Async wrapper for reject_po_record_sync."""
    return reject_po_record_sync(po_id, rejector)
