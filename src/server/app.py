"""Autonomous Procurement Multi-Agent Real-Time Traceability & ERP Hub."""

import asyncio
import json
import logging
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse

# Mark current process as running the central ERP server & datastore
os.environ["IS_ERP_SERVER_PROCESS"] = "true"

from src.server.traceability import (
    ActivityLogRequest,
    activities_db,
    broadcast_event,
    record_activity,
    subscribers,
)
from src.server.erp import (
    CreatePurchaseOrderRequest,
    IssuePurchaseOrderRequest,
    PurchaseOrderResponse,
    RejectPurchaseOrderRequest,
    STORE_CATALOG,
    create_po_record,
    issue_po_record,
    purchase_orders_db,
    reject_po_record,
    resolve_product_info,
)

logger = logging.getLogger("agent_hub")

app = FastAPI(
    title="Autonomous Procurement Multi-Agent Traceability & ERP Hub",
    version="3.0.0",
    description="Unified Agent Server providing real-time activity streaming, HITL traceability, and embedded Mock ERP.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

STATIC_DIR = Path(__file__).parent / "static"


# ==============================================================================
# UI Static Files & Dashboard Endpoints
# ==============================================================================

@app.get("/", response_class=HTMLResponse, tags=["Dashboard UI"])
@app.get("/dashboard", response_class=HTMLResponse, tags=["Dashboard UI"])
def get_dashboard():
    """Serves the clean light-mode autonomous procurement command center UI."""
    index_file = STATIC_DIR / "index.html"
    if not index_file.exists():
        return HTMLResponse("<h1>Dashboard index.html not found</h1>", status_code=404)
    return FileResponse(index_file)


@app.get("/static/{filename:path}", tags=["Dashboard UI"])
def get_static_asset(filename: str):
    """Serves static files (CSS, JS, icons)."""
    file_path = STATIC_DIR / filename
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(file_path)


@app.get("/health", tags=["System"])
def health_check():
    """Health check endpoint for agent server and embedded ERP."""
    return {"status": "ok", "service": "agent-hub", "embedded_erp": True}


@app.delete("/api/v1/clear", tags=["System"])
async def clear_all_records():
    """Clear in-memory purchase orders and activity history."""
    purchase_orders_db.clear()
    activities_db.clear()
    await broadcast_event("CLEARED", {})
    return {"status": "ok", "message": "All records cleared"}


# ==============================================================================
# Real-Time Streaming & Activity Endpoints
# ==============================================================================

@app.get("/api/v1/stream", tags=["Streaming"])
@app.get("/api/v1/events/stream", tags=["Streaming"])
async def real_time_event_stream(request: Request):
    """Server-Sent Events (SSE) stream pushing live PO and agent activity events."""
    async def event_generator():
        queue = asyncio.Queue()
        subscribers.append(queue)
        try:
            welcome = {"type": "CONNECTED", "data": {"message": "Connected to Agent Traceability & ERP Stream"}}
            yield f"data: {json.dumps(welcome)}\n\n"

            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
        finally:
            if queue in subscribers:
                subscribers.remove(queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/api/v1/activities", tags=["Activities"])
def get_recent_activities():
    """Returns recent multi-agent activities from the agent traceability log."""
    return activities_db


@app.post("/api/v1/activities", status_code=status.HTTP_201_CREATED, tags=["Activities"])
async def log_activity(payload: ActivityLogRequest):
    """Push an agent activity event into the real-time traceability log."""
    product_title = payload.product_name or payload.details.get("product_name") or payload.sku
    record = record_activity(
        agent=payload.agent,
        event_type=payload.event_type,
        title=payload.title,
        description=payload.description,
        details=payload.details,
        sku=payload.sku,
        po_number=payload.po_number,
        product_name=product_title,
    )
    await broadcast_event("ACTIVITY", record)
    return record


# ==============================================================================
# Real-Time E-Commerce Products Endpoints (SMK Backend Live Data)
# ==============================================================================

@app.get("/api/v1/products", tags=["E-Commerce Products"])
def get_ecommerce_products():
    """
    Fetch active products from SMK server backend in real-time.
    Frontend UI uses this to populate dropdowns and display live stock.
    """
    try:
        from src.tools.ecommerce import fetch_active_products
        products = fetch_active_products()
        if products:
            formatted = []
            for p in products:
                formatted.append({
                    "id": p.get("id"),
                    "slug": p.get("slug"),
                    "title": p.get("title"),
                    "stock": p.get("stock", 0),
                    "price": float(p.get("price", 0.0)),
                    "mrp": float(p.get("mrp", 0.0)),
                    "category": p.get("categoryName", "Store Product"),
                    "variant": p.get("variant"),
                    "size": p.get("sizeName"),
                    "color": p.get("colorCode"),
                    "img": p.get("img"),
                })
            return {
                "success": True,
                "count": len(formatted),
                "source": "smk-backend-live",
                "products": formatted,
            }
    except Exception:
        pass

    # Fallback to store catalog if SMK backend is momentarily unreachable
    fallback_items = []
    for slug, details in STORE_CATALOG.items():
        if not slug.startswith("PROD-") and not slug.startswith("SKU-") and slug != "TSHIRT-BLK-M":
            fallback_items.append({
                "id": None,
                "slug": slug,
                "title": details["name"],
                "stock": 10,
                "price": round(details["unit_price"] * 2.2, 2),
                "mrp": round(details["unit_price"] * 3.0, 2),
                "category": "Store Catalog",
                "variant": "Standard",
                "size": "Standard",
                "color": "#000000",
                "img": None,
            })
    return {
        "success": True,
        "count": len(fallback_items),
        "source": "store-catalog-fallback",
        "products": fallback_items,
    }


@app.get("/api/v1/products/{slug}", tags=["E-Commerce Products"])
def get_ecommerce_product_by_slug(slug: str):
    """Fetch product details by slug in real-time from SMK server backend."""
    try:
        from src.tools.ecommerce import fetch_product_by_slug
        product = fetch_product_by_slug(slug)
        if product:
            return {"success": True, "source": "smk-backend-live", "data": product}
    except Exception:
        pass

    info = resolve_product_info(slug)
    return {
        "success": True,
        "source": "catalog-fallback",
        "data": {
            "slug": slug,
            "title": info["name"],
            "stock": info.get("current_stock", 0),
            "price": info.get("retail_price", round(info["unit_price"] * 2.2, 2)),
            "moq": info.get("moq", 30),
            "vendor_name": info.get("vendor_name"),
        },
    }


# ==============================================================================
# Embedded Mock ERP Purchase Order Endpoints
# ==============================================================================

@app.post(
    "/api/v1/purchase-orders",
    response_model=PurchaseOrderResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Purchase Orders"],
)
async def create_purchase_order(payload: CreatePurchaseOrderRequest):
    """Create a new purchase order in embedded Mock ERP datastore."""
    return await create_po_record(payload)


@app.patch(
    "/api/v1/purchase-orders/{po_id}/issue",
    response_model=PurchaseOrderResponse,
    tags=["Purchase Orders"],
)
async def issue_purchase_order(po_id: str, payload: IssuePurchaseOrderRequest):
    """Issue a purchase order in embedded Mock ERP and notify Slack."""
    po = await issue_po_record(po_id=po_id, approver=payload.approver)
    if not po:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Purchase order with ID '{po_id}' not found.",
        )
    return po


@app.patch(
    "/api/v1/purchase-orders/{po_id}/reject",
    response_model=PurchaseOrderResponse,
    tags=["Purchase Orders"],
)
async def reject_purchase_order(po_id: str, payload: RejectPurchaseOrderRequest):
    """Reject a purchase order in embedded Mock ERP and notify Slack."""
    rejector = payload.rejector or "approver"
    po = await reject_po_record(po_id=po_id, rejector=rejector)
    if not po:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Purchase order with ID '{po_id}' not found.",
        )
    return po


@app.get(
    "/api/v1/purchase-orders",
    response_model=List[PurchaseOrderResponse],
    tags=["Purchase Orders"],
)
def get_all_purchase_orders():
    """Returns all created purchase orders sorted newest first."""
    return sorted(
        purchase_orders_db.values(),
        key=lambda x: x.get("created_at", ""),
        reverse=True,
    )


@app.get(
    "/api/v1/purchase-orders/{po_id}",
    response_model=PurchaseOrderResponse,
    tags=["Purchase Orders"],
)
def get_purchase_order(po_id: str):
    """Returns a specific purchase order by ID."""
    if po_id not in purchase_orders_db:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Purchase order with ID '{po_id}' not found.",
        )
    return purchase_orders_db[po_id]


# ==============================================================================
# Simulation Endpoint (Interactive Testing with Store Products)
# ==============================================================================

class SimulateEventRequest(CreatePurchaseOrderRequest.__class__):
    pass


@app.post("/api/v1/simulate-event", tags=["Simulation"])
async def simulate_inventory_event(request_data: Dict[str, Any]):
    """
    Simulate real-time replenishment triggered by a store product item stock drop.
    Executes the autonomous multi-agent workflow directly, eliminating duplicate PO generation.
    """
    sku = request_data.get("sku", "PROD-101")
    current_stock = int(request_data.get("current_stock", 4))
    threshold = int(request_data.get("reorder_threshold", 10))
    prod_info = resolve_product_info(sku, custom_name=request_data.get("product_name"))
    product_name = prod_info["name"]

    # Step 1: Record E-Commerce Store Event in traceability stream
    act_store = record_activity(
        agent="E-Commerce Store",
        event_type="INVENTORY_EVENT",
        title=f"Stock Drop: {product_name}",
        description=f"Item stock updated to {current_stock} units (safety threshold: {threshold}). Triggering replenishment workflow.",
        details={"product": product_name, "sku": sku, "stock": current_stock, "threshold": threshold},
        sku=sku,
    )
    await broadcast_event("ACTIVITY", act_store)

    # Step 2: Deterministic Pre-Filter check
    if current_stock > threshold:
        act_filter = record_activity(
            agent="Deterministic Pre-Filter",
            event_type="PRE_FILTER_SKIPPED",
            title=f"Pre-Filter: {product_name} Stock Healthy",
            description=f"Available stock ({current_stock}) > threshold ({threshold}). Zero LLM tokens spent. No order required.",
            details={"status": "HEALTHY", "stock": current_stock, "threshold": threshold},
            sku=sku,
        )
        await broadcast_event("ACTIVITY", act_filter)
        return {"status": "SKIPPED", "message": f"Stock is healthy ({current_stock} > {threshold})"}

    # Step 3: Trigger the unified Multi-Agent Workflow
    from src.workers.kafka_consumer import process_inventory_event

    result = process_inventory_event({
        "sku": sku,
        "product_name": product_name,
        "current_stock": current_stock,
        "reorder_threshold": threshold,
    })

    if not result:
        return {"status": "SKIPPED", "message": f"Stock is healthy ({current_stock} > {threshold})"}

    return {
        "status": result.get("status", "AWAITING_APPROVAL"),
        "po_number": result.get("po_number"),
        "product_name": product_name,
        "sku": sku,
        "quantity": result.get("target_reorder_qty", 0),
        "total_cost": result.get("total_estimated_cost", 0.0),
    }
