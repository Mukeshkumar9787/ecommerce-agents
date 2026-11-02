"""Real-time Multi-Agent Activity Tracking and Traceability Stream."""

import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

logger = logging.getLogger("agent_traceability")

# In-memory Agent Activity Datastore
activities_db: List[dict] = []
subscribers: List[asyncio.Queue] = []


class ActivityLogRequest(BaseModel):
    agent: str = Field(..., description="Agent or subsystem identifier (e.g. Supervisor, Inventory Analyst, PO Manager)")
    event_type: str = Field(..., description="Type of event (e.g. STATE_ROUTED, ANALYSIS_COMPLETED, DRAFT_CREATED)")
    title: str = Field(..., description="Brief event summary title")
    description: str = Field("", description="Detailed explanation of the action or reasoning")
    details: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Structured metadata dictionary")
    sku: Optional[str] = Field(None, description="Associated product SKU or slug")
    product_name: Optional[str] = Field(None, description="Human-readable product name")
    po_number: Optional[str] = Field(None, description="Associated Purchase Order number (PO-XXXX)")


async def broadcast_event(event_type: str, data: dict):
    """Broadcast an event payload to all connected SSE browser clients."""
    payload = {"type": event_type, "data": data}
    for queue in list(subscribers):
        try:
            await queue.put(payload)
        except Exception:
            pass


def record_activity(
    agent: str,
    event_type: str,
    title: str,
    description: str,
    details: Optional[dict] = None,
    sku: Optional[str] = None,
    po_number: Optional[str] = None,
    product_name: Optional[str] = None,
) -> dict:
    """Record an activity in the in-memory agent traceability log."""
    act_id = f"act-{len(activities_db) + 1:04d}-{random.randint(100, 999)}"
    record = {
        "id": act_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "agent": agent,
        "event_type": event_type,
        "title": title,
        "description": description,
        "details": details or {},
        "sku": sku,
        "po_number": po_number,
        "product_name": product_name or (details.get("product_name") if details else None),
    }
    activities_db.insert(0, record)
    if len(activities_db) > 250:
        activities_db.pop()
    return record
