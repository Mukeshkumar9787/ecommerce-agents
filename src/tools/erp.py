"""ERP integration tool interfacing with embedded Mock ERP datastore and REST API."""

import logging
import os
from typing import Any, Dict, List, Optional
import httpx
from langchain_core.tools import tool
from src.config import get_settings
from src.schemas.tools import CreateDraftPOInput, IssuePOInput

logger = logging.getLogger(__name__)


def is_running_in_server() -> bool:
    """Check if execution is happening directly inside the ERP FastAPI server process."""
    return os.environ.get("IS_ERP_SERVER_PROCESS") == "true"


def create_draft_purchase_order(
    sku: str,
    quantity: int,
    vendor_id: str,
    total_cost: float,
    base_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generate a new Purchase Order in DRAFT status.
    If running inside the server process, writes directly to in-memory datastore.
    If in an external worker daemon (Kafka consumer, Slack listener), calls ERP REST API via HTTP.
    Falls back to in-memory if HTTP is unreachable (e.g. during standalone tests).
    """
    if is_running_in_server():
        try:
            from src.server.erp import create_po_record_sync, CreatePurchaseOrderRequest
            req = CreatePurchaseOrderRequest(
                sku=sku,
                quantity=quantity,
                vendor_id=vendor_id,
                total_cost=total_cost,
            )
            res = create_po_record_sync(req)
            if res:
                logger.info("Successfully created draft PO in embedded ERP (in-memory): %s", res.get("po_number"))
                return res
        except Exception as in_mem_err:
            logger.debug("In-memory PO creation skipped (%s), falling back to HTTP", in_mem_err)

    url = (base_url or get_settings().MOCK_ERP_URL).rstrip("/") + "/api/v1/purchase-orders"
    payload = {
        "sku": sku,
        "quantity": quantity,
        "vendor_id": vendor_id,
        "total_cost": total_cost,
    }

    try:
        logger.info("Submitting draft purchase order to ERP via HTTP: %s | payload=%s", url, payload)
        with httpx.Client(timeout=5.0) as client:
            response = client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            logger.info("Successfully created draft PO in ERP via HTTP: %s", data.get("po_number"))
            return data
    except Exception as http_err:
        logger.debug("HTTP PO creation failed or server unreachable (%s), falling back to in-memory", http_err)
        from src.server.erp import create_po_record_sync, CreatePurchaseOrderRequest
        req = CreatePurchaseOrderRequest(
            sku=sku,
            quantity=quantity,
            vendor_id=vendor_id,
            total_cost=total_cost,
        )
        return create_po_record_sync(req)


def issue_purchase_order(
    po_id: str,
    approver: str,
    base_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Update purchase order status to ISSUED.
    If running inside the server process, updates in-memory directly.
    If in an external worker (Slack listener, Kafka consumer), calls ERP REST API via HTTP.
    Falls back to in-memory if HTTP is unreachable.
    """
    if is_running_in_server():
        try:
            from src.server.erp import issue_po_record_sync, purchase_orders_db
            if po_id in purchase_orders_db:
                res = issue_po_record_sync(po_id=po_id, approver=approver)
                if res:
                    logger.info("Successfully issued PO %s in embedded ERP (in-memory)", po_id)
                    return res
        except Exception as in_mem_err:
            logger.debug("In-memory PO issue skipped (%s), falling back to HTTP", in_mem_err)

    url = (
        (base_url or get_settings().MOCK_ERP_URL).rstrip("/")
        + f"/api/v1/purchase-orders/{po_id}/issue"
    )
    payload = {"approver": approver}

    try:
        logger.info("Issuing purchase order in ERP via HTTP: %s | approver=%s", url, approver)
        with httpx.Client(timeout=5.0) as client:
            response = client.patch(url, json=payload)
            response.raise_for_status()
            data = response.json()
            logger.info("Successfully issued PO %s with status %s", po_id, data.get("status"))
            return data
    except Exception as http_err:
        logger.debug("HTTP PO issue failed (%s), falling back to in-memory", http_err)
        from src.server.erp import issue_po_record_sync
        return issue_po_record_sync(po_id=po_id, approver=approver)


def list_purchase_orders(base_url: Optional[str] = None) -> List[Dict[str, Any]]:
    """Retrieve all purchase orders from ERP."""
    if is_running_in_server():
        try:
            from src.server.erp import purchase_orders_db
            return sorted(
                purchase_orders_db.values(),
                key=lambda x: x.get("created_at", ""),
                reverse=True,
            )
        except Exception:
            pass

    url = (base_url or get_settings().MOCK_ERP_URL).rstrip("/") + "/api/v1/purchase-orders"
    try:
        with httpx.Client(timeout=3.0) as client:
            response = client.get(url)
            if response.status_code == 200:
                return response.json()
    except Exception:
        pass

    try:
        from src.server.erp import purchase_orders_db
        return sorted(
            purchase_orders_db.values(),
            key=lambda x: x.get("created_at", ""),
            reverse=True,
        )
    except Exception:
        pass
    return []


def reject_purchase_order(
    po_id: str,
    rejector: str,
    base_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Update purchase order status to REJECTED.
    If running inside server process, updates in-memory directly.
    If in external worker, calls ERP REST API via HTTP.
    Falls back to in-memory if HTTP is unreachable.
    """
    if is_running_in_server():
        try:
            from src.server.erp import reject_po_record_sync, purchase_orders_db
            if po_id in purchase_orders_db:
                res = reject_po_record_sync(po_id=po_id, rejector=rejector)
                if res:
                    logger.info("Successfully marked PO %s as REJECTED in embedded ERP (in-memory)", po_id)
                    return res
        except Exception as in_mem_err:
            logger.debug("In-memory PO reject skipped (%s), falling back to HTTP", in_mem_err)

    url = (
        (base_url or get_settings().MOCK_ERP_URL).rstrip("/")
        + f"/api/v1/purchase-orders/{po_id}/reject"
    )
    payload = {"rejector": rejector}

    try:
        logger.info("Rejecting purchase order in ERP via HTTP: %s | rejector=%s", url, rejector)
        with httpx.Client(timeout=5.0) as client:
            response = client.patch(url, json=payload)
            response.raise_for_status()
            data = response.json()
            logger.info("Successfully marked PO %s as REJECTED", po_id)
            return data
    except Exception as http_err:
        logger.debug("HTTP PO reject failed (%s), falling back to in-memory", http_err)
        from src.server.erp import reject_po_record_sync
        return reject_po_record_sync(po_id=po_id, rejector=rejector)


def log_agent_activity(
    agent: str,
    event_type: str,
    title: str,
    description: str = "",
    details: Optional[Dict[str, Any]] = None,
    sku: Optional[str] = None,
    po_number: Optional[str] = None,
    base_url: Optional[str] = None,
) -> None:
    """
    Post an activity event to Agent Traceability & ERP server for real-time dashboard display.
    If running inside server process, records in-memory and broadcasts.
    If in external worker, posts to HTTP /api/v1/activities endpoint.
    """
    if is_running_in_server():
        try:
            from src.server.traceability import record_activity, broadcast_event
            import asyncio
            record = record_activity(
                agent=agent,
                event_type=event_type,
                title=title,
                description=description,
                details=details,
                sku=sku,
                po_number=po_number,
            )
            try:
                loop = asyncio.get_running_loop()
                if loop and loop.is_running():
                    asyncio.create_task(broadcast_event("ACTIVITY", record))
            except Exception:
                pass
            return
        except Exception:
            pass

    try:
        url = (base_url or get_settings().MOCK_ERP_URL).rstrip("/") + "/api/v1/activities"
        payload = {
            "agent": agent,
            "event_type": event_type,
            "title": title,
            "description": description,
            "details": details or {},
            "sku": sku,
            "po_number": po_number,
        }
        with httpx.Client(timeout=2.0) as client:
            client.post(url, json=payload)
    except Exception:
        # Best effort telemetry
        pass


@tool("create_draft_po_tool", args_schema=CreateDraftPOInput)
def create_draft_po_tool(sku: str, quantity: int, vendor_id: str, total_cost: float) -> dict:
    """Create a draft purchase order in the enterprise ERP system."""
    return create_draft_purchase_order(
        sku=sku, quantity=quantity, vendor_id=vendor_id, total_cost=total_cost
    )


@tool("issue_po_tool", args_schema=IssuePOInput)
def issue_po_tool(po_id: str, approver: str) -> dict:
    """Issue an approved purchase order in the enterprise ERP system."""
    return issue_purchase_order(po_id=po_id, approver=approver)


# Alias matching architecture specification
create_erp_purchase_order = create_draft_purchase_order
