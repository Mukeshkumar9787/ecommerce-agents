"""ERP integration tool interfacing with Mock ERP REST API."""

import logging
from typing import Any, Dict, List, Optional
import httpx
from langchain_core.tools import tool
from src.config import get_settings
from src.schemas.tools import CreateDraftPOInput, IssuePOInput

logger = logging.getLogger(__name__)


def create_draft_purchase_order(
    sku: str,
    quantity: int,
    vendor_id: str,
    total_cost: float,
    base_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Call ERP POST /api/v1/purchase-orders to generate a new Purchase Order in DRAFT status.

    Args:
        sku: Stock Keeping Unit identifier.
        quantity: Units to order.
        vendor_id: Vendor identifier.
        total_cost: Total cost of the order.
        base_url: Base URL of ERP (defaults to configured MOCK_ERP_URL).

    Returns:
        Dictionary representation of created purchase order record.
    """
    url = (base_url or get_settings().MOCK_ERP_URL).rstrip("/") + "/api/v1/purchase-orders"
    payload = {
        "sku": sku,
        "quantity": quantity,
        "vendor_id": vendor_id,
        "total_cost": total_cost,
    }

    logger.info("Submitting draft purchase order to ERP: %s | payload=%s", url, payload)
    with httpx.Client(timeout=10.0) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()
        logger.info("Successfully created draft PO in ERP: %s", data.get("po_number"))
        return data


def issue_purchase_order(
    po_id: str,
    approver: str,
    base_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Call ERP PATCH /api/v1/purchase-orders/{po_id}/issue to update status to ISSUED.

    Args:
        po_id: Purchase order number (PO-XXXX).
        approver: Approver username or Slack ID.
        base_url: Base URL of ERP (defaults to configured MOCK_ERP_URL).

    Returns:
        Updated purchase order record.
    """
    url = (
        (base_url or get_settings().MOCK_ERP_URL).rstrip("/")
        + f"/api/v1/purchase-orders/{po_id}/issue"
    )
    payload = {"approver": approver}

    logger.info("Issuing purchase order in ERP: %s | approver=%s", url, approver)
    with httpx.Client(timeout=10.0) as client:
        response = client.patch(url, json=payload)
        response.raise_for_status()
        data = response.json()
        logger.info("Successfully issued PO %s with status %s", po_id, data.get("status"))
        return data


def list_purchase_orders(base_url: Optional[str] = None) -> List[Dict[str, Any]]:
    """Retrieve all purchase orders from Mock ERP."""
    url = (base_url or get_settings().MOCK_ERP_URL).rstrip("/") + "/api/v1/purchase-orders"
    with httpx.Client(timeout=10.0) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.json()


def reject_purchase_order(
    po_id: str,
    rejector: str,
    base_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Call ERP PATCH /api/v1/purchase-orders/{po_id}/reject to update status to REJECTED.

    Args:
        po_id: Purchase order number (PO-XXXX).
        rejector: Rejector username or Slack ID.
        base_url: Base URL of ERP (defaults to configured MOCK_ERP_URL).

    Returns:
        Updated purchase order record.
    """
    url = (
        (base_url or get_settings().MOCK_ERP_URL).rstrip("/")
        + f"/api/v1/purchase-orders/{po_id}/reject"
    )
    payload = {"rejector": rejector}

    logger.info("Rejecting purchase order in ERP: %s | rejector=%s", url, rejector)
    with httpx.Client(timeout=10.0) as client:
        response = client.patch(url, json=payload)
        response.raise_for_status()
        data = response.json()
        logger.info("Successfully marked PO %s as REJECTED", po_id)
        return data


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
    Post an activity event to Mock ERP for real-time dashboard display.
    Fails silently so network hiccups do not disrupt the main agent workflow.
    """
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

