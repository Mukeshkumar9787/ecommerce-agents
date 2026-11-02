"""Purchase Order Manager Node for draft creation and human-in-the-loop dispatch."""

import logging
from typing import Any, Dict, Optional
from langchain_core.runnables import RunnableConfig
from src.schemas.state import ProcurementState
from src.tools.erp import create_draft_purchase_order
from src.tools.slack import send_slack_approval_card

logger = logging.getLogger(__name__)


def po_manager_node(
    state: ProcurementState,
    config: Optional[RunnableConfig] = None,
) -> Dict[str, Any]:
    """
    PO Manager Agent Node:
    1. Calls ERP API to create a DRAFT Purchase Order.
    2. Calls Slack integration to dispatch Block Kit approval card with encoded thread_id and po_number.
    3. Updates state with generated po_number and sets status to AWAITING_APPROVAL.
    4. Records step in audit_log.
    """
    sku = state.get("sku")
    quantity = state.get("target_reorder_qty", 0)
    vendor_id = state.get("vendor_id", "UNKNOWN_VENDOR")
    total_cost = state.get("total_estimated_cost", 0.0)

    # Extract thread_id from LangGraph execution context
    thread_id = "default-thread"
    if config and "configurable" in config:
        thread_id = config["configurable"].get("thread_id", "default-thread")

    logger.info(
        "PO Manager Node: Creating Draft PO for SKU=%s, Qty=%d, Vendor=%s, TotalCost=%.2f, Thread=%s",
        sku,
        quantity,
        vendor_id,
        total_cost,
        thread_id,
    )

    # 1. Call ERP tool to create draft purchase order
    erp_response = create_draft_purchase_order(
        sku=sku,
        quantity=quantity,
        vendor_id=vendor_id,
        total_cost=total_cost,
    )
    po_number = erp_response.get("po_number")

    # 2. Dispatch Slack interactive Block Kit card
    current_stock = state.get("current_stock")
    slack_response = send_slack_approval_card(
        thread_id=thread_id,
        po_number=po_number,
        sku=sku,
        quantity=quantity,
        vendor_id=vendor_id,
        total_cost=total_cost,
        current_stock=current_stock,
    )

    log_entry = (
        f"PO Manager: Created draft PO '{po_number}' in ERP (Total Cost: ${total_cost:,.2f}). "
        f"Dispatched Slack interactive approval card for thread '{thread_id}'. "
        f"Graph status transitioning to AWAITING_APPROVAL."
    )
    from src.tools.erp import log_agent_activity
    log_agent_activity(
        agent="PO Manager",
        event_type="DRAFT_CREATED",
        title=f"Draft {po_number} Created & Card Dispatched",
        description=log_entry,
        details={
            "po": po_number,
            "sku": sku,
            "qty": quantity,
            "vendor": vendor_id,
            "total_cost": f"${total_cost:,.2f}",
            "thread_id": thread_id,
        },
        sku=sku,
        po_number=po_number,
    )

    return {
        "po_number": po_number,
        "status": "AWAITING_APPROVAL",
        "audit_log": [log_entry],
    }
