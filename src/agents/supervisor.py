"""Supervisor Node directing multi-agent routing and state workflow."""

import logging
from typing import Any, Dict, Optional
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END
from src.schemas.state import ProcurementState
from src.tools.erp import log_agent_activity

logger = logging.getLogger(__name__)


def supervisor_node(
    state: ProcurementState,
    config: Optional[RunnableConfig] = None,
) -> Dict[str, Any]:
    """
    Supervisor Agent Node:
    Monitors high-level state, enforces policy, and records orchestration logs.
    """
    status = state.get("status", "PENDING_ANALYSIS")
    sku = state.get("sku", "UNKNOWN")

    logger.info("Supervisor coordinating workflow for SKU=%s | Current Status=%s", sku, status)
    audit_entry = f"Supervisor: Coordinating workflow step. Current state status is '{status}'."

    next_agent = "inventory_analyst" if status == "PENDING_ANALYSIS" else ("po_manager" if status in ("DRAFT_NEEDED", "DRAFT_CREATED", "ANALYSIS_COMPLETE") else "END")
    log_agent_activity(
        agent="Supervisor Agent",
        event_type="STATE_ROUTED",
        title=f"Supervisor: Coordinating {sku} ({status})",
        description=f"Evaluating state '{status}'. Delegating task execution to {next_agent}.",
        details={"status": status, "delegated_agent": next_agent},
        sku=sku,
    )

    return {
        "audit_log": [audit_entry],
    }


def supervisor_router(state: ProcurementState) -> str:
    """
    Conditional edge routing logic:
    - If status is PENDING_ANALYSIS -> route to 'inventory_analyst'
    - If status is DRAFT_NEEDED -> route to 'po_manager'
    - If status is AWAITING_APPROVAL, APPROVED, or REJECTED -> route to END
    """
    status = state.get("status", "PENDING_ANALYSIS")

    if status == "PENDING_ANALYSIS":
        logger.debug("Supervisor routing to inventory_analyst")
        return "inventory_analyst"
    elif status in ("DRAFT_NEEDED", "DRAFT_CREATED", "ANALYSIS_COMPLETE"):
        logger.debug("Supervisor routing to po_manager")
        return "po_manager"
    elif status in ("AWAITING_APPROVAL", "APPROVED", "REJECTED"):
        logger.debug("Supervisor routing to END (Workflow paused or complete)")
        return END

    logger.warning("Unknown status '%s' encountered in supervisor; routing to END", status)
    return END
