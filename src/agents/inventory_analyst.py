"""Inventory Analyst Node for reorder computation and replenishment forecasting."""

import logging
import math
from typing import Any, Dict, Optional
from langchain_core.runnables import RunnableConfig
from src.schemas.state import ProcurementState
from src.tools.analytics import fetch_sales_velocity
from src.tools.supplier import fetch_supplier_info
from src.llm import get_gemini_llm

logger = logging.getLogger(__name__)


def calculate_reorder_quantity(
    current_stock: int,
    burn_rate: float,
    lead_time: int,
    moq: int,
) -> int:
    """
    Compute target reorder quantity according to the specification:
    max(MOQ, ceil((Burn Rate * Lead Time) + (Burn Rate * 30) - Current Stock))
    """
    safety_stock_lead_demand = burn_rate * lead_time
    thirty_day_demand = burn_rate * 30
    projected_net_shortage = (safety_stock_lead_demand + thirty_day_demand) - current_stock
    calculated_qty = math.ceil(projected_net_shortage)
    final_reorder_qty = max(moq, calculated_qty)
    return final_reorder_qty


def inventory_analyst_node(
    state: ProcurementState,
    config: Optional[RunnableConfig] = None,
) -> Dict[str, Any]:
    """
    Inventory Analyst Agent Node:
    1. Pulls sales velocity and vendor terms.
    2. Computes reorder quantity using standard formula.
    3. Calculates total estimated cost.
    4. Appends reasoning to audit_log and updates state.
    """
    sku = state.get("sku")
    current_stock = state.get("current_stock", 0)
    reorder_threshold = state.get("reorder_threshold", 0)

    logger.info("Executing Inventory Analyst node for SKU=%s, CurrentStock=%d", sku, current_stock)

    # 1. Fetch analytics & supplier metadata
    velocity_data = fetch_sales_velocity(sku=sku, days=30)
    supplier_data = fetch_supplier_info(sku=sku)

    burn_rate = velocity_data.daily_burn_rate
    lead_time = supplier_data.lead_time_days
    moq = supplier_data.moq
    unit_cost = supplier_data.unit_price
    vendor_id = supplier_data.vendor_id

    # 2. Compute reorder quantity
    reorder_qty = calculate_reorder_quantity(
        current_stock=current_stock,
        burn_rate=burn_rate,
        lead_time=lead_time,
        moq=moq,
    )
    total_cost = round(reorder_qty * unit_cost, 2)

    # Optional Gemini LLM narrative synthesis
    llm = get_gemini_llm()
    gemini_note = ""
    if llm:
        try:
            prompt = (
                f"You are an AI Inventory Analyst. In 1 sentence, summarize replenishment for SKU '{sku}': "
                f"current stock {current_stock}, burn rate {burn_rate:.1f}/day, lead time {lead_time}d, MOQ {moq}, "
                f"recommended reorder {reorder_qty} units (total estimated cost: ${total_cost:,.2f})."
            )
            llm_response = llm.invoke(prompt)
            gemini_note = f" [Gemini AI Analysis: {llm_response.content.strip()}]"
        except Exception as err:
            logger.debug("Gemini synthesis skipped: %s", err)

    reasoning = (
        f"Inventory Analyst: Evaluated SKU={sku}. Current stock ({current_stock}) <= threshold ({reorder_threshold}). "
        f"Daily burn rate={burn_rate:.1f} units/day, Vendor={vendor_id} (LeadTime={lead_time}d, MOQ={moq}, UnitCost=${unit_cost:.2f}). "
        f"Computed reorder quantity={reorder_qty} units. Total estimated cost=${total_cost:,.2f}.{gemini_note}"
    )
    from src.tools.erp import log_agent_activity
    log_agent_activity(
        agent="Inventory Analyst",
        event_type="ANALYSIS_COMPLETED",
        title=f"Replenishment Computed: {reorder_qty} units",
        description=reasoning,
        details={
            "sku": sku,
            "burn_rate": f"{burn_rate:.1f}/d",
            "lead_time": f"{lead_time}d",
            "moq": moq,
            "target_qty": reorder_qty,
            "total_cost": f"${total_cost:,.2f}",
        },
        sku=sku,
    )

    return {
        "daily_burn_rate": burn_rate,
        "target_reorder_qty": reorder_qty,
        "vendor_id": vendor_id,
        "unit_cost": unit_cost,
        "total_estimated_cost": total_cost,
        "status": "ANALYSIS_COMPLETE",
        "audit_log": [reasoning],
    }
