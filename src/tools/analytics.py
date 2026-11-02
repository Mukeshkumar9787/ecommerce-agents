"""Inventory and sales velocity analytics tool."""

import logging
from typing import Dict
from langchain_core.tools import tool
from src.schemas.tools import SalesVelocityInput, SalesVelocityResult

logger = logging.getLogger(__name__)

# Mock catalog of SKU historical consumption rates
MOCK_ANALYTICS_DATA: Dict[str, dict] = {
    "SKU-WIDGET-01": {
        "thirty_day_velocity": 150,
        "daily_burn_rate": 5.0,
        "safety_stock_recommendation": 25,
    },
    "SKU-CABLE-02": {
        "thirty_day_velocity": 300,
        "daily_burn_rate": 10.0,
        "safety_stock_recommendation": 50,
    },
    "SKU-SENSOR-03": {
        "thirty_day_velocity": 60,
        "daily_burn_rate": 2.0,
        "safety_stock_recommendation": 15,
    },
    "TSHIRT-BLK-M": {
        "thirty_day_velocity": 150,
        "daily_burn_rate": 5.0,
        "safety_stock_recommendation": 30,
    },
}


def fetch_sales_velocity(sku: str, days: int = 30) -> SalesVelocityResult:
    """
    Deterministic query function to fetch sales velocity and burn rate for a SKU.

    Args:
        sku: Stock Keeping Unit identifier.
        days: Historical window in days (default: 30).

    Returns:
        SalesVelocityResult containing velocity and burn rate metrics.
    """
    logger.info("Fetching sales velocity for SKU=%s over %d days", sku, days)
    data = MOCK_ANALYTICS_DATA.get(
        sku,
        {
            "thirty_day_velocity": 150,
            "daily_burn_rate": 5.0,
            "safety_stock_recommendation": 25,
        },
    )
    return SalesVelocityResult(
        sku=sku,
        thirty_day_velocity=data["thirty_day_velocity"],
        daily_burn_rate=float(data["daily_burn_rate"]),
        safety_stock_recommendation=data["safety_stock_recommendation"],
    )


@tool("get_sales_velocity", args_schema=SalesVelocityInput)
def get_sales_velocity(sku: str, days: int = 30) -> dict:
    """Return historical 30-day velocity, daily burn rate, and safety stock recommendations for a given SKU."""
    result = fetch_sales_velocity(sku=sku, days=days)
    return result.model_dump()


# Alias matching architecture specification
get_store_sales_velocity = get_sales_velocity
