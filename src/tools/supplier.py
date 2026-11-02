"""Supplier metadata and vendor catalog tool."""

import logging
from typing import Dict
from langchain_core.tools import tool
from src.schemas.tools import SupplierInfoInput, SupplierInfoResult

logger = logging.getLogger(__name__)

# Catalog of supplier agreements
MOCK_SUPPLIER_CATALOG: Dict[str, dict] = {
    "SKU-WIDGET-01": {
        "vendor_id": "VEND-ACME-001",
        "vendor_name": "Acme Industrial Supplies",
        "moq": 50,
        "unit_price": 12.50,
        "lead_time_days": 7,
    },
    "SKU-CABLE-02": {
        "vendor_id": "VEND-NEXUS-002",
        "vendor_name": "Nexus Wire & Harness",
        "moq": 100,
        "unit_price": 4.25,
        "lead_time_days": 5,
    },
    "SKU-SENSOR-03": {
        "vendor_id": "VEND-OPTIC-003",
        "vendor_name": "OpticSensors Corp",
        "moq": 25,
        "unit_price": 48.00,
        "lead_time_days": 14,
    },
    "TSHIRT-BLK-M": {
        "vendor_id": "VEND-102",
        "vendor_name": "Apex Textiles",
        "moq": 100,
        "unit_price": 8.50,
        "lead_time_days": 6,
    },
}


def fetch_supplier_info(sku: str) -> SupplierInfoResult:
    """
    Deterministic query to retrieve supplier contractual terms for a SKU.

    Args:
        sku: Stock Keeping Unit identifier.

    Returns:
        SupplierInfoResult with vendor ID, MOQ, unit price, and lead time.
    """
    logger.info("Fetching supplier metadata for SKU/Slug=%s", sku)
    if sku in MOCK_SUPPLIER_CATALOG:
        data = MOCK_SUPPLIER_CATALOG[sku]
    else:
        # Check live SMK store backend by slug
        try:
            from src.tools.ecommerce import fetch_product_by_slug
            live_prod = fetch_product_by_slug(sku)
        except Exception:
            live_prod = None

        if live_prod:
            retail = float(live_prod.get("price") or live_prod.get("mrp") or 30.0)
            data = {
                "vendor_id": "VEND-APEX-102",
                "vendor_name": "Apex Direct Global",
                "moq": 30,
                "unit_price": max(5.0, round(retail * 0.45, 2)),
                "lead_time_days": 5,
            }
        else:
            data = {
                "vendor_id": "VEND-GLOBAL-DEFAULT",
                "vendor_name": "Global Direct Components",
                "moq": 50,
                "unit_price": 15.00,
                "lead_time_days": 7,
            }
    return SupplierInfoResult(
        sku=sku,
        vendor_id=data["vendor_id"],
        vendor_name=data["vendor_name"],
        moq=data["moq"],
        unit_price=float(data["unit_price"]),
        lead_time_days=data["lead_time_days"],
    )


@tool("get_supplier_info", args_schema=SupplierInfoInput)
def get_supplier_info(sku: str) -> dict:
    """Return supplier metadata for SKUs: preferred vendor, minimum order quantity (MOQ), unit price, and lead time in days."""
    result = fetch_supplier_info(sku=sku)
    return result.model_dump()


# Alias matching architecture specification
get_supplier_catalog = get_supplier_info
