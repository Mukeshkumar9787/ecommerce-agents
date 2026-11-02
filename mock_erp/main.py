"""Backward-compatible proxy exporting agent server, traceability, and embedded ERP.

Real-time agent updates and mock ERP have been unified directly inside the agent
package at `src.server.app`. This file maintains backward compatibility for any
legacy imports.
"""

from src.server.app import app
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
    send_slack_channel_message,
)
from src.server.traceability import (
    ActivityLogRequest,
    activities_db,
    broadcast_event,
    record_activity,
    subscribers,
)

__all__ = [
    "app",
    "purchase_orders_db",
    "activities_db",
    "subscribers",
    "broadcast_event",
    "record_activity",
    "create_po_record",
    "issue_po_record",
    "reject_po_record",
    "resolve_product_info",
    "send_slack_channel_message",
    "CreatePurchaseOrderRequest",
    "IssuePurchaseOrderRequest",
    "RejectPurchaseOrderRequest",
    "PurchaseOrderResponse",
    "ActivityLogRequest",
    "STORE_CATALOG",
]
