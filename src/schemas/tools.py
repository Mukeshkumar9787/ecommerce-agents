"""Pydantic schemas and input models for multi-agent tools."""

from pydantic import BaseModel, Field


class SalesVelocityInput(BaseModel):
    """Input model for querying SKU sales velocity."""

    sku: str = Field(..., description="Stock Keeping Unit identifier")
    days: int = Field(default=30, ge=1, description="Historical lookback window in days")


class SalesVelocityResult(BaseModel):
    """Result model containing historical sales analytics."""

    sku: str
    thirty_day_velocity: int
    daily_burn_rate: float
    safety_stock_recommendation: int


class SupplierInfoInput(BaseModel):
    """Input model for querying supplier terms for a SKU."""

    sku: str = Field(..., description="Stock Keeping Unit identifier")


class SupplierInfoResult(BaseModel):
    """Result model containing vendor catalog terms."""

    sku: str
    vendor_id: str
    vendor_name: str
    moq: int = Field(..., description="Minimum Order Quantity")
    unit_price: float
    lead_time_days: int


class CreateDraftPOInput(BaseModel):
    """Input model for creating a draft purchase order in ERP."""

    sku: str = Field(..., description="Stock Keeping Unit identifier")
    quantity: int = Field(..., gt=0, description="Quantity to purchase")
    vendor_id: str = Field(..., description="Vendor/Supplier identifier")
    total_cost: float = Field(..., gt=0, description="Total estimated cost")


class IssuePOInput(BaseModel):
    """Input model for issuing an approved purchase order in ERP."""

    po_id: str = Field(..., description="Purchase order number/ID (PO-XXXX)")
    approver: str = Field(..., description="User identifier who authorized issuance")


class SlackApprovalCardInput(BaseModel):
    """Input model for dispatching a Slack Block Kit interactive card."""

    channel_id: str = Field(..., description="Target Slack channel")
    thread_id: str = Field(..., description="Workflow execution thread identifier")
    po_number: str = Field(..., description="Purchase Order number")
    sku: str = Field(..., description="Stock Keeping Unit identifier")
    quantity: int = Field(..., description="Order quantity")
    vendor_id: str = Field(..., description="Vendor identifier")
    total_cost: float = Field(..., description="Total cost formatted")


class SlackButtonActionValue(BaseModel):
    """Payload serialized into Slack approval/rejection button values."""

    thread_id: str
    po_number: str
