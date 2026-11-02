"""Slack interactive Block Kit notification tool."""

import json
import logging
from typing import Any, Dict, Optional
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from src.config import get_settings

logger = logging.getLogger(__name__)


def build_approval_blocks(
    thread_id: str,
    po_number: str,
    sku: str,
    quantity: int,
    vendor_id: str,
    total_cost: float,
    current_stock: Optional[int] = None,
) -> list[dict]:
    """Construct Slack Block Kit interactive message layout with encoded action values."""
    action_value = json.dumps({"thread_id": thread_id, "po_number": po_number})

    fields = [
        {"type": "mrkdwn", "text": f"*SKU:*\n`{sku}`"},
        {"type": "mrkdwn", "text": f"*PO Number:*\n`{po_number}`"},
    ]
    if current_stock is not None:
        fields.append({"type": "mrkdwn", "text": f"*Current Stock:*\n{current_stock} units"})
    fields.extend([
        {"type": "mrkdwn", "text": f"*Suggested Reorder:*\n{quantity} units"},
        {"type": "mrkdwn", "text": f"*Vendor ID:*\n{vendor_id}"},
        {"type": "mrkdwn", "text": f"*Total Cost:*\n${total_cost:,.2f}"},
        {"type": "mrkdwn", "text": f"*Workflow Thread:*\n`{thread_id}`"},
    ])

    return [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "📦 Reorder Approval Required",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": fields,
        },
        {"type": "divider"},
        {
            "type": "actions",
            "block_id": "procurement_approval_actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Approve Reorder",
                        "emoji": True,
                    },
                    "style": "primary",
                    "action_id": "approve_reorder",
                    "value": action_value,
                },
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Reject Reorder",
                        "emoji": True,
                    },
                    "style": "danger",
                    "action_id": "reject_reorder",
                    "value": action_value,
                },
            ],
        },
    ]


def send_slack_approval_card(
    thread_id: str,
    po_number: str,
    sku: str,
    quantity: int,
    vendor_id: str,
    total_cost: float,
    current_stock: Optional[int] = None,
    channel_id: Optional[str] = None,
    bot_token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Send a Slack Block Kit interactive card to the designated procurement channel.

    Encodes thread_id and po_number in the button value for callback handling.

    Args:
        thread_id: Unique LangGraph workflow execution ID.
        po_number: Generated ERP Purchase Order number.
        sku: Stock Keeping Unit identifier.
        quantity: Calculated reorder quantity.
        vendor_id: Vendor identifier.
        total_cost: Total estimated purchase cost.
        current_stock: Current warehouse inventory level.
        channel_id: Optional channel override.
        bot_token: Optional Slack bot token override.

    Returns:
        Slack API response dictionary or simulated response in test mode.
    """
    settings = get_settings()
    token = bot_token or settings.SLACK_BOT_TOKEN
    channel = channel_id or settings.SLACK_CHANNEL_ID

    blocks = build_approval_blocks(
        thread_id=thread_id,
        po_number=po_number,
        sku=sku,
        quantity=quantity,
        vendor_id=vendor_id,
        total_cost=total_cost,
        current_stock=current_stock,
    )

    if not token or token.startswith("xoxb-placeholder") or not channel:
        logger.warning(
            "SLACK_BOT_TOKEN or SLACK_CHANNEL_ID not configured. Simulating Slack card dispatch for thread_id=%s, po=%s",
            thread_id,
            po_number,
        )
        return {
            "ok": True,
            "ts": "mock.ts.123456.789",
            "channel": channel or "mock-channel",
            "blocks": blocks,
            "mocked": True,
        }

    try:
        client = WebClient(token=token)
        response = client.chat_postMessage(
            channel=channel,
            text=f"Reorder Approval Required: {sku} ({quantity} units)",
            blocks=blocks,
        )
        logger.info(
            "Dispatched Slack approval card to channel %s (ts=%s)",
            channel,
            response.get("ts"),
        )
        return response.data
    except SlackApiError as err:
        logger.error("Failed to dispatch Slack card: %s", err.response.get("error"))
        raise


def send_slack_po_approval_notification(
    po_number: str,
    sku: str,
    product_name: str,
    quantity: int,
    total_cost: float,
    approver: str,
    vendor_name: Optional[str] = None,
    channel_id: Optional[str] = None,
    bot_token: Optional[str] = None,
) -> Dict[str, Any]:
    """Send an official notification to Slack channel when a purchase order is approved and issued."""
    settings = get_settings()
    token = bot_token or settings.SLACK_BOT_TOKEN
    channel = channel_id or settings.SLACK_CHANNEL_ID

    title = product_name or sku
    vendor = vendor_name or "Assigned Supplier"
    text_summary = f"✅ Purchase Order {po_number} APPROVED by @{approver}: {quantity} units of '{title}' (${total_cost:,.2f}) issued to {vendor}."

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "✅ Purchase Order Approved & Issued",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*PO Number:*\n`{po_number}`"},
                {"type": "mrkdwn", "text": f"*Status:*\n*ISSUED*"},
                {"type": "mrkdwn", "text": f"*Product Item:*\n*{title}* (`{sku}`)"},
                {"type": "mrkdwn", "text": f"*Order Quantity:*\n{quantity:,} units"},
                {"type": "mrkdwn", "text": f"*Total Cost:*\n${total_cost:,.2f}"},
                {"type": "mrkdwn", "text": f"*Supplier:*\n{vendor}"},
                {"type": "mrkdwn", "text": f"*Approved By:*\n@{approver}"},
            ],
        },
    ]

    if not token or token.startswith("xoxb-placeholder") or not channel:
        logger.warning("Slack token or channel missing. Simulating approval broadcast for %s", po_number)
        return {"ok": True, "ts": "mock.ts.approval", "channel": channel or "mock-channel", "mocked": True}

    try:
        client = WebClient(token=token)
        response = client.chat_postMessage(channel=channel, text=text_summary, blocks=blocks)
        logger.info("Sent PO approval notification to Slack channel %s for %s", channel, po_number)
        return response.data
    except Exception as exc:
        logger.error("Failed sending Slack approval notification: %s", exc)
        return {"ok": False, "error": str(exc)}


def send_slack_po_rejection_notification(
    po_number: str,
    sku: str,
    product_name: str,
    rejector: str,
    quantity: Optional[int] = None,
    vendor_name: Optional[str] = None,
    channel_id: Optional[str] = None,
    bot_token: Optional[str] = None,
) -> Dict[str, Any]:
    """Send an official notification to Slack channel when a purchase order is declined/rejected."""
    settings = get_settings()
    token = bot_token or settings.SLACK_BOT_TOKEN
    channel = channel_id or settings.SLACK_CHANNEL_ID

    title = product_name or sku
    text_summary = f"❌ Purchase Order {po_number} REJECTED by @{rejector} for item '{title}'."

    fields = [
        {"type": "mrkdwn", "text": f"*PO Number:*\n`{po_number}`"},
        {"type": "mrkdwn", "text": f"*Status:*\n*REJECTED*"},
        {"type": "mrkdwn", "text": f"*Product Item:*\n*{title}* (`{sku}`)"},
        {"type": "mrkdwn", "text": f"*Rejected By:*\n@{rejector}"},
    ]
    if quantity:
        fields.append({"type": "mrkdwn", "text": f"*Proposed Qty:*\n{quantity:,} units"})
    if vendor_name:
        fields.append({"type": "mrkdwn", "text": f"*Supplier:*\n{vendor_name}"})

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "❌ Purchase Order Declined",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": fields,
        },
    ]

    if not token or token.startswith("xoxb-placeholder") or not channel:
        logger.warning("Slack token or channel missing. Simulating rejection broadcast for %s", po_number)
        return {"ok": True, "ts": "mock.ts.rejection", "channel": channel or "mock-channel", "mocked": True}

    try:
        client = WebClient(token=token)
        response = client.chat_postMessage(channel=channel, text=text_summary, blocks=blocks)
        logger.info("Sent PO rejection notification to Slack channel %s for %s", channel, po_number)
        return response.data
    except Exception as exc:
        logger.error("Failed sending Slack rejection notification: %s", exc)
        return {"ok": False, "error": str(exc)}


# Alias matching architecture specification
dispatch_slack_approval = send_slack_approval_card
