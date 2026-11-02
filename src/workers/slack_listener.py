"""Slack Socket Mode listener for Human-in-the-Loop approval and resumption."""

import json
import logging
from typing import Any, Optional
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from src.config import get_settings
from src.graph import create_procurement_app
from src.tools.erp import issue_purchase_order, reject_purchase_order

from urllib.parse import parse_qs
from langgraph.types import Command

logger = logging.getLogger(__name__)


def parse_action_value(raw_val: Any) -> tuple[str, str]:
    """Parse button metadata from either JSON or URL-encoded query string format."""
    if isinstance(raw_val, dict):
        return raw_val.get("thread_id", "default-thread"), raw_val.get("po_number") or raw_val.get("po", "UNKNOWN-PO")
    if isinstance(raw_val, str):
        try:
            data = json.loads(raw_val)
            return data.get("thread_id", "default-thread"), data.get("po_number") or data.get("po", "UNKNOWN-PO")
        except Exception:
            qs = parse_qs(raw_val)
            tid = qs.get("thread_id", ["default-thread"])[0]
            po = qs.get("po", qs.get("po_number", ["UNKNOWN-PO"]))[0]
            return tid, po
    return "default-thread", "UNKNOWN-PO"


def handle_approval_action(
    body: dict,
    ack: Any,
    client: Any,
    respond: Any,
    procurement_app: Optional[Any] = None,
) -> dict:
    """
    Handle 'approve_reorder' interactive button click:
    1. Acknowledge event.
    2. Extract thread_id and po_number from button value.
    3. Call Mock ERP to issue purchase order.
    4. Resume paused LangGraph workflow with approved state.
    5. Update Slack message in-place.
    """
    ack()

    user_info = body.get("user", {})
    user_name = user_info.get("username") or user_info.get("name") or user_info.get("id", "approver")
    user_id = user_info.get("id", "unknown")

    actions = body.get("actions", [])
    if not actions:
        logger.warning("No action found in Slack approval payload")
        return {"ok": False, "error": "No actions in payload"}

    raw_value = actions[0].get("value", "{}")
    thread_id, po_number = parse_action_value(raw_value)

    logger.info(
        "Received approval from @%s for PO=%s on thread=%s",
        user_name,
        po_number,
        thread_id,
    )

    # 1. Update ERP status to ISSUED
    erp_result = {}
    try:
        erp_result = issue_purchase_order(po_id=po_number, approver=user_name)
    except Exception as exc:
        logger.exception("Failed to issue PO %s in ERP: %s", po_number, exc)

    # 2. Resume paused LangGraph state machine
    if procurement_app is None:
        procurement_app = create_procurement_app()

    from src.graph import get_langfuse_callback, flush_langfuse
    lf_cb = get_langfuse_callback()
    callbacks = [lf_cb] if lf_cb else []
    config = {"configurable": {"thread_id": thread_id}, "callbacks": callbacks}

    try:
        procurement_app.update_state(
            config,
            {
                "status": "APPROVED",
                "approval_user": user_name,
                "audit_log": [
                    f"Slack HITL: Approved by @{user_name} (<@{user_id}>). "
                    f"PO '{po_number}' issued in ERP. Workflow resumed and marked APPROVED."
                ],
            },
            as_node="po_manager",
        )
        procurement_app.invoke(None, config=config)
        flush_langfuse()
        logger.info("Successfully resumed and finalized workflow thread '%s'", thread_id)
    except Exception as exc:
        logger.exception("Error resuming LangGraph workflow for thread '%s': %s", thread_id, exc)

    # 3. Retrieve state snapshot for rich confirmation card
    qty_str = ""
    vendor_str = ""
    try:
        cur_state = procurement_app.get_state(config)
        qty = cur_state.values.get("target_reorder_qty")
        vendor = cur_state.values.get("vendor_id")
        if qty:
            qty_str = f" for {qty} units"
        if vendor:
            vendor_str = f" to {vendor}"
    except Exception:
        pass

    approval_text = f"✅ Approved by @{user_name}: Purchase Order {po_number}{qty_str} issued{vendor_str}."
    updated_blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"✅ *Approved by <@{user_id}>* (@{user_name})\n"
                    f"• Purchase Order: *`{po_number}`*\n"
                    f"• ERP Status: *ISSUED*\n"
                    f"• Workflow Thread: *`{thread_id}`*\n"
                    f"Procurement workflow has completed successfully."
                ),
            },
        }
    ]

    try:
        if respond:
            respond(text=approval_text, blocks=updated_blocks, replace_original=True)
        elif client and "channel" in body and "message" in body:
            client.chat_update(
                channel=body["channel"]["id"],
                ts=body["message"]["ts"],
                text=approval_text,
                blocks=updated_blocks,
            )
    except Exception as exc:
        logger.error("Failed to update Slack card in-place: %s", exc)

    return {"ok": True, "po_number": po_number, "status": "APPROVED"}


def handle_rejection_action(
    body: dict,
    ack: Any,
    client: Any,
    respond: Any,
    procurement_app: Optional[Any] = None,
) -> dict:
    """
    Handle 'reject_reorder' interactive button click:
    1. Acknowledge event.
    2. Extract thread_id and po_number.
    3. Update LangGraph workflow state to REJECTED.
    4. Update Slack card in-place to show rejection status.
    """
    ack()

    user_info = body.get("user", {})
    user_name = user_info.get("username") or user_info.get("name") or user_info.get("id", "rejector")
    user_id = user_info.get("id", "unknown")

    actions = body.get("actions", [])
    raw_value = actions[0].get("value", "{}") if actions else "{}"
    thread_id, po_number = parse_action_value(raw_value)

    logger.info("Received rejection from @%s for PO=%s on thread=%s", user_name, po_number, thread_id)

    # 1. Update ERP status to REJECTED
    try:
        reject_purchase_order(po_id=po_number, rejector=user_name)
    except Exception as exc:
        logger.warning("Could not sync rejection to Mock ERP: %s", exc)

    # 2. Resume / update workflow state to REJECTED
    if procurement_app is None:
        procurement_app = create_procurement_app()

    from src.graph import get_langfuse_callback, flush_langfuse
    lf_cb = get_langfuse_callback()
    callbacks = [lf_cb] if lf_cb else []
    config = {"configurable": {"thread_id": thread_id}, "callbacks": callbacks}

    try:
        procurement_app.update_state(
            config,
            {
                "status": "REJECTED",
                "approval_user": user_name,
                "audit_log": [
                    f"Slack HITL: Rejection submitted by @{user_name} (<@{user_id}>). "
                    f"Reorder for PO '{po_number}' was declined. Workflow terminated."
                ],
            },
            as_node="po_manager",
        )
        procurement_app.invoke(None, config=config)
        flush_langfuse()
    except Exception as exc:
        logger.exception("Error recording rejection on LangGraph thread '%s': %s", thread_id, exc)

    # 2. Update Slack card in-place
    rejection_text = f"❌ Rejected by @{user_name}."
    updated_blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"❌ *Rejected by <@{user_id}>* (@{user_name})\n"
                    f"• Purchase Order: *`{po_number}`*\n"
                    f"• Decision: *REJECTED / CANCELLED*\n"
                    f"• Workflow Thread: *`{thread_id}`*\n"
                    f"No order has been issued to the vendor."
                ),
            },
        }
    ]

    try:
        if respond:
            respond(text=rejection_text, blocks=updated_blocks, replace_original=True)
        elif client and "channel" in body and "message" in body:
            client.chat_update(
                channel=body["channel"]["id"],
                ts=body["message"]["ts"],
                text=rejection_text,
                blocks=updated_blocks,
            )
    except Exception as exc:
        logger.error("Failed to update Slack card in-place: %s", exc)

    return {"ok": True, "po_number": po_number, "status": "REJECTED"}


def create_slack_app(procurement_app: Optional[Any] = None) -> App:
    """Initialize Slack Bolt App and register action handlers."""
    settings = get_settings()
    app = App(token=settings.SLACK_BOT_TOKEN or "xoxb-placeholder")

    compiled_graph = procurement_app or create_procurement_app()

    @app.action("approve_reorder")
    def on_approve(body, ack, client, respond):
        handle_approval_action(
            body=body,
            ack=ack,
            client=client,
            respond=respond,
            procurement_app=compiled_graph,
        )

    @app.action("reject_reorder")
    def on_reject(body, ack, client, respond):
        handle_rejection_action(
            body=body,
            ack=ack,
            client=client,
            respond=respond,
            procurement_app=compiled_graph,
        )

    return app


def start_slack_socket_listener():
    """Start Slack Socket Mode event listener process."""
    settings = get_settings()
    if not settings.SLACK_APP_TOKEN or not settings.SLACK_BOT_TOKEN:
        logger.error("SLACK_BOT_TOKEN and SLACK_APP_TOKEN must be configured to run Slack Socket Mode listener.")
        return

    slack_app = create_slack_app()
    handler = SocketModeHandler(slack_app, settings.SLACK_APP_TOKEN)
    logger.info("Starting Slack Bolt Socket Mode Handler...")
    handler.start()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    start_slack_socket_listener()
