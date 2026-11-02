"""Integration and flow tests for LangGraph state machine and HITL resumption."""

import json
from unittest.mock import MagicMock, patch
import pytest
from fastapi.testclient import TestClient
from langgraph.checkpoint.memory import MemorySaver

from mock_erp.main import app as erp_fastapi_app
from src.agents.inventory_analyst import calculate_reorder_quantity
from src.graph import create_procurement_app
from src.workers.slack_listener import handle_approval_action, handle_rejection_action


class TestReorderCalculation:
    """Validate mathematical formula for inventory replenishment."""

    def test_reorder_quantity_formula(self):
        """
        Formula: max(MOQ, ceil((Burn Rate * Lead Time) + (Burn Rate * 30) - Current Stock))
        Burn Rate = 5.0, Lead Time = 7, Current Stock = 10, MOQ = 50:
        Lead Demand = 35
        30-day Demand = 150
        Projected Demand = 185
        Net Shortage = 185 - 10 = 175
        max(50, 175) -> 175
        """
        qty = calculate_reorder_quantity(
            current_stock=10,
            burn_rate=5.0,
            lead_time=7,
            moq=50,
        )
        assert qty == 175

    def test_reorder_quantity_respects_moq(self):
        """When calculated shortage is lower than MOQ, MOQ should be selected."""
        qty = calculate_reorder_quantity(
            current_stock=180,
            burn_rate=5.0,
            lead_time=7,
            moq=50,
        )
        # Net shortage = 185 - 180 = 5. MOQ = 50 -> 50
        assert qty == 50


class TestMockERPAPI:
    """Test Mock ERP endpoints directly using FastAPI TestClient."""

    @pytest.fixture
    def client(self):
        return TestClient(erp_fastapi_app)

    def test_create_and_issue_purchase_order(self, client):
        # 1. Create Draft PO
        create_resp = client.post(
            "/api/v1/purchase-orders",
            json={
                "sku": "SKU-WIDGET-01",
                "quantity": 100,
                "vendor_id": "VEND-ACME-001",
                "total_cost": 1250.00,
            },
        )
        assert create_resp.status_code == 201
        data = create_resp.json()
        po_number = data["po_number"]
        assert po_number.startswith("PO-")
        assert data["status"] == "DRAFT"
        assert data["quantity"] == 100

        # 2. Issue PO
        issue_resp = client.patch(
            f"/api/v1/purchase-orders/{po_number}/issue",
            json={"approver": "test_director"},
        )
        assert issue_resp.status_code == 200
        issued_data = issue_resp.json()
        assert issued_data["status"] == "ISSUED"
        assert issued_data["approver"] == "test_director"

        # 3. List POs
        list_resp = client.get("/api/v1/purchase-orders")
        assert list_resp.status_code == 200
        orders = list_resp.json()
        assert any(order["po_number"] == po_number for order in orders)


class TestGraphWorkflowAndHITL:
    """Validate full LangGraph state machine execution, interruption, and HITL resumption."""

    @patch("src.agents.po_manager.create_draft_purchase_order")
    @patch("src.agents.po_manager.send_slack_approval_card")
    def test_graph_flow_to_approval_breakpoint(self, mock_slack, mock_erp):
        """
        Run state machine from START:
        - supervisor evaluates PENDING_ANALYSIS
        - inventory_analyst computes target_reorder_qty and costs
        - po_manager calls ERP & Slack, sets status to AWAITING_APPROVAL
        - Graph halts after po_manager via interrupt_after
        """
        mock_erp.return_value = {"po_number": "PO-9876", "status": "DRAFT"}
        mock_slack.return_value = {"ok": True, "ts": "1234.5678"}

        saver = MemorySaver()
        app = create_procurement_app(checkpointer=saver, interrupt_after=["po_manager"])

        thread_id = "test-thread-flow-001"
        config = {"configurable": {"thread_id": thread_id}}

        initial_state = {
            "sku": "SKU-WIDGET-01",
            "current_stock": 10,
            "reorder_threshold": 25,
            "status": "PENDING_ANALYSIS",
            "audit_log": ["Event received"],
        }

        # Execute to breakpoint
        result = app.invoke(initial_state, config=config)

        # Assert state after interrupt
        assert result["status"] == "AWAITING_APPROVAL"
        assert result["po_number"] == "PO-9876"
        assert result["target_reorder_qty"] == 175
        assert result["vendor_id"] == "VEND-ACME-001"
        assert result["unit_cost"] == 12.50
        assert result["total_estimated_cost"] == 2187.50

        # Verify audit log captures both nodes
        audit_text = " ".join(result["audit_log"])
        assert "Inventory Analyst:" in audit_text
        assert "PO Manager:" in audit_text

        # Verify Mock ERP and Slack were called
        mock_erp.assert_called_once_with(
            sku="SKU-WIDGET-01",
            quantity=175,
            vendor_id="VEND-ACME-001",
            total_cost=2187.50,
        )
        mock_slack.assert_called_once()

    @patch("src.workers.slack_listener.issue_purchase_order")
    @patch("src.agents.po_manager.create_draft_purchase_order")
    @patch("src.agents.po_manager.send_slack_approval_card")
    def test_hitl_approval_resumption(self, mock_slack, mock_erp_create, mock_erp_issue):
        """Verify that Slack approval action resumes the graph and completes with APPROVED."""
        mock_erp_create.return_value = {"po_number": "PO-AUTO-11", "status": "DRAFT"}
        mock_slack.return_value = {"ok": True}
        mock_erp_issue.return_value = {"po_number": "PO-AUTO-11", "status": "ISSUED"}

        saver = MemorySaver()
        app = create_procurement_app(checkpointer=saver, interrupt_after=["po_manager"])

        thread_id = "test-thread-approval-002"
        config = {"configurable": {"thread_id": thread_id}}

        initial_state = {
            "sku": "SKU-WIDGET-01",
            "current_stock": 5,
            "reorder_threshold": 20,
            "status": "PENDING_ANALYSIS",
            "audit_log": ["Triggered"],
        }

        # 1. Run until approval interrupt
        app.invoke(initial_state, config=config)
        state_at_interrupt = app.get_state(config)
        assert state_at_interrupt.values["status"] == "AWAITING_APPROVAL"

        # 2. Simulate Slack approval button payload
        mock_ack = MagicMock()
        mock_respond = MagicMock()
        body = {
            "user": {"id": "U9876", "username": "sarah_procurement"},
            "actions": [
                {
                    "value": json.dumps({"thread_id": thread_id, "po_number": "PO-AUTO-11"}),
                }
            ],
        }

        # 3. Invoke Slack listener handler
        handle_approval_action(
            body=body,
            ack=mock_ack,
            client=MagicMock(),
            respond=mock_respond,
            procurement_app=app,
        )

        # 4. Verify ERP update & Slack card update
        mock_ack.assert_called_once()
        mock_erp_issue.assert_called_once_with(po_id="PO-AUTO-11", approver="sarah_procurement")
        mock_respond.assert_called_once()
        assert "Approved by @sarah_procurement" in mock_respond.call_args[1]["text"]

        # 5. Check resumed LangGraph state
        final_state = app.get_state(config)
        assert final_state.values["status"] == "APPROVED"
        assert final_state.values["approval_user"] == "sarah_procurement"

    @patch("src.agents.po_manager.create_draft_purchase_order")
    @patch("src.agents.po_manager.send_slack_approval_card")
    def test_hitl_rejection_resumption(self, mock_slack, mock_erp_create):
        """Verify that Slack rejection marks the workflow REJECTED and halts."""
        mock_erp_create.return_value = {"po_number": "PO-REJ-22", "status": "DRAFT"}
        mock_slack.return_value = {"ok": True}

        saver = MemorySaver()
        app = create_procurement_app(checkpointer=saver, interrupt_after=["po_manager"])

        thread_id = "test-thread-rejection-003"
        config = {"configurable": {"thread_id": thread_id}}

        initial_state = {
            "sku": "SKU-WIDGET-01",
            "current_stock": 5,
            "reorder_threshold": 20,
            "status": "PENDING_ANALYSIS",
            "audit_log": ["Triggered"],
        }

        app.invoke(initial_state, config=config)

        mock_ack = MagicMock()
        mock_respond = MagicMock()
        body = {
            "user": {"id": "U1234", "username": "john_cfo"},
            "actions": [
                {
                    "value": json.dumps({"thread_id": thread_id, "po_number": "PO-REJ-22"}),
                }
            ],
        }

        handle_rejection_action(
            body=body,
            ack=mock_ack,
            client=MagicMock(),
            respond=mock_respond,
            procurement_app=app,
        )

        mock_ack.assert_called_once()
        mock_respond.assert_called_once()
        assert "Rejected by @john_cfo" in mock_respond.call_args[1]["text"]

        final_state = app.get_state(config)
        assert final_state.values["status"] == "REJECTED"
        assert final_state.values["approval_user"] == "john_cfo"

    @patch("src.workers.slack_listener.issue_purchase_order")
    @patch("src.agents.po_manager.create_draft_purchase_order")
    @patch("src.agents.po_manager.send_slack_approval_card")
    def test_end_to_end_tshirt_trace(self, mock_slack, mock_erp_create, mock_erp_issue):
        """
        Verify the complete step-by-step trace from user specification:
        SKU: TSHIRT-BLK-M, stock 4, threshold 10
        Apex Textiles (VEND-102), MOQ 100, price $8.50, lead time 6d
        Calculated qty = 176, total cost = $1,496.00
        PO Number = PO-9142
        Slack approval by @mukesh -> issued in ERP.
        """
        mock_erp_create.return_value = {"po_number": "PO-9142", "status": "DRAFT"}
        mock_slack.return_value = {"ok": True}
        mock_erp_issue.return_value = {"po_number": "PO-9142", "status": "ISSUED"}

        saver = MemorySaver()
        app = create_procurement_app(checkpointer=saver, interrupt_after=["po_manager"])

        thread_id = "reorder-TSHIRT-BLK-M-1730538000"
        config = {"configurable": {"thread_id": thread_id}}

        # Step 1 & 2: Kafka event ingestion
        initial_state = {
            "sku": "TSHIRT-BLK-M",
            "current_stock": 4,
            "reorder_threshold": 10,
            "status": "PENDING_ANALYSIS",
            "audit_log": ["Event received for TSHIRT-BLK-M"],
        }

        # Step 3, 4, 5, 6: Supervisor -> Inventory Analyst -> PO Manager -> Checkpoint Breakpoint
        result = app.invoke(initial_state, config=config)

        # Assert Step 4 & 5 calculations
        assert result["daily_burn_rate"] == 5.0
        assert result["target_reorder_qty"] == 176
        assert result["vendor_id"] == "VEND-102"
        assert result["unit_cost"] == 8.50
        assert result["total_estimated_cost"] == 1496.00
        assert result["po_number"] == "PO-9142"
        assert result["status"] == "AWAITING_APPROVAL"

        # Assert ERP & Slack calls
        mock_erp_create.assert_called_once_with(
            sku="TSHIRT-BLK-M",
            quantity=176,
            vendor_id="VEND-102",
            total_cost=1496.00,
        )
        mock_slack.assert_called_once()
        slack_kwargs = mock_slack.call_args[1]
        assert slack_kwargs["sku"] == "TSHIRT-BLK-M"
        assert slack_kwargs["quantity"] == 176
        assert slack_kwargs["current_stock"] == 4

        # Step 7 & 8: Slack interactive approval with query-string format button value
        mock_ack = MagicMock()
        mock_respond = MagicMock()
        button_query_val = "thread_id=reorder-TSHIRT-BLK-M-1730538000&po=PO-9142"

        approval_body = {
            "user": {"id": "U_MUKESH", "username": "mukesh"},
            "actions": [{"value": button_query_val}],
        }

        handle_approval_action(
            body=approval_body,
            ack=mock_ack,
            client=MagicMock(),
            respond=mock_respond,
            procurement_app=app,
        )

        # Verify acknowledgment and ERP issue
        mock_ack.assert_called_once()
        mock_erp_issue.assert_called_once_with(po_id="PO-9142", approver="mukesh")

        # Verify Slack card updated in-place with exact format
        mock_respond.assert_called_once()
        resp_call = mock_respond.call_args
        assert "✅ Approved by @mukesh: Purchase Order PO-9142 for 176 units issued to VEND-102." in resp_call[1]["text"]
        assert resp_call[1]["replace_original"] is True

        # Verify finalized state in checkpointer
        final_state = app.get_state(config)
        assert final_state.values["status"] == "APPROVED"
        assert final_state.values["approval_user"] == "mukesh"

