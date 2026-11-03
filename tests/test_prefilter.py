"""Unit tests for deterministic inventory pre-filtering logic."""

from unittest.mock import MagicMock, patch
import pytest
from src.workers.kafka_consumer import is_stock_low, process_inventory_event


class TestInventoryPreFilter:
    """Validate zero-token pre-filter logic for Kafka inventory ingestion."""

    def test_stock_healthy_filter_skips(self):
        """When current_stock > reorder_threshold, pre-filter should return False."""
        current_stock = 150
        reorder_threshold = 50
        assert is_stock_low(current_stock, reorder_threshold) is False

    def test_stock_low_filter_triggers(self):
        """When current_stock < reorder_threshold, pre-filter should return True."""
        current_stock = 20
        reorder_threshold = 50
        assert is_stock_low(current_stock, reorder_threshold) is True

    def test_stock_equal_threshold_triggers(self):
        """Boundary test: when current_stock == reorder_threshold, should trigger."""
        current_stock = 50
        reorder_threshold = 50
        assert is_stock_low(current_stock, reorder_threshold) is True

    def test_process_event_healthy_stock_does_not_invoke_agent(self):
        """Verify that healthy inventory events are skipped and do not invoke the LangGraph agent."""
        mock_app = MagicMock()
        payload = {
            "sku": "SKU-WIDGET-01",
            "current_stock": 200,
            "reorder_threshold": 30,
        }

        result = process_inventory_event(payload=payload, app=mock_app)

        assert result is None
        mock_app.invoke.assert_not_called()

    def test_process_event_low_stock_triggers_agent(self):
        """Verify that low inventory events trigger agent workflow with generated thread ID."""
        mock_app = MagicMock()
        mock_app.invoke.return_value = {
            "status": "AWAITING_APPROVAL",
            "po_number": "PO-TEST999",
        }

        payload = {
            "sku": "SKU-WIDGET-01",
            "current_stock": 10,
            "reorder_threshold": 30,
        }

        result = process_inventory_event(payload=payload, app=mock_app)

        assert result is not None
        assert result["status"] == "AWAITING_APPROVAL"
        assert result["po_number"] == "PO-TEST999"

        # Assert invoke was called
        mock_app.invoke.assert_called_once()
        invoked_state, invoked_config = mock_app.invoke.call_args[0][0], mock_app.invoke.call_args[1]["config"]

        assert invoked_state["sku"] == "SKU-WIDGET-01"
        assert invoked_state["current_stock"] == 10
        assert invoked_state["reorder_threshold"] == 30
        assert invoked_state["status"] == "PENDING_ANALYSIS"
        assert "thread_id" in invoked_config["configurable"]
        assert invoked_config["configurable"]["thread_id"].startswith("reorder-SKU-WIDGET-01-")

    def test_malformed_payload_handled_safely(self):
        """Malformed payload missing required keys should return None without raising."""
        mock_app = MagicMock()
        payload = {"sku": "SKU-INCOMPLETE"}

        result = process_inventory_event(payload=payload, app=mock_app)
        assert result is None
        mock_app.invoke.assert_not_called()

    @patch("src.workers.kafka_consumer.list_purchase_orders")
    def test_duplicate_draft_po_skipped_by_prefilter(self, mock_list_pos):
        """Verify that when a DRAFT PO already exists for a SKU, duplicate PO creation is skipped."""
        mock_list_pos.return_value = [
            {"po_number": "PO-EXISTING-1", "sku": "TSHIRT-BLK-M", "status": "DRAFT"}
        ]
        mock_app = MagicMock()
        payload = {
            "sku": "TSHIRT-BLK-M",
            "current_stock": 2,
            "reorder_threshold": 10,
        }

        result = process_inventory_event(payload=payload, app=mock_app)

        assert result is not None
        assert result["po_number"] == "PO-EXISTING-1"
        # LangGraph invoke should NOT be called because an active draft already exists
        mock_app.invoke.assert_not_called()

    @patch("src.producer.confluent_kafka.Producer")
    def test_produce_inventory_event_when_enabled(self, mock_producer_cls):
        """Verify produce_inventory_event dispatches Kafka message with SKU key when enabled."""
        from src.producer import produce_inventory_event

        mock_producer_inst = MagicMock()
        mock_producer_cls.return_value = mock_producer_inst

        result = produce_inventory_event(
            sku="TSHIRT-BLK-M",
            current_stock=4,
            reorder_threshold=10,
        )

        assert result is not None
        assert result["sku"] == "TSHIRT-BLK-M"
        assert result["current_stock"] == 4
        assert result["reorder_threshold"] == 10
        assert "event_id" in result
        assert "timestamp" in result

        mock_producer_inst.produce.assert_called_once()
        call_kwargs = mock_producer_inst.produce.call_args[1]
        assert call_kwargs["key"] == b"TSHIRT-BLK-M"
        assert b"TSHIRT-BLK-M" in call_kwargs["value"]

    def test_produce_inventory_event_when_disabled(self):
        """Verify produce_inventory_event skips dispatch when INVENTORY_AGENT_ENABLED is False."""
        from src.producer import produce_inventory_event
        from src.config import get_settings

        settings = get_settings()
        original_flag = settings.INVENTORY_AGENT_ENABLED
        try:
            settings.INVENTORY_AGENT_ENABLED = False
            result = produce_inventory_event(
                sku="TSHIRT-BLK-M",
                current_stock=4,
                reorder_threshold=10,
            )
            assert result is None
        finally:
            settings.INVENTORY_AGENT_ENABLED = original_flag
