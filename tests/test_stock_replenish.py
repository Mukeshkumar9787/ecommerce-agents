import pytest
from unittest.mock import MagicMock, patch
from src.agents.inventory_analyst import calculate_reorder_quantity
from src.server.erp import (
    CreatePurchaseOrderRequest,
    create_po_record_sync,
    issue_po_record_sync,
    purchase_orders_db,
)
from src.tools.ecommerce import restock_ecommerce_product
from src.tools.slack import build_approval_blocks


def test_dynamic_reorder_quantity_varies_within_20():
    """Verify suggested quantity varies based on stock deficit, velocity, and lead time, and stays strictly < 20."""
    # Case 1: Low stock (2 units), high burn rate (1.8/day), 4d lead time -> high urgency
    q1 = calculate_reorder_quantity(
        current_stock=2,
        burn_rate=1.8,
        lead_time=4,
        moq=10,
        reorder_threshold=10,
        max_quantity=19,
    )
    # Deficit = 10 - 2 = 8, Lead demand = ceil(1.8 * 4) = 8, Buffer = ceil(1.8 * 5) = 9 -> Raw = 25 -> Capped at 19
    assert q1 == 19
    assert q1 < 20

    # Case 2: Moderate stock (6 units), moderate burn rate (1.2/day), 4d lead time -> moderate urgency
    q2 = calculate_reorder_quantity(
        current_stock=6,
        burn_rate=1.2,
        lead_time=4,
        moq=10,
        reorder_threshold=10,
        max_quantity=19,
    )
    # Deficit = 10 - 6 = 4, Lead demand = ceil(1.2 * 4) = 5, Buffer = ceil(1.2 * 5) = 6 -> Raw = 15
    assert q2 == 15
    assert q2 < 20

    # Case 3: Near-threshold stock (9 units), lower burn rate (0.8/day), 3d lead time -> low urgency
    q3 = calculate_reorder_quantity(
        current_stock=9,
        burn_rate=0.8,
        lead_time=3,
        moq=5,
        reorder_threshold=10,
        max_quantity=19,
    )
    # Deficit = 10 - 9 = 1, Lead demand = ceil(0.8 * 3) = 3, Buffer = ceil(0.8 * 5) = 4 -> Raw = 8
    assert q3 == 8
    assert q3 < 20

    # Verify they vary based on the metrics
    assert q1 > q2 > q3


def test_po_slack_and_store_stock_quantity_parity():
    """Verify the exact suggested quantity in PO request matches the Slack card and the quantity restocked."""
    suggested_qty = calculate_reorder_quantity(
        current_stock=5,
        burn_rate=1.4,
        lead_time=4,
        moq=10,
        reorder_threshold=10,
        max_quantity=19,
    )
    # Deficit: 5, Lead: 6, Buffer: 7 -> 18
    assert suggested_qty == 18
    assert suggested_qty < 20

    # 1. Slack approval card displays the exact suggested quantity
    blocks = build_approval_blocks(
        thread_id="test-thread-1",
        po_number="PO-PARITY-01",
        sku="Polarized-Vintage-Metal-Frame-Aviator-Sunglasses",
        quantity=suggested_qty,
        vendor_id="VEND-APEX-102",
        total_cost=270.00,
        current_stock=5,
    )
    suggested_field = [f["text"] for f in blocks[1]["fields"] if "Suggested Reorder" in f["text"]][0]
    assert f"{suggested_qty} units" in suggested_field

    # 2. Draft PO in ERP created with exact suggested quantity
    po_payload = CreatePurchaseOrderRequest(
        sku="Polarized-Vintage-Metal-Frame-Aviator-Sunglasses",
        product_name="Polarized Vintage Metal Frame Aviator Sunglasses",
        quantity=suggested_qty,
        vendor_id="VEND-APEX-102",
        vendor_name="Apex Direct Global",
        total_cost=270.00,
    )
    po = create_po_record_sync(po_payload)
    assert po["quantity"] == suggested_qty

    # 3. Upon approval, exact suggested quantity is restocked into e-commerce store
    mock_restock = {
        "productId": 110,
        "title": "Polarized Vintage Metal Frame Aviator Sunglasses",
        "slug": "Polarized-Vintage-Metal-Frame-Aviator-Sunglasses",
        "addedQuantity": suggested_qty,
        "previousStock": 5,
        "currentStock": 5 + suggested_qty,
        "poNumber": po["po_number"],
    }
    with patch("src.tools.ecommerce.restock_ecommerce_product", return_value=mock_restock) as mock_api:
        issued = issue_po_record_sync(po_id=po["po_number"], approver="mukesh_manager")
        mock_api.assert_called_once_with(
            slug="Polarized-Vintage-Metal-Frame-Aviator-Sunglasses",
            quantity=suggested_qty,
            po_number=po["po_number"],
        )
        assert issued["quantity"] == suggested_qty
        assert f"+{suggested_qty} units" in issued["audit_trail"][-1]


def test_produce_restock_event_dispatches_kafka_message():
    """Verify produce_restock_event produces an event to Kafka topic inventory-restock for quantity < 20."""
    with patch("confluent_kafka.Producer") as mock_producer_cls:
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        res = restock_ecommerce_product(
            slug="Polarized-Vintage-Metal-Frame-Aviator-Sunglasses",
            quantity=15,
            po_number="PO-TEST-123",
        )
        assert res is not None
        assert res["success"] is True
        assert res["addedQuantity"] == 15
        assert res["addedQuantity"] < 20
        assert res["slug"] == "Polarized-Vintage-Metal-Frame-Aviator-Sunglasses"

        mock_producer.produce.assert_called_once()
        call_kwargs = mock_producer.produce.call_args[1]
        assert call_kwargs["topic"] == "inventory-restock"
        assert call_kwargs["key"] == b"Polarized-Vintage-Metal-Frame-Aviator-Sunglasses"
        import json
        payload = json.loads(call_kwargs["value"].decode("utf-8"))
        assert payload["event_type"] == "STOCK_RESTOCK"
        assert payload["quantity"] == 15
        assert payload["po_number"] == "PO-TEST-123"
        mock_producer.flush.assert_called_once()


def test_produce_restock_event_caps_to_less_than_20():
    """Verify that any restock request with quantity >= 20 is capped to 19 (< 20) in Kafka event."""
    with patch("confluent_kafka.Producer") as mock_producer_cls:
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        res = restock_ecommerce_product(
            slug="Polarized-Vintage-Metal-Frame-Aviator-Sunglasses",
            quantity=50,
            po_number="PO-TEST-CAP",
        )
        assert res is not None
        assert res["addedQuantity"] == 19
        assert res["addedQuantity"] < 20

        call_kwargs = mock_producer.produce.call_args[1]
        import json
        payload = json.loads(call_kwargs["value"].decode("utf-8"))
        assert payload["quantity"] == 19
        assert payload["quantity"] < 20


def test_produce_restock_event_with_custom_topic():
    """Verify restock event respects KAFKA_TOPIC_RESTOCK_EVENTS override."""
    with patch("confluent_kafka.Producer") as mock_producer_cls, patch.dict("os.environ", {"KAFKA_TOPIC_RESTOCK_EVENTS": "custom-restock-topic"}):
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        res = restock_ecommerce_product(
            slug="classic-heavyweight-organic-cotton-t-shirt",
            quantity=5,
            po_number="PO-CUSTOM-01",
        )
        assert res is not None
        mock_producer.produce.assert_called_once()
        call_kwargs = mock_producer.produce.call_args[1]
        assert call_kwargs["topic"] == "custom-restock-topic"


