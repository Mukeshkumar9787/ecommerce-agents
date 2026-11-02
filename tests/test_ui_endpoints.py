"""Tests for real-time dashboard UI endpoints, activities, and simulation."""

import pytest
from fastapi.testclient import TestClient
from src.server.app import app as agent_fastapi_app
from mock_erp.main import app as erp_fastapi_app, purchase_orders_db, activities_db


class TestDashboardAndRealtimeUI:
    """Validate HTML dashboard, SSE stream endpoint, activity logging, and simulation."""

    @pytest.fixture
    def client(self):
        return TestClient(agent_fastapi_app)

    def test_health_check_endpoint(self, client):
        """GET /health should return 200 with embedded_erp=True."""
        res = client.get("/health")
        assert res.status_code == 200
        data = res.json()
        assert data["status"] == "ok"
        assert data["embedded_erp"] is True

    def test_dashboard_html_served(self, client):
        """Root / and /dashboard should return 200 with HTML content."""
        res_root = client.get("/")
        assert res_root.status_code == 200
        assert "Autonomous Procurement" in res_root.text
        assert "text/html" in res_root.headers.get("content-type", "")

        res_dash = client.get("/dashboard")
        assert res_dash.status_code == 200
        assert "Command Center" in res_dash.text

    def test_static_assets_served(self, client):
        """Static assets (/static/style.css, /static/app.js) should be accessible."""
        res_css = client.get("/static/style.css")
        assert res_css.status_code == 200
        assert ":root" in res_css.text

        res_js = client.get("/static/app.js")
        assert res_js.status_code == 200
        assert "initApp" in res_js.text

    def test_activity_logging_and_retrieval(self, client):
        """Verify posting and retrieving agent activities."""
        post_res = client.post(
            "/api/v1/activities",
            json={
                "agent": "Test Agent",
                "event_type": "TEST_EVENT",
                "title": "Unit Test Activity",
                "description": "Testing real-time activity stream recording",
                "details": {"key": "val"},
                "sku": "TSHIRT-BLK-M",
            },
        )
        assert post_res.status_code == 201
        data = post_res.json()
        assert data["agent"] == "Test Agent"
        assert data["title"] == "Unit Test Activity"

        get_res = client.get("/api/v1/activities")
        assert get_res.status_code == 200
        activities = get_res.json()
        assert any(a["title"] == "Unit Test Activity" for a in activities)

    def test_simulation_endpoint_creates_po(self, client):
        """Simulate low stock for TSHIRT-BLK-M (stock=4, threshold=10)."""
        sim_res = client.post(
            "/api/v1/simulate-event",
            json={
                "sku": "TSHIRT-BLK-M",
                "current_stock": 4,
                "reorder_threshold": 10,
            },
        )
        assert sim_res.status_code == 200
        data = sim_res.json()
        assert data["status"] == "AWAITING_APPROVAL"
        assert data["quantity"] == 176
        assert data["total_cost"] == 1496.00
        po_number = data["po_number"]
        assert po_number.startswith("PO-")

        # Verify created in DB
        get_po = client.get(f"/api/v1/purchase-orders/{po_number}")
        assert get_po.status_code == 200
        po_data = get_po.json()
        assert po_data["status"] == "DRAFT"
        assert po_data["quantity"] == 176

    def test_reject_purchase_order_endpoint(self, client):
        """Verify PATCH /api/v1/purchase-orders/{po_id}/reject."""
        # Create a PO first
        create_res = client.post(
            "/api/v1/purchase-orders",
            json={
                "sku": "SKU-WIDGET-01",
                "quantity": 50,
                "vendor_id": "VEND-ACME-001",
                "total_cost": 625.00,
            },
        )
        po_num = create_res.json()["po_number"]

        reject_res = client.patch(
            f"/api/v1/purchase-orders/{po_num}/reject",
            json={"rejector": "procurement_cfo"},
        )
        assert reject_res.status_code == 200
        assert reject_res.json()["status"] == "REJECTED"
        assert reject_res.json()["approver"] == "procurement_cfo"

    def test_get_ecommerce_products(self, client):
        """Verify GET /api/v1/products returns real-time or catalog products."""
        res = client.get("/api/v1/products")
        assert res.status_code == 200
        data = res.json()
        assert data["success"] is True
        assert "products" in data
        assert len(data["products"]) > 0
        first = data["products"][0]
        assert "slug" in first
        assert "title" in first
        assert "stock" in first

    def test_get_ecommerce_product_by_slug(self, client):
        """Verify GET /api/v1/products/{slug} retrieves details by slug."""
        slug = "classic-heavyweight-organic-cotton-t-shirt"
        res = client.get(f"/api/v1/products/{slug}")
        assert res.status_code == 200
        data = res.json()
        assert data["success"] is True
        assert data["data"]["slug"] == slug
        assert "title" in data["data"]
