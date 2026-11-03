# Event-Driven Autonomous Procurement Multi-Agent System

An enterprise-grade, event-driven multi-agent procurement system packaged as a **Unified Modular Worker Daemon**. Built with **Python 3.11+**, **LangGraph**, **Apache Kafka (KRaft mode)**, **PostgreSQL Checkpointing (`PostgresSaver`)**, **FastAPI Mock ERP**, and **Slack Socket Mode Human-in-the-Loop (HITL)**.

---

## 🏗️ Architecture & Topology

```text
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 SMK E-COMMERCE STOREFRONT & BACKEND                              │
│                                                                                                  │
│   [Customer Order Placed] ──► [Stock Decrement] ──► [Kafka Producer: `inventory-events`]        │
│                                                                   │                              │
│   [Stock Restocked in DB] ◄── [Stock Consumer]  ◄── [Kafka Topic: `inventory-restock`]          │
│            │                                                      ▲                              │
│            ▼                                                      │ (produce_restock_event)      │
│   [Customer Email Alerts]                                         │                              │
└───────────────────────────────────────────────────────────────────┼──────────────────────────────┘
                                    │                               │
                      Kafka Topic: `inventory-events`               │
                                    │                               │
                                    ▼                               │
┌───────────────────────────────────────────────────────────────────┴──────────────────────────────┐
│                               AUTONOMOUS PROCUREMENT MULTI-AGENT SYSTEM                          │
│                                                                                                  │
│   ┌─────────────────────────┐                                                                    │
│   │ Deterministic Pre-Filter│ (Zero-Token Filter: stock <= threshold? Draft PO already pending?) │
│   └───────────┬─────────────┘                                                                    │
│               │ (Passed: e.g. TSHIRT-BLK-M stock 4 <= 10)                                        │
│               ▼                                                                                  │
│   ┌─────────────────────────┐                                                                    │
│   │ LangGraph State Machine │ (PostgresSaver / MemorySaver Checkpointing)                        │
│   └───────────┬─────────────┘                                                                    │
│               │                                                                                  │
│       ┌───────▼───────┐                                                                          │
│       │  Supervisor   │◄────────────────┐                                                        │
│       └───────┬───────┘                 │                                                        │
│               │ (PENDING_ANALYSIS)      │                                                        │
│               ▼                         │                                                        │
│       ┌───────────────────────┐         │                                                        │
│       │   Inventory Analyst   │─────────┤                                                        │
│       │ (Velocity & Cap < 20) │         │ (DRAFT_NEEDED)                                         │
│       └───────────────────────┘         │                                                        │
│               │                         │                                                        │
│               ▼                         │                                                        │
│       ┌───────────────────────┐         │                                                        │
│       │      PO Manager       │─────────┘                                                        │
│       └───────┬───────────────┘                                                                  │
│               │                                                                                  │
│       ┌───────┴─────────────────────────────────┐                                                │
│       ▼                                         ▼                                                │
│   ┌─────────────────┐                 ┌────────────────────┐                                     │
│   │ Mock ERP REST   │                 │ Slack Interactive  │ (Also Command Center UI)            │
│   │  (Draft PO)     │                 │     Block Kit      │                                     │
│   │  Port: 8002     │                 └─────────┬──────────┘                                     │
│   └─────────────────┘                           │ (Human Approval / Rejection)                   │
│                                                 ▼                                                │
│                                       ┌────────────────────┐                                     │
│                                       │   Slack Listener   │                                     │
│                                       │   (Socket Mode)    │                                     │
│                                       └─────────┬──────────┘                                     │
│                                                 │                                                │
│               ┌─────────────────────────────────┴────────────────────────────────┐               │
│               ▼                                                                  ▼               │
│      [Mock ERP: PATCH /issue]                                         [LangGraph Resume State]   │
│      - Status updated to ISSUED                                       - Status updated: APPROVED │
│      - Dispatches Kafka `inventory-restock` event (qty < 20) ─────────┘                          │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🔄 End-to-End Event-Driven Workflow: How Kafka Triggers & Completes the Reorder Loop

The procurement lifecycle is entirely event-driven, operating across two coordinated Kafka topics:
1. `inventory-events`: Forward trigger stream (SMK Storefront $\to$ Multi-Agent System).
2. `inventory-restock`: Reverse replenishment stream (Multi-Agent System $\to$ SMK Storefront).

```mermaid
sequenceDiagram
    autonumber
    actor Customer as Customer
    participant SMK as SMK Store Backend
    participant KafkaTrig as Kafka: inventory-events
    participant Worker as Kafka Consumer & Pre-Filter
    participant Graph as LangGraph Multi-Agent
    participant ERP as Mock ERP (:8002)
    participant Slack as Slack / Web UI (HITL)
    actor Approver as Human Approver
    participant KafkaRestock as Kafka: inventory-restock
    participant RestockCons as SMK Restock Consumer

    Customer->>SMK: Place Order & Pay (Razorpay)
    SMK->>SMK: Decrement Stock in DB
    SMK->>KafkaTrig: publishInventoryEvent(slug, currentStock, threshold)
    KafkaTrig->>Worker: Poll Message
    Worker->>Worker: Pre-Filter: stock <= threshold? Draft PO pending?
    Note over Worker,Graph: If healthy or pending PO: 0 tokens spent, commit offset
    Worker->>Graph: Invoke State Machine (thread_id: reorder-{sku}-{ts})
    Graph->>Graph: Supervisor -> Inventory Analyst (Velocity, Lead Time, Cap < 20)
    Graph->>Graph: Supervisor -> PO Manager
    Graph->>ERP: Create Draft PO (POST /purchase-orders)
    Graph->>Slack: Send Interactive Approval Card
    Note over Graph: Graph pauses at breakpoint (interrupt_after=["po_manager"])
    Approver->>Slack: Click "Approve Reorder"
    Slack->>ERP: Issue PO (PATCH /purchase-orders/{id}/issue)
    Slack->>Graph: Resume Graph -> Status: APPROVED -> END
    ERP->>KafkaRestock: produce_restock_event(slug, qty < 20, po_number)
    KafkaRestock->>RestockCons: Ingest Restock Event
    RestockCons->>RestockCons: Validate & Cap qty < 20
    RestockCons->>SMK: productService.restockProduct(slug, qty, po)
    SMK->>Customer: Send Back-in-Stock Email (if prev stock was 0)
    Note over SMK,RestockCons: Closed-Loop Restock Complete!
```

### Detailed Lifecycle Stages

#### Stage 1: Trigger — Storefront Checkout & Payment
- When a customer purchases items on the [SMK Website](../smk), `updateOrderStatusByPaymentId` transitions the order to `PLACED`.
- Stock is deducted in the database.
- [`inventoryAgentProducer.js`](../smk/server/services/inventoryAgentProducer.js) emits a JSON payload to topic `inventory-events` keyed by the product SKU/slug.

#### Stage 2: Ingestion & Zero-Token Pre-Filter Gate
- [`src/workers/kafka_consumer.py`](src/workers/kafka_consumer.py) polls `inventory-events` via `confluent-kafka`.
- **Cost-Optimization Gate**:
  - **Healthy Inventory Check**: If `current_stock > reorder_threshold`, the event is skipped and the Kafka offset is committed immediately without invoking any LLM API calls (0 token spend).
  - **In-Flight PO Check**: If a `DRAFT` PO is already awaiting human approval for this SKU, duplicate graph executions are prevented.
- **Workflow Instantiation**: When stock is truly low, a unique thread ID `reorder-{sku}-{timestamp}` is generated and LangGraph is invoked.

#### Stage 3: Multi-Agent Analysis & Reorder Calculation
- **Supervisor Agent** coordinates routing and registers execution tracing.
- **Inventory Analyst Agent**:
  - Pulls 30-day sales velocity and daily burn rate (`src/tools/analytics.py`).
  - Fetches supplier MOQ, unit cost, and delivery lead time (`src/tools/supplier.py`).
  - Computes reorder quantity:
    $$\text{Reorder Qty} = \min\Big(19,\; \max\big(\text{MOQ},\; (\text{Threshold} - \text{Stock}) + \lceil\text{Burn} \times \text{Lead}\rceil + \lceil\text{Burn} \times \text{Buffer}\rceil\big)\Big)$$
  - *Policy Guardrail*: Automatically caps replenishment orders for store products to strictly **less than 20 units** (max 19).
- **PO Manager Agent**:
  - Posts a `DRAFT` Purchase Order to the ERP (`POST /api/v1/purchase-orders`).
  - Formats and dispatches a Slack Block Kit interactive card to `#procurement-alerts` containing item specs, total cost, supplier terms, and embedded `thread_id` and `po_number` in the button payloads.
  - Updates workflow state to `AWAITING_APPROVAL`.
- **State Pause**: LangGraph checkpoints the state in PostgreSQL (`PostgresSaver`) and interrupts execution at `interrupt_after=["po_manager"]`.

#### Stage 4: Human-in-the-Loop Approval Decision
- A human operator reviews the draft order in Slack or on the Command Center Web UI (`http://localhost:8002`).
- Clicking **"Approve Reorder"**:
  - **Slack**: Handled by [`src/workers/slack_listener.py`](src/workers/slack_listener.py) via bi-directional Socket Mode (zero open firewall ports required).
  - **Web UI**: Handled by [`src/server/app.py`](src/server/app.py) via `POST /api/v1/purchase-orders/{po_id}/issue`.
- Resumes the paused LangGraph workflow with state `APPROVED`, logs the approver username, and terminates cleanly at `END`.

#### Stage 5: Reverse Replenishment Event Emission
- In [`src/server/erp.py`](src/server/erp.py), `issue_po_record_sync` transitions the PO status to `ISSUED`.
- Calls [`produce_restock_event`](src/tools/ecommerce.py) which:
  - Enforces the business policy: `quantity < 20` (capped to 19).
  - Serializes and dispatches an event to Kafka topic `inventory-restock` (`KAFKA_TOPIC_RESTOCK_EVENTS`).
  - Flushes the message to the Kafka broker with the product slug as partition key.

#### Stage 6: Store Stock Replenishment & Cycle Completion
- The SMK backend restock consumer ([`inventoryRestockConsumer.js`](../smk/server/services/inventoryRestockConsumer.js)) continuously listens to `inventory-restock`.
- On receipt of the event:
  - Validates and enforces quantity $< 20$.
  - Executes [`productService.restockProduct(...)`](../smk/server/services/products.js) to insert a new `Stock` record (type: `PRODUCT`) into PostgreSQL.
  - If previous stock was 0, automatically sends restock notification emails to waiting shoppers.
  - Store stock is fully replenished in real time, closing the autonomous procurement loop.

---

### 📦 Kafka Event Payloads Reference

#### Outbound Trigger Event: `inventory-events`
Published by SMK Storefront when stock drops or order is placed:
```json
{
  "event_id": "evt-1730635200000-abc123",
  "timestamp": "2026-11-03T12:00:00.000Z",
  "sku": "classic-heavyweight-cotton-t-shirt",
  "slug": "classic-heavyweight-cotton-t-shirt",
  "product_id": 101,
  "product_name": "Classic Heavyweight Cotton T-Shirt",
  "current_stock": 4,
  "reorder_threshold": 10
}
```

#### Inbound Replenishment Event: `inventory-restock`
Published by Multi-Agent System upon PO approval to replenish store stock:
```json
{
  "event_id": "restock-a1b2c3d4",
  "event_type": "STOCK_RESTOCK",
  "action": "RESTOCK",
  "timestamp": "2026-11-03T12:05:00.000Z",
  "slug": "classic-heavyweight-cotton-t-shirt",
  "sku": "classic-heavyweight-cotton-t-shirt",
  "productId": 101,
  "quantity": 19,
  "po_number": "PO-9142",
  "poNumber": "PO-9142"
}
```

---

## ⚙️ Tech Stack

- **Framework**: LangGraph, LangChain Core, LangChain Google GenAI (`langchain-google-genai`), LangChain OpenAI
- **LLM**: Google Gemini (`gemini-2.5-flash`, `gemini-1.5-pro`) via `GEMINI_API_KEY`
- **Messaging**: Apache Kafka 3.x in KRaft mode (No ZooKeeper required), confluent-kafka
- **Persistence & Checkpointing**: PostgreSQL 16 via `langgraph-checkpoint-postgres` (`PostgresSaver`) with fallback to `MemorySaver`
- **ERP Integration**: FastAPI, Uvicorn, HTTPX, Pydantic v2 (Exposed on port **8002**)
- **Human-in-the-Loop**: Slack Bolt (Socket Mode), Slack SDK
- **Telemetry & Tracing**: Langfuse OpenTelemetry callbacks
- **Testing**: Pytest, Pytest-Asyncio, Pytest-Mock

---

## 📁 Project Directory Structure

```text
ecommerce-agents/
├── docker-compose.yml          # Kafka (KRaft), Postgres 16, Mock ERP (8002)
├── pyproject.toml              # Dependencies and build configuration
├── .env.example                # Environment variables template
├── .gitignore                  # Git ignore rules for Python, venv, IDEs
├── README.md                   # System documentation and operational runbook
├── mock_erp/
│   ├── __init__.py
│   └── main.py                 # Backward-compatible proxy re-exporting src.server
├── src/
│   ├── __init__.py
│   ├── config.py               # Pydantic-settings configuration loader
│   ├── llm.py                  # Google Gemini LLM provider factory
│   ├── main.py                 # Unified supervisor starting all workers & server
│   ├── producer.py             # Kafka inventory event publisher CLI & utility
│   ├── server/
│   │   ├── __init__.py
│   │   ├── app.py              # Unified FastAPI server: Real-time UI & Embedded ERP (:8002)
│   │   ├── erp.py              # Embedded Mock ERP datastore, POs, and Slack announcements
│   │   ├── traceability.py     # Multi-Agent event recording and real-time SSE stream
│   │   └── static/             # Light-mode Command Center UI (HTML, CSS, JS)
│   ├── schemas/
│   │   ├── __init__.py
│   │   ├── state.py            # ProcurementState TypedDict with operator.add
│   │   └── tools.py            # Pydantic input/output schemas for all tools
│   ├── tools/
│   │   ├── __init__.py
│   │   ├── analytics.py        # Sales velocity & daily burn rate analytics
│   │   ├── supplier.py         # Vendor terms, MOQ, and unit pricing
│   │   ├── ecommerce.py       # Live product fetch from SMK server backend
│   │   ├── erp.py              # Client for ERP REST endpoints and activity logging
│   │   └── slack.py            # Slack Block Kit approval card dispatcher
│   ├── agents/
│   │   ├── __init__.py
│   │   ├── supervisor.py       # Workflow state evaluation, router, and traceability
│   │   ├── inventory_analyst.py# Dynamic reorder formula computation node
│   │   └── po_manager.py       # ERP PO creator & Slack HITL dispatch node
│   ├── graph.py                # StateGraph assembly, checkpointer, and compile
│   └── workers/
│       ├── __init__.py
│       ├── kafka_consumer.py   # Kafka consumer with deterministic pre-filter
│       └── slack_listener.py   # Slack Socket Mode listener & workflow resumption
└── tests/
    ├── __init__.py
    ├── test_prefilter.py       # Unit tests for zero-token deterministic filter & producer
    ├── test_graph_flow.py      # Integration tests for graph execution & HITL
    └── test_ui_endpoints.py    # Tests for UI, SSE stream, and embedded ERP endpoints
```

---

## 🚀 How to Run All Applications (Step-by-Step)

Follow these steps to run all containers, services, and worker daemons.

### Step 1: Environment Setup
1. Copy the environment configuration template:
   ```bash
   cp .env.example .env
   ```
2. Edit `.env` and fill in your keys:
   ```env
   # Google Gemini LLM Settings
   GEMINI_API_KEY=your-gemini-api-key
   GEMINI_MODEL=gemini-2.5-flash

   # Kafka Messaging Settings
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   KAFKA_TOPIC_INVENTORY_EVENTS=inventory-events
   KAFKA_CONSUMER_GROUP_ID=procurement-agent-group

   # Inventory Agent Feature Flag
   INVENTORY_AGENT_ENABLED=true

   # PostgreSQL Checkpointer URI
   POSTGRES_URI=postgresql://postgres:postgrespassword@localhost:5432/procurement_db

   # Mock ERP Service URL (Port 8002)
   MOCK_ERP_URL=http://localhost:8002

   # Slack Socket Mode Settings (HITL)
   SLACK_BOT_TOKEN=xoxb-your-bot-token
   SLACK_APP_TOKEN=xapp-your-app-level-token
   SLACK_CHANNEL_ID=C0123456789
   ```

### Step 2: Install Python Dependencies
Create and activate the Python virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Step 3: Start Infrastructure Containers (Kafka & Postgres)
Start the Docker Compose services in detached mode (Docker manages only data infrastructure; Mock ERP and Agent Hub are integrated into the Python runtime):
```bash
docker compose up -d
```

Verify that data infrastructure containers are running:
```bash
docker compose ps
```
- **Apache Kafka (KRaft)**: Running on `localhost:9092`
- **PostgreSQL 16**: Running on `localhost:5432` (`procurement_db`)

### Step 4: Start Multi-Agent System & Real-Time Hub
Start all agent daemons, the Real-Time Activity Hub, and the embedded Mock ERP server concurrently on port 8002:
```bash
make start
# OR: .venv/bin/python -m src.main
```
This launches:
- **Agent Traceability & Embedded ERP Hub**: `http://localhost:8002` (Command Center UI & REST API)
- **Kafka Consumer Worker**: Listens on `inventory-events` topic with deterministic pre-filtering
- **Slack Socket Mode Listener**: Bi-directional zero-ingress human-in-the-loop approvals
You can run all three workers concurrently via `make start`:
```bash
make start
# Or directly:
source .venv/bin/activate
python -m src.main
```

---

#### Option B: Individual Worker Processes (Separate Terminals)

**Terminal 1 — Kafka Ingestion Worker:**
```bash
source .venv/bin/activate
python -m src.workers.kafka_consumer
```
*Continuously polls the `inventory-events` topic on Kafka, executes the deterministic pre-filter ($stock \le threshold$), and triggers the LangGraph multi-agent flow.*

**Terminal 2 — Slack Socket Mode Listener (HITL):**
```bash
source .venv/bin/activate
python -m src.workers.slack_listener
```
*Maintains a persistent outbound WebSocket connection to Slack to process human approvals and resumes the paused LangGraph workflow with zero ingress webhooks.*

### Step 6: Produce an Inventory Event
In another terminal, publish a low-stock inventory event using the built-in CLI utility:
```bash
source .venv/bin/activate

# Low-stock event (Stock 4 <= Threshold 10 triggers multi-agent workflow):
python -m src.producer --sku TSHIRT-BLK-M --stock 4 --threshold 10
```

*(To test the zero-token cost optimization filter where stock is healthy)*:
```bash
# Healthy event (Stock 50 > Threshold 10 is skipped with 0 LLM tokens):
python -m src.producer --sku TSHIRT-BLK-M --stock 50 --threshold 10
```

---

## 🖥️ Real-Time Multi-Agent Command Center & PO Monitor (Web UI)

Open your web browser and navigate directly to:
👉 **[http://localhost:8002](http://localhost:8002)** (or `http://localhost:8002/dashboard`)

![Autonomous Procurement UI Dashboard](https://images.unsplash.com/photo-1551288049-bebda4e38f71?auto=format&fit=crop&w=1200&q=80)

### What You Can See & Do in the UI:
1. **Real-Time Agent Activity Feed**:
   - Live Server-Sent Events (SSE) stream pushing events dynamically without page refresh.
   - Watch each agent's thoughts and actions:
     - ⚡ **Kafka Ingestion**: SKU stock decrement event.
     - 🛡️ **Deterministic Pre-Filter**: Zero-token cost gate check.
     - 🧠 **Supervisor Agent**: State routing.
     - 📊 **Inventory Analyst**: Real-time sales velocity analysis, burn rate, and replenishment formula breakdown (`max(MOQ, ceil((burn * lead) + (burn * 30) - stock))`).
     - 📝 **PO Manager**: Draft PO generation and Slack Block Kit card dispatch.
     - 👤 **Human Approver (HITL)**: Real-time sync with Slack button clicks or dashboard approvals!
2. **Purchase Orders (POs) Management**:
   - Live listing of all created POs (`PO-XXXX`), SKU, Vendor, Quantity, Unit Price, and Total Value.
   - Status indicators: `DRAFT` (pulsing amber), `ISSUED` (emerald green), `REJECTED` (rose red).
   - Approver name and timestamp (e.g., `@mukesh` from Slack or `@mukesh_dashboard`).
3. **One-Click Human Approval from the UI**:
   - In addition to Slack, you can click **"Approve"** or **"Reject"** directly on any draft PO row to issue or decline it in real time!
4. **PO Details & Audit Modal**:
   - Click **"Details"** on any PO to open a modal showing full item specifications, formula breakdown, and chronological audit trail.
5. **Interactive Low-Stock Simulation**:
   - Select any SKU (`TSHIRT-BLK-M`, `SKU-WIDGET-01`, `SKU-CABLE-02`, `SKU-SENSOR-03`), set current stock and threshold, and click **"Simulate Low Stock Event"** to watch the multi-agent system spring to life!

---

## 🔍 Where & How to See the PO Details Once Approved

Once the approval button (**"Approve Reorder"** in Slack or **"Approve"** in the Web UI) is clicked by an authorized user, you can view the finalized Purchase Order details in multiple ways:

### 1. View in the Real-Time Web Dashboard (Recommended)
Open **[http://localhost:8002](http://localhost:8002)**:
- Filter by the **"Issued"** tab to see all approved Purchase Orders.
- View the PO number, vendor name, quantity, total value, and the user who approved it.
- Click **"Details"** to inspect the complete invoice specs and audit trace.

### 2. View in Mock ERP Interactive Swagger API
Open **[http://localhost:8002/docs](http://localhost:8002/docs)**:
1. Scroll down to the **Purchase Orders** section.
2. Click **`GET /api/v1/purchase-orders`** -> Click **"Try it out"** -> Click **"Execute"**.
3. You will see the complete list of Purchase Orders with their updated status (`ISSUED`), the authorized approver name, and the timestamp.
4. Or use **`GET /api/v1/purchase-orders/{po_id}`** with your PO number (e.g., `PO-9142`).

---

### 2. View via Terminal / cURL Command
You can query the Mock ERP API directly from your terminal:

#### List All Purchase Orders:
```bash
curl -s http://localhost:8002/api/v1/purchase-orders | jq .
```

#### View Specific Purchase Order by ID:
```bash
curl -s http://localhost:8002/api/v1/purchase-orders/PO-9142 | jq .
```

#### Example Output Response:
```json
{
  "po_number": "PO-9142",
  "sku": "TSHIRT-BLK-M",
  "quantity": 176,
  "vendor_id": "VEND-102",
  "total_cost": 1496.0,
  "status": "ISSUED",
  "approver": "mukesh",
  "created_at": "2026-11-02T09:30:05.123456+00:00",
  "updated_at": "2026-11-02T09:35:12.654321+00:00"
}
```

Field description:
- `po_number`: Unique identifier formatted as `PO-XXXX`.
- `status`: Transitions from `DRAFT` $\to$ **`ISSUED`** upon approval.
- `approver`: Slack user who authorized the procurement (e.g. `mukesh`).
- `quantity`: Mathematically calculated replenishment quantity.
- `total_cost`: Calculated based on vendor catalog unit price ($176 \times \$8.50 = \$1,496.00$).

---

### 3. View In-Place in Slack
In your Slack `#procurement-alerts` channel:
- The initial interactive approval buttons disappear to prevent duplicate clicks.
- The card updates in-place to display:
  ```text
  ✅ Approved by @mukesh: Purchase Order PO-9142 for 176 units issued to Apex Textiles.
  ```

---

### 4. View in PostgreSQL Checkpoint Database
To inspect the serialized LangGraph state machine state in Postgres:
```bash
docker exec -it procurement-postgres psql -U postgres -d procurement_db -c "
SELECT 
    thread_id, 
    checkpoint_id, 
    parent_checkpoint_id 
FROM checkpoints 
ORDER BY checkpoint_id DESC 
LIMIT 5;
"
```

---

### 5. View in Langfuse Observability
If `LANGFUSE_PUBLIC_KEY` and `LANGFUSE_SECRET_KEY` are configured:
- Navigate to **[https://cloud.langfuse.com](https://cloud.langfuse.com)**
- Inspect the full execution trace under the session `reorder-TSHIRT-BLK-M-<timestamp>`:
  - Agent step latencies
  - Tool execution outputs
  - Gemini LLM synthesis and token spend
  - HITL resume breakpoint and final audit log

---

## 🧪 Running Verification Tests

Run the full pytest suite to verify all 15 unit and integration tests:
```bash
pytest -v
```

Tests cover:
- Deterministic inventory pre-filtering (healthy vs. low stock).
- `produce_inventory_event` publisher with `INVENTORY_AGENT_ENABLED` feature flag check.
- Dynamic mathematical replenishment calculation: $\max(\text{MOQ}, \lceil(\text{Burn Rate} \times \text{Lead Time}) + (\text{Burn Rate} \times 30) - \text{Current Stock}\rceil)$.
- Mock ERP Purchase Order creation (`DRAFT`) and issuance (`ISSUED`).
- LangGraph execution to approval breakpoint (`interrupt_after=["po_manager"]`).
- Slack Socket Mode approval and rejection resumption callbacks.
- Complete end-to-end `TSHIRT-BLK-M` trace from Kafka event to ERP issuance.

---

## 📊 Replenishment Formula Reference

The Inventory Analyst Agent computes the order quantity using:
$$\text{Reorder Quantity} = \max\Big(\text{MOQ},\; \big\lceil (\text{Burn Rate} \times \text{Lead Time}) + (\text{Burn Rate} \times 30) - \text{Current Stock} \big\rceil\Big)$$

Where:
- $\text{Burn Rate}$: Average daily consumption (units/day)
- $\text{Lead Time}$: Supplier delivery timeline in days
- $\text{MOQ}$: Supplier Minimum Order Quantity
- $\text{Current Stock}$: Present warehouse stock level
