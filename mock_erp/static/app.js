/**
 * E-Commerce Autonomous Procurement Center - Light Theme Frontend Logic
 * Live Server-Sent Events (SSE) Stream & Real Product Item Lifecycle Tracking.
 */

const state = {
  purchaseOrders: [],
  activities: [],
  activeFilter: 'ALL',
  selectedPO: null,
};

const dom = {
  liveIndicator: document.getElementById('liveIndicator'),
  indicatorText: document.getElementById('indicatorText'),
  clearDataBtn: document.getElementById('clearDataBtn'),

  statTotalPOs: document.getElementById('statTotalPOs'),
  statDraftPOs: document.getElementById('statDraftPOs'),
  statIssuedPOs: document.getElementById('statIssuedPOs'),
  statTotalSpend: document.getElementById('statTotalSpend'),

  productSelect: document.getElementById('productSelect'),
  stockInput: document.getElementById('stockInput'),
  thresholdInput: document.getElementById('thresholdInput'),
  simulateBtn: document.getElementById('simulateBtn'),
  metaSlug: document.getElementById('metaSlug'),
  metaStock: document.getElementById('metaStock'),
  metaPrice: document.getElementById('metaPrice'),

  poTableBody: document.getElementById('poTableBody'),
  orderCount: document.getElementById('orderCount'),
  tabBtns: document.querySelectorAll('.tab-btn'),

  activityFeed: document.getElementById('activityFeed'),
  activityCount: document.getElementById('activityCount'),

  poModal: document.getElementById('poModal'),
  modalCloseBtn: document.getElementById('modalCloseBtn'),
  modalPoNumber: document.getElementById('modalPoNumber'),
  modalPoStatus: document.getElementById('modalPoStatus'),
  modalProductName: document.getElementById('modalProductName'),
  modalSku: document.getElementById('modalSku'),
  modalVendor: document.getElementById('modalVendor'),
  modalQty: document.getElementById('modalQty'),
  modalCost: document.getElementById('modalCost'),
  modalApprover: document.getElementById('modalApprover'),
  modalFormula: document.getElementById('modalFormula'),
  modalAuditTrail: document.getElementById('modalAuditTrail'),
  modalApproveBtn: document.getElementById('modalApproveBtn'),
  modalRejectBtn: document.getElementById('modalRejectBtn'),
};

function formatCurrency(amount) {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
  }).format(amount || 0);
}

function formatTime(isoString) {
  if (!isoString) return '';
  const d = new Date(isoString);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function initApp() {
  setupEventListeners();
  fetchStoreProducts();
  fetchInitialData();
  initEventStream();
  setInterval(pollSync, 4000);
}

function setupEventListeners() {
  // Tabs
  dom.tabBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      dom.tabBtns.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      state.activeFilter = btn.dataset.filter;
      renderOrders();
    });
  });

  // Simulate Button
  dom.simulateBtn.addEventListener('click', triggerSimulation);

  // Clear Data Button
  dom.clearDataBtn.addEventListener('click', async () => {
    if (!confirm('Clear all purchase orders and activity history?')) return;
    try {
      await fetch('/api/v1/clear', { method: 'DELETE' });
      state.purchaseOrders = [];
      state.activities = [];
      renderOrders();
      renderActivities();
      updateStats();
    } catch (e) {
      alert('Error clearing data: ' + e.message);
    }
  });

  // Modal Close
  dom.modalCloseBtn.addEventListener('click', closeModal);
  dom.poModal.addEventListener('click', (e) => {
    if (e.target === dom.poModal) closeModal();
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeModal();
  });

  // Product Select Change
  dom.productSelect.addEventListener('change', updateSelectedProductMeta);

  // Modal Actions
  dom.modalApproveBtn.addEventListener('click', () => {
    if (state.selectedPO) issuePurchaseOrder(state.selectedPO.po_number);
  });
  dom.modalRejectBtn.addEventListener('click', () => {
    if (state.selectedPO) rejectPurchaseOrder(state.selectedPO.po_number);
  });
}

function updateSelectedProductMeta() {
  const sel = dom.productSelect;
  if (!sel || sel.selectedIndex < 0) return;
  const opt = sel.options[sel.selectedIndex];
  if (!opt) return;

  const slug = opt.value;
  const stock = opt.dataset.stock;
  const price = opt.dataset.price;

  if (dom.metaSlug) dom.metaSlug.textContent = slug;
  if (dom.metaStock) dom.metaStock.textContent = stock !== undefined ? `${stock} units` : '-';
  if (dom.metaPrice) dom.metaPrice.textContent = price ? `$${parseFloat(price).toFixed(2)}` : '-';
}

async function fetchStoreProducts() {
  try {
    const res = await fetch('/api/v1/products');
    if (!res.ok) return;
    const data = await res.json();
    if (data.success && Array.isArray(data.products) && data.products.length > 0) {
      dom.productSelect.innerHTML = '';
      data.products.forEach(p => {
        const opt = document.createElement('option');
        opt.value = p.slug;
        opt.dataset.name = p.title;
        opt.dataset.stock = p.stock !== undefined ? p.stock : 0;
        opt.dataset.price = p.price !== undefined ? p.price : 0;
        opt.textContent = `${p.title} (${p.category || 'Store'}) — $${parseFloat(p.price || 0).toFixed(2)}`;
        dom.productSelect.appendChild(opt);
      });
      updateSelectedProductMeta();
    }
  } catch (err) {
    console.debug('Failed loading store products:', err);
  }
}

async function fetchInitialData() {
  try {
    const [poRes, actRes] = await Promise.all([
      fetch('/api/v1/purchase-orders'),
      fetch('/api/v1/activities')
    ]);

    if (poRes.ok) {
      state.purchaseOrders = await poRes.json();
      renderOrders();
    }
    if (actRes.ok) {
      state.activities = await actRes.json();
      renderActivities();
    }
    updateStats();
  } catch (err) {
    console.warn('Initial load error:', err);
  }
}

async function pollSync() {
  try {
    const poRes = await fetch('/api/v1/purchase-orders');
    if (poRes.ok) {
      const pos = await poRes.json();
      if (JSON.stringify(pos) !== JSON.stringify(state.purchaseOrders)) {
        state.purchaseOrders = pos;
        renderOrders();
        updateStats();
      }
    }
  } catch (_) {}
}

function initEventStream() {
  const evtSource = new EventSource('/api/v1/stream');

  evtSource.onopen = () => {
    dom.liveIndicator.className = 'live-badge';
    dom.indicatorText.textContent = 'LIVE STREAM CONNECTED';
  };

  evtSource.onmessage = (event) => {
    try {
      const payload = JSON.parse(event.data);
      handleLiveEvent(payload);
    } catch (_) {}
  };

  evtSource.onerror = () => {
    dom.liveIndicator.className = 'live-badge';
    dom.indicatorText.textContent = 'RECONNECTING...';
  };
}

function handleLiveEvent(evt) {
  const { type, data } = evt;

  if (type === 'PO_CREATED') {
    const exists = state.purchaseOrders.find(p => p.po_number === data.po_number);
    if (!exists) {
      state.purchaseOrders.unshift(data);
    } else {
      Object.assign(exists, data);
    }
    renderOrders();
    updateStats();
  } else if (type === 'PO_UPDATED') {
    const idx = state.purchaseOrders.findIndex(p => p.po_number === data.po_number);
    if (idx !== -1) {
      state.purchaseOrders[idx] = data;
    } else {
      state.purchaseOrders.unshift(data);
    }
    if (state.selectedPO && state.selectedPO.po_number === data.po_number) {
      state.selectedPO = data;
      showModalDetails(data);
    }
    renderOrders();
    updateStats();
  } else if (type === 'ACTIVITY') {
    state.activities.unshift(data);
    renderActivities();
    updateStats();
  } else if (type === 'CLEARED') {
    state.purchaseOrders = [];
    state.activities = [];
    renderOrders();
    renderActivities();
    updateStats();
  }
}

function renderOrders() {
  const tbody = dom.poTableBody;
  tbody.innerHTML = '';

  const filtered = state.purchaseOrders.filter(po => {
    if (state.activeFilter === 'ALL') return true;
    return (po.status || '').toUpperCase() === state.activeFilter;
  });

  dom.orderCount.textContent = `${filtered.length} Orders`;

  if (filtered.length === 0) {
    tbody.innerHTML = `
      <tr>
        <td colspan="7">
          <div class="empty-box">
            <svg fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M16 11V7a4 4 0 00-8 0v4M5 9h14l1 12H4L5 9z"/>
            </svg>
            <p>No purchase orders found matching "${state.activeFilter}".</p>
            <span style="font-size:0.78rem; color:var(--text-light);">Place an order in the e-commerce store or click "Simulate Stock Event" above.</span>
          </div>
        </td>
      </tr>
    `;
    return;
  }

  filtered.forEach(po => {
    const tr = document.createElement('tr');
    const status = (po.status || 'DRAFT').toUpperCase();
    const isDraft = status === 'DRAFT';

    const prodName = po.product_name || po.sku;

    tr.innerHTML = `
      <td>
        <strong style="font-family:var(--font-mono); font-size:0.85rem; color:var(--primary);">${po.po_number}</strong>
      </td>
      <td>
        <div class="prod-info-box">
          <span class="prod-title">${prodName}</span>
          <span class="prod-sub">
            <span class="sku-badge">${po.sku}</span>
          </span>
        </div>
      </td>
      <td>
        <span>${po.vendor_name || po.vendor_id}</span>
      </td>
      <td>
        <strong>${(po.quantity || 0).toLocaleString()}</strong> units
      </td>
      <td>
        <span class="cost-text">${formatCurrency(po.total_cost)}</span>
      </td>
      <td>
        <span class="status-pill ${status.toLowerCase()}">${status}</span>
        ${po.approver ? `<div style="font-size:0.7rem; color:var(--text-muted); margin-top:2px;">by @${po.approver}</div>` : ''}
      </td>
      <td>
        <div class="btn-actions">
          ${isDraft ? `
            <button class="btn btn-success btn-sm" onclick="issuePurchaseOrder('${po.po_number}')" title="Approve & Issue">
              Approve
            </button>
            <button class="btn btn-danger-outline btn-sm" onclick="rejectPurchaseOrder('${po.po_number}')" title="Reject">
              ✕
            </button>
          ` : ''}
          <button class="btn btn-secondary btn-sm" onclick="openDetails('${po.po_number}')">
            Details
          </button>
        </div>
      </td>
    `;
    tbody.appendChild(tr);
  });
}

function renderActivities() {
  const container = dom.activityFeed;
  container.innerHTML = '';

  dom.activityCount.textContent = `${state.activities.length} Events`;

  if (state.activities.length === 0) {
    container.innerHTML = `
      <div class="empty-box">
        <svg fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M13 10V3L4 14h7v7l9-11h-7z"/>
        </svg>
        <p>No activity yet.</p>
        <span style="font-size:0.78rem; color:var(--text-light);">Events will appear live when stock drops or orders are created.</span>
      </div>
    `;
    return;
  }

  const recent = state.activities.slice(0, 30);

  recent.forEach(act => {
    const card = document.createElement('div');
    card.className = 'activity-card';

    let specsHtml = '';
    if (act.details && Object.keys(act.details).length > 0) {
      specsHtml = '<div class="act-specs">';
      for (const [k, v] of Object.entries(act.details)) {
        specsHtml += `
          <div class="act-spec-item">${k}: <span class="act-spec-val">${v}</span></div>
        `;
      }
      specsHtml += '</div>';
    }

    card.innerHTML = `
      <div class="act-head">
        <span class="agent-label">🤖 ${act.agent}</span>
        <span class="act-time">${formatTime(act.timestamp)}</span>
      </div>
      <div class="act-title">${act.title}</div>
      <div class="act-desc">${act.description}</div>
      ${specsHtml}
    `;
    container.appendChild(card);
  });
}

function updateStats() {
  const total = state.purchaseOrders.length;
  const draft = state.purchaseOrders.filter(p => (p.status || '').toUpperCase() === 'DRAFT').length;
  const issued = state.purchaseOrders.filter(p => (p.status || '').toUpperCase() === 'ISSUED').length;
  const spend = state.purchaseOrders
    .filter(p => (p.status || '').toUpperCase() === 'ISSUED')
    .reduce((sum, p) => sum + (parseFloat(p.total_cost) || 0), 0);

  dom.statTotalPOs.textContent = total;
  dom.statDraftPOs.textContent = draft;
  dom.statIssuedPOs.textContent = issued;
  dom.statTotalSpend.textContent = formatCurrency(spend);
}

async function triggerSimulation() {
  const selectElem = dom.productSelect;
  const sku = selectElem.value;
  const selectedOption = selectElem.options[selectElem.selectedIndex];
  const productName = selectedOption.getAttribute('data-name') || selectedOption.text;

  const stock = parseInt(dom.stockInput.value, 10);
  const threshold = parseInt(dom.thresholdInput.value, 10);

  if (isNaN(stock) || isNaN(threshold)) {
    alert('Please enter valid numeric stock and threshold values.');
    return;
  }

  dom.simulateBtn.disabled = true;
  dom.simulateBtn.textContent = 'Processing Agents...';

  try {
    const res = await fetch('/api/v1/simulate-event', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sku,
        product_name: productName,
        current_stock: stock,
        reorder_threshold: threshold,
      }),
    });

    const data = await res.json();
    if (!res.ok) {
      alert(`Simulation Error: ${data.detail || 'Failed to simulate'}`);
    } else {
      setTimeout(fetchInitialData, 400);
    }
  } catch (err) {
    alert(`Simulation failed: ${err.message}`);
  } finally {
    dom.simulateBtn.disabled = false;
    dom.simulateBtn.innerHTML = `
      <svg width="16" height="16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"/>
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>
      </svg>
      Simulate Stock Event
    `;
  }
}

async function issuePurchaseOrder(poNumber) {
  try {
    const res = await fetch(`/api/v1/purchase-orders/${poNumber}/issue`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ approver: 'mukesh_dashboard' }),
    });

    if (res.ok) {
      const updated = await res.json();
      const idx = state.purchaseOrders.findIndex(p => p.po_number === poNumber);
      if (idx !== -1) state.purchaseOrders[idx] = updated;
      if (state.selectedPO && state.selectedPO.po_number === poNumber) {
        state.selectedPO = updated;
        showModalDetails(updated);
      }
      renderOrders();
      updateStats();
    } else {
      const err = await res.json();
      alert(`Approval failed: ${err.detail || 'Error'}`);
    }
  } catch (e) {
    alert(`Network error: ${e.message}`);
  }
}

async function rejectPurchaseOrder(poNumber) {
  if (!confirm(`Are you sure you want to decline PO ${poNumber}?`)) return;

  try {
    const res = await fetch(`/api/v1/purchase-orders/${poNumber}/reject`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ rejector: 'mukesh_dashboard' }),
    });

    if (res.ok) {
      const updated = await res.json();
      const idx = state.purchaseOrders.findIndex(p => p.po_number === poNumber);
      if (idx !== -1) state.purchaseOrders[idx] = updated;
      if (state.selectedPO && state.selectedPO.po_number === poNumber) {
        state.selectedPO = updated;
        showModalDetails(updated);
      }
      renderOrders();
      updateStats();
    } else {
      const err = await res.json();
      alert(`Rejection failed: ${err.detail || 'Error'}`);
    }
  } catch (e) {
    alert(`Network error: ${e.message}`);
  }
}

function openDetails(poNumber) {
  const po = state.purchaseOrders.find(p => p.po_number === poNumber);
  if (!po) return;

  state.selectedPO = po;
  showModalDetails(po);
  dom.poModal.classList.add('open');
}

function showModalDetails(po) {
  dom.modalPoNumber.textContent = po.po_number;
  const status = (po.status || 'DRAFT').toUpperCase();
  dom.modalPoStatus.className = `status-pill ${status.toLowerCase()}`;
  dom.modalPoStatus.textContent = status;

  dom.modalProductName.textContent = po.product_name || po.sku;
  dom.modalSku.textContent = po.sku;
  dom.modalVendor.textContent = po.vendor_name || po.vendor_id;
  dom.modalQty.textContent = `${(po.quantity || 0).toLocaleString()} units`;
  dom.modalCost.textContent = formatCurrency(po.total_cost);
  dom.modalApprover.textContent = po.approver ? `@${po.approver}` : 'Pending Slack / UI Approval';
  dom.modalFormula.textContent = po.formula_breakdown || 'Inventory Analyst replenishment calculation';

  dom.modalAuditTrail.innerHTML = '';
  const trail = po.audit_trail || [
    `Order generated at ${po.created_at}`,
    po.status === 'ISSUED' ? `Approved and issued by @${po.approver}` : 'Awaiting review'
  ];

  trail.forEach(line => {
    const div = document.createElement('div');
    div.className = 'audit-line';
    div.textContent = typeof line === 'string' ? line : `${line.agent}: ${line.message}`;
    dom.modalAuditTrail.appendChild(div);
  });

  const isDraft = status === 'DRAFT';
  dom.modalApproveBtn.style.display = isDraft ? 'inline-flex' : 'none';
  dom.modalRejectBtn.style.display = isDraft ? 'inline-flex' : 'none';
}

function closeModal() {
  dom.poModal.classList.remove('open');
  state.selectedPO = null;
}

window.issuePurchaseOrder = issuePurchaseOrder;
window.rejectPurchaseOrder = rejectPurchaseOrder;
window.openDetails = openDetails;

document.addEventListener('DOMContentLoaded', initApp);
