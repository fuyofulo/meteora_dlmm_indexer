export function byId(id) {
  const node = document.getElementById(id);
  if (!node) {
    throw new Error(`missing element ${id}`);
  }
  return node;
}

export function getInput(id) {
  return byId(id).value.trim();
}

export function setMessage(message) {
  byId("status-message").textContent = message;
}

export function setApiStatus(ok, detail) {
  const node = byId("status-api");
  node.textContent = `${ok ? "online" : "degraded"} · ${detail}`;
  node.className = `status-value ${ok ? "tone-ok" : "tone-bad"}`;
}

export function statusPill(label, id, value) {
  return `
    <div class="status-pill">
      <span class="status-label">${escapeHtml(label)}</span>
      <span id="${id}" class="status-value">${escapeHtml(value)}</span>
    </div>
  `;
}

export function fmtDateTime(ms) {
  const value = Number(ms);
  if (!Number.isFinite(value) || value <= 0) {
    return "-";
  }
  return new Date(value).toLocaleString("en-US", { hour12: false });
}

export function stamp() {
  const d = new Date();
  const p = (v) => String(v).padStart(2, "0");
  return `${d.getFullYear()}${p(d.getMonth() + 1)}${p(d.getDate())}_${p(d.getHours())}${p(d.getMinutes())}${p(d.getSeconds())}`;
}

export function errorMessage(error) {
  if (error && typeof error === "object" && "message" in error && typeof error.message === "string" && error.message.length > 0) {
    return error.message;
  }
  if (typeof error === "string" && error.length > 0) {
    return error;
  }
  return "unknown";
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
