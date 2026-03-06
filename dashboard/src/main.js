import "./styles.css";
import { byId, errorMessage, fmtDateTime, setApiStatus, setMessage, stamp } from "./lib/dom";
import { apiGet } from "./lib/http";
import { ENDPOINT_OPTIONS, buildCsvRequest, buildRequestFromForm } from "./lib/requests";
import { shellHtml } from "./lib/template";

const state = {
  endpoint: "pools_top",
  intervalSeconds: 5,
  pollTimer: null,
  inFlight: false,
  autoRun: true
};

renderShell();
bindEvents();
restartPollTimer();
refreshHealth();
runApiRequest();

function renderShell() {
  byId("app").innerHTML = shellHtml(state.endpoint, ENDPOINT_OPTIONS);
}

function bindEvents() {
  byId("interval-seconds").addEventListener("change", (event) => {
    const parsed = Number(event.target.value);
    state.intervalSeconds = Number.isFinite(parsed) && parsed > 0 ? parsed : 5;
    restartPollTimer();
  });
  byId("endpoint").addEventListener("change", (event) => {
    state.endpoint = event.target.value;
  });
  byId("auto-run").addEventListener("change", (event) => {
    state.autoRun = Boolean(event.target.checked);
  });

  byId("run-request").addEventListener("click", runApiRequest);
  byId("refresh-health").addEventListener("click", refreshHealth);
  byId("copy-curl").addEventListener("click", copyCurl);
  byId("clear-response").addEventListener("click", () => {
    byId("response-view").textContent = "Response cleared.";
  });
  byId("download-csv").addEventListener("click", downloadCsv);
  byId("preview-csv").addEventListener("click", previewCsv);
}

function restartPollTimer() {
  if (state.pollTimer) {
    window.clearInterval(state.pollTimer);
  }
  const interval = Math.max(3, state.intervalSeconds) * 1000;
  state.pollTimer = window.setInterval(() => {
    refreshHealth();
    if (state.autoRun) {
      runApiRequest();
    }
  }, interval);
}

async function refreshHealth() {
  try {
    const health = await apiGet("/health");
    setApiStatus(Boolean(health.clickhouse_ok), health.clickhouse_ok ? "ok" : "degraded");
    byId("status-refresh").textContent = fmtDateTime(Date.now());
  } catch (error) {
    setApiStatus(false, "error");
    setMessage(`health failed: ${errorMessage(error)}`);
  }
}

async function runApiRequest() {
  if (state.inFlight) return;
  state.inFlight = true;
  const startedAt = performance.now();
  try {
    const request = buildRequestFromForm();
    byId("status-endpoint").textContent = request.path;
    byId("request-url").textContent = request.url;
    const data = await apiGet(request.path, request.params);
    const latencyMs = Math.round(performance.now() - startedAt);
    byId("request-latency").textContent = `${latencyMs} ms`;
    byId("response-view").textContent = JSON.stringify(data, null, 2);
    byId("status-refresh").textContent = fmtDateTime(Date.now());
    setMessage("request completed");
  } catch (error) {
    byId("response-view").textContent = String(errorMessage(error));
    setMessage(`request failed: ${errorMessage(error)}`);
  } finally {
    state.inFlight = false;
  }
}

async function downloadCsv() {
  try {
    const { response } = await fetchCsv();
    const blob = await response.blob();
    const href = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = href;
    anchor.download = `dlmm_events_${stamp()}.csv`;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    URL.revokeObjectURL(href);
    setMessage("csv downloaded");
  } catch (error) {
    setMessage(`csv failed: ${errorMessage(error)}`);
  }
}

async function previewCsv() {
  try {
    const { response } = await fetchCsv();
    const text = await response.text();
    byId("csv-preview").textContent = text
      .split("\n")
      .slice(0, 20)
      .join("\n");
    setMessage("csv preview updated");
  } catch (error) {
    setMessage(`csv preview failed: ${errorMessage(error)}`);
  }
}

async function fetchCsv() {
  const { url } = buildCsvRequest();
  byId("csv-url").textContent = url;
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error((await response.text()).slice(0, 300));
  }
  return { url, response };
}

async function copyCurl() {
  try {
    const req = buildRequestFromForm();
    const curl = `curl -sS '${req.url.replaceAll("'", "'\\''")}'`;
    await navigator.clipboard.writeText(curl);
    setMessage("cURL copied");
  } catch (error) {
    setMessage(`copy failed: ${errorMessage(error)}`);
  }
}
