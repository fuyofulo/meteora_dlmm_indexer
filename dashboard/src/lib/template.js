import { statusPill } from "./dom";

export function shellHtml(endpoint, endpointOptions) {
  return `
    <main class="app-shell">
      <section class="status-row">
        ${statusPill("API", "status-api", "booting")}
        ${statusPill("Last Refresh", "status-refresh", "-")}
        ${statusPill("Last Endpoint", "status-endpoint", "-")}
        ${statusPill("Message", "status-message", "starting")}
      </section>

      <section class="grid-two">
        <article class="card">
          <h2>API Runner</h2>
          <div class="runner-top">
            <div class="control">
              <label for="interval-seconds">Refresh Interval</label>
              <select id="interval-seconds">
                <option value="3">3s</option>
                <option value="5" selected>5s</option>
                <option value="10">10s</option>
                <option value="20">20s</option>
              </select>
            </div>
            <div class="control check">
              <input id="auto-run" type="checkbox" checked />
              <label for="auto-run">Auto-run selected endpoint</label>
            </div>
          </div>
          <div class="form-grid">
            <div class="control">
              <label for="endpoint">Endpoint</label>
              <select id="endpoint">
                ${endpointOptions
                  .map(
                    (option) =>
                      `<option value="${option.value}" ${option.value === endpoint ? "selected" : ""}>${option.label}</option>`
                  )
                  .join("")}
              </select>
            </div>
            <div class="control">
              <label for="pool">Pool</label>
              <input id="pool" type="text" placeholder="Pool pubkey (required for pool_* endpoints)" />
            </div>
            <div class="control">
              <label for="user">User</label>
              <input id="user" type="text" placeholder="Optional user pubkey" />
            </div>
            <div class="control">
              <label for="event">Event Filter</label>
              <input id="event" type="text" placeholder="e.g. swap,swap2" />
            </div>
            <div class="control">
              <label for="minutes">Minutes</label>
              <input id="minutes" type="number" value="60" min="1" max="10080" />
            </div>
            <div class="control">
              <label for="limit">Limit</label>
              <input id="limit" type="number" value="100" min="1" max="100000" />
            </div>
            <div class="control">
              <label for="from-slot">From Slot</label>
              <input id="from-slot" type="number" min="0" placeholder="optional" />
            </div>
            <div class="control">
              <label for="to-slot">To Slot</label>
              <input id="to-slot" type="number" min="0" placeholder="optional" />
            </div>
          </div>
          <div class="action-row">
            <button id="run-request" class="btn btn-primary">Run Request</button>
            <button id="refresh-health" class="btn">Refresh Health</button>
            <button id="copy-curl" class="btn">Copy cURL</button>
            <button id="clear-response" class="btn">Clear Response</button>
          </div>
          <div class="request-meta">
            <div><span class="k">Request URL</span><code id="request-url">-</code></div>
            <div><span class="k">Latency</span><span id="request-latency">-</span></div>
          </div>
        </article>

        <article class="card">
          <h2>Response</h2>
          <pre id="response-view" class="response">Run a request to view JSON response.</pre>
        </article>
      </section>

      <section class="card">
        <h2>CSV Export</h2>
        <div class="form-grid">
          <div class="control">
            <label for="csv-pool">Pool</label>
            <input id="csv-pool" type="text" placeholder="optional pool pubkey" />
          </div>
          <div class="control">
            <label for="csv-user">User</label>
            <input id="csv-user" type="text" placeholder="optional user pubkey" />
          </div>
          <div class="control">
            <label for="csv-event">Event Filter</label>
            <input id="csv-event" type="text" placeholder="swap,swap2" />
          </div>
          <div class="control">
            <label for="csv-limit">CSV Limit</label>
            <input id="csv-limit" type="number" value="5000" min="1" max="100000" />
          </div>
          <div class="control">
            <label for="csv-from-slot">From Slot</label>
            <input id="csv-from-slot" type="number" min="0" placeholder="optional" />
          </div>
          <div class="control">
            <label for="csv-to-slot">To Slot</label>
            <input id="csv-to-slot" type="number" min="0" placeholder="optional" />
          </div>
        </div>
        <div class="action-row">
          <button id="download-csv" class="btn btn-primary">Download CSV</button>
          <button id="preview-csv" class="btn">Preview CSV (first lines)</button>
        </div>
        <div class="request-meta">
          <div><span class="k">CSV URL</span><code id="csv-url">-</code></div>
        </div>
        <pre id="csv-preview" class="response small">CSV preview will appear here.</pre>
      </section>
    </main>
  `;
}
