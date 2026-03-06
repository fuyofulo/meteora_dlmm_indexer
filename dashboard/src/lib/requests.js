import { byId, getInput } from "./dom";
import { buildUrl } from "./http";

export const ENDPOINT_OPTIONS = [
  { value: "health", label: "GET /health" },
  { value: "ingestion_lag", label: "GET /v1/ingestion/lag" },
  { value: "swaps", label: "GET /v1/swaps" },
  { value: "pools_top", label: "GET /v1/pools/top" },
  { value: "pool_summary", label: "GET /v1/pools/{pool}/summary" },
  { value: "pool_events", label: "GET /v1/pools/{pool}/events" }
];

export function buildRequestFromForm() {
  const endpoint = byId("endpoint").value;
  const pool = getInput("pool");
  const user = getInput("user");
  const event = getInput("event");
  const minutes = getInput("minutes");
  const limit = getInput("limit");
  const fromSlot = getInput("from-slot");
  const toSlot = getInput("to-slot");

  const params = {};

  if (endpoint === "health") {
    return toRequest("/health", params);
  }
  if (endpoint === "ingestion_lag") {
    return toRequest("/v1/ingestion/lag", params);
  }
  if (endpoint === "swaps") {
    setIfPresent(params, "limit", limit);
    setIfPresent(params, "pool", pool);
    setIfPresent(params, "user", user);
    setIfPresent(params, "from_slot", fromSlot);
    setIfPresent(params, "to_slot", toSlot);
    return toRequest("/v1/swaps", params);
  }
  if (endpoint === "pools_top") {
    setIfPresent(params, "minutes", minutes);
    setIfPresent(params, "limit", limit);
    return toRequest("/v1/pools/top", params);
  }
  if (endpoint === "pool_summary") {
    if (!pool) {
      throw new Error("pool is required for /v1/pools/{pool}/summary");
    }
    setIfPresent(params, "minutes", minutes);
    return toRequest(`/v1/pools/${encodeURIComponent(pool)}/summary`, params);
  }
  if (endpoint === "pool_events") {
    if (!pool) {
      throw new Error("pool is required for /v1/pools/{pool}/events");
    }
    setIfPresent(params, "limit", limit);
    setIfPresent(params, "event", event);
    setIfPresent(params, "user", user);
    setIfPresent(params, "from_slot", fromSlot);
    setIfPresent(params, "to_slot", toSlot);
    return toRequest(`/v1/pools/${encodeURIComponent(pool)}/events`, params);
  }

  return toRequest("/health", params);
}

export function buildCsvRequest() {
  const params = {};
  setIfPresent(params, "pool", getInput("csv-pool"));
  setIfPresent(params, "user", getInput("csv-user"));
  setIfPresent(params, "event", getInput("csv-event"));
  setIfPresent(params, "limit", getInput("csv-limit"));
  setIfPresent(params, "from_slot", getInput("csv-from-slot"));
  setIfPresent(params, "to_slot", getInput("csv-to-slot"));

  const url = buildUrl("/v1/export/events.csv", params);
  return { params, url: url.toString() };
}

function toRequest(path, params) {
  const url = buildUrl(path, params);
  return { path, params, url: url.toString() };
}

function setIfPresent(params, key, value) {
  if (value !== "") {
    params[key] = value;
  }
}
