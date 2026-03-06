export function apiBase() {
  const configured = import.meta.env.VITE_API_BASE;
  if (typeof configured === "string" && configured.length > 0) {
    return configured;
  }
  return window.location.origin;
}

export function buildUrl(path, params = {}) {
  const url = new URL(path, apiBase());
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") {
      return;
    }
    url.searchParams.set(key, String(value));
  });
  return url;
}

export async function apiGet(path, params = {}) {
  const url = buildUrl(path, params);
  const response = await fetch(url.toString(), {
    cache: "no-store",
    headers: { Accept: "application/json" }
  });

  const text = await response.text();
  if (!response.ok) {
    const message = text.length > 0 ? text : `http ${response.status}`;
    throw new Error(message);
  }

  const parsed = JSON.parse(text);
  if (parsed && typeof parsed === "object" && "error" in parsed && parsed.error) {
    throw new Error(String(parsed.error));
  }
  return parsed;
}
