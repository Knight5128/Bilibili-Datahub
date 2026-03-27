function trackerHeaders(env, extraHeaders = {}) {
  const headers = {
    Authorization: `Bearer ${env.TRACKER_ADMIN_TOKEN}`,
    ...extraHeaders,
  };
  return headers;
}

export async function trackerFetch(env, path, init = {}) {
  if (!env.TRACKER_BASE_URL) {
    throw new Error("Missing TRACKER_BASE_URL.");
  }
  const url = new URL(path, env.TRACKER_BASE_URL.endsWith("/") ? env.TRACKER_BASE_URL : `${env.TRACKER_BASE_URL}/`);
  const response = await fetch(url.toString(), {
    ...init,
    headers: trackerHeaders(env, init.headers || {}),
  });
  if (init.expectRaw) {
    return response;
  }
  const text = await response.text();
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = { raw: text };
    }
  }
  if (!response.ok) {
    throw new Error(`Tracker API ${response.status}: ${JSON.stringify(data)}`);
  }
  return data;
}

export async function getTrackerStatus(env) {
  return trackerFetch(env, "/admin/status");
}

export async function getTrackerMetrics(env) {
  return trackerFetch(env, "/admin/metrics");
}

export async function getTrackerRunLogs(env, limit = 20) {
  return trackerFetch(env, `/admin/run-logs?limit=${encodeURIComponent(limit)}`);
}

export async function getTrackerAuthors(env) {
  return trackerFetch(env, "/admin/authors");
}

export async function updateTrackerConfig(env, payload) {
  return trackerFetch(env, "/admin/config/update", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function runTrackerCycle(env, force = false) {
  return trackerFetch(env, `/run${force ? "?force=true" : ""}`, {
    method: "POST",
  });
}

export async function proxyTrackerCsv(env, path) {
  const response = await trackerFetch(env, path, { method: "GET", expectRaw: true });
  if (!response.ok) {
    throw new Error(`Tracker CSV export failed: ${response.status} ${await response.text()}`);
  }
  return response;
}
