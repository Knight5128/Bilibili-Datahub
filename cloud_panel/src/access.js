export function requireAccess(request, env) {
  const required = (env.CF_ACCESS_REQUIRED || "true").toLowerCase() !== "false";
  if (!required) {
    return { ok: true, email: "access-disabled@example.local" };
  }
  const email =
    request.headers.get("CF-Access-Authenticated-User-Email") ||
    request.headers.get("Cf-Access-Authenticated-User-Email") ||
    "";
  if (!email) {
    return { ok: false, response: jsonResponse({ error: "Cloudflare Access authentication required." }, 401) };
  }
  const allowedRaw = (env.CF_ACCESS_ALLOWED_EMAILS || "").trim();
  if (allowedRaw) {
    const allowed = allowedRaw
      .split(",")
      .map((item) => item.trim().toLowerCase())
      .filter(Boolean);
    if (!allowed.includes(email.toLowerCase())) {
      return { ok: false, response: jsonResponse({ error: "Access denied for this account." }, 403) };
    }
  }
  return { ok: true, email };
}

export function jsonResponse(body, status = 200, init = {}) {
  return new Response(JSON.stringify(body, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...(init.headers || {}),
    },
    ...init,
  });
}

export function textResponse(body, status = 200, contentType = "text/plain; charset=utf-8") {
  return new Response(body, {
    status,
    headers: { "content-type": contentType },
  });
}
