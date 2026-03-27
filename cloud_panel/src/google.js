const GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token";
const CLOUD_RUN_BASE = "https://run.googleapis.com/v2";
const CLOUD_SCHEDULER_BASE = "https://cloudscheduler.googleapis.com/v1";

function toBase64Url(input) {
  const bytes = typeof input === "string" ? new TextEncoder().encode(input) : input;
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function pemToArrayBuffer(pem) {
  const normalized = pem
    .replace("-----BEGIN PRIVATE KEY-----", "")
    .replace("-----END PRIVATE KEY-----", "")
    .replace(/\s+/g, "");
  const raw = atob(normalized);
  const bytes = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i += 1) {
    bytes[i] = raw.charCodeAt(i);
  }
  return bytes.buffer;
}

async function importPrivateKey(privateKeyPem) {
  return crypto.subtle.importKey(
    "pkcs8",
    pemToArrayBuffer(privateKeyPem),
    {
      name: "RSASSA-PKCS1-v1_5",
      hash: "SHA-256",
    },
    false,
    ["sign"],
  );
}

export async function getGoogleAccessToken(env) {
  const raw = env.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!raw) {
    throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_JSON secret.");
  }
  const serviceAccount = JSON.parse(raw);
  const issuedAt = Math.floor(Date.now() / 1000);
  const header = {
    alg: "RS256",
    typ: "JWT",
    kid: serviceAccount.private_key_id,
  };
  const claim = {
    iss: serviceAccount.client_email,
    scope: "https://www.googleapis.com/auth/cloud-platform",
    aud: GOOGLE_TOKEN_URL,
    exp: issuedAt + 3600,
    iat: issuedAt,
  };
  const unsigned = `${toBase64Url(JSON.stringify(header))}.${toBase64Url(JSON.stringify(claim))}`;
  const key = await importPrivateKey(serviceAccount.private_key);
  const signature = await crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, new TextEncoder().encode(unsigned));
  const jwt = `${unsigned}.${toBase64Url(new Uint8Array(signature))}`;
  const response = await fetch(GOOGLE_TOKEN_URL, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion: jwt,
    }),
  });
  if (!response.ok) {
    throw new Error(`Failed to obtain Google access token: ${response.status} ${await response.text()}`);
  }
  const payload = await response.json();
  return payload.access_token;
}

async function googleFetch(env, url, init = {}) {
  const token = await getGoogleAccessToken(env);
  const response = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "content-type": "application/json",
      ...(init.headers || {}),
    },
  });
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
    throw new Error(`Google API ${response.status}: ${JSON.stringify(data)}`);
  }
  return data;
}

export async function getCloudRunService(env) {
  const url = `${CLOUD_RUN_BASE}/projects/${env.GCP_PROJECT_ID}/locations/${env.GCP_REGION}/services/${env.CLOUD_RUN_SERVICE}`;
  return googleFetch(env, url, { method: "GET" });
}

export function extractServiceEnv(service) {
  const containers = service?.template?.containers || [];
  const container = containers[0] || {};
  const envVars = {};
  for (const item of container.env || []) {
    if (item?.name) {
      envVars[item.name] = item.value ?? "";
    }
  }
  return {
    serviceName: service?.name || "",
    uri: service?.uri || "",
    latestReadyRevision: service?.latestReadyRevision || "",
    image: container.image || "",
    timeout: container.timeout || "",
    env: envVars,
    scaling: service?.scaling || {},
  };
}

export async function updateCloudRunEnv(env, updates) {
  const service = await getCloudRunService(env);
  const currentContainers = service?.template?.containers || [];
  const firstContainer = currentContainers[0];
  if (!firstContainer) {
    throw new Error("Cloud Run service has no container definition.");
  }
  const currentEnv = new Map((firstContainer.env || []).map((item) => [item.name, item.value ?? ""]));
  for (const [key, value] of Object.entries(updates)) {
    if (value === null || value === undefined || value === "") {
      currentEnv.delete(key);
    } else {
      currentEnv.set(key, String(value));
    }
  }
  const nextContainers = [...currentContainers];
  nextContainers[0] = {
    ...firstContainer,
    env: [...currentEnv.entries()].map(([name, value]) => ({ name, value })),
  };
  const body = {
    template: {
      containers: nextContainers,
    },
  };
  const url = `${CLOUD_RUN_BASE}/projects/${env.GCP_PROJECT_ID}/locations/${env.GCP_REGION}/services/${env.CLOUD_RUN_SERVICE}?updateMask=template.containers`;
  return googleFetch(env, url, {
    method: "PATCH",
    body: JSON.stringify(body),
  });
}

export async function getSchedulerJob(env) {
  const url = `${CLOUD_SCHEDULER_BASE}/projects/${env.GCP_PROJECT_ID}/locations/${env.CLOUD_SCHEDULER_LOCATION}/jobs/${env.CLOUD_SCHEDULER_JOB}`;
  return googleFetch(env, url, { method: "GET" });
}

export async function pauseSchedulerJob(env) {
  const url = `${CLOUD_SCHEDULER_BASE}/projects/${env.GCP_PROJECT_ID}/locations/${env.CLOUD_SCHEDULER_LOCATION}/jobs/${env.CLOUD_SCHEDULER_JOB}:pause`;
  return googleFetch(env, url, { method: "POST", body: "{}" });
}

export async function resumeSchedulerJob(env) {
  const url = `${CLOUD_SCHEDULER_BASE}/projects/${env.GCP_PROJECT_ID}/locations/${env.CLOUD_SCHEDULER_LOCATION}/jobs/${env.CLOUD_SCHEDULER_JOB}:resume`;
  return googleFetch(env, url, { method: "POST", body: "{}" });
}

export async function fetchControlPlaneStatus(env) {
  const [service, scheduler] = await Promise.all([getCloudRunService(env), getSchedulerJob(env)]);
  return {
    cloudRun: extractServiceEnv(service),
    scheduler,
  };
}
