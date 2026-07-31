/**
 * SwitchBoard Brain Gateway
 * -------------------------
 * The translator that lets Telnyx voice call the product Hermes brain,
 * and the ONE place per-tenant walls are enforced.
 *
 * Telnyx AI Assistants support a "custom LLM": any endpoint that speaks the
 * OpenAI Chat Completions API (POST /v1/chat/completions). This service is that
 * endpoint. Each tenant's Telnyx assistant is pointed at:
 *
 *     https://<gateway-domain>/t/<tenantId>/v1
 *
 * so every request arrives stamped with which company it belongs to. We resolve
 * the tenant, load ONLY that tenant's context, forward the conversation to the
 * Hermes brain over Railway's private network, and stream the answer back.
 *
 * Design rules (non-negotiable):
 *  - FAIL CLOSED. Unknown/missing tenant -> refuse. Never guess, never mix.
 *  - NO BLEED. A request for tenant A can never touch tenant B's data.
 *  - NEVER DEAD AIR. If the brain is slow or errors, return a holding line.
 */

import express from "express";

const app = express();
app.use(express.json({ limit: "2mb" }));

// -- Config (all from env; safe defaults where harmless) ----------------------
const PORT = process.env.PORT || 3000;

// The product Hermes brain, reached over Railway's private network.
// e.g. http://hermes-agent.railway.internal:8642
const HERMES_URL = (process.env.HERMES_URL || "http://hermes-agent.railway.internal:8642").replace(/\/+$/, "");
const HERMES_API_KEY = process.env.HERMES_API_KEY || ""; // the brain's API_SERVER_KEY

// Auth for callers (Telnyx). Telnyx sends this as a Bearer token.
const GATEWAY_API_KEY = process.env.GATEWAY_API_KEY || "";

// Tenant allowlist - comma-separated tenant ids that are permitted.
// v1 uses this env list; swap resolveTenant() for a DB lookup when ready.
// Empty list = deny everyone (fail closed) - set at least one to go live.
const ALLOWED_TENANTS = new Set(
    (process.env.ALLOWED_TENANTS || "").split(",").map((s) => s.trim()).filter(Boolean)
  );

// How long we'll wait on the brain before we bail to a holding line (voice-tuned).
const BRAIN_DEADLINE_MS = Number(process.env.BRAIN_DEADLINE_MS || 18000);

const HOLDING_LINE = process.env.HOLDING_LINE || "Give me just one second.";

// -- Small helpers ------------------------------------------------------------
function log(tenant, ...args) {
    console.log(`[gateway]${tenant ? ` [t:${tenant}]` : ""}`, ...args);
}

function requireCallerAuth(req, res) {
    if (!GATEWAY_API_KEY) return true; // not configured -> allow (set it in prod)
  const auth = req.headers["authorization"] || "";
    const token = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";
    if (token !== GATEWAY_API_KEY) {
          res.status(401).json({ error: { message: "Invalid gateway credentials", type: "auth_error" } });
          return false;
    }
    return true;
}

/**
 * Resolve + validate the tenant. FAIL CLOSED.
 * v1: id comes from the URL path (/t/:tenantId/...) and must be on the allowlist.
 * TODO(v1.1): replace the allowlist check with a lookup in the tenants DB
 *             (active? not past-due?) so the admin kill-switch works here.
 */
function resolveTenant(req, res) {
    const tenantId = String(req.params.tenantId || "").trim();
    if (!tenantId) {
          res.status(400).json({ error: { message: "Missing tenant", type: "invalid_request" } });
          return null;
    }
    if (!ALLOWED_TENANTS.has(tenantId)) {
          // Unknown tenant -> refuse. This is the wall.
      log(tenantId, "REFUSED - tenant not on allowlist");
          res.status(403).json({ error: { message: "Unknown or disabled tenant", type: "tenant_denied" } });
          return null;
    }
    return tenantId;
}

/**
 * Per-tenant memory hooks. v1 is a no-op with clear seams so v1.1 can drop in
 * a real store (Postgres) WITHOUT touching the call flow. Each function is
 * strictly scoped to one tenantId - that scoping is the isolation guarantee.
 */
async function loadTenantMemory(/* tenantId */) {
    // TODO(v1.1): SELECT memory for this tenant only; return a short context blob.
  return "";
}
async function saveTenantTurn(/* tenantId, userText, assistantText */) {
    // TODO(v1.1): INSERT this turn into the tenant's own memory rows.
}
/**
 * Flatten an OpenAI messages[] array into a single transcript for Hermes.
 * Telnyx sends the whole conversation each turn, so we hand Hermes the context
 * it needs and ask it to continue as the assistant.
 */
function buildBrainInput(messages, memory) {
    const parts = [];
    if (memory) parts.push(`What you remember about this contact/company:\n${memory}\n`);
    for (const m of messages || []) {
          const role = m.role === "assistant" ? "Assistant" : m.role === "system" ? "Instructions" : "Caller";
          const content = typeof m.content === "string" ? m.content : JSON.stringify(m.content);
          parts.push(`${role}: ${content}`);
    }
    parts.push("Assistant:");
    return parts.join("\n");
}

/**
 * Run the Hermes brain for one turn and return its reply text.
 * Creates a run, streams the event feed, collects the output. Throws on
 * timeout/error so the caller can fall back to a holding line (never dead air).
 */
async function runBrain(input, tenantId) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), BRAIN_DEADLINE_MS);
    try {
          // 1. Start the run
      const startRes = await fetch(`${HERMES_URL}/v1/runs`, {
              method: "POST",
              headers: {
                        Authorization: `Bearer ${HERMES_API_KEY}`,
                        "Content-Type": "application/json",
              },
              body: JSON.stringify({ input }),
              signal: ctrl.signal,
      });
          if (!startRes.ok) throw new Error(`brain start HTTP ${startRes.status}`);
          const startJson = await startRes.json();
          const runId = startJson.run_id || startJson.id || (startJson.data && startJson.data.run_id);
          if (!runId) throw new Error("brain returned no run id");

      // 2. Stream the events, collect the final output
      const evRes = await fetch(`${HERMES_URL}/v1/runs/${runId}/events`, {
              headers: { Authorization: `Bearer ${HERMES_API_KEY}` },
              signal: ctrl.signal,
      });
          if (!evRes.ok || !evRes.body) throw new Error(`brain events HTTP ${evRes.status}`);

      const reader = evRes.body.getReader();
          const decoder = new TextDecoder();
          let buf = "";
          let output = "";
          let deltas = "";
          while (true) {
                  const { done, value } = await reader.read();
                  if (done) break;
                  buf += decoder.decode(value, { stream: true });
                  const lines = buf.split("\n");
                  buf = lines.pop() || "";
                  for (const line of lines) {
                            const s = line.trim();
                            if (!s.startsWith("data:")) continue;
                            const payload = s.slice(5).trim();
                            if (!payload || payload === "[DONE]") continue;
                            let ev;
                            try { ev = JSON.parse(payload); } catch { continue; }
                            if (ev.event === "message.delta" && typeof ev.delta === "string") deltas += ev.delta;
                            if (ev.event === "run.completed") {
                                        output = ev.output || deltas || output;
                            }
                  }
          }
          const text = (output || deltas || "").trim();
          if (!text) throw new Error("brain produced empty output");
          return text;
    } finally {
          clearTimeout(timer);
    }
}
// Strip things the TTS engine shouldn't read aloud (markdown, stray symbols).
function cleanForVoice(text) {
    return String(text).replace(/[*_#`>]/g, "").replace(/\s+\n/g, "\n").replace(/[ \t]{2,}/g, " ").trim();
}

// -- OpenAI-compatible response shapes ----------------------------------------
function completionJson(model, content) {
    return {
          id: `chatcmpl-${Date.now()}`,
          object: "chat.completion",
          created: Math.floor(Date.now() / 1000),
          model: model || "switchboard-brain",
          choices: [{ index: 0, message: { role: "assistant", content }, finish_reason: "stop" }],
          usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
    };
}

function streamChunks(res, model, content) {
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    const id = `chatcmpl-${Date.now()}`;
    const base = { id, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model: model || "switchboard-brain" };
    // role chunk, then the content in one delta, then stop. (Token-by-token
  // streaming from the brain is a v1.1 tuning item for snappier voice.)
  res.write(`data: ${JSON.stringify({ ...base, choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] })}\n\n`);
    res.write(`data: ${JSON.stringify({ ...base, choices: [{ index: 0, delta: { content }, finish_reason: null }] })}\n\n`);
    res.write(`data: ${JSON.stringify({ ...base, choices: [{ index: 0, delta: {}, finish_reason: "stop" }] })}\n\n`);
    res.write("data: [DONE]\n\n");
    res.end();
}

// -- Routes -------------------------------------------------------------------
app.get("/health", async (_req, res) => {
    let brain = "unknown";
    try {
          const r = await fetch(`${HERMES_URL}/health`, { signal: AbortSignal.timeout(4000) });
          brain = r.ok ? "ok" : `http_${r.status}`;
    } catch { brain = "unreachable"; }
    res.json({ status: "ok", service: "switchboard-brain-gateway", brain, tenants: ALLOWED_TENANTS.size });
});

// Some clients probe /models - answer with our single virtual model.
app.get("/t/:tenantId/v1/models", (req, res) => {
    if (!requireCallerAuth(req, res)) return;
    const tenantId = resolveTenant(req, res);
    if (!tenantId) return;
    res.json({ object: "list", data: [{ id: "switchboard-brain", object: "model", owned_by: "switchboard" }] });
});

// The main event: Telnyx (custom LLM) calls this every turn of every call.
app.post("/t/:tenantId/v1/chat/completions", async (req, res) => {
    if (!requireCallerAuth(req, res)) return;
    const tenantId = resolveTenant(req, res);
    if (!tenantId) return;

           const { messages, model, stream } = req.body || {};
    if (!Array.isArray(messages) || messages.length === 0) {
          return res.status(400).json({ error: { message: "messages[] required", type: "invalid_request" } });
    }

           let reply;
    try {
          const memory = await loadTenantMemory(tenantId);       // scoped to THIS tenant only
      const input = buildBrainInput(messages, memory);
          const t0 = Date.now();
          reply = cleanForVoice(await runBrain(input, tenantId));
          log(tenantId, `brain replied in ${Date.now() - t0}ms`);
          const lastUser = [...messages].reverse().find((m) => m.role === "user");
          saveTenantTurn(tenantId, lastUser && lastUser.content, reply).catch((e) => log(tenantId, "memory save failed:", e.message));
    } catch (e) {
          // Never dead air. Hand back a natural holding line and keep the call alive.
      log(tenantId, "brain failed:", e.message, "-> holding line");
          reply = HOLDING_LINE;
    }

           if (stream) return streamChunks(res, model, reply);
    return res.json(completionJson(model, reply));
});

app.get("/", (_req, res) => res.json({ service: "switchboard-brain-gateway", ok: true }));

app.listen(PORT, () => {
    log(null, `listening on :${PORT}`);
    log(null, `brain -> ${HERMES_URL}`);
    log(null, `tenants allowed: ${ALLOWED_TENANTS.size ? [...ALLOWED_TENANTS].join(", ") : "(none - fail closed)"}`);
});
