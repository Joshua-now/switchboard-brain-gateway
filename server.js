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
 *  - FAIL CLOSED. Unknown/missing/disabled tenant -> refuse. Never guess, never mix.
 *  - NO BLEED. A request for tenant A can never touch tenant B's data.
 *  - NEVER DEAD AIR. If the brain is slow or errors, return a holding line.
 *
 * Tenant source of truth (bulletproof):
 *  - If DATABASE_URL is set, the gateway reads the SAME Postgres SwitchBoard uses.
 *    A tenant is valid only if a row exists in "clients" with status = 'ACTIVE'.
 *    That means your admin kill-switch (disable / past-due) reaches the brain too,
 *    and there is ONE tenant roster, not two.
 *  - If DATABASE_URL is NOT set, we fall back to the ALLOWED_TENANTS env list.
 *    So this upgrade is additive and safe: nothing breaks before the DB is wired.
 */

import express from "express";
import pg from "pg";

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

// Tenant allowlist - comma-separated tenant ids. Used ONLY when DATABASE_URL is
// not set (fallback mode). Empty list = deny everyone (fail closed).
const ALLOWED_TENANTS = new Set(
      (process.env.ALLOWED_TENANTS || "").split(",").map((s) => s.trim()).filter(Boolean)
    );

// Shared Postgres (SwitchBoard's Supabase DB). When present, this is the tenant
// source of truth. When absent, we use ALLOWED_TENANTS above.
const DATABASE_URL = process.env.DATABASE_URL || "";

// How long a tenant "is this client active?" answer is trusted before we re-check
// the DB. Keeps us off the DB on every single voice turn.
const TENANT_CACHE_TTL_MS = Number(process.env.TENANT_CACHE_TTL_MS || 30000);

// How long we'll wait on the brain before we bail to a holding line (voice-tuned).
const BRAIN_DEADLINE_MS = Number(process.env.BRAIN_DEADLINE_MS || 18000);

const HOLDING_LINE = process.env.HOLDING_LINE || "Give me just one second.";

// -- Postgres pool (only when a DATABASE_URL is configured) -------------------
let pool = null;
if (DATABASE_URL) {
      pool = new pg.Pool({
              connectionString: DATABASE_URL,
              max: 5,
              idleTimeoutMillis: 30000,
              // Supabase/managed Postgres needs TLS; localhost does not.
              ssl: /localhost|127\.0\.0\.1/.test(DATABASE_URL) ? false : { rejectUnauthorized: false },
      });
      pool.on("error", (e) => console.log("[gateway] pg pool error:", e.message));
}

// tenantId -> { ok: boolean, t: epoch_ms }. Short-lived, with a stale-safe read
// on DB errors so a brief DB blip never drops calls for a known-active tenant.
const tenantCache = new Map();

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
 * Is this tenant allowed to use the gateway right now?
 *  - DB mode: exists in "clients" AND status = 'ACTIVE'. Cached for TTL.
 *             On a DB error we reuse the last known answer if we have one
 *             (stale but safe), otherwise FAIL CLOSED.
 *  - Fallback mode (no DATABASE_URL): must be in ALLOWED_TENANTS.
 */
async function isTenantAllowed(tenantId) {
      if (!pool) return ALLOWED_TENANTS.has(tenantId);

  const cached = tenantCache.get(tenantId);
      if (cached && Date.now() - cached.t < TENANT_CACHE_TTL_MS) return cached.ok;

  try {
          const r = await pool.query("SELECT status FROM clients WHERE id = $1 LIMIT 1", [tenantId]);
          const ok = r.rows.length > 0 && String(r.rows[0].status).toUpperCase() === "ACTIVE";
          tenantCache.set(tenantId, { ok, t: Date.now() });
          return ok;
  } catch (e) {
          log(tenantId, "tenant DB check failed:", e.message);
          if (cached) return cached.ok; // stale-safe: don't drop a known tenant on a blip
        return false; // never seen it + DB down -> fail closed
  }
}

/**
 * Resolve + validate the tenant. FAIL CLOSED. Async because it may hit the DB.
 * id comes from the URL path (/t/:tenantId/...) and, in DB mode, must map to an
 * ACTIVE client row. That row IS the wall and the kill-switch.
 */
async function resolveTenant(req, res) {
      const tenantId = String(req.params.tenantId || "").trim();
      if (!tenantId) {
              res.status(400).json({ error: { message: "Missing tenant", type: "invalid_request" } });
              return null;
      }
      if (!(await isTenantAllowed(tenantId))) {
              log(tenantId, "REFUSED - unknown or not ACTIVE");
              res.status(403).json({ error: { message: "Unknown or disabled tenant", type: "tenant_denied" } });
              return null;
      }
      return tenantId;
}

/**
 * Per-tenant memory hooks. Still stubbed on purpose: memory gets its own focused
 * pass so we design it right (keyed per caller, what to keep, how to recall) in
 * this SAME shared DB, scoped by tenant. Left as clean seams so wiring it later
 * does not touch the call flow. Each function is strictly scoped to one tenantId.
 */
async function loadTenantMemory(/* tenantId */) {
      // TODO(memory pass): SELECT memory for this tenant only; return a short blob.
  return "";
}
async function saveTenantTurn(/* tenantId, userText, assistantText */) {
      // TODO(memory pass): INSERT this turn into the tenant's own memory rows.
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

          const tenantSource = pool ? "database" : "allowlist";
      let db = pool ? "unknown" : "off";
      if (pool) {
              try { await pool.query("SELECT 1"); db = "ok"; } catch { db = "unreachable"; }
      }

          res.json({
                  status: "ok",
                  service: "switchboard-brain-gateway",
                  brain,
                  tenantSource,
                  db,
                  tenants: pool ? undefined : ALLOWED_TENANTS.size,
          });
});

// Some clients probe /models - answer with our single virtual model.
app.get("/t/:tenantId/v1/models", async (req, res) => {
      if (!requireCallerAuth(req, res)) return;
      const tenantId = await resolveTenant(req, res);
      if (!tenantId) return;
      res.json({ object: "list", data: [{ id: "switchboard-brain", object: "model", owned_by: "switchboard" }] });
});

// The main event: Telnyx (custom LLM) calls this every turn of every call.
app.post("/t/:tenantId/v1/chat/completions", async (req, res) => {
      if (!requireCallerAuth(req, res)) return;
      const tenantId = await resolveTenant(req, res);
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
      log(null, `tenant source: ${pool ? "DATABASE (clients.status = ACTIVE)" : `allowlist [${ALLOWED_TENANTS.size ? [...ALLOWED_TENANTS].join(", ") : "none - fail closed"}]`}`);
});
