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
 * Hermes brain over Railway's private network, and return the answer.
 *
 * TOOLS (transfer / booking): when the assistant has tools, Telnyx sends them in
 * the request (OpenAI function-calling) and expects tool_calls back. Hermes is a
 * text brain, so we describe the tools to it and ask it to emit a single ACTION
 * line when one is needed; we translate that into OpenAI tool_calls. If Hermes
 * doesn't ask for a tool, we return its words. Fail-safe: if anything about the
 * action is malformed, we SPEAK instead of firing a bad tool call.
 *
 * Design rules (non-negotiable):
 *  - FAIL CLOSED. Unknown/missing/disabled tenant -> refuse. Never guess, never mix.
 *  - NO BLEED. A request for tenant A can never touch tenant B's data.
 *  - NEVER DEAD AIR. If the brain is slow or errors, return a holding line.
 *  - NEVER A BAD TOOL CALL. A malformed action becomes speech, not a wrong transfer.
 *
 * Tenant source of truth: if DATABASE_URL is set, the gateway reads the SAME
 * Postgres SwitchBoard uses (clients.status = 'ACTIVE'); otherwise it falls back
 * to the ALLOWED_TENANTS env list. Additive and safe.
 */

import express from "express";
import pg from "pg";

const app = express();
app.use(express.json({ limit: "2mb" }));

// -- Config (all from env; safe defaults where harmless) ----------------------
const PORT = process.env.PORT || 3000;

const HERMES_URL = (process.env.HERMES_URL || "http://hermes-agent.railway.internal:8642").replace(/\/+$/, "");
const HERMES_API_KEY = process.env.HERMES_API_KEY || ""; // the brain's API_SERVER_KEY

const GATEWAY_API_KEY = process.env.GATEWAY_API_KEY || ""; // Telnyx presents this as Bearer

// Tenant allowlist - used ONLY when DATABASE_URL is not set (fallback mode).
const ALLOWED_TENANTS = new Set(
        (process.env.ALLOWED_TENANTS || "").split(",").map((s) => s.trim()).filter(Boolean)
      );

// Shared Postgres (SwitchBoard's Supabase DB). When present, tenant source of truth.
const DATABASE_URL = process.env.DATABASE_URL || "";
const TENANT_CACHE_TTL_MS = Number(process.env.TENANT_CACHE_TTL_MS || 30000);

const BRAIN_DEADLINE_MS = Number(process.env.BRAIN_DEADLINE_MS || 18000);
const HOLDING_LINE = process.env.HOLDING_LINE || "Give me just one second.";

// Log the shape of each incoming turn (tool presence, names) so we can verify the
// exact Telnyx custom-LLM contract from deploy logs. No caller content is logged.
const LOG_REQUEST_SHAPE = process.env.LOG_REQUEST_SHAPE !== "false";

// -- Postgres pool (only when a DATABASE_URL is configured) -------------------
let pool = null;
if (DATABASE_URL) {
        pool = new pg.Pool({
                  connectionString: DATABASE_URL,
                  max: 5,
                  idleTimeoutMillis: 30000,
                  ssl: /localhost|127\.0\.0\.1/.test(DATABASE_URL) ? false : { rejectUnauthorized: false },
        });
        pool.on("error", (e) => console.log("[gateway] pg pool error:", e.message));
}

const tenantCache = new Map(); // tenantId -> { ok, t }

// -- Small helpers ------------------------------------------------------------
function log(tenant, ...args) {
        console.log(`[gateway]${tenant ? ` [t:${tenant}]` : ""}`, ...args);
}

function requireCallerAuth(req, res) {
        if (!GATEWAY_API_KEY) return true;
        const auth = req.headers["authorization"] || "";
        const token = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";
        if (token !== GATEWAY_API_KEY) {
                  res.status(401).json({ error: { message: "Invalid gateway credentials", type: "auth_error" } });
                  return false;
        }
        return true;
}

/**
 * Is this tenant allowed right now? DB mode: exists in "clients" AND ACTIVE,
 * cached, stale-safe on DB error. Fallback mode: ALLOWED_TENANTS env list.
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
                  if (cached) return cached.ok; // stale-safe
          return false; // fail closed
        }
}

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
 * Per-tenant memory hooks (still stubbed on purpose - its own pass). Note: with
 * Telnyx, memory already rides in as {{memory}} inside the system prompt, so this
 * is a future secondary store, not the primary path.
 */
async function loadTenantMemory(/* tenantId */) { return ""; }
async function saveTenantTurn(/* tenantId, userText, assistantText */) {}

// Normalize the tool list Telnyx sends into { name, description, parameters }.
function normalizeTools(tools) {
        if (!Array.isArray(tools)) return [];
        return tools
          .map((t) => (t && t.function ? t.function : t))
          .filter((f) => f && typeof f.name === "string")
          .map((f) => ({ name: f.name, description: f.description || "", parameters: f.parameters || {} }));
}

/**
 * Flatten the OpenAI messages[] into a single transcript for Hermes, and - when
 * tools are present - describe them and how to invoke one via a single ACTION line.
 */
function buildBrainInput(messages, memory, tools) {
        const parts = [];
        if (memory) parts.push(`What you remember about this contact/company:\n${memory}\n`);
        for (const m of messages || []) {
                  let role;
                  if (m.role === "assistant") role = "Assistant";
                  else if (m.role === "system") role = "Instructions";
                  else if (m.role === "tool") role = "Tool result";
                  else role = "Caller";
                  const content = typeof m.content === "string" ? m.content : JSON.stringify(m.content);
                  parts.push(`${role}: ${content}`);
        }
        if (tools && tools.length) {
                  parts.push("");
                  parts.push("=== ACTIONS YOU CAN TAKE ===");
                  parts.push("Besides speaking to the caller, you may trigger ONE of these actions when it is clearly what the caller needs:");
                  for (const f of tools) {
                              parts.push(`- ${f.name}: ${f.description || "(no description)"}  [arguments: ${JSON.stringify(f.parameters || {})}]`);
                  }
                  parts.push("");
                  parts.push('To take an action, reply with EXACTLY one line and NOTHING else:');
                  parts.push('ACTION: {"name":"<action name>","arguments":{ ...match the schema... }}');
                  parts.push("Otherwise, reply normally with what to say out loud. Never output both speech and an ACTION. Only act when the caller clearly needs it.");
        }
        parts.push("Assistant:");
        return parts.join("\n");
}

/**
 * Interpret Hermes' raw output. If it asked for a valid, known tool -> a toolCall.
 * Anything malformed or unknown -> treat as speech (fail-safe: never a bad action).
 */
function parseBrainReply(raw, tools) {
        const text = String(raw || "").trim();
        const toolNames = new Set((tools || []).map((f) => f.name));
        const m = text.match(/ACTION:\s*(\{[\s\S]*\})\s*$/);
        if (m) {
                  try {
                              const obj = JSON.parse(m[1]);
                              if (obj && typeof obj.name === "string" && toolNames.has(obj.name)) {
                                            const args = obj.arguments && typeof obj.arguments === "object" ? obj.arguments : {};
                                            return { toolCall: { name: obj.name, arguments: args } };
                              }
                  } catch { /* fall through to speech */ }
                  // ACTION was present but malformed/unknown -> speak whatever came before it.
          const before = text.slice(0, m.index).trim();
                  return { content: before || HOLDING_LINE };
        }
        return { content: text };
}

/**
 * Run the Hermes brain for one turn: create a run, stream events, collect output.
 * Throws on timeout/error so the caller can fall back to a holding line.
 */
async function runBrain(input) {
        const ctrl = new AbortController();
        const timer = setTimeout(() => ctrl.abort(), BRAIN_DEADLINE_MS);
        try {
                  const startRes = await fetch(`${HERMES_URL}/v1/runs`, {
                              method: "POST",
                              headers: { Authorization: `Bearer ${HERMES_API_KEY}`, "Content-Type": "application/json" },
                              body: JSON.stringify({ input }),
                              signal: ctrl.signal,
                  });
                  if (!startRes.ok) throw new Error(`brain start HTTP ${startRes.status}`);
                  const startJson = await startRes.json();
                  const runId = startJson.run_id || startJson.id || (startJson.data && startJson.data.run_id);
                  if (!runId) throw new Error("brain returned no run id");

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
                                            if (ev.event === "run.completed") output = ev.output || deltas || output;
                              }
                  }
                  const text = (output || deltas || "").trim();
                  if (!text) throw new Error("brain produced empty output");
                  return text;
        } finally {
                  clearTimeout(timer);
        }
}

// Strip things the TTS engine shouldn't read aloud (only applied to spoken text).
function cleanForVoice(text) {
        return String(text).replace(/[*_#`>]/g, "").replace(/\s+\n/g, "\n").replace(/[ \t]{2,}/g, " ").trim();
}

// -- OpenAI-compatible response shapes ----------------------------------------
function buildMessage(reply) {
        if (reply.toolCall) {
                  return {
                              role: "assistant",
                              content: null,
                              tool_calls: [{
                                            id: `call_${Date.now()}`,
                                            type: "function",
                                            function: { name: reply.toolCall.name, arguments: JSON.stringify(reply.toolCall.arguments || {}) },
                              }],
                  };
        }
        return { role: "assistant", content: reply.content };
}

function completionJson(model, reply) {
        return {
                  id: `chatcmpl-${Date.now()}`,
                  object: "chat.completion",
                  created: Math.floor(Date.now() / 1000),
                  model: model || "switchboard-brain",
                  choices: [{ index: 0, message: buildMessage(reply), finish_reason: reply.toolCall ? "tool_calls" : "stop" }],
                  usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
        };
}

// Telnyx voice sends stream=true. Instead of waiting for the ENTIRE brain reply
// (which ran ~3-7s and timed out the turn -> dead air), open the Telnyx stream
// immediately and forward the brain's words AS THEY ARE GENERATED. Speech streams
// live; a tool ACTION (which the brain emits as the whole reply) is buffered, then
// sent as a tool_call. Never dead air: a stall/error still yields a holding line.
async function streamBrainToTelnyx(res, tenantId, model, tools, messages) {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  const id = `chatcmpl-${Date.now()}`;
  const base = { id, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model: model || "switchboard-brain" };
  const send = (delta, fin) => res.write(`data: ${JSON.stringify({ ...base, choices: [{ index: 0, delta, finish_reason: fin || null }] })}\n\n`);
  const finish = () => { try { res.write("data: [DONE]\n\n"); res.end(); } catch {} };

  send({ role: "assistant" }, null); // open the turn immediately so Telnyx sees the stream is live

  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), BRAIN_DEADLINE_MS);
  const t0 = Date.now();
  let firstAt = 0;          // ms to first token (reveals whether the brain streams)
  let mode = "deciding";    // deciding | speech | action
  let full = "";
  let spoke = false;

  const flushSpeech = () => {
    const cleaned = cleanForVoice(full);
    if (cleaned) { send({ content: cleaned }, null); spoke = true; }
    mode = "speech";
  };

  try {
    const memory = await loadTenantMemory(tenantId);
    const input = buildBrainInput(messages, memory, tools);

    const startRes = await fetch(`${HERMES_URL}/v1/runs`, {
      method: "POST",
      headers: { Authorization: `Bearer ${HERMES_API_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({ input }),
      signal: ctrl.signal,
    });
    if (!startRes.ok) throw new Error(`brain start HTTP ${startRes.status}`);
    const startJson = await startRes.json();
    const runId = startJson.run_id || startJson.id || (startJson.data && startJson.data.run_id);
    if (!runId) throw new Error("brain returned no run id");

    const evRes = await fetch(`${HERMES_URL}/v1/runs/${runId}/events`, {
      headers: { Authorization: `Bearer ${HERMES_API_KEY}` },
      signal: ctrl.signal,
    });
    if (!evRes.ok || !evRes.body) throw new Error(`brain events HTTP ${evRes.status}`);

    const reader = evRes.body.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    let finalOutput = "";
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
        if (ev.event === "message.delta" && typeof ev.delta === "string") {
          if (!firstAt) firstAt = Date.now() - t0;
          full += ev.delta;
          if (mode === "deciding") {
            const head = full.replace(/^\s+/, "");
            if (head.toUpperCase().startsWith("ACTION:")) mode = "action";
            else if (head.length >= 7) flushSpeech(); // enough to know it is NOT an action
          } else if (mode === "speech") {
            const cleaned = cleanForVoice(ev.delta);
            if (cleaned) { send({ content: cleaned }, null); spoke = true; }
          }
          // action mode: keep accumulating in `full`, emit nothing yet
        }
        if (ev.event === "run.completed") finalOutput = ev.output || full || finalOutput;
      }
    }
    clearTimeout(timer);

    const finalText = (finalOutput || full || "").trim();
    if (mode === "deciding") { // reply too short to decide mid-stream — decide now
      const head = finalText.replace(/^\s+/, "");
      if (head.toUpperCase().startsWith("ACTION:")) mode = "action";
      else { full = finalText; flushSpeech(); }
    }

    if (mode === "action") {
      const reply = parseBrainReply(finalText, tools);
      if (reply.toolCall) {
        send({ tool_calls: [{ index: 0, id: `call_${Date.now()}`, type: "function", function: { name: reply.toolCall.name, arguments: JSON.stringify(reply.toolCall.arguments || {}) } }] }, null);
        send({}, "tool_calls");
        log(tenantId, `brain streamed in ${Date.now() - t0}ms (first ${firstAt || "-"}ms) -> action:${reply.toolCall.name}`);
      } else { // malformed/unknown ACTION -> fail-safe: speak it
        const spoken = cleanForVoice(reply.content || finalText || HOLDING_LINE);
        if (!spoke && spoken) send({ content: spoken }, null);
        send({}, "stop");
        log(tenantId, `brain streamed in ${Date.now() - t0}ms -> speech (action fallback)`);
      }
    } else {
      if (!spoke) send({ content: HOLDING_LINE }, null);
      send({}, "stop");
      log(tenantId, `brain streamed in ${Date.now() - t0}ms (first token ${firstAt || "-"}ms) -> speech`);
    }
    finish();
    const lastUser = [...messages].reverse().find((m) => m.role === "user");
    saveTenantTurn(tenantId, lastUser && lastUser.content, finalText).catch(() => {});
  } catch (e) {
    clearTimeout(timer);
    log(tenantId, "brain stream failed:", e.message, "-> holding line");
    if (!spoke) { try { send({ content: HOLDING_LINE }, null); } catch {} }
    try { send({}, "stop"); } catch {}
    finish();
  }
}

function streamChunks(res, model, reply) {
        res.setHeader("Content-Type", "text/event-stream");
        res.setHeader("Cache-Control", "no-cache");
        res.setHeader("Connection", "keep-alive");
        const id = `chatcmpl-${Date.now()}`;
        const base = { id, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model: model || "switchboard-brain" };
        const send = (delta, finish) =>
                  res.write(`data: ${JSON.stringify({ ...base, choices: [{ index: 0, delta, finish_reason: finish || null }] })}\n\n`);

  send({ role: "assistant" }, null);
        if (reply.toolCall) {
                  send({ tool_calls: [{ index: 0, id: `call_${Date.now()}`, type: "function", function: { name: reply.toolCall.name, arguments: JSON.stringify(reply.toolCall.arguments || {}) } }] }, null);
                  send({}, "tool_calls");
        } else {
                  send({ content: reply.content }, null);
                  send({}, "stop");
        }
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
        if (pool) { try { await pool.query("SELECT 1"); db = "ok"; } catch { db = "unreachable"; } }
        res.json({ status: "ok", service: "switchboard-brain-gateway", brain, tenantSource, db, tenants: pool ? undefined : ALLOWED_TENANTS.size });
});

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

           const { messages, model, stream, tool_choice } = req.body || {};
        const tools = normalizeTools(req.body && req.body.tools);
        if (!Array.isArray(messages) || messages.length === 0) {
                  return res.status(400).json({ error: { message: "messages[] required", type: "invalid_request" } });
        }

           // Contract probe: record what Telnyx actually sends (shape only, no content).
           if (LOG_REQUEST_SHAPE) {
                     log(tenantId, `turn: msgs=${messages.length} tools=${tools.length}${tools.length ? " [" + tools.map((t) => t.name).join(",") + "]" : ""} tool_choice=${JSON.stringify(tool_choice) || "none"} stream=${!!stream}`);
           }

           // Telnyx voice uses stream=true -> STREAM the brain's words as they are
           // generated (first words in ~1s instead of waiting the full 3-7s, which
           // timed out the turn). Non-stream callers get the buffered path below.
           if (stream) return streamBrainToTelnyx(res, tenantId, model, tools, messages);

           let reply;
        try {
                  const memory = await loadTenantMemory(tenantId); // scoped to THIS tenant only
          const input = buildBrainInput(messages, memory, tools);
                  const t0 = Date.now();
                  const raw = await runBrain(input);
                  reply = parseBrainReply(raw, tools);
                  if (reply.content) reply.content = cleanForVoice(reply.content);
                  log(tenantId, `brain replied in ${Date.now() - t0}ms ${reply.toolCall ? "-> action:" + reply.toolCall.name : "-> speech"}`);
                  const lastUser = [...messages].reverse().find((m) => m.role === "user");
                  saveTenantTurn(tenantId, lastUser && lastUser.content, reply.content).catch((e) => log(tenantId, "memory save failed:", e.message));
        } catch (e) {
                  // Never dead air. Hand back a natural holding line and keep the call alive.
          log(tenantId, "brain failed:", e.message, "-> holding line");
                  reply = { content: HOLDING_LINE };
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
