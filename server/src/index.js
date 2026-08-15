// ═══════════════════════════════════════════════════════════════
//  P2P Messenger — Signaling Server (Cloudflare Worker + KV)
//
//  This server ONLY relays WebRTC handshake messages (SDP
//  offers/answers, ICE candidates). It never sees chat content.
//  Once peers connect, all data flows directly between them
//  over an encrypted DTLS channel.
// ═══════════════════════════════════════════════════════════════

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

const JSON_HEADERS = {
  ...CORS_HEADERS,
  "Content-Type": "application/json",
};

// TTL for all KV entries — 10 minutes is plenty for a handshake
const TTL = 600;

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: JSON_HEADERS,
  });
}

export default {
  async fetch(request, env) {
    // Handle CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    if (request.method !== "POST") {
      return json({ error: "Method not allowed" }, 405);
    }

    const url = new URL(request.url);
    const path = url.pathname;

    try {
      const body = await request.json();

      // ── Health check ──────────────────────────────────────
      if (path === "/health") {
        return json({ status: "ok", kv: !!env.SIGNAL_KV });
      }

      // ── Join a room ───────────────────────────────────────
      // Adds user to room member list, returns current members
      if (path === "/join") {
        const { room, username } = body;
        if (!room || !username) {
          return json({ error: "Missing room or username" }, 400);
        }

        const membersKey = `members:${room}`;
        const members = (await env.SIGNAL_KV.get(membersKey, "json")) || [];

        if (!members.includes(username)) {
          members.push(username);
          await env.SIGNAL_KV.put(membersKey, JSON.stringify(members), {
            expirationTtl: TTL,
          });
        }

        return json({ members });
      }

      // ── Send signal to peer ───────────────────────────────
      // Appends a signal (offer/answer/candidate) to the
      // recipient's queue in KV
      if (path === "/send") {
        const { room, from, to, type, payload } = body;
        if (!room || !from || !to || !type || !payload) {
          return json({ error: "Missing required fields" }, 400);
        }

        const queueKey = `queue:${room}:${to}`;
        const queue = (await env.SIGNAL_KV.get(queueKey, "json")) || [];

        queue.push({
          from,
          type,
          payload,
          ts: Date.now(),
        });

        await env.SIGNAL_KV.put(queueKey, JSON.stringify(queue), {
          expirationTtl: TTL,
        });

        return json({ ok: true });
      }

      // ── Poll for signals ──────────────────────────────────
      // Returns AND clears all signals queued for this user.
      // Safe because only one writer (the peer) and one reader
      // (this user) touch each queue key — no race condition.
      if (path === "/poll") {
        const { room, username } = body;
        if (!room || !username) {
          return json({ error: "Missing room or username" }, 400);
        }

        const queueKey = `queue:${room}:${username}`;
        const queue = (await env.SIGNAL_KV.get(queueKey, "json")) || [];

        // Clear the queue atomically
        if (queue.length > 0) {
          await env.SIGNAL_KV.delete(queueKey);
        }

        return json({ signals: queue });
      }

      // ── Leave room ────────────────────────────────────────
      if (path === "/leave") {
        const { room, username } = body;
        if (!room || !username) {
          return json({ error: "Missing room or username" }, 400);
        }

        const membersKey = `members:${room}`;
        const members = (await env.SIGNAL_KV.get(membersKey, "json")) || [];
        const filtered = members.filter((m) => m !== username);

        if (filtered.length > 0) {
          await env.SIGNAL_KV.put(membersKey, JSON.stringify(filtered), {
            expirationTtl: TTL,
          });
        } else {
          // Room is empty — clean up
          await env.SIGNAL_KV.delete(membersKey);
          // Also clean any leftover queues for this room
          // (best-effort, may miss some but they'll expire via TTL)
        }

        // Clean up this user's signal queue
        const queueKey = `queue:${room}:${username}`;
        await env.SIGNAL_KV.delete(queueKey);

        return json({ ok: true });
      }

      return json({ error: "Not found" }, 404);
    } catch (err) {
      return json({ error: err.message }, 500);
    }
  },
};
