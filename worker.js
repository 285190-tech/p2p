import { DurableObject } from "cloudflare:workers";

const ID_RE = /^[\w-]{16,64}$/;
const MAX_PEERS = 2;
const MAX_SIGNAL_BYTES = 16 * 1024;
const FALLBACK_ICE = { iceServers: [{ urls: "stun:stun.cloudflare.com:3478" }] };

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // WebSocket signaling: /ws/:roomId?cid=:clientId
    if (url.pathname.startsWith("/ws/")) {
      const roomId = url.pathname.slice(4);
      if (!ID_RE.test(roomId)) return new Response("Bad room id", { status: 400 });
      if (request.headers.get("Upgrade") !== "websocket")
        return new Response("Expected WebSocket", { status: 426 });
      // Reject cross-origin upgrades (browsers always send Origin on WS).
      if (request.headers.get("Origin") !== url.origin)
        return new Response("Forbidden origin", { status: 403 });
      const stub = env.ROOM.get(env.ROOM.idFromName(roomId));
      return stub.fetch(request);
    }

    // Short-lived TURN credentials. The API token never reaches the browser.
    if (url.pathname === "/api/ice") {
      if (env.TURN_KEY_ID && env.TURN_KEY_API_TOKEN) {
        try {
          const r = await fetch(
            `https://rtc.live.cloudflare.com/v1/turn/keys/${env.TURN_KEY_ID}/credentials/generate-ice-servers`,
            {
              method: "POST",
              headers: {
                Authorization: `Bearer ${env.TURN_KEY_API_TOKEN}`,
                "Content-Type": "application/json",
              },
              body: JSON.stringify({ ttl: 86400 }),
            }
          );
          if (r.ok) return Response.json(await r.json(), { headers: { "cache-control": "no-store" } });
          console.error("TURN API error", r.status);
        } catch (e) {
          console.error("TURN API failure", e);
        }
      }
      return Response.json(FALLBACK_ICE, { headers: { "cache-control": "no-store" } });
    }

    return env.ASSETS.fetch(request);
  },
};

export class Room extends DurableObject {
  #live(exceptWs) {
    return this.ctx
      .getWebSockets()
      .filter((w) => w !== exceptWs && w.readyState === 1 && !w.deserializeAttachment()?.replaced);
  }

  async fetch(request) {
    const cid = new URL(request.url).searchParams.get("cid") || "";
    if (!ID_RE.test(cid)) return new Response("Bad client id", { status: 400 });

    // Same client reconnecting: retire the ghost socket silently.
    for (const ws of this.ctx.getWebSockets()) {
      if (ws.deserializeAttachment()?.cid === cid) {
        ws.serializeAttachment({ cid, replaced: true });
        try { ws.close(4000, "replaced"); } catch {}
      }
    }

    const [client, server] = Object.values(new WebSocketPair());
    const others = this.#live();

    if (others.length >= MAX_PEERS) {
      server.accept();
      server.close(4003, "room full");
      return new Response(null, { status: 101, webSocket: client });
    }

    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ cid }); // survives hibernation
    server.send(JSON.stringify({ type: "joined", peers: others.map((w) => w.deserializeAttachment().cid) }));
    for (const o of others) o.send(JSON.stringify({ type: "peer-joined", id: cid }));
    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketMessage(ws, message) {
    if (typeof message !== "string" || message.length > MAX_SIGNAL_BYTES) return;
    let data;
    try { data = JSON.parse(message); } catch { return; }
    const out = JSON.stringify({ type: "signal", data });
    for (const o of this.#live(ws)) o.send(out);
  }

  #gone(ws) {
    try { ws.close(); } catch {}
    const a = ws.deserializeAttachment();
    if (!a || a.replaced) return;
    for (const o of this.#live(ws)) o.send(JSON.stringify({ type: "peer-left", id: a.cid }));
  }

  webSocketClose(ws) { this.#gone(ws); }
  webSocketError(ws) { this.#gone(ws); }
}
