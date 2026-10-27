import { createRxForwardReq, createRxNostr } from "rx-nostr-v3";
import { RxNostr } from "rx-nostr-v4";
import { expect, it, vi } from "vitest";

import { WebSocketPool } from "../index.js";
import { Transport } from "./transport.js";

it("shares a transport through the real v3.7.8 and v4.0.0-beta.2 injection APIs", async () => {
  Transport.instances = [];
  const pool = new WebSocketPool(Transport);
  const url = "wss://relay.example/";
  const v3 = createRxNostr({
    websocketCtor: pool.WebSocket,
    verifier: async () => true,
    skipFetchNip11: true,
  });
  const v4 = new RxNostr({
    WebSocket: pool.WebSocket,
    verifier: { verifyEvent: async () => true },
    skipFetchNip11: true,
    authenticator: false,
  });
  const receivedV3 = vi.fn();
  const receivedV4 = vi.fn();
  const request = createRxForwardReq("v3");
  const subV3 = v3.use(request, { relays: [url] }).subscribe(receivedV3);
  const subV4 = v4.forward([url], [{ kinds: [1] }]).subscribe(receivedV4);

  try {
    request.emit({ kinds: [1] });
    await vi.waitFor(() => expect(Transport.instances).toHaveLength(1));
    const transport = Transport.instances[0];
    transport.open();
    await vi.waitFor(() =>
      expect(transport.send.mock.calls.length).toBeGreaterThanOrEqual(2),
    );
    const requests = transport.send.mock.calls
      .map(([data]) => JSON.parse(data as string))
      .filter(([type]) => type === "REQ");
    expect(requests).toHaveLength(2);
    expect(requests[0][1]).not.toBe(requests[1][1]);
    const event = {
      id: "a".repeat(64),
      pubkey: "b".repeat(64),
      sig: "c".repeat(128),
      created_at: Math.floor(Date.now() / 1000),
      kind: 1,
      tags: [],
      content: "hello",
    };
    for (const [, id] of requests) {
      transport.message(JSON.stringify(["EVENT", id, event]));
    }
    await vi.waitFor(() => {
      expect(receivedV3).toHaveBeenCalledTimes(1);
      expect(receivedV4).toHaveBeenCalledTimes(1);
    });
    subV3.unsubscribe();
    v3.dispose();
    expect(transport.close).not.toHaveBeenCalled();

    const v4Id = requests.find(([, id]) => !String(id).startsWith("v3:"))![1];
    transport.message(
      JSON.stringify(["EVENT", v4Id, { ...event, id: "d".repeat(64) }]),
    );
    await vi.waitFor(() => expect(receivedV4).toHaveBeenCalledTimes(2));
    subV4.unsubscribe();
    v4.dispose();
    expect(transport.close).toHaveBeenCalledTimes(1);
  } finally {
    subV3.unsubscribe();
    subV4.unsubscribe();
    v3.dispose();
    v4.dispose();
  }
});
