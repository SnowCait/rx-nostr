import { createRxForwardReq, createRxNostr } from "rx-nostr-v3";
import { ExponentialBackoffReconnector, RxNostr } from "rx-nostr-v4";
import { afterEach, beforeEach, expect, it, vi } from "vitest";

import { WebSocketPool } from "../index.js";
import { Transport } from "./transport.js";

beforeEach(() => {
  Transport.instances = [];
  vi.useFakeTimers();
});
afterEach(() => {
  vi.useRealTimers();
});

it.each([
  { first: "v3", v3Delay: 10, v4Delay: 20 },
  { first: "v4", v3Delay: 20, v4Delay: 10 },
])(
  "shares a transport after both clients reconnect ($first retries first)",
  async ({ v3Delay, v4Delay }) => {
    const pool = new WebSocketPool(Transport);
    const url = "wss://relay.example/";
    const v3 = createRxNostr({
      websocketCtor: pool.WebSocket,
      verifier: async () => true,
      skipFetchNip11: true,
      retry: {
        strategy: "linear",
        interval: v3Delay,
        maxCount: 1,
        polite: false,
      },
    });
    const reconnector = new ExponentialBackoffReconnector({
      initialDelay: v4Delay,
      maxDelay: v4Delay,
      maxRetries: 1,
      jitter: 0,
    });
    const reconnect = vi.spyOn(reconnector, "reconnect");
    const v4 = new RxNostr({
      WebSocket: pool.WebSocket,
      verifier: { verifyEvent: async () => true },
      skipFetchNip11: true,
      authenticator: false,
      reconnector,
    });
    const receivedV3 = vi.fn();
    const receivedV4 = vi.fn();
    const request = createRxForwardReq("v3");
    const subV3 = v3.use(request, { relays: [url] }).subscribe(receivedV3);
    const subV4 = v4.forward([url], [{ kinds: [1] }]).subscribe(receivedV4);

    try {
      request.emit({ kinds: [1] });
      await vi.advanceTimersByTimeAsync(0);
      expect(Transport.instances).toHaveLength(1);
      const transport = Transport.instances[0];
      transport.open();
      await vi.advanceTimersByTimeAsync(0);
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
      await vi.advanceTimersByTimeAsync(0);
      expect(receivedV3).toHaveBeenCalledTimes(1);
      expect(receivedV4).toHaveBeenCalledTimes(1);

      transport.disconnect(); // Abnormal closure; only the clients own retry policy.
      await vi.advanceTimersByTimeAsync(9);
      expect(Transport.instances).toHaveLength(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(Transport.instances).toHaveLength(2);
      const replacement = Transport.instances[1];
      replacement.open();
      await vi.advanceTimersByTimeAsync(0);
      expect(replacement.send.mock.calls).toHaveLength(1);
      // The second retry joins the replacement transport after it is OPEN.
      await vi.advanceTimersByTimeAsync(10);
      // The late joiner emits open in the next timer task.
      await vi.advanceTimersByTimeAsync(1);
      expect(Transport.instances).toHaveLength(2);
      expect(reconnect).toHaveBeenCalledTimes(1);
      expect(reconnect).toHaveBeenCalledWith(
        expect.objectContaining({ phase: "recovery", attempt: 1 }),
      );
      const restoredRequests = replacement.send.mock.calls
        .map(([data]) => JSON.parse(data as string))
        .filter(([type]) => type === "REQ");
      expect(restoredRequests).toHaveLength(2);
      expect(restoredRequests[0][1]).not.toBe(restoredRequests[1][1]);
      for (const [, id] of restoredRequests) {
        replacement.message(
          JSON.stringify(["EVENT", id, { ...event, id: "d".repeat(64) }]),
        );
      }
      await vi.advanceTimersByTimeAsync(0);
      expect(receivedV3).toHaveBeenCalledTimes(2);
      expect(receivedV4).toHaveBeenCalledTimes(2);

      subV3.unsubscribe();
      v3.dispose();
      expect(replacement.close).not.toHaveBeenCalled();

      const v4Id = restoredRequests.find(
        ([, id]) => !String(id).startsWith("v3:"),
      )![1];
      replacement.message(
        JSON.stringify(["EVENT", v4Id, { ...event, id: "e".repeat(64) }]),
      );
      await vi.advanceTimersByTimeAsync(0);
      expect(receivedV4).toHaveBeenCalledTimes(3);
      subV4.unsubscribe();
      v4.dispose();
      expect(replacement.close).toHaveBeenCalledTimes(1);
      replacement.disconnect(1000, "", true);
      await vi.advanceTimersByTimeAsync(100);
      expect(Transport.instances).toHaveLength(2);
      expect(transport.close).not.toHaveBeenCalled();
    } finally {
      subV3.unsubscribe();
      subV4.unsubscribe();
      v3.dispose();
      v4.dispose();
      await vi.advanceTimersByTimeAsync(0);
    }
  },
);
