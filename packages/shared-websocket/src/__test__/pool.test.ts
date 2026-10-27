import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { WebSocketPool } from "../index.js";
import { Transport } from "./transport.js";

const url = "wss://relay.example";

beforeEach(() => {
  Transport.instances = [];
  vi.useFakeTimers();
});
afterEach(() => {
  vi.useRealTimers();
});

describe("WebSocketPool", () => {
  it("shares by exact URL within a pool only", () => {
    const pool = new WebSocketPool(Transport);
    const a = new pool.WebSocket(url);
    const b = new pool.WebSocket(new URL(url));
    // URL objects stringify with a trailing slash; strings are not normalized.
    const c = new pool.WebSocket(url);
    expect(a).not.toBe(c);
    expect(Transport.instances).toHaveLength(2);
    expect(b.url).toBe(`${url}/`);
    new pool.WebSocket("wss://other.example");
    new new WebSocketPool(Transport).WebSocket(url);
    expect(Transport.instances).toHaveLength(4);
  });

  it("opens all clients that joined during CONNECTING", () => {
    const pool = new WebSocketPool(Transport);
    const a = new pool.WebSocket(url);
    const b = new pool.WebSocket(url);
    const opened = vi.fn();
    a.addEventListener("open", opened);
    b.onopen = opened;
    expect(a.readyState).toBe(pool.WebSocket.CONNECTING);
    expect(b.readyState).toBe(b.CONNECTING);
    Transport.instances[0].open();
    expect(opened).toHaveBeenCalledTimes(2);
    expect(a.readyState).toBe(a.OPEN);
    expect(b.readyState).toBe(b.OPEN);
    expect(opened.mock.calls[0][0].target).toBe(a);
    expect(opened.mock.calls[1][0].target).toBe(b);
    expect(Transport.instances).toHaveLength(1);
  });

  it("asynchronously opens a late joiner before delivering its messages", () => {
    const pool = new WebSocketPool(Transport);
    new pool.WebSocket(url);
    const transport = Transport.instances[0];
    transport.open();
    const b = new pool.WebSocket(url);
    const events: string[] = [];
    b.onopen = () => events.push("open");
    b.onmessage = (event) => events.push(event.data);
    transport.message("hello");
    expect(b.readyState).toBe(b.CONNECTING);
    expect(events).toEqual([]);
    vi.runAllTimers();
    expect(events).toEqual(["open", "hello"]);
    expect(b.readyState).toBe(b.OPEN);
    expect(Transport.instances).toHaveLength(1);
  });

  it("forwards sends unchanged and broadcasts distinct message and error events", () => {
    const pool = new WebSocketPool(Transport);
    const a = new pool.WebSocket(url);
    const b = new pool.WebSocket(url);
    const transport = Transport.instances[0];
    const messages = vi.fn();
    const errors = vi.fn();
    a.addEventListener("message", messages);
    b.onmessage = messages;
    a.onerror = errors;
    b.addEventListener("error", errors);
    expect(() => a.send("early")).toThrow(/connecting/);
    transport.open();
    const data = new Uint8Array([1, 2]);
    a.send(data);
    b.send("text");
    expect(transport.send.mock.calls).toEqual([[data], ["text"]]);
    expect(transport.send.mock.calls[0][0]).toBe(data);
    transport.message(data);
    transport.dispatchEvent(new Event("error"));
    expect(messages).toHaveBeenCalledTimes(2);
    expect(messages.mock.calls[0][0].data).toBe(data);
    expect(messages.mock.calls[0][0]).not.toBe(messages.mock.calls[1][0]);
    expect(messages.mock.calls[1][0].target).toBe(b);
    expect(errors).toHaveBeenCalledTimes(2);
    expect(a.readyState).toBe(a.OPEN);
  });

  it("supports listener removal, once, abort and independent handler replacement", () => {
    const pool = new WebSocketPool(Transport);
    const a = new pool.WebSocket(url);
    const b = new pool.WebSocket(url);
    const removed = vi.fn();
    const once = vi.fn();
    const aborted = vi.fn();
    const old = vi.fn();
    const current = vi.fn();
    const controller = new AbortController();
    a.addEventListener("message", removed);
    a.removeEventListener("message", removed);
    a.addEventListener("message", once, { once: true });
    a.addEventListener("message", aborted, { signal: controller.signal });
    controller.abort();
    a.onmessage = old;
    a.onmessage = current;
    const transport = Transport.instances[0];
    transport.open();
    transport.message("one");
    transport.message("two");
    expect(removed).not.toHaveBeenCalled();
    expect(aborted).not.toHaveBeenCalled();
    expect(old).not.toHaveBeenCalled();
    expect(once).toHaveBeenCalledTimes(1);
    expect(current).toHaveBeenCalledTimes(2);
    expect(current.mock.instances).toEqual([a, a]);
    expect(b.onmessage).toBeNull();
    a.onmessage = null;
    transport.message("three");
    expect(current).toHaveBeenCalledTimes(2);
  });

  it("closes one client independently and closes the transport only for the last", () => {
    const pool = new WebSocketPool(Transport);
    const a = new pool.WebSocket(url);
    const b = new pool.WebSocket(url);
    const transport = Transport.instances[0];
    transport.open();
    const closed = vi.fn();
    const messages = vi.fn();
    a.onclose = closed;
    a.onmessage = messages;
    b.onmessage = messages;
    a.close(3000, "done");
    a.close();
    a.send("ignored");
    expect(a.readyState).toBe(a.CLOSING);
    expect(closed).not.toHaveBeenCalled();
    expect(transport.close).not.toHaveBeenCalled();
    transport.message("still here");
    b.send("alive");
    expect(messages).toHaveBeenCalledTimes(1);
    expect(transport.send).toHaveBeenCalledWith("alive");
    vi.runAllTimers();
    expect(a.readyState).toBe(a.CLOSED);
    expect(closed).toHaveBeenCalledTimes(1);
    expect(closed.mock.calls[0][0]).toMatchObject({
      code: 3000,
      reason: "done",
    });
    b.close();
    expect(transport.close).toHaveBeenCalledTimes(1);
    expect(transport.close).toHaveBeenCalledWith(undefined, "");
    vi.runAllTimers();
    expect(b.readyState).toBe(b.CLOSED);
    new pool.WebSocket(url);
    expect(Transport.instances).toHaveLength(2);
  });

  it.each([false, true])(
    "can close during CONNECTING (late join: %s)",
    (late) => {
      const pool = new WebSocketPool(Transport);
      const a = new pool.WebSocket(url);
      if (late) Transport.instances[0].open();
      const b = new pool.WebSocket(url);
      const opened = vi.fn();
      const closed = vi.fn();
      b.onopen = opened;
      b.onclose = closed;
      b.close();
      if (!late) Transport.instances[0].open();
      vi.runAllTimers();
      expect(opened).not.toHaveBeenCalled();
      expect(closed).toHaveBeenCalledTimes(1);
      expect(a.readyState).toBe(a.OPEN);
      expect(Transport.instances[0].close).not.toHaveBeenCalled();
    },
  );

  it("releases listeners and notifies all clients when the transport disconnects", () => {
    const pool = new WebSocketPool(Transport);
    const a = new pool.WebSocket(url);
    const b = new pool.WebSocket(url);
    const transport = Transport.instances[0];
    const remove = vi.spyOn(transport, "removeEventListener");
    const closed = vi.fn();
    a.onclose = closed;
    b.onclose = closed;
    transport.open();
    transport.disconnect(4001, "bye", true);
    expect(a.readyState).toBe(a.CLOSED);
    expect(b.readyState).toBe(b.CLOSED);
    expect(closed).toHaveBeenCalledTimes(2);
    expect(closed.mock.calls[0][0]).toMatchObject({
      code: 4001,
      reason: "bye",
      wasClean: true,
    });
    expect(remove.mock.calls.map(([type]) => type).sort()).toEqual([
      "close",
      "error",
      "message",
      "open",
    ]);
    transport.disconnect();
    a.close();
    vi.runAllTimers();
    expect(closed).toHaveBeenCalledTimes(2);
    expect(transport.close).not.toHaveBeenCalled();
    expect(Transport.instances).toHaveLength(1); // no automatic reconnect
    const c = new pool.WebSocket(url);
    Transport.instances[1].open();
    expect(c.readyState).toBe(c.OPEN);
  });

  it("handles physical disconnect before open and reconnects from a close handler", () => {
    const pool = new WebSocketPool(Transport);
    const a = new pool.WebSocket(url);
    const opened = vi.fn();
    a.onopen = opened;
    a.onclose = () => {
      new pool.WebSocket(url);
    };
    Transport.instances[0].disconnect();
    expect(opened).not.toHaveBeenCalled();
    expect(a.readyState).toBe(a.CLOSED);
    expect(Transport.instances).toHaveLength(2);
    new pool.WebSocket(url);
    expect(Transport.instances).toHaveLength(2);
  });

  it("marks all clients CLOSED before invoking physical close handlers", () => {
    const pool = new WebSocketPool(Transport);
    const a = new pool.WebSocket(url);
    const b = new pool.WebSocket(url);
    const closed = vi.fn();
    b.onclose = closed;
    a.onclose = () => {
      expect(b.readyState).toBe(b.CLOSED);
      b.close();
      new pool.WebSocket(url);
    };
    Transport.instances[0].disconnect();
    vi.runAllTimers();
    expect(closed).toHaveBeenCalledTimes(1);
    expect(Transport.instances[0].close).not.toHaveBeenCalled();
    expect(Transport.instances).toHaveLength(2);
  });

  it("releases the last CONNECTING client and ignores stale transport events", () => {
    const pool = new WebSocketPool(Transport);
    const a = new pool.WebSocket(url);
    const transport = Transport.instances[0];
    const remove = vi.spyOn(transport, "removeEventListener");
    a.close();
    expect(transport.close).toHaveBeenCalledTimes(1);
    // Keep listeners through the physical close handshake: transports such as
    // ws can emit an error when closed during CONNECTING.
    expect(remove).not.toHaveBeenCalled();
    const b = new pool.WebSocket(url);
    transport.open();
    transport.disconnect();
    expect(remove).toHaveBeenCalledTimes(4);
    vi.runAllTimers();
    expect(a.readyState).toBe(a.CLOSED);
    expect(b.readyState).toBe(b.CONNECTING);
    new pool.WebSocket(url);
    expect(Transport.instances).toHaveLength(2);
  });

  it("does not reuse a CLOSING transport or let its close remove the replacement", () => {
    const pool = new WebSocketPool(Transport);
    new pool.WebSocket(url);
    const old = Transport.instances[0];
    old.readyState = 2;
    new pool.WebSocket(url);
    old.disconnect();
    new pool.WebSocket(url);
    expect(Transport.instances).toHaveLength(2);
  });

  it("validates close arguments without changing state", () => {
    const pool = new WebSocketPool(Transport);
    const a = new pool.WebSocket(url);
    expect(() => a.close(1006)).toThrow(/code/);
    expect(() => a.close(3000, "あ".repeat(42))).toThrow(/123 bytes/);
    expect(a.readyState).toBe(a.CONNECTING);
    expect(Transport.instances[0].close).not.toHaveBeenCalled();
  });

  it("allows retry after a transport constructor throws", () => {
    let fail = true;
    class UnreliableTransport extends Transport {
      constructor(url: string) {
        if (fail) throw new Error("failed");
        super(url);
      }
    }
    const pool = new WebSocketPool(UnreliableTransport);
    expect(() => new pool.WebSocket(url)).toThrow("failed");
    fail = false;
    new pool.WebSocket(url);
    expect(Transport.instances).toHaveLength(1);
  });
});
