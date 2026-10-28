import { afterEach, beforeEach, expect, test, vi } from "vitest";

import {
  createRxBackwardReq,
  createRxForwardReq,
  createRxNostr,
  noopVerifier,
  type RxNostr,
} from "../index.js";
import { faker } from "./helper.js";

// Use the injectable WebSocket interface so opening and failing each retry is
// independent of timers. No network or mock-server scheduling is involved.
class ControlledSocket extends EventTarget {
  static instances: ControlledSocket[] = [];
  readyState = 0;
  sent: unknown[][] = [];

  constructor(readonly url: string) {
    super();
    ControlledSocket.instances.push(this);
  }

  open() {
    this.readyState = 1;
    this.dispatchEvent(new Event("open"));
  }

  send(data: string) {
    this.sent.push(JSON.parse(data));
  }

  receive(message: unknown[]) {
    this.dispatchEvent(
      new MessageEvent("message", { data: JSON.stringify(message) }),
    );
  }

  close(code = 1006) {
    this.readyState = 2;
    queueMicrotask(() => {
      this.readyState = 3;
      this.dispatchEvent(
        Object.assign(new Event("close"), { code, reason: "" }),
      );
    });
  }
}

const HEALTHY = "wss://healthy.example";
const FAILING = "wss://failing.example";
let rxNostr: RxNostr;

beforeEach(() => {
  vi.useFakeTimers();
  ControlledSocket.instances = [];
});

afterEach(() => {
  rxNostr.dispose();
  vi.useRealTimers();
});

function setup(write: boolean, disconnectTimeout = 1000) {
  rxNostr = createRxNostr({
    verifier: noopVerifier,
    skipVerify: true,
    skipFetchNip11: true,
    websocketCtor: ControlledSocket,
    connectionStrategy: "lazy-keep",
    retry: { strategy: "linear", interval: 100, maxCount: 1 },
    disconnectTimeout,
    eoseTimeout: 10000,
  });
  rxNostr.setDefaultRelays({
    [HEALTHY]: { read: true, write: false },
    [FAILING]: { read: true, write },
  });
  rxNostr.createConnectionStateObservable().subscribe(({ from, state }) => {
    if (from !== FAILING) return;
    if (state === "waiting-for-retrying") setRead(false);
    if (state === "connected") setRead(true);
  });
}

function setRead(read: boolean) {
  rxNostr.setDefaultRelays({
    ...rxNostr.getDefaultRelays(),
    [FAILING]: { ...rxNostr.getDefaultRelay(FAILING)!, read },
  });
}

function state() {
  return rxNostr.getRelayStatus(FAILING)?.connection;
}

test.each([false, true])(
  "read exclusion completes backward REQ and preserves retries with write=%s",
  async (write) => {
    setup(write);
    const req = createRxBackwardReq("history");
    const complete = vi.fn();
    rxNostr.use(req).subscribe({ complete });
    req.emit({ kinds: [1] });
    req.over();
    await vi.advanceTimersByTimeAsync(0);
    const [healthy, failing] = ControlledSocket.instances;
    healthy.open();
    failing.open();
    healthy.receive(["EOSE", "history:0"]);
    expect(complete).not.toHaveBeenCalled();

    failing.close();
    await vi.advanceTimersByTimeAsync(0);
    expect(state()).toBe("waiting-for-retrying");
    expect(complete).toHaveBeenCalledOnce();
    expect(rxNostr.getDefaultRelay(FAILING)).toMatchObject({
      read: false,
      write,
    });
    await vi.advanceTimersByTimeAsync(100);
    expect(state()).toBe("retrying");
    const retry = ControlledSocket.instances[2];
    retry.open();
    expect(state()).toBe("connected");
    expect(rxNostr.getDefaultRelay(FAILING)).toMatchObject({
      read: true,
      write,
    });
    expect(retry.sent.filter(([type]) => type === "REQ")).toEqual([]);

    // Characterize the stale idle timer: lazy-keep does not cancel it on restore.
    await vi.advanceTimersByTimeAsync(1000);
    expect(state()).toBe("dormant");
  },
);

test.each([0, 1000])(
  "forward recovery characterizes idle disconnection with disconnectTimeout=%s",
  async (timeout) => {
    setup(true, timeout);
    const req = createRxForwardReq("live");
    const next = vi.fn();
    const sub = rxNostr.use(req).subscribe(next);
    req.emit({ kinds: [1] });
    await vi.advanceTimersByTimeAsync(0);
    const [healthy, failing] = ControlledSocket.instances;
    healthy.open();
    failing.open();
    failing.close();
    await vi.advanceTimersByTimeAsync(0);
    expect(state()).toBe("waiting-for-retrying");
    await vi.advanceTimersByTimeAsync(100);
    let retry = ControlledSocket.instances[2];
    retry.open();
    await vi.advanceTimersByTimeAsync(0);
    // Restoring read in the connected callback resets the strategy while the
    // connection is still non-default and has no logical subscriptions.
    if (timeout === 0) {
      expect(retry.readyState).toBe(3);
      expect(state()).toBe("connecting");
      retry = ControlledSocket.instances[3];
      retry.open();
      await vi.advanceTimersByTimeAsync(0);
    }
    expect(retry.sent).toContainEqual(["REQ", "live:0", { kinds: [1] }]);
    const event = faker.event({ kind: 1 });
    retry.receive(["EVENT", "live:0", event]);
    await vi.advanceTimersByTimeAsync(0);
    expect(next).toHaveBeenCalledWith(expect.objectContaining({ event }));
    await vi.advanceTimersByTimeAsync(1000);
    expect(state()).toBe("connected");
    sub.unsubscribe();
    expect(retry.sent).toContainEqual(["CLOSE", "live:0"]);
    await vi.advanceTimersByTimeAsync(2000);
    expect(state()).toBe("connected");
  },
);

test.each([1006, 4000])(
  "terminal close %s stays stopped until explicit reconnect",
  async (code) => {
    setup(false);
    const req = createRxForwardReq("live");
    rxNostr.use(req).subscribe();
    req.emit({ kinds: [1] });
    await vi.advanceTimersByTimeAsync(0);
    ControlledSocket.instances[0].open();
    ControlledSocket.instances[1].open();
    ControlledSocket.instances[1].close();
    await vi.advanceTimersByTimeAsync(100);
    ControlledSocket.instances[2].close(code);
    await vi.advanceTimersByTimeAsync(0);
    const terminal = code === 4000 ? "rejected" : "error";
    expect(state()).toBe(terminal);
    expect(rxNostr.getDefaultRelay(FAILING)?.read).toBe(false);
    await vi.advanceTimersByTimeAsync(5000);
    expect(state()).toBe(terminal);
    expect(ControlledSocket.instances).toHaveLength(3);
    expect(() => rxNostr.reconnect(FAILING)).toThrow();
    setRead(true);
    req.emit({ kinds: [2] });
    await vi.advanceTimersByTimeAsync(5000);
    expect(state()).toBe(terminal);
    expect(ControlledSocket.instances).toHaveLength(3);
    rxNostr.reconnect(FAILING);
    ControlledSocket.instances[3].open();
    expect(state()).toBe("connected");
  },
);
