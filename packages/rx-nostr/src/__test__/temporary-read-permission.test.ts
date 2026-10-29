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

  failConnection() {
    expect(this.readyState).toBe(0);
    // A failed handshake emits error and then an abnormal close, without open.
    queueMicrotask(() => {
      this.readyState = 3;
      this.dispatchEvent(new Event("error"));
      this.dispatchEvent(
        Object.assign(new Event("close"), { code: 1006, reason: "" }),
      );
    });
  }

  close(code = 1000) {
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
  let excludedRead: boolean | undefined;
  rxNostr.createConnectionStateObservable().subscribe(({ from, state }) => {
    if (from !== FAILING) return;
    expect(rxNostr.getDefaultRelay(FAILING)?.write).toBe(write);
    if (state === "waiting-for-retrying" && excludedRead === undefined) {
      excludedRead = rxNostr.getDefaultRelay(FAILING)!.read;
      setRead(false);
    }
    if (state === "connected" && excludedRead !== undefined) {
      const read = excludedRead;
      excludedRead = undefined;
      setRead(read);
    }
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
  "initial failure: backward completion and retry with write=%s; reproduces known stale idle timer issue",
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
    expect(healthy.sent).toContainEqual(["REQ", "history:0", { kinds: [1] }]);
    healthy.receive(["EOSE", "history:0"]);
    expect(complete).not.toHaveBeenCalled();

    expect(failing.sent).toEqual([]);
    const failedAt = Date.now();
    failing.failConnection();
    await vi.advanceTimersByTimeAsync(0);
    expect(Date.now()).toBe(failedAt);
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

    // Known issue, not intended lazy-keep behavior: restoration leaves an idle timer.
    await vi.advanceTimersByTimeAsync(1000);
    expect(state()).toBe("dormant");
  },
);

test.each([0, 1000])(
  "initial failure: forward recovery with disconnectTimeout=%s (zero reproduces known extra connection issue)",
  async (timeout) => {
    setup(true, timeout);
    const req = createRxForwardReq("live");
    const next = vi.fn();
    const sub = rxNostr.use(req).subscribe(next);
    req.emit({ kinds: [1] });
    await vi.advanceTimersByTimeAsync(0);
    const [healthy, failing] = ControlledSocket.instances;
    healthy.open();
    failing.failConnection();
    await vi.advanceTimersByTimeAsync(0);
    expect(state()).toBe("waiting-for-retrying");
    await vi.advanceTimersByTimeAsync(100);
    let retry = ControlledSocket.instances[2];
    retry.open();
    await vi.advanceTimersByTimeAsync(0);
    // Known issue, not intended behavior: restoring read resets the strategy while the
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

test("initial failures exhaust retries and remain in error without restoring read", async () => {
  setup(false);
  const req = createRxForwardReq("live");
  rxNostr.use(req).subscribe();
  req.emit({ kinds: [1] });
  await vi.advanceTimersByTimeAsync(0);
  ControlledSocket.instances[0].open();
  ControlledSocket.instances[1].failConnection();
  await vi.advanceTimersByTimeAsync(0);
  expect(state()).toBe("waiting-for-retrying");
  await vi.advanceTimersByTimeAsync(100);
  expect(state()).toBe("retrying");
  ControlledSocket.instances[2].failConnection();
  await vi.advanceTimersByTimeAsync(0);
  expect(state()).toBe("error");
  expect(rxNostr.getDefaultRelay(FAILING)).toMatchObject({
    read: false,
    write: false,
  });
  await vi.advanceTimersByTimeAsync(20000);
  expect(state()).toBe("error");
  expect(ControlledSocket.instances).toHaveLength(3);
  expect(rxNostr.getDefaultRelay(FAILING)).toMatchObject({
    read: false,
    write: false,
  });
});
