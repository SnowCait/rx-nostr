import { afterEach, beforeEach, expect, test, vi } from "vitest";

import {
  createRxBackwardReq,
  createRxForwardReq,
  createRxNostr,
  noopVerifier,
  type RxNostr,
} from "../index.js";
import { faker } from "./helper.js";

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

const RELAY_A = "wss://relay-a.example";
const RELAY_B = "wss://relay-b.example";
const DISCONNECT_TIMEOUT = 10000;
let rxNostr: RxNostr;

beforeEach(() => {
  vi.useFakeTimers();
  vi.spyOn(Math, "random").mockReturnValue(0.5);
  ControlledSocket.instances = [];
});

afterEach(() => {
  rxNostr.dispose();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

async function recoverWithReadExclusion(write: boolean, keepForward: boolean) {
  rxNostr = createRxNostr({
    verifier: noopVerifier,
    skipVerify: true,
    skipFetchNip11: true,
    websocketCtor: ControlledSocket,
    connectionStrategy: "lazy-keep",
    retry: {
      strategy: "exponential",
      maxCount: 5,
      initialDelay: 1000,
      polite: true,
    },
    disconnectTimeout: DISCONNECT_TIMEOUT,
    eoseTimeout: 30000,
  });
  rxNostr.setDefaultRelays({
    [RELAY_A]: { read: true, write: false },
    [RELAY_B]: { read: true, write },
  });
  const states: string[] = [];
  let excludedRead: boolean | undefined;
  rxNostr.createConnectionStateObservable().subscribe(({ from, state }) => {
    if (from !== RELAY_B) return;
    states.push(state);
    expect(rxNostr.getDefaultRelay(RELAY_B)?.write).toBe(write);
    if (state === "waiting-for-retrying" && excludedRead === undefined) {
      excludedRead = rxNostr.getDefaultRelay(RELAY_B)!.read;
      setRead(false);
    } else if (state === "connected" && excludedRead !== undefined) {
      const read = excludedRead;
      excludedRead = undefined;
      setRead(read);
    }
  });

  const next = vi.fn();
  const forward = createRxForwardReq("live");
  const forwardSub = rxNostr.use(forward).subscribe(next);
  forward.emit({ kinds: [1] });
  await vi.advanceTimersByTimeAsync(0);
  const [relayA, relayB] = ControlledSocket.instances;
  relayA.open();
  relayB.open();
  expect(relayA.sent).toContainEqual(["REQ", "live:0", { kinds: [1] }]);
  expect(relayB.sent).toContainEqual(["REQ", "live:0", { kinds: [1] }]);
  if (!keepForward) forwardSub.unsubscribe();

  const backward = createRxBackwardReq("history");
  const complete = vi.fn();
  const error = vi.fn();
  rxNostr.use(backward).subscribe({ complete, error });
  const startedAt = Date.now();
  backward.emit({ kinds: [1] });
  backward.over();
  await vi.advanceTimersByTimeAsync(0);
  expect(relayA.sent).toContainEqual(["REQ", "history:0", { kinds: [1] }]);
  expect(relayB.sent).toContainEqual(["REQ", "history:0", { kinds: [1] }]);
  relayA.receive(["EOSE", "history:0"]);
  expect(relayA.sent).toContainEqual(["CLOSE", "history:0"]);
  expect(complete).not.toHaveBeenCalled();

  relayB.close(1006);
  await vi.advanceTimersByTimeAsync(0);
  expect(rxNostr.getRelayStatus(RELAY_B)?.connection).toBe(
    "waiting-for-retrying",
  );
  expect(rxNostr.getDefaultRelay(RELAY_B)).toMatchObject({
    read: false,
    write,
  });
  expect(complete).toHaveBeenCalledOnce();
  expect(error).not.toHaveBeenCalled();
  expect(Date.now() - startedAt).toBe(0);

  await vi.advanceTimersByTimeAsync(1000);
  expect(rxNostr.getRelayStatus(RELAY_B)?.connection).toBe("retrying");
  expect(rxNostr.getDefaultRelay(RELAY_B)).toMatchObject({
    read: false,
    write,
  });
  const retry = ControlledSocket.instances[2];
  expect(retry.url).toBe(RELAY_B);
  retry.open();
  await vi.advanceTimersByTimeAsync(0);
  expect(rxNostr.getRelayStatus(RELAY_B)?.connection).toBe("connected");
  expect(rxNostr.getDefaultRelay(RELAY_B)).toMatchObject({ read: true, write });
  expect(states).toEqual([
    "connecting",
    "connected",
    "waiting-for-retrying",
    "retrying",
    "connected",
  ]);
  return { retry, next, forwardSub, states };
}

function setRead(read: boolean) {
  rxNostr.setDefaultRelays({
    ...rxNostr.getDefaultRelays(),
    [RELAY_B]: { ...rxNostr.getDefaultRelay(RELAY_B)!, read },
  });
}

test.each([false, true])(
  "read exclusion resumes a continuing subscription and keeps its connection with write=%s",
  async (write) => {
    const { retry, next, forwardSub } = await recoverWithReadExclusion(
      write,
      true,
    );
    expect(retry.sent).toContainEqual(["REQ", "live:0", { kinds: [1] }]);
    expect(retry.sent).not.toContainEqual(["REQ", "history:0", { kinds: [1] }]);
    const event = faker.event({ kind: 1 });
    retry.receive(["EVENT", "live:0", event]);
    await vi.advanceTimersByTimeAsync(0);
    expect(next).toHaveBeenCalledWith(
      expect.objectContaining({ from: RELAY_B, event }),
    );
    await vi.advanceTimersByTimeAsync(DISCONNECT_TIMEOUT);
    expect(rxNostr.getRelayStatus(RELAY_B)?.connection).toBe("connected");
    forwardSub.unsubscribe();
    expect(retry.sent).toContainEqual(["CLOSE", "live:0"]);
    await vi.advanceTimersByTimeAsync(DISCONNECT_TIMEOUT * 2);
    expect(rxNostr.getRelayStatus(RELAY_B)?.connection).toBe("connected");
    expect(rxNostr.getDefaultRelay(RELAY_B)).toMatchObject({
      read: true,
      write,
    });
  },
);

test.each([false, true])(
  "a new backward REQ recovers from the known stale idle timer disconnection with write=%s",
  async (write) => {
    const { retry, states } = await recoverWithReadExclusion(write, false);
    await vi.advanceTimersByTimeAsync(DISCONNECT_TIMEOUT - 1);
    expect(rxNostr.getRelayStatus(RELAY_B)?.connection).toBe("connected");
    await vi.advanceTimersByTimeAsync(1);
    // Known issue: restoring a readable default relay does not cancel its idle timer.
    expect(rxNostr.getRelayStatus(RELAY_B)?.connection).toBe("dormant");
    expect(retry.readyState).toBe(3);
    expect(rxNostr.getRelayStatus(RELAY_A)?.connection).toBe("dormant");

    const startedAt = Date.now();
    const req = createRxBackwardReq("next");
    const complete = vi.fn();
    const error = vi.fn();
    rxNostr.use(req).subscribe({ complete, error });
    req.emit({ kinds: [1] });
    req.over();
    await vi.advanceTimersByTimeAsync(0);
    expect(rxNostr.getRelayStatus(RELAY_B)?.connection).toBe("connecting");
    const newSockets = ControlledSocket.instances.slice(3);
    const relayA = newSockets.find(({ url }) => url === RELAY_A)!;
    const socket = newSockets.find(({ url }) => url === RELAY_B)!;
    relayA.open();
    socket.open();
    expect(socket.sent).toContainEqual(["REQ", "next:0", { kinds: [1] }]);
    expect(relayA.sent).toContainEqual(["REQ", "next:0", { kinds: [1] }]);
    relayA.receive(["EOSE", "next:0"]);
    expect(complete).not.toHaveBeenCalled();
    socket.receive(["EOSE", "next:0"]);
    await vi.advanceTimersByTimeAsync(0);
    expect(socket.sent).toContainEqual(["CLOSE", "next:0"]);
    expect(Date.now() - startedAt).toBe(0);
    expect(complete).toHaveBeenCalledOnce();
    expect(error).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(DISCONNECT_TIMEOUT * 2);
    expect(rxNostr.getRelayStatus(RELAY_B)?.connection).toBe("connected");
    expect(socket.readyState).toBe(1);
    expect(ControlledSocket.instances).toHaveLength(5);
    expect(states).toEqual([
      "connecting",
      "connected",
      "waiting-for-retrying",
      "retrying",
      "connected",
      "dormant",
      "connecting",
      "connected",
    ]);
    expect(rxNostr.getDefaultRelay(RELAY_B)).toMatchObject({
      read: true,
      write,
    });
  },
);
