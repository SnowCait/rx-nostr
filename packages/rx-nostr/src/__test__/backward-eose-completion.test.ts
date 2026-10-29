import { afterEach, beforeEach, expect, test, vi } from "vitest";

import {
  createRxBackwardReq,
  createRxNostr,
  noopVerifier,
  type RxNostr,
} from "../index.js";

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
let rxNostr: RxNostr;

beforeEach(() => {
  vi.useFakeTimers();
  vi.spyOn(Math, "random").mockReturnValue(0.5);
  ControlledSocket.instances = [];
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
    eoseTimeout: 30000,
  });
  rxNostr.setDefaultRelays([RELAY_A, RELAY_B]);
});

afterEach(() => {
  rxNostr.dispose();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

async function startBackwardReq() {
  const states: string[] = [];
  rxNostr.createConnectionStateObservable().subscribe(({ from, state }) => {
    if (from === RELAY_B) states.push(state);
  });
  const message = vi.fn();
  rxNostr.createAllMessageObservable().subscribe(message);
  const complete = vi.fn();
  const error = vi.fn();
  const req = createRxBackwardReq("history");
  const sub = rxNostr.use(req).subscribe({ complete, error });
  const startedAt = Date.now();
  req.emit({ kinds: [1] });
  req.over();
  await vi.advanceTimersByTimeAsync(0);
  const [relayA, relayB] = ControlledSocket.instances;
  expect(relayA.url).toBe(RELAY_A);
  expect(relayB.url).toBe(RELAY_B);
  relayA.open();
  expect(rxNostr.getRelayStatus(RELAY_A)?.connection).toBe("connected");
  expect(rxNostr.getRelayStatus(RELAY_B)?.connection).toBe("connecting");
  expect(relayA.sent).toEqual([["REQ", "history:0", { kinds: [1] }]]);
  expect(relayB.sent).toEqual([]);
  return { relayA, relayB, states, message, complete, error, sub, startedAt };
}

async function receiveEose(relay: ControlledSocket) {
  relay.receive(["EOSE", "history:0"]);
  await vi.advanceTimersByTimeAsync(0);
  expect(relay.sent).toContainEqual(["CLOSE", "history:0"]);
}

test("reproduces backward EOSE waiting while another previously connected relay retries", async () => {
  const { relayA, relayB, states, message, complete, error, sub, startedAt } =
    await startBackwardReq();
  relayB.open();
  expect(rxNostr.getRelayStatus(RELAY_B)?.connection).toBe("connected");
  expect(relayB.sent).toEqual([["REQ", "history:0", { kinds: [1] }]]);
  await receiveEose(relayA);
  expect(message).toHaveBeenCalledOnce();
  expect(message).toHaveBeenCalledWith(
    expect.objectContaining({
      from: RELAY_A,
      type: "EOSE",
      subId: "history:0",
    }),
  );
  expect(complete).not.toHaveBeenCalled();
  expect(sub.closed).toBe(false);

  relayB.close(1006);
  await vi.advanceTimersByTimeAsync(0);
  expect(rxNostr.getRelayStatus(RELAY_B)?.connection).toBe(
    "waiting-for-retrying",
  );
  expect(complete).not.toHaveBeenCalled();
  expect(sub.closed).toBe(false);
  expect(Date.now() - startedAt).toBe(0);

  await vi.advanceTimersByTimeAsync(1000);
  expect(rxNostr.getRelayStatus(RELAY_B)?.connection).toBe("retrying");
  expect(states).toEqual([
    "connecting",
    "connected",
    "waiting-for-retrying",
    "retrying",
  ]);
  expect(ControlledSocket.instances).toHaveLength(3);
  expect(ControlledSocket.instances[2].url).toBe(RELAY_B);
  expect(ControlledSocket.instances[2].readyState).toBe(0);
  expect(Date.now() - startedAt).toBe(1000);
  expect(complete).not.toHaveBeenCalled();
  expect(error).not.toHaveBeenCalled();
  expect(sub.closed).toBe(false);

  const retry = ControlledSocket.instances[2];
  retry.open();
  expect(retry.sent).toContainEqual(["REQ", "history:0", { kinds: [1] }]);
  expect(complete).not.toHaveBeenCalled();
  await receiveEose(retry);
  expect(Date.now() - startedAt).toBe(1000);
  expect(complete).toHaveBeenCalledOnce();
  expect(error).not.toHaveBeenCalled();
  expect(sub.closed).toBe(true);
});

test("initial connection failure with polite retry completes backward REQ after healthy EOSE", async () => {
  const { relayA, relayB, states, complete, error, sub, startedAt } =
    await startBackwardReq();
  relayB.dispatchEvent(new Event("error"));
  relayB.close(1006);
  await vi.advanceTimersByTimeAsync(0);
  expect(rxNostr.getRelayStatus(RELAY_B)?.connection).toBe("error");
  expect(states).toEqual(["connecting", "error"]);
  expect(complete).not.toHaveBeenCalled();
  await receiveEose(relayA);
  expect(Date.now() - startedAt).toBe(0);
  expect(complete).toHaveBeenCalledOnce();
  expect(error).not.toHaveBeenCalled();
  expect(sub.closed).toBe(true);

  await vi.advanceTimersByTimeAsync(30000);
  expect(states).toEqual(["connecting", "error"]);
  expect(ControlledSocket.instances).toHaveLength(2);
});

test("initial connection stalled in connecting keeps backward REQ open until EOSE timeout", async () => {
  const { relayA, relayB, states, complete, error, sub, startedAt } =
    await startBackwardReq();
  await receiveEose(relayA);
  await vi.advanceTimersByTimeAsync(29999);
  expect(Date.now() - startedAt).toBe(29999);
  expect(rxNostr.getRelayStatus(RELAY_B)?.connection).toBe("connecting");
  expect(complete).not.toHaveBeenCalled();
  expect(sub.closed).toBe(false);

  await vi.advanceTimersByTimeAsync(1);
  expect(Date.now() - startedAt).toBe(30000);
  expect(complete).toHaveBeenCalledOnce();
  expect(error).not.toHaveBeenCalled();
  expect(sub.closed).toBe(true);
  expect(states).toEqual(["connecting"]);
  expect(relayB.readyState).toBe(0);
  expect(relayB.sent).toEqual([]);
  expect(ControlledSocket.instances).toHaveLength(2);
});
