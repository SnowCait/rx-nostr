import { afterEach, beforeEach, expect, test, vi } from "vitest";

import {
  createRxBackwardReq,
  createRxNostr,
  noopVerifier,
  type RxNostr,
} from "../index.js";

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

test("reproduces backward EOSE waiting while another previously connected relay retries", async () => {
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
  relayB.open();
  expect(rxNostr.getRelayStatus(RELAY_A)?.connection).toBe("connected");
  expect(rxNostr.getRelayStatus(RELAY_B)?.connection).toBe("connected");
  expect(relayA.sent).toEqual([["REQ", "history:0", { kinds: [1] }]]);
  expect(relayB.sent).toEqual([["REQ", "history:0", { kinds: [1] }]]);

  relayA.receive(["EOSE", "history:0"]);
  await vi.advanceTimersByTimeAsync(0);
  expect(message).toHaveBeenCalledOnce();
  expect(message).toHaveBeenCalledWith(
    expect.objectContaining({
      from: RELAY_A,
      type: "EOSE",
      subId: "history:0",
    }),
  );
  expect(relayA.sent).toContainEqual(["CLOSE", "history:0"]);
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
});
