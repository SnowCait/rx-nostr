// Type-check the built package without importing either rx-nostr dependency.
import {
  WebSocketPool,
  type WebSocketTransportConstructor,
} from "@rx-nostr/shared-websocket";

const nativeTransport: WebSocketTransportConstructor = WebSocket;
const pool = new WebSocketPool(nativeTransport);

const socket = new pool.WebSocket(new URL("wss://relay.example/"));
const listener = (event: MessageEvent) => {
  void event.data;
};
socket.addEventListener("message", listener, { once: true });
socket.removeEventListener("message", listener);
socket.addEventListener("message", { handleEvent: listener });
socket.onmessage = listener;
socket.onclose = (event) => {
  const details: [number, string, boolean] = [
    event.code,
    event.reason,
    event.wasClean,
  ];
  void details;
};
socket.send(new Uint8Array([1, 2]));
socket.close(1000, "done");
