# @rx-nostr/shared-websocket

A protocol-independent WebSocket pool with no runtime dependencies. Each pool
shares one physical connection per **exact URL string**. Different pools never
share connections. URL strings are not normalized; a `URL` object is stringified.

```ts
import { WebSocketPool } from "@rx-nostr/shared-websocket";

const pool = new WebSocketPool(); // Uses globalThis.WebSocket.
// Or: new WebSocketPool(MyBrowserStyleWebSocketConstructor)
const a = new pool.WebSocket("wss://relay.example/");
const b = new pool.WebSocket("wss://relay.example/");

a.addEventListener("open", () => a.send("hello"));
b.onmessage = ({ data }) => console.log(data);
a.close(); // Only a closes; b keeps the physical connection alive.
b.close(); // The last client closes the physical connection.
```

## rx-nostr v3 and v4

Pass the same `pool.WebSocket` to each client. For applications using both versions,
install npm aliases alongside this package:

```sh
npm install @rx-nostr/shared-websocket rx-nostr-v3@npm:rx-nostr@3.7.8 rx-nostr-v4@npm:rx-nostr@4.0.0-beta.2
```

```ts
import { WebSocketPool } from "@rx-nostr/shared-websocket";
import { createRxNostr } from "rx-nostr-v3";
import { RxNostr } from "rx-nostr-v4";

const pool = new WebSocketPool();
const v3 = createRxNostr({ websocketCtor: pool.WebSocket });
const v4 = new RxNostr({ WebSocket: pool.WebSocket });
// Configure each version's verifier, signer, and relay requests as usual.
// Use matching relay URL strings to share a physical connection.

// Dispose each client when finished:
v3.dispose();
v4.dispose();
```

The integration test uses the published v3.7.8 and v4.0.0-beta.2 packages. These
are development dependencies only. v4's runtime requirements still apply (Node.js
24+ or equivalent browser features); this package installs no polyfills.

The pool's declarations support Node.js TypeScript projects using current
`@types/node` with `"lib": ["ESNext"]`, `"types": ["node"]`, and
`"skipLibCheck": false`; the DOM lib is not required. The v3/v4 injection APIs
remain structurally compatible. However, v4.0.0-beta.2 itself references the DOM
`Worker` type in its declarations, so projects importing v4 without DOM typings
need to address that upstream declaration or use `skipLibCheck: true`.

The build checks the generated declarations in Node and browser consumer fixtures;
`npm run test:types -w packages/shared-websocket` reruns those checks. Library
checking stays enabled for the pool-only Node fixture and browser injection
fixture; only the Node fixture
that imports v4 skips library checking because of its `Worker` reference.

## Supported interface and lifecycle

- `pool.WebSocket` accepts one URL argument (`string | URL`). The constructor and
  instances expose `CONNECTING`, `OPEN`, `CLOSING`, and `CLOSED` constants.
- Each client has its own read-only `url` and `readyState`, event listeners, and
  `onopen`, `onmessage`, `onerror`, `onclose` properties. `addEventListener` and
  `removeEventListener` use the runtime's `EventTarget`, including `once` and
  `AbortSignal`. Events target the virtual client, with independent event objects.
- Clients begin in `CONNECTING`. An existing OPEN transport gives a new client an
  asynchronous `open` event; messages received before that event are queued until
  it opens. Clients joining a CONNECTING transport open when that transport opens.
- `send(data)` forwards strings, buffers, views, and blobs unchanged. It throws an
  `InvalidStateError` during CONNECTING and ignores sends during CLOSING/CLOSED.
  Incoming messages are broadcast to all active clients without interpretation.
  Binary data uses the physical transport's default representation; payloads are
  shared by reference, so consumers should not mutate them.
- `close(code?, reason?)` immediately detaches that client and enters CLOSING,
  then asynchronously enters CLOSED and emits one logical close event. Repeated
  closes have no additional effect. Codes must be 1000 or 3000–4999, and the UTF-8
  reason must fit in 123 bytes. Only the last client's arguments reach the physical
  transport. A logical close reports code 1000 when omitted, and `wasClean` reflects
  whether the client was OPEN; it does not report a physical close handshake.
- A physical close marks every remaining client CLOSED, releases the pool entry
  and physical listeners, then broadcasts its code, reason, and `wasClean`.
  Physical errors are broadcast as error events; they do not by themselves close
  the connection. No reconnect is attempted. A new client after disconnection
  creates a fresh transport; the consuming clients own their retry policy.

The injected transport must implement browser-style `readyState`, `send`, `close`,
`addEventListener`, and `removeEventListener`, and emit asynchronous browser-style
open/message/error/close events. The runtime must provide `EventTarget`, `Event`,
`MessageEvent`, `DOMException`, timers, and `TextEncoder`. `CloseEvent` is not
required. Protocol negotiation, `binaryType`, `bufferedAmount`, and the full native
WebSocket API are outside this package's interface. Sharing is limited to one
pool object in one JavaScript context, not across tabs, workers, or processes.

## Nostr limitations

This pool does not parse, transform, or route Nostr messages. It does not coordinate
REQ ID collisions, relay subscription limits (NIP-11), AUTH conflicts (NIP-42),
or duplicate EVENT/OK messages. Every client receives every incoming message and
must tolerate traffic belonging to other clients. Coordinate subscription IDs
outside the pool when necessary.

**Do not share a pool across different accounts or authentication contexts.** A
physical connection has one shared relay-side authentication context. Use a
separate pool for each context, even when the relay URL is the same.
