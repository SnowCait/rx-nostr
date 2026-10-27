// v4.0.0-beta.2 references Worker in its own declarations. Only the Node
// injection check uses skipLibCheck; the pool-only Node check stays strict.
import { WebSocketPool } from "@rx-nostr/shared-websocket";
import type { RxNostrConfig as V3Config } from "rx-nostr-v3";
import type { RxNostrConfig as V4Config } from "rx-nostr-v4";

const pool = new WebSocketPool();
export const v3Config: V3Config = { websocketCtor: pool.WebSocket };
export const v4Config: V4Config = { WebSocket: pool.WebSocket };
