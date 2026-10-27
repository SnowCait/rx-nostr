/** The native, browser-style transport required by a pool. */
export type WebSocketTransport = Pick<
  WebSocket,
  "readyState" | "send" | "close" | "addEventListener" | "removeEventListener"
>;

export type WebSocketTransportConstructor = new (
  url: string,
) => WebSocketTransport;

type EventType = keyof WebSocketEventMap;
type Handler<K extends EventType> = (
  this: SharedWebSocket,
  event: WebSocketEventMap[K],
) => unknown;

interface Connection {
  socket: WebSocketTransport;
  clients: Set<PooledWebSocket>;
  forget(): void;
  release(): void;
}

function closeEvent(
  code: number,
  reason: string,
  wasClean: boolean,
): CloseEvent {
  // CloseEvent is not available in every runtime that provides WebSocket.
  return Object.assign(new Event("close"), { code, reason, wasClean });
}

/** A logical connection. Construct it through pool.WebSocket. */
class PooledWebSocket extends EventTarget {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;
  readonly CONNECTING = 0;
  readonly OPEN = 1;
  readonly CLOSING = 2;
  readonly CLOSED = 3;

  private state: number = PooledWebSocket.CONNECTING;
  private connection?: Connection;
  private pendingMessages: Event[] = [];

  declare onopen: Handler<"open"> | null;
  declare onmessage: Handler<"message"> | null;
  declare onerror: Handler<"error"> | null;
  declare onclose: Handler<"close"> | null;

  constructor(readonly url: string) {
    super();
    for (const type of ["open", "message", "error", "close"] as const) {
      let handler: EventListener | null = null;
      const listener: EventListener = (event) => handler?.call(this, event);
      Object.defineProperty(this, `on${type}`, {
        enumerable: true,
        get: () => handler,
        set: (value: EventListener | null) => {
          const next = typeof value === "function" ? value : null;
          if (handler && !next) this.removeEventListener(type, listener);
          if (!handler && next) this.addEventListener(type, listener);
          handler = next;
        },
      });
    }
  }

  get readyState(): number {
    return this.state;
  }

  // Typed overloads retain the familiar browser listener API.
  addEventListener<K extends EventType>(
    type: K,
    listener: (this: SharedWebSocket, event: WebSocketEventMap[K]) => unknown,
    options?: boolean | AddEventListenerOptions,
  ): void;
  addEventListener(
    type: string,
    listener: EventListenerOrEventListenerObject | null,
    options?: boolean | AddEventListenerOptions,
  ): void;
  addEventListener(
    type: string,
    listener: EventListenerOrEventListenerObject | null,
    options?: boolean | AddEventListenerOptions,
  ): void {
    super.addEventListener(type, listener, options);
  }

  removeEventListener<K extends EventType>(
    type: K,
    listener: (this: SharedWebSocket, event: WebSocketEventMap[K]) => unknown,
    options?: boolean | EventListenerOptions,
  ): void;
  removeEventListener(
    type: string,
    listener: EventListenerOrEventListenerObject | null,
    options?: boolean | EventListenerOptions,
  ): void;
  removeEventListener(
    type: string,
    listener: EventListenerOrEventListenerObject | null,
    options?: boolean | EventListenerOptions,
  ): void {
    super.removeEventListener(type, listener, options);
  }

  attach(connection: Connection): void {
    this.connection = connection;
    connection.clients.add(this);
    if (connection.socket.readyState === this.OPEN) {
      setTimeout(() => this.open(), 0);
    }
  }

  open(): void {
    if (this.state !== this.CONNECTING) return;
    this.state = this.OPEN;
    this.dispatchEvent(new Event("open"));
    for (const event of this.pendingMessages.splice(0)) {
      this.receive("message", event);
    }
  }

  receive(type: "message" | "error", event: Event): void {
    if (this.state >= this.CLOSING) return;
    if (type === "message" && this.state === this.CONNECTING) {
      this.pendingMessages.push(event);
      return;
    }
    this.dispatchEvent(event);
  }

  prepareClose(): void {
    this.state = this.CLOSED;
    this.connection = undefined;
    this.pendingMessages = [];
  }

  finish(event: CloseEvent): void {
    this.dispatchEvent(event);
    this.onopen = this.onmessage = this.onerror = this.onclose = null;
  }

  send(data: string | ArrayBufferLike | Blob | ArrayBufferView): void {
    if (this.state === this.CONNECTING) {
      throw new DOMException("WebSocket is connecting", "InvalidStateError");
    }
    if (this.state === this.OPEN) this.connection?.socket.send(data);
  }

  close(code?: number, reason = ""): void {
    if (
      code !== undefined &&
      code !== 1000 &&
      (code < 3000 || code > 4999 || !Number.isInteger(code))
    ) {
      throw new DOMException("Invalid close code", "InvalidAccessError");
    }
    if (new TextEncoder().encode(reason).length > 123) {
      throw new DOMException("Close reason exceeds 123 bytes", "SyntaxError");
    }
    if (this.state >= this.CLOSING) return;
    const wasClean = this.state === this.OPEN;
    this.state = this.CLOSING;
    this.pendingMessages = [];
    const connection = this.connection;
    this.connection = undefined;
    connection?.clients.delete(this);
    setTimeout(() => {
      this.prepareClose();
      this.finish(closeEvent(code ?? 1000, reason, wasClean));
    }, 0);
    // Detach before closing: a synchronous transport close cannot affect a
    // replacement connection created by a close handler.
    if (connection?.clients.size === 0) {
      connection.forget();
      connection.socket.close(code, reason);
    }
  }
}

/** The supported logical WebSocket surface, without pool internals. */
export type SharedWebSocket = Pick<
  PooledWebSocket,
  | keyof EventTarget
  | "url"
  | "readyState"
  | "CONNECTING"
  | "OPEN"
  | "CLOSING"
  | "CLOSED"
  | "onopen"
  | "onmessage"
  | "onerror"
  | "onclose"
  | "send"
  | "close"
>;

/** Shares one physical connection per exact URL string within this pool. */
export class WebSocketPool {
  readonly WebSocket: {
    new (url: string | URL): SharedWebSocket;
    readonly CONNECTING: 0;
    readonly OPEN: 1;
    readonly CLOSING: 2;
    readonly CLOSED: 3;
  };

  private readonly connections = new Map<string, Connection>();

  constructor(Transport: WebSocketTransportConstructor = globalThis.WebSocket) {
    if (!Transport) throw new TypeError("A WebSocket transport is required");
    const acquire = (url: string): Connection => {
      const connection = this.connections.get(url);
      return connection &&
        connection.socket.readyState < PooledWebSocket.CLOSING
        ? connection
        : this.connect(url, Transport);
    };
    this.WebSocket = class extends PooledWebSocket {
      constructor(url: string | URL) {
        const key = String(url);
        super(key);
        this.attach(acquire(key));
      }
    };
  }

  private connect(
    url: string,
    Transport: WebSocketTransportConstructor,
  ): Connection {
    const socket = new Transport(url);
    const clients = new Set<PooledWebSocket>();
    const listeners: Record<EventType, EventListener> = {
      open: () => {
        for (const client of [...clients]) client.open();
      },
      message: (event) => {
        const message = event as MessageEvent;
        for (const client of [...clients]) {
          client.receive(
            "message",
            new MessageEvent("message", {
              data: message.data,
              origin: message.origin,
              lastEventId: message.lastEventId,
            }),
          );
        }
      },
      error: (event) => {
        for (const client of [...clients]) {
          client.receive(
            "error",
            Object.assign(new Event("error"), {
              cause: (event as ErrorEvent).error,
            }),
          );
        }
      },
      close: (event) => {
        const closed = event as CloseEvent;
        const remaining = [...clients];
        connection.release();
        clients.clear();
        for (const client of remaining) client.prepareClose();
        for (const client of remaining) {
          client.finish(
            closeEvent(closed.code, closed.reason, closed.wasClean),
          );
        }
      },
    };
    const connection: Connection = {
      socket,
      clients,
      forget: () => {
        if (this.connections.get(url) === connection)
          this.connections.delete(url);
      },
      release: () => {
        connection.forget();
        for (const type of Object.keys(listeners) as EventType[]) {
          socket.removeEventListener(type, listeners[type]);
        }
      },
    };
    for (const type of Object.keys(listeners) as EventType[]) {
      socket.addEventListener(type, listeners[type]);
    }
    this.connections.set(url, connection);
    return connection;
  }
}
