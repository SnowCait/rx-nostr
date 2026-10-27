import { vi } from "vitest";

/** A manually driven browser-style transport; no network or relay involved. */
export class Transport extends EventTarget {
  static instances: Transport[] = [];
  readyState = 0;
  send = vi.fn<
    [data: string | ArrayBufferLike | Blob | ArrayBufferView],
    void
  >();
  close = vi.fn<[code?: number, reason?: string], void>(() => {
    this.readyState = 2;
  });

  constructor(readonly url: string) {
    super();
    Transport.instances.push(this);
  }

  open(): void {
    this.readyState = 1;
    this.dispatchEvent(new Event("open"));
  }

  message(data: unknown): void {
    this.dispatchEvent(new MessageEvent("message", { data }));
  }

  disconnect(code = 1006, reason = "lost", wasClean = false): void {
    this.readyState = 3;
    this.dispatchEvent(
      Object.assign(new Event("close"), { code, reason, wasClean }),
    );
  }
}
