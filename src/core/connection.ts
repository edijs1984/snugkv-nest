import { EventEmitter } from 'node:events';
import * as net from 'node:net';
import * as tls from 'node:tls';
import { Arg, encodeCommand, Reply, RespError, RespParser } from './resp';

export class ConnectionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ConnectionError';
  }
}

export interface ConnectionOptions {
  host: string;
  port: number;
  username?: string;
  password?: string;
  /** `true` for default TLS, or Node TLS options. */
  tls?: boolean | tls.ConnectionOptions;
  /** Name shown in CLIENT LIST. */
  name?: string;
  connectTimeoutMs?: number;
  /** Reconnect after the connection drops. Default true. */
  reconnect?: boolean;
  retryMinMs?: number;
  retryMaxMs?: number;
  keepAliveMs?: number;
  /** Queue commands while disconnected instead of failing them. Default true. */
  offlineQueue?: boolean;
}

interface Pending {
  resolve: (r: Reply) => void;
  reject: (e: Error) => void;
}

interface Queued {
  commands: Arg[][];
  pipeline: boolean;
  resolve: (r: Reply | Reply[]) => void;
  reject: (e: Error) => void;
}

type State = 'idle' | 'connecting' | 'backoff' | 'ready' | 'closed';

/**
 * One TCP/TLS connection speaking RESP2. Replies are matched to commands in
 * order. A dropped connection fails the commands that were already sent
 * (they may or may not have run) and reconnects with backoff. Commands issued
 * while offline wait in a queue and are sent after the reconnect.
 */
export class Connection extends EventEmitter {
  private socket: net.Socket | null = null;
  private parser = new RespParser();
  private inflight: Pending[] = [];
  private queue: Queued[] = [];
  private state: State = 'idle';
  private attempt = 0;
  private retryTimer: NodeJS.Timeout | null = null;
  /** Receives replies that arrive with no command waiting (pub/sub pushes and confirmations). */
  pushHandler: ((reply: Reply) => void) | null = null;
  private readyWaiters: Array<{ resolve: () => void; reject: (e: Error) => void }> = [];
  private readonly opts: Required<Pick<ConnectionOptions, 'connectTimeoutMs' | 'reconnect' | 'retryMinMs' | 'retryMaxMs' | 'keepAliveMs' | 'offlineQueue'>> &
    ConnectionOptions;

  constructor(opts: ConnectionOptions) {
    super();
    this.opts = {
      connectTimeoutMs: 10_000,
      reconnect: true,
      retryMinMs: 100,
      retryMaxMs: 5_000,
      keepAliveMs: 30_000,
      offlineQueue: true,
      ...opts,
    };
  }

  get isReady(): boolean {
    return this.state === 'ready';
  }

  get isClosed(): boolean {
    return this.state === 'closed';
  }

  /** Resolves once the connection is ready, rejects after `timeoutMs`. */
  connect(timeoutMs = 10_000): Promise<void> {
    if (this.state === 'closed') return Promise.reject(new ConnectionError('connection closed'));
    if (this.state === 'ready') return Promise.resolve();
    if (this.state === 'idle') this.open();
    return new Promise((resolve, reject) => {
      const waiter = {
        resolve: () => {
          clearTimeout(timer);
          resolve();
        },
        reject: (e: Error) => {
          clearTimeout(timer);
          reject(e);
        },
      };
      const timer = setTimeout(() => {
        this.readyWaiters = this.readyWaiters.filter((w) => w !== waiter);
        reject(new ConnectionError(`could not connect to ${this.opts.host}:${this.opts.port} within ${timeoutMs}ms`));
      }, timeoutMs);
      this.readyWaiters.push(waiter);
    });
  }

  send(args: Arg[]): Promise<Reply> {
    return this.enqueue([args], false) as Promise<Reply>;
  }

  /** Writes a command whose replies arrive through `pushHandler` (SUBSCRIBE and friends). Dropped when not connected. */
  sendNoReply(args: Arg[]): void {
    if (this.state === 'ready' && this.socket) this.socket.write(encodeCommand(args));
  }

  /** Sends several commands in one write. Error replies come back as RespError values. */
  pipeline(commands: Arg[][]): Promise<Reply[]> {
    if (commands.length === 0) return Promise.resolve([]);
    return this.enqueue(commands, true) as Promise<Reply[]>;
  }

  /** Closes for good. Pending commands fail. */
  close(): void {
    if (this.state === 'closed') return;
    this.state = 'closed';
    if (this.retryTimer) clearTimeout(this.retryTimer);
    this.retryTimer = null;
    const err = new ConnectionError('connection closed');
    this.failAll(err);
    for (const w of this.readyWaiters.splice(0)) w.reject(err);
    const s = this.socket;
    this.socket = null;
    s?.removeAllListeners();
    s?.on('error', () => undefined);
    s?.destroy();
    this.emit('end');
  }

  private enqueue(commands: Arg[][], pipeline: boolean): Promise<Reply | Reply[]> {
    return new Promise((resolve, reject) => {
      if (this.state === 'closed') return reject(new ConnectionError('connection closed'));
      if (this.state === 'ready') return this.write({ commands, pipeline, resolve, reject });
      if (!this.opts.offlineQueue) return reject(new ConnectionError('not connected'));
      this.queue.push({ commands, pipeline, resolve, reject });
      if (this.state === 'idle') this.open();
    });
  }

  private write(q: Queued): void {
    const socket = this.socket;
    if (!socket) return q.reject(new ConnectionError('not connected'));
    if (!q.pipeline) {
      this.inflight.push({ resolve: q.resolve as (r: Reply) => void, reject: q.reject });
      socket.write(encodeCommand(q.commands[0]!));
      return;
    }
    const results: Reply[] = new Array(q.commands.length);
    let left = q.commands.length;
    let failed = false;
    q.commands.forEach((cmd, i) => {
      this.inflight.push({
        resolve: (r) => {
          results[i] = r;
          if (--left === 0 && !failed) q.resolve(results);
        },
        reject: (e) => {
          if (e instanceof RespError) {
            results[i] = e;
            if (--left === 0 && !failed) q.resolve(results);
          } else if (!failed) {
            failed = true;
            q.reject(e);
          }
        },
      });
    });
    socket.write(Buffer.concat(q.commands.map((c) => encodeCommand(c))));
  }

  private open(): void {
    this.state = 'connecting';
    this.parser = new RespParser();
    const { host, port } = this.opts;
    const tlsOpt = this.opts.tls;
    const socket = tlsOpt
      ? tls.connect({ host, port, servername: host, ...(typeof tlsOpt === 'object' ? tlsOpt : {}) })
      : net.connect({ host, port });
    this.socket = socket;
    socket.setNoDelay(true);
    socket.setKeepAlive(true, this.opts.keepAliveMs);
    socket.setTimeout(this.opts.connectTimeoutMs, () => socket.destroy(new ConnectionError('connect timeout')));
    socket.on('data', (chunk: Buffer) => this.onData(socket, chunk));
    socket.on('error', (err) => {
      if (this.socket === socket) this.emit('error', err);
    });
    socket.on('close', () => this.onClose(socket));
    socket.once(tlsOpt ? 'secureConnect' : 'connect', () => {
      socket.setTimeout(0);
      this.handshake(socket).then(
        () => this.onReady(socket),
        (err) => {
          this.emit('error', err);
          socket.destroy();
        },
      );
    });
  }

  private async handshake(socket: net.Socket): Promise<void> {
    const run = (args: Arg[]) =>
      new Promise<Reply>((resolve, reject) => {
        this.inflight.push({ resolve, reject });
        socket.write(encodeCommand(args));
      });
    const { username, password, name } = this.opts;
    if (password !== undefined) {
      await run(username ? ['AUTH', username, password] : ['AUTH', password]);
    }
    if (name) {
      try {
        await run(['CLIENT', 'SETNAME', name]);
      } catch {
        // Not every server supports it; the name is only informational.
      }
    }
  }

  private onReady(socket: net.Socket): void {
    if (this.socket !== socket || this.state === 'closed') return;
    this.state = 'ready';
    this.attempt = 0;
    const waiting = this.queue.splice(0);
    for (const q of waiting) this.write(q);
    for (const w of this.readyWaiters.splice(0)) w.resolve();
    this.emit('ready');
  }

  private onData(socket: net.Socket, chunk: Buffer): void {
    if (this.socket !== socket) return;
    try {
      this.parser.push(chunk, (reply) => {
        const p = this.inflight.shift();
        if (!p) {
          if (this.pushHandler) this.pushHandler(reply);
          return;
        }
        if (reply instanceof RespError) p.reject(reply);
        else p.resolve(reply);
      });
    } catch (err) {
      this.emit('error', err);
      socket.destroy();
    }
  }

  private onClose(socket: net.Socket): void {
    if (this.socket !== socket) return;
    this.socket = null;
    if (this.state === 'closed') return;
    this.failAll(new ConnectionError('connection lost'));
    const wasReady = this.state === 'ready';
    if (wasReady) this.emit('close');
    if (!this.opts.reconnect) {
      this.close();
      return;
    }
    const base = Math.min(this.opts.retryMaxMs, this.opts.retryMinMs * 2 ** Math.min(this.attempt, 16));
    const delay = Math.round(base * (0.5 + Math.random() / 2));
    this.attempt++;
    this.state = 'backoff';
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      if (this.state === 'backoff') this.open();
    }, delay);
    this.emit('reconnecting', delay);
  }

  private failAll(err: Error): void {
    const inflight = this.inflight.splice(0);
    for (const p of inflight) p.reject(err);
    if (this.state === 'closed') {
      for (const q of this.queue.splice(0)) q.reject(err);
    }
  }
}
