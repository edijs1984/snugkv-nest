import { EventEmitter } from 'node:events';
import type { SnugKvClient } from './client';
import { Reply } from './resp';

export type MessageListener = (message: string, channel: string) => void;
export type PatternListener = (message: string, channel: string, pattern: string) => void;

/**
 * Pub/sub on its own connection. Subscriptions are restored after a
 * reconnect. Messages published while disconnected are not replayed; use a
 * stream (see `SnugKvBus`) when you need delivery guarantees.
 */
export class SnugKvSubscriber extends EventEmitter {
  private readonly channels = new Map<string, Set<MessageListener>>();
  private readonly patterns = new Map<string, Set<PatternListener>>();
  private started = false;

  constructor(private readonly client: SnugKvClient) {
    super();
    const conn = client.connection;
    conn.pushHandler = (r) => this.onPush(r);
    client.on('ready', () => this.restore());
    client.on('error', (e) => {
      if (this.listenerCount('error') > 0) this.emit('error', e);
    });
  }

  async connect(timeoutMs?: number): Promise<void> {
    await this.client.connect(timeoutMs);
    this.started = true;
  }

  async subscribe(channel: string, listener: MessageListener): Promise<() => Promise<void>> {
    let set = this.channels.get(channel);
    if (!set) {
      set = new Set();
      this.channels.set(channel, set);
      await this.send('SUBSCRIBE', channel);
    }
    set.add(listener);
    return () => this.unsubscribe(channel, listener);
  }

  async psubscribe(pattern: string, listener: PatternListener): Promise<() => Promise<void>> {
    let set = this.patterns.get(pattern);
    if (!set) {
      set = new Set();
      this.patterns.set(pattern, set);
      await this.send('PSUBSCRIBE', pattern);
    }
    set.add(listener);
    return () => this.punsubscribe(pattern, listener);
  }

  async unsubscribe(channel: string, listener?: MessageListener): Promise<void> {
    const set = this.channels.get(channel);
    if (!set) return;
    if (listener) set.delete(listener);
    if (!listener || set.size === 0) {
      this.channels.delete(channel);
      await this.send('UNSUBSCRIBE', channel);
    }
  }

  async punsubscribe(pattern: string, listener?: PatternListener): Promise<void> {
    const set = this.patterns.get(pattern);
    if (!set) return;
    if (listener) set.delete(listener);
    if (!listener || set.size === 0) {
      this.patterns.delete(pattern);
      await this.send('PUNSUBSCRIBE', pattern);
    }
  }

  close(): void {
    this.client.close();
  }

  private async send(...args: string[]): Promise<void> {
    if (!this.started) await this.connect();
    const conn = this.client.connection;
    conn.sendNoReply(args);
    // Confirmation arrives as a push; give the server a tick so callers can publish right after.
    await this.confirmation(args[0]!.toLowerCase(), args[1]!);
  }

  private pendingConfirms: Array<{ kind: string; name: string; done: () => void }> = [];

  private confirmation(kind: string, name: string): Promise<void> {
    return new Promise((resolve) => {
      const timer = setTimeout(done, 2000);
      function done() {
        clearTimeout(timer);
        resolve();
      }
      this.pendingConfirms.push({ kind, name, done });
    });
  }

  private restore(): void {
    if (!this.started) return;
    const conn = this.client.connection;
    for (const c of this.channels.keys()) conn.sendNoReply(['SUBSCRIBE', c]);
    for (const p of this.patterns.keys()) conn.sendNoReply(['PSUBSCRIBE', p]);
  }

  private onPush(reply: Reply): void {
    if (!Array.isArray(reply) || typeof reply[0] !== 'string') return;
    const kind = reply[0];
    if (kind === 'message') {
      const channel = String(reply[1]);
      for (const l of [...(this.channels.get(channel) ?? [])]) this.safe(() => l(String(reply[2]), channel));
    } else if (kind === 'pmessage') {
      const pattern = String(reply[1]);
      for (const l of [...(this.patterns.get(pattern) ?? [])]) this.safe(() => l(String(reply[3]), String(reply[2]), pattern));
    } else {
      const i = this.pendingConfirms.findIndex((p) => p.kind === kind && p.name === String(reply[1]));
      if (i >= 0) this.pendingConfirms.splice(i, 1)[0]!.done();
    }
  }

  private safe(fn: () => void): void {
    try {
      fn();
    } catch (err) {
      if (this.listenerCount('error') > 0) this.emit('error', err);
    }
  }
}
