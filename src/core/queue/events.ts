/* eslint-disable @typescript-eslint/no-explicit-any */
import { EventEmitter } from 'node:events';
import { SnugKvClient, SnugKvClientOptions, toEntries } from '../client';
import { parseJson } from './types';

export interface QueueEventsOptions {
  client?: SnugKvClient;
  connection?: SnugKvClientOptions;
  prefix?: string;
  /** Where to start reading: `$` (default) only new events, or a stream id. */
  lastEventId?: string;
  /** How long one blocking read waits, ms. Default 10000. */
  blockingTimeout?: number;
  autorun?: boolean;
}

/**
 * Listens to a queue's event stream: `added`, `waiting`, `delayed`, `active`, `progress`, `completed`,
 * `failed`, `retries`, `stalled`, `removed`, `paused`, `resumed`, `duplicated`, `deduplicated`,
 * `waiting-children`. Listeners get `(args, eventId)`; `args.jobId` names the job. Uses its own connection.
 */
export class QueueEvents extends EventEmitter {
  readonly key: string;
  private readonly read: SnugKvClient;
  private closing = false;
  private readyPromise?: Promise<void>;

  constructor(readonly name: string, private readonly options: QueueEventsOptions = {}) {
    super();
    this.key = `${options.prefix ?? 'snugq'}:{${name}}:events`;
    this.read = options.client ? options.client.duplicate({ name: `queue-events:${name}` }) : new SnugKvClient({ ...options.connection, name: `queue-events:${name}` });
    this.read.on('error', (e: Error) => {
      if (this.listenerCount('error') > 0) this.emit('error', e);
    });
    if (options.autorun !== false) void this.run().catch(() => undefined);
  }

  /** Resolves when events are being read. */
  waitUntilReady(): Promise<void> {
    return this.run();
  }

  run(): Promise<void> {
    this.readyPromise ??= this.start();
    return this.readyPromise;
  }

  private async start(): Promise<void> {
    await this.read.connect(10_000);
    let last = this.options.lastEventId ?? '$';
    if (last === '$') {
      const tail = toEntries(await this.read.call('XREVRANGE', this.key, '+', '-', 'COUNT', 1));
      last = tail[0]?.id ?? '0-0';
    }
    void this.pump(last);
  }

  private async pump(from: string): Promise<void> {
    let last = from;
    const block = this.options.blockingTimeout ?? 10_000;
    while (!this.closing) {
      try {
        const raw = await this.read.call('XREAD', 'COUNT', 100, 'BLOCK', block, 'STREAMS', this.key, last);
        if (!Array.isArray(raw)) continue;
        for (const stream of raw) {
          if (!Array.isArray(stream)) continue;
          for (const e of toEntries(stream[1] ?? null)) {
            last = e.id;
            const { event, ...rest } = e.fields;
            if (!event) continue;
            this.emit(event, convert(event, rest), e.id);
          }
        }
      } catch (err) {
        if (this.closing) return;
        if (this.listenerCount('error') > 0) this.emit('error', err);
        await new Promise((r) => setTimeout(r, 500));
      }
    }
  }

  async close(): Promise<void> {
    this.closing = true;
    this.read.close();
    this.removeAllListeners();
  }
}

function convert(event: string, f: Record<string, string>): Record<string, any> {
  const out: Record<string, any> = { ...f };
  if (event === 'completed') out.returnvalue = parseJson(f.returnvalue);
  if (event === 'progress') out.data = parseJson(f.data);
  if (f.delay !== undefined) out.delay = Number(f.delay);
  if (f.attemptsMade !== undefined) out.attemptsMade = Number(f.attemptsMade);
  return out;
}
