import { currentTrace, getTracer, traceHeader } from './trace';
import { SnugKvClient } from './client';
import { HOST, PresenceReporter, presenceKey } from './presence';
import { Arg } from './resp';
import { uuid } from './util';

export interface Serializer {
  serialize(value: unknown): string;
  deserialize(text: string): unknown;
}

export const jsonSerializer: Serializer = {
  serialize: (v) => JSON.stringify(v === undefined ? null : v),
  deserialize: (t) => JSON.parse(t),
};

export interface PublishOptions {
  /** Event name, e.g. `order.created`. Stored in the `event` field. */
  event?: string;
  /** Extra string headers (trace ids, tenant, …). */
  headers?: Record<string, string>;
  /** Trim the stream to about this many entries. Overrides the bus default. */
  maxLen?: number;
}

export interface BatchMessage<T = unknown> {
  payload: T;
  event?: string;
  headers?: Record<string, string>;
}

export interface PublisherOptions {
  service: string;
  instance: string;
  keyPrefix: string;
  defaultMaxLen?: number;
  serializer: Serializer;
}

/**
 * Message layout (all fields are strings):
 * `payload` serialized body · `event` · `mid` message uuid · `ts` publish time (ms)
 * · `producer` `service/instance` · `headers` JSON object, only when given.
 */
export class Publisher {
  private readonly counts = new Map<string, number>();
  private readonly startedAt = Date.now();

  constructor(
    private readonly client: SnugKvClient,
    private readonly opts: PublisherOptions,
    private readonly presence?: PresenceReporter,
  ) {}

  /** Stream key for a topic. */
  keyFor(topic: string): string {
    return this.opts.keyPrefix + topic;
  }

  async publish<T = unknown>(topic: string, payload: T, options: PublishOptions = {}): Promise<string> {
    const args = this.build(topic, { payload, event: options.event, headers: options.headers }, options.maxLen);
    const tracer = getTracer();
    if (tracer && currentTrace()) tracer.event(`publish ${topic}`, { topic, event: options.event });
    const id = (await this.client.call(...args)) as string;
    this.note(topic, 1);
    return id;
  }

  /** Publishes many messages in one round trip. Returns the entry ids in order. */
  async publishBatch<T = unknown>(topic: string, messages: Array<BatchMessage<T>>, options: { maxLen?: number } = {}): Promise<string[]> {
    if (messages.length === 0) return [];
    const replies = await this.client.pipeline(messages.map((m) => this.build(topic, m, options.maxLen)));
    const ids: string[] = [];
    for (const r of replies) {
      if (r instanceof Error) throw r;
      ids.push(String(r));
    }
    this.note(topic, ids.length);
    return ids;
  }

  private build(topic: string, m: BatchMessage, maxLen?: number): Arg[] {
    const fields: Record<string, string> = {
      payload: this.opts.serializer.serialize(m.payload),
      event: m.event ?? '',
      mid: uuid(),
      ts: String(Date.now()),
      producer: `${this.opts.service}/${this.opts.instance}`,
    };
    const headers = { ...traceHeader(), ...(m.headers ?? {}) };
    if (Object.keys(headers).length > 0) fields.headers = JSON.stringify(headers);
    return SnugKvClient.xaddArgs(this.keyFor(topic), fields, { maxLen: maxLen ?? this.opts.defaultMaxLen });
  }

  private note(topic: string, n: number): void {
    const first = !this.counts.has(topic);
    this.counts.set(topic, (this.counts.get(topic) ?? 0) + n);
    if (first && this.presence) {
      const key = presenceKey('publisher', topic, this.opts.service, this.opts.instance);
      this.presence.register(key, () => ({
        v: 1,
        role: 'publisher',
        service: this.opts.service,
        instance: this.opts.instance,
        topic,
        host: HOST,
        pid: process.pid,
        startedAt: this.startedAt,
        updatedAt: Date.now(),
        published: this.counts.get(topic) ?? 0,
      }));
    }
  }

  /** Messages published per topic since start. */
  stats(): Record<string, number> {
    return Object.fromEntries(this.counts);
  }
}
