import { getTracer, parseTraceHeader, TRACE_HEADER } from './trace';
import { SnugKvClient, StreamEntry } from './client';
import { HOST, PresenceReporter, presenceKey } from './presence';
import { Serializer } from './publisher';
import { RespError } from './resp';
import { BusLogger, errorMessage, sleep } from './util';

export interface MessageContext {
  /** Stream entry id, e.g. `1760000000000-0`. */
  id: string;
  topic: string;
  group: string;
  consumer: string;
  /** Event name given at publish time (may be empty). */
  event: string;
  /** Message uuid assigned by the publisher (empty for foreign entries). */
  messageId: string;
  /** `service/instance` of the publisher. */
  producer: string;
  /** Publish time in ms since epoch, or null. */
  publishedAt: number | null;
  headers: Record<string, string>;
  /** 1 on the first delivery. Counts earlier deliveries after a crash or recovery. */
  attempt: number;
  /** All raw stream fields. */
  fields: Record<string, string>;
  /** Acknowledge now. Only needed with `autoAck: false`. Safe to call twice. */
  ack(): Promise<void>;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type MessageHandler<T = any> = (payload: T, ctx: MessageContext) => unknown | Promise<unknown>;

export interface SubscribeOptions {
  topic: string;
  /** Consumer group. Services with the same group share the messages; different groups each get every message. Default: the bus `service` name. */
  group?: string;
  /** Consumer name inside the group. Default: `<service>-<instance>`. */
  consumer?: string;
  /** Where a brand-new group starts: `$` = only new messages (default), `0` = from the oldest retained message. */
  startFrom?: '$' | '0';
  /** Messages handled at the same time. Default 1 (keeps order). */
  concurrency?: number;
  /** Messages fetched ahead. Default `max(10, concurrency * 2)`. */
  prefetch?: number;
  /** Blocking read time per call. Default 5000 ms. */
  blockMs?: number;
  /** Acknowledge after the handler returns. Default true. */
  autoAck?: boolean;
  /** Handler runs per message before it is dead-lettered. Default 3. */
  maxAttempts?: number;
  /** Delay before a retry. Number of ms, or a function of the failed attempt number. Default 200 ms doubling to 10 s. */
  retryDelayMs?: number | ((failedAttempt: number) => number);
  /** Topic for messages that ran out of attempts. Default `<topic>.dlq`. `false` drops them (after logging). */
  deadLetterTopic?: string | false;
  /** Take over messages other consumers left unacknowledged for this long. Default 60 s. Keep it above your slowest handler. */
  claimMinIdleMs?: number;
  /** How often to look for such messages. Default 15 s. */
  claimIntervalMs?: number;
  /** How long `stop()` waits for running handlers. Default 10 s. */
  shutdownTimeoutMs?: number;
}

export interface ConsumerStats {
  topic: string;
  group: string;
  consumer: string;
  processed: number;
  failed: number;
  deadLettered: number;
  inflight: number;
  queued: number;
}

interface Task {
  entry: StreamEntry;
  /** Deliveries that happened before this one. */
  priorDeliveries: number;
}

export interface ConsumerDeps {
  /** Normal connection for acks, claims and dead-lettering. */
  cmd: SnugKvClient;
  /** Own connection for blocking reads. Closed by `stop()`. */
  read: SnugKvClient;
  service: string;
  instance: string;
  keyPrefix: string;
  serializer: Serializer;
  logger: BusLogger;
  presence?: PresenceReporter;
}

const defaultRetryDelay = (attempt: number): number => Math.min(10_000, 200 * 2 ** (attempt - 1));

export class Consumer {
  readonly topic: string;
  readonly group: string;
  readonly consumer: string;
  readonly key: string;

  private readonly o: Required<Pick<SubscribeOptions, 'startFrom' | 'concurrency' | 'prefetch' | 'blockMs' | 'autoAck' | 'maxAttempts' | 'claimMinIdleMs' | 'claimIntervalMs' | 'shutdownTimeoutMs'>>;
  private readonly retryDelay: (failedAttempt: number) => number;
  private readonly dlqKey: string | null;
  private readonly abort = new AbortController();
  private readonly queue: Task[] = [];
  private takers: Array<(t: Task | null) => void> = [];
  private roomWaiters: Array<() => void> = [];
  private readonly active = new Set<string>();
  private loops: Array<Promise<void>> = [];
  private claimTimer: NodeJS.Timeout | null = null;
  private claiming = false;
  private inflight = 0;
  private processed = 0;
  private failed = 0;
  private deadLettered = 0;
  private readonly startedAt = Date.now();
  private presenceKey: string | null = null;
  private started = false;

  constructor(
    opts: SubscribeOptions,
    private readonly handler: MessageHandler,
    private readonly d: ConsumerDeps,
  ) {
    this.topic = opts.topic;
    this.group = opts.group ?? d.service;
    this.consumer = opts.consumer ?? `${d.service}-${d.instance}`;
    this.key = d.keyPrefix + opts.topic;
    const concurrency = Math.max(1, Math.floor(opts.concurrency ?? 1));
    this.o = {
      startFrom: opts.startFrom ?? '$',
      concurrency,
      prefetch: Math.max(1, Math.floor(opts.prefetch ?? Math.max(10, concurrency * 2))),
      blockMs: opts.blockMs ?? 5_000,
      autoAck: opts.autoAck ?? true,
      maxAttempts: Math.max(1, Math.floor(opts.maxAttempts ?? 3)),
      claimMinIdleMs: opts.claimMinIdleMs ?? 60_000,
      claimIntervalMs: opts.claimIntervalMs ?? 15_000,
      shutdownTimeoutMs: opts.shutdownTimeoutMs ?? 10_000,
    };
    const r = opts.retryDelayMs;
    this.retryDelay = typeof r === 'function' ? r : typeof r === 'number' ? () => r : defaultRetryDelay;
    const dlq = opts.deadLetterTopic === undefined ? `${opts.topic}.dlq` : opts.deadLetterTopic;
    this.dlqKey = dlq === false ? null : d.keyPrefix + dlq;
  }

  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;
    await this.d.read.connect();
    await this.d.cmd.xgroupCreate(this.key, this.group, this.o.startFrom);
    if (this.d.presence) {
      this.presenceKey = presenceKey('consumer', this.topic, this.group, this.consumer);
      this.d.presence.register(this.presenceKey, () => ({
        v: 1,
        role: 'consumer',
        service: this.d.service,
        instance: this.d.instance,
        topic: this.topic,
        group: this.group,
        consumer: this.consumer,
        host: HOST,
        pid: process.pid,
        startedAt: this.startedAt,
        updatedAt: Date.now(),
        processed: this.processed,
        failed: this.failed,
        deadLettered: this.deadLettered,
        inflight: this.inflight,
        concurrency: this.o.concurrency,
      }));
    }
    for (let i = 0; i < this.o.concurrency; i++) this.loops.push(this.worker());
    this.loops.push(this.reader());
    this.claimTimer = setInterval(() => void this.claimStale(), this.o.claimIntervalMs);
    this.claimTimer.unref?.();
  }

  stats(): ConsumerStats {
    return {
      topic: this.topic,
      group: this.group,
      consumer: this.consumer,
      processed: this.processed,
      failed: this.failed,
      deadLettered: this.deadLettered,
      inflight: this.inflight,
      queued: this.queue.length,
    };
  }

  /** Stops reading, lets running handlers finish (up to `shutdownTimeoutMs`) and leaves the rest unacknowledged for other consumers. */
  async stop(): Promise<void> {
    if (this.abort.signal.aborted) return;
    this.abort.abort();
    if (this.claimTimer) clearInterval(this.claimTimer);
    this.claimTimer = null;
    for (const t of this.takers.splice(0)) t(null);
    for (const w of this.roomWaiters.splice(0)) w();
    this.d.read.close();
    const timeout = Symbol('timeout');
    const res = await Promise.race([Promise.allSettled(this.loops), sleep(this.o.shutdownTimeoutMs).then(() => timeout)]);
    if (res === timeout) this.d.logger.warn(`${this.topic}/${this.group}: ${this.inflight} handler(s) still running at shutdown; their messages stay pending`);
    if (this.presenceKey && this.d.presence) await this.d.presence.unregister(this.presenceKey);
    try {
      const pending = await this.d.cmd.xpending(this.key, this.group, '-', '+', 1, this.consumer);
      if (pending.length === 0) await this.d.cmd.xgroupDelConsumer(this.key, this.group, this.consumer);
    } catch {
      // the consumer entry is only cosmetic
    }
  }

  // ----- reading

  private async reader(): Promise<void> {
    const sig = this.abort.signal;
    let history = true;
    let cursor = '0';
    while (!sig.aborted) {
      try {
        await this.waitForRoom();
        if (sig.aborted) break;
        const count = Math.max(1, this.o.prefetch - this.queue.length);
        if (history) {
          const r = await this.d.read.xreadgroup({ group: this.group, consumer: this.consumer, streams: [{ key: this.key, id: cursor }], count });
          const entries = r?.[0]?.entries ?? [];
          if (entries.length === 0) {
            history = false;
            continue;
          }
          cursor = entries[entries.length - 1]!.id;
          await this.enqueue(entries, 'history');
        } else {
          const r = await this.d.read.xreadgroup({ group: this.group, consumer: this.consumer, streams: [{ key: this.key, id: '>' }], count, blockMs: this.o.blockMs });
          const entries = r?.[0]?.entries ?? [];
          if (entries.length) await this.enqueue(entries, 'new');
        }
      } catch (err) {
        if (sig.aborted) break;
        if (err instanceof RespError && err.code === 'NOGROUP') {
          try {
            await this.d.cmd.xgroupCreate(this.key, this.group, this.o.startFrom);
          } catch (e) {
            this.d.logger.warn(`${this.topic}/${this.group}: cannot recreate group: ${errorMessage(e)}`);
          }
        } else {
          this.d.logger.warn(`${this.topic}/${this.group}: read failed: ${errorMessage(err)}`);
        }
        history = true;
        cursor = '0';
        await sleep(1_000, sig);
      }
    }
  }

  private waitForRoom(): Promise<void> {
    if (this.queue.length < this.o.prefetch) return Promise.resolve();
    return new Promise((resolve) => this.roomWaiters.push(resolve));
  }

  private async enqueue(entries: StreamEntry[], mode: 'new' | 'history' | 'claimed'): Promise<void> {
    const fresh: StreamEntry[] = [];
    const gone: string[] = [];
    for (const e of entries) {
      if (this.active.has(e.id)) continue;
      if (Object.keys(e.fields).length === 0) gone.push(e.id); // trimmed or deleted while pending
      else fresh.push(e);
    }
    if (gone.length) await this.d.cmd.xack(this.key, this.group, ...gone).catch(() => undefined);
    if (fresh.length === 0) return;
    let counts: number[] = fresh.map(() => 0);
    if (mode !== 'new') {
      counts = await this.deliveryCounts(fresh.map((e) => e.id));
      // XAUTOCLAIM has already counted the delivery it hands us.
      if (mode === 'claimed') counts = counts.map((c) => Math.max(0, c - 1));
    }
    fresh.forEach((entry, i) => {
      this.active.add(entry.id);
      const task: Task = { entry, priorDeliveries: counts[i] ?? 0 };
      const taker = this.takers.shift();
      if (taker) taker(task);
      else this.queue.push(task);
    });
  }

  private async deliveryCounts(ids: string[]): Promise<number[]> {
    try {
      const replies = await this.d.cmd.pipeline(ids.map((id) => ['XPENDING', this.key, this.group, id, id, 1]));
      return replies.map((r) => {
        const row = Array.isArray(r) ? r[0] : null;
        return Array.isArray(row) ? Number(row[3]) || 0 : 0;
      });
    } catch {
      return ids.map(() => 0);
    }
  }

  private async claimStale(): Promise<void> {
    if (this.claiming || this.abort.signal.aborted || !this.d.cmd.isReady) return;
    this.claiming = true;
    try {
      let cursor = '0-0';
      for (let page = 0; page < 20 && !this.abort.signal.aborted; page++) {
        const r = await this.d.cmd.xautoclaim(this.key, this.group, this.consumer, this.o.claimMinIdleMs, cursor, 50);
        if (r.deleted.length) await this.d.cmd.xack(this.key, this.group, ...r.deleted);
        const fresh = r.entries.filter((e) => !this.active.has(e.id));
        if (fresh.length) await this.enqueue(fresh, 'claimed');
        cursor = r.next;
        if (cursor === '0-0') break;
      }
    } catch (err) {
      if (!this.abort.signal.aborted) this.d.logger.warn(`${this.topic}/${this.group}: claim failed: ${errorMessage(err)}`);
    } finally {
      this.claiming = false;
    }
  }

  // ----- processing

  private take(): Promise<Task | null> {
    if (this.abort.signal.aborted) return Promise.resolve(null);
    const t = this.queue.shift();
    if (t) {
      this.roomWaiters.shift()?.();
      return Promise.resolve(t);
    }
    return new Promise((resolve) => this.takers.push(resolve));
  }

  private async worker(): Promise<void> {
    for (;;) {
      const task = await this.take();
      if (!task) return;
      this.inflight++;
      try {
        await this.process(task);
      } catch (err) {
        this.d.logger.error(`${this.topic}/${this.group}: unexpected error for ${task.entry.id}: ${errorMessage(err)}`);
      } finally {
        this.inflight--;
        this.active.delete(task.entry.id);
      }
    }
  }

  private async process({ entry, priorDeliveries }: Task): Promise<void> {
    const f = entry.fields;
    let acked = false;
    const ack = async (): Promise<void> => {
      if (acked) return;
      acked = true;
      try {
        await this.d.cmd.xack(this.key, this.group, entry.id);
      } catch (err) {
        acked = false;
        throw err;
      }
    };
    const ts = Number(f.ts);
    const ctx: MessageContext = {
      id: entry.id,
      topic: this.topic,
      group: this.group,
      consumer: this.consumer,
      event: f.event ?? '',
      messageId: f.mid ?? '',
      producer: f.producer ?? '',
      publishedAt: Number.isFinite(ts) && f.ts ? ts : null,
      headers: parseHeaders(f.headers),
      attempt: priorDeliveries + 1,
      fields: f,
      ack,
    };

    let payload: unknown;
    try {
      payload = f.payload === undefined ? { ...f } : this.d.serializer.deserialize(f.payload);
    } catch (err) {
      await this.deadLetter(entry, new Error(`undecodable payload: ${errorMessage(err)}`), ctx.attempt);
      return;
    }

    let lastError: unknown = new Error('delivery limit exceeded');
    for (let attempt = ctx.attempt; ; attempt++) {
      if (attempt > this.o.maxAttempts) {
        await this.deadLetter(entry, lastError, attempt - 1);
        return;
      }
      ctx.attempt = attempt;
      try {
        const tracer = getTracer();
        if (tracer) {
          await tracer.continue(
            parseTraceHeader(ctx.headers[TRACE_HEADER]),
            `${this.topic}:${ctx.event || 'message'}`,
            { topic: this.topic, group: this.group, event: ctx.event, messageId: ctx.messageId, attempt },
            () => this.handler(payload, ctx),
          );
        } else {
          await this.handler(payload, ctx);
        }
      } catch (err) {
        lastError = err;
        this.failed++;
        if (attempt >= this.o.maxAttempts) {
          await this.deadLetter(entry, err, attempt);
          return;
        }
        this.d.logger.warn(`${this.topic}/${this.group}: ${entry.id} failed (attempt ${attempt}/${this.o.maxAttempts}): ${errorMessage(err)}`);
        if (!(await sleep(this.retryDelay(attempt), this.abort.signal))) return; // shutting down: stays pending
        continue;
      }
      this.processed++;
      if (this.o.autoAck) {
        try {
          await ack();
        } catch (err) {
          this.d.logger.warn(`${this.topic}/${this.group}: ack of ${entry.id} failed, it will be redelivered: ${errorMessage(err)}`);
        }
      }
      return;
    }
  }

  private async deadLetter(entry: StreamEntry, err: unknown, attempts: number): Promise<void> {
    const reason = errorMessage(err);
    try {
      if (this.dlqKey) {
        await this.d.cmd.xadd(this.dlqKey, {
          ...entry.fields,
          dlq_error: reason.slice(0, 2000),
          dlq_source: this.topic,
          dlq_group: this.group,
          dlq_id: entry.id,
          dlq_attempts: String(attempts),
          dlq_at: String(Date.now()),
        });
      } else {
        this.d.logger.error(`${this.topic}/${this.group}: dropping ${entry.id} after ${attempts} attempt(s): ${reason}`);
      }
      await this.d.cmd.xack(this.key, this.group, entry.id);
      this.deadLettered++;
    } catch (e) {
      this.d.logger.error(`${this.topic}/${this.group}: could not dead-letter ${entry.id}, it stays pending: ${errorMessage(e)}`);
    }
  }
}

function parseHeaders(raw: string | undefined): Record<string, string> {
  if (!raw) return {};
  try {
    const v = JSON.parse(raw) as unknown;
    return v && typeof v === 'object' ? (v as Record<string, string>) : {};
  } catch {
    return {};
  }
}
