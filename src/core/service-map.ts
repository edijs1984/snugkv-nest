import { hostname } from 'node:os';
import type { SnugKvClient } from './client';

/**
 * Service cards: each running app describes itself in SnugKV so tools (the
 * SnugKV console's Map page) can draw the whole system without any
 * hand-written diagram. A card lists the app's HTTP routes, the queues it adds
 * jobs to and the ones it processes, and the topics it publishes and reads.
 *
 * Cards are JSON strings at `snug:svc:<service>:<instance>`, refreshed every
 * 15 s and expiring after 45 s, so a stopped instance disappears by itself.
 */

export const SERVICE_CARD_PREFIX = 'snug:svc:';

export interface ServiceRoute {
  method: string;
  path: string;
  /** `Controller.method` */
  handler?: string;
}

export interface ServiceCard {
  service: string;
  instance: string;
  host: string;
  pid: number;
  startedAt: number;
  updatedAt: number;
  sdk: string;
  http: ServiceRoute[];
  queues: {
    /** Queues this app adds jobs to. */
    produces: string[];
    /** Queues this app works on. */
    processes: Array<{ queue: string; concurrency?: number }>;
  };
  topics: {
    publishes: string[];
    subscribes: Array<{ topic: string; group?: string }>;
  };
}

/** What this process does with queues and topics, filled in as it runs. */
const seen = {
  produces: new Set<string>(),
  processes: new Map<string, number | undefined>(),
  publishes: new Set<string>(),
  subscribes: new Map<string, string | undefined>(),
};

/** @internal Called by Queue.add, registered queues and FlowProducer. */
export function noteQueueProducer(queue: string): void {
  seen.produces.add(queue);
}

/** @internal Called when a Worker starts. */
export function noteQueueWorker(queue: string, concurrency?: number): void {
  seen.processes.set(queue, concurrency);
}

/** @internal Called by Publisher.publish. */
export function notePublish(topic: string): void {
  seen.publishes.add(topic);
}

/** @internal Called when a stream consumer starts. */
export function noteSubscribe(topic: string, group?: string): void {
  seen.subscribes.set(topic, group);
}

/** @internal Tests. */
export function resetServiceMap(): void {
  seen.produces.clear();
  seen.processes.clear();
  seen.publishes.clear();
  seen.subscribes.clear();
}

export interface ServiceCardOptions {
  service: string;
  instance: string;
  /** Routes of this app (Nest fills these in). */
  http?: ServiceRoute[];
  /** How often the card is written, ms (default 15 s). */
  intervalMs?: number;
}

/** Writes this process's card until stopped. Never throws into the app. */
export class ServiceCardReporter {
  private timer: NodeJS.Timeout | undefined;
  private readonly startedAt = Date.now();
  private readonly intervalMs: number;

  constructor(
    private readonly client: Pick<SnugKvClient, 'call'>,
    private readonly options: ServiceCardOptions,
  ) {
    this.intervalMs = options.intervalMs ?? 15_000;
  }

  get key(): string {
    return `${SERVICE_CARD_PREFIX}${this.options.service}:${this.options.instance}`;
  }

  card(): ServiceCard {
    return {
      service: this.options.service,
      instance: this.options.instance,
      host: hostname(),
      pid: process.pid,
      startedAt: this.startedAt,
      updatedAt: Date.now(),
      sdk: 'snugkv-nest',
      http: this.options.http ?? [],
      queues: {
        produces: [...seen.produces].sort(),
        processes: [...seen.processes].map(([queue, concurrency]) => ({ queue, concurrency })).sort((a, b) => a.queue.localeCompare(b.queue)),
      },
      topics: {
        publishes: [...seen.publishes].sort(),
        subscribes: [...seen.subscribes].map(([topic, group]) => ({ topic, group })).sort((a, b) => a.topic.localeCompare(b.topic)),
      },
    };
  }

  /** Writes the card now. */
  async write(): Promise<void> {
    const ttl = Math.ceil((this.intervalMs * 3) / 1000);
    await this.client.call('SET', this.key, JSON.stringify(this.card()), 'EX', ttl).catch(() => undefined);
  }

  start(): void {
    if (this.timer) return;
    void this.write();
    this.timer = setInterval(() => void this.write(), this.intervalMs);
    this.timer.unref?.();
  }

  /** Stops writing and removes the card. */
  async stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    await this.client.call('DEL', this.key).catch(() => undefined);
  }
}
