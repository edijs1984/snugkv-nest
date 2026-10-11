import { hostname } from 'node:os';
import { SnugKvClient } from './client';
import { BusLogger, errorMessage } from './util';

/** Key prefix the stream UI scans for. Part of the public presence protocol. */
export const PRESENCE_PREFIX = 'snugkv:presence:';

export interface PresenceRecord {
  /** Protocol version. */
  v: 1;
  role: 'publisher' | 'consumer';
  service: string;
  instance: string;
  topic: string;
  /** Consumer group (consumers only). */
  group?: string;
  /** Consumer name inside the group (consumers only). */
  consumer?: string;
  host: string;
  pid: number;
  startedAt: number;
  updatedAt: number;
  /** Counters since the process started. */
  published?: number;
  processed?: number;
  failed?: number;
  deadLettered?: number;
  inflight?: number;
  concurrency?: number;
}

export function presenceKey(role: 'publisher' | 'consumer', topic: string, owner: string, instance: string): string {
  return `${PRESENCE_PREFIX}${role}:${topic}:${owner}:${instance}`;
}

export interface PresenceOptions {
  /** Time a record lives without a refresh. Default 15 s. */
  ttlSeconds?: number;
  /** Refresh period. Default ttl / 3. */
  intervalMs?: number;
}

export const HOST = hostname();

/**
 * Writes small expiring records so tools can show who publishes and who
 * consumes. A record disappears by itself `ttlSeconds` after its process stops.
 */
export class PresenceReporter {
  private readonly entries = new Map<string, () => PresenceRecord>();
  private timer: NodeJS.Timeout | null = null;
  private readonly ttl: number;
  private readonly interval: number;

  constructor(
    private readonly client: SnugKvClient,
    private readonly logger: BusLogger,
    opts: PresenceOptions = {},
  ) {
    this.ttl = Math.max(3, opts.ttlSeconds ?? 15);
    this.interval = opts.intervalMs ?? Math.floor((this.ttl * 1000) / 3);
  }

  register(key: string, build: () => PresenceRecord): void {
    this.entries.set(key, build);
    void this.flush();
    if (!this.timer) {
      this.timer = setInterval(() => void this.flush(), this.interval);
      this.timer.unref?.();
    }
  }

  async unregister(key: string): Promise<void> {
    this.entries.delete(key);
    try {
      await this.client.del(key);
    } catch {
      // expires on its own
    }
  }

  async flush(): Promise<void> {
    if (this.entries.size === 0 || !this.client.isReady) return;
    const now = Date.now();
    const cmds = [...this.entries.entries()].map(([key, build]) => {
      const rec = build();
      rec.updatedAt = now;
      return ['SET', key, JSON.stringify(rec), 'EX', this.ttl];
    });
    try {
      await this.client.pipeline(cmds);
    } catch (err) {
      this.logger.warn(`presence update failed: ${errorMessage(err)}`);
    }
  }

  async stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    const keys = [...this.entries.keys()];
    this.entries.clear();
    if (keys.length === 0) return;
    try {
      await this.client.del(...keys);
    } catch {
      // expires on its own
    }
  }
}
