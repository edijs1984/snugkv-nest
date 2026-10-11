/* eslint-disable @typescript-eslint/no-explicit-any */
import { EventEmitter } from 'node:events';
import { SnugKvClient, SnugKvClientOptions } from '../client';
import { Arg, RespError } from '../resp';
import { nextCronTime } from './cron';
import { Job } from './job';
import { JobsOptions, JobState, JobType, jobArgs, parseJson, QueueLike, toJson } from './types';

export interface QueueOptions {
  /** Share an existing client. It is not closed with the queue. */
  client?: SnugKvClient;
  /** Or give connection settings; the queue then owns and closes its client. */
  connection?: SnugKvClientOptions;
  /** Key prefix. Default `snugq`. All services using one queue must agree on it. */
  prefix?: string;
  /** Options merged into every `add`. */
  defaultJobOptions?: JobsOptions;
}

export interface RepeatOptions {
  /** Repeat every N ms (computed on the server). */
  every?: number;
  /** Cron pattern with 5 fields, or 6 with seconds first. */
  pattern?: string;
  /** IANA time zone for the pattern. Default UTC. */
  tz?: string;
  /** Stop after this many runs. */
  limit?: number;
  /** Stop after this time. */
  endDate?: number | Date;
}

export interface JobTemplate<D = any> {
  name?: string;
  data?: D;
  opts?: JobsOptions;
}

export interface JobScheduler {
  id: string;
  name: string;
  every?: number;
  pattern?: string;
  tz?: string;
  limit?: number;
  endDate?: number;
  /** Runs created so far. */
  count: number;
  /** When the next run is due (ms since epoch). */
  next: number;
  template: { name: string; data: any; opts: JobsOptions };
}

export type JobCounts = Record<JobType, number>;

const ALL_TYPES: JobType[] = ['wait', 'paused', 'active', 'delayed', 'prioritized', 'completed', 'failed', 'waiting-children'];

/** Builds the stored job from what we sent, so `add` needs no second round trip. */
export function localJob(q: QueueLike, id: string, name: string, data: unknown, opts: JobsOptions, parent?: { base: string; id: string }): Job {
  const a = jobArgs(name, data, opts);
  const raw: Record<string, string> = {
    name,
    data: String(a[1]),
    ts: String(Date.now()),
    delay: String(a[3]),
    prio: String(a[4]),
    att: String(a[6]),
    atm: '0',
    pend: '0',
  };
  if (a[5] === 1) raw.lifo = '1';
  if (a[7]) {
    raw.bt = String(a[7]);
    raw.bd = String(a[8]);
  }
  if (parent) {
    raw.pq = parent.base;
    raw.pid = parent.id;
  }
  return new Job(q, id, raw);
}

/**
 * A named queue of jobs. Producers call `add`; a {@link Worker} processes the jobs;
 * {@link QueueEvents} reports what happens. Needs a SnugKV server with the built-in functions on.
 */
export class Queue<D = any, R = any> extends EventEmitter implements QueueLike {
  readonly client: SnugKvClient;
  readonly base: string;
  readonly prefix: string;
  private readonly owns: boolean;
  private readyPromise?: Promise<void>;
  private readonly defaults: JobsOptions;

  constructor(readonly name: string, options: QueueOptions = {}) {
    super();
    if (!name || /[{}|]/.test(name)) throw new Error('queue name must be non-empty and contain no { } |');
    this.prefix = options.prefix ?? 'snugq';
    this.base = `${this.prefix}:{${name}}:`;
    this.owns = !options.client;
    this.client = options.client ?? new SnugKvClient(options.connection ?? {});
    this.defaults = options.defaultJobOptions ?? {};
    if (this.owns) {
      this.client.on('error', (e: Error) => {
        if (this.listenerCount('error') > 0) this.emit('error', e);
      });
    }
  }

  ready(): Promise<void> {
    if (this.client.isReady) return Promise.resolve();
    this.readyPromise ??= this.client.connect(10_000).finally(() => {
      this.readyPromise = undefined;
    });
    return this.readyPromise;
  }

  async fcall(fn: string, ...args: Arg[]): Promise<any> {
    await this.ready();
    return this.client.call('FCALL', fn, 1, this.base, ...args);
  }

  private async fcallRo(fn: string, ...args: Arg[]): Promise<any> {
    await this.ready();
    return this.client.call('FCALL_RO', fn, 1, this.base, ...args);
  }

  // ----- adding

  async add(name: string, data: D, opts: JobsOptions = {}): Promise<Job<D, R>> {
    const o = { ...this.defaults, ...opts };
    const parent = o.parent ? { base: this.baseOf(o.parent.queue), id: o.parent.id } : undefined;
    const reply = (await this.fcall('snugq_add', ...jobArgs(name, data, o, { parentBase: parent?.base, parentId: parent?.id }))) as [string, number];
    if (reply[1] === 0) return (await this.getJob(reply[0])) ?? localJob(this, reply[0], name, data, o, parent);
    return localJob(this, reply[0], name, data, o, parent);
  }

  /** Adds many jobs in one round trip. Not atomic: each job is added on its own. */
  async addBulk(jobs: Array<{ name: string; data: D; opts?: JobsOptions }>): Promise<Array<Job<D, R>>> {
    await this.ready();
    const prepared = jobs.map((j) => {
      const o = { ...this.defaults, ...j.opts };
      const parent = o.parent ? { base: this.baseOf(o.parent.queue), id: o.parent.id } : undefined;
      return { j, o, parent };
    });
    const replies = await this.client.pipeline(
      prepared.map((p) => ['FCALL', 'snugq_add', 1, this.base, ...jobArgs(p.j.name, p.j.data, p.o, { parentBase: p.parent?.base, parentId: p.parent?.id })] as Arg[]),
    );
    const out: Array<Job<D, R>> = [];
    for (let i = 0; i < replies.length; i++) {
      const r = replies[i];
      if (r instanceof RespError) throw r;
      const [id, added] = r as [string, number];
      const p = prepared[i]!;
      out.push(added === 0 ? ((await this.getJob(id)) ?? localJob(this, id, p.j.name, p.j.data, p.o, p.parent)) : localJob(this, id, p.j.name, p.j.data, p.o, p.parent));
    }
    return out;
  }

  /** Base key of another queue with the same prefix. */
  baseOf(queueName: string): string {
    return `${this.prefix}:{${queueName}}:`;
  }

  // ----- schedulers

  /**
   * Creates or replaces a repeatable job. `every` repeats on the server; `pattern` (cron) is
   * computed by the workers, so keep at least one worker running. Returns true when the first run was created.
   */
  async upsertJobScheduler(id: string, repeat: RepeatOptions, template: JobTemplate<D> = {}): Promise<boolean> {
    if (!id) throw new Error('scheduler id is required');
    if ((repeat.every === undefined) === (repeat.pattern === undefined)) throw new Error('set exactly one of "every" and "pattern"');
    const o = { ...this.defaults, ...template.opts };
    const endAt = repeat.endDate instanceof Date ? repeat.endDate.getTime() : (repeat.endDate ?? 0);
    let pat = '';
    let nextMs = 0;
    if (repeat.pattern !== undefined) {
      pat = JSON.stringify({ p: repeat.pattern, tz: repeat.tz ?? 'UTC' });
      nextMs = nextCronTime(repeat.pattern, Date.now(), repeat.tz ?? 'UTC');
    } else if (!(repeat.every! > 0)) {
      throw new Error('"every" must be positive');
    }
    const r = await this.fcall(
      'snugq_upsert_scheduler',
      id,
      repeat.every ?? 0,
      pat,
      nextMs,
      repeat.limit ?? 0,
      endAt,
      ...jobArgs(template.name ?? id, template.data ?? null, { ...o, delay: 0, jobId: undefined, parent: undefined }),
    );
    return r === 1;
  }

  async removeJobScheduler(id: string): Promise<boolean> {
    return ((await this.fcall('snugq_remove_scheduler', id)) as number) === 1;
  }

  async getJobSchedulers(start = 0, end = -1): Promise<JobScheduler[]> {
    await this.ready();
    const rows = (await this.client.call('ZRANGE', `${this.base}repeat`, start, end, 'WITHSCORES')) as string[];
    const out: JobScheduler[] = [];
    for (let i = 0; i + 1 < rows.length; i += 2) {
      const h = await this.client.hgetall(`${this.base}repeat:${rows[i]}`);
      if (!h.name) continue;
      const pat = h.pat ? (JSON.parse(h.pat) as { p: string; tz: string }) : undefined;
      out.push({
        id: rows[i]!,
        name: h.name,
        every: Number(h.every) > 0 ? Number(h.every) : undefined,
        pattern: pat?.p,
        tz: pat?.tz,
        limit: Number(h.limit) > 0 ? Number(h.limit) : undefined,
        endDate: Number(h.endAt) > 0 ? Number(h.endAt) : undefined,
        count: Number(h.count ?? 0),
        next: Number(rows[i + 1]),
        template: {
          name: h.name,
          data: parseJson(h.data),
          opts: {
            priority: Number(h.prio ?? 0),
            attempts: Number(h.att ?? 1),
            backoff: h.bt ? { type: h.bt as any, delay: Number(h.bd ?? 0) } : undefined,
          },
        },
      });
    }
    return out;
  }

  // ----- reading

  async getJob(id: string): Promise<Job<D, R> | null> {
    await this.ready();
    const raw = await this.client.hgetall(`${this.base}job:${id}`);
    return raw.name === undefined ? null : new Job<D, R>(this, id, raw);
  }

  async getJobState(id: string): Promise<JobState> {
    return (await this.fcallRo('snugq_state', id)) as JobState;
  }

  /** Jobs in the given states. Lists are in processing order; sorted sets by time or priority. */
  async getJobs(types: JobType | JobType[] = ALL_TYPES, start = 0, end = -1, asc = true): Promise<Array<Job<D, R>>> {
    const list = Array.isArray(types) ? types : [types];
    const out: Array<Job<D, R>> = [];
    for (const t of list) {
      const rows = (await this.fcallRo('snugq_jobs', t, start, end, asc ? 1 : 0)) as Array<[string, string[]]>;
      for (const [id, flat] of rows) out.push(Job.fromFlat(this, id, flat) as Job<D, R>);
    }
    return out;
  }

  getWaiting = (start = 0, end = -1) => this.getJobs('wait', start, end);
  getActive = (start = 0, end = -1) => this.getJobs('active', start, end);
  getDelayed = (start = 0, end = -1) => this.getJobs('delayed', start, end);
  getPrioritized = (start = 0, end = -1) => this.getJobs('prioritized', start, end);
  getWaitingChildren = (start = 0, end = -1) => this.getJobs('waiting-children', start, end);
  getCompleted = (start = 0, end = -1) => this.getJobs('completed', start, end, false);
  getFailed = (start = 0, end = -1) => this.getJobs('failed', start, end, false);

  async getJobCounts(...types: JobType[]): Promise<Partial<JobCounts>> {
    const flat = (await this.fcallRo('snugq_counts')) as Array<string | number>;
    const all: Record<string, number> = {};
    for (let i = 0; i + 1 < flat.length; i += 2) all[String(flat[i])] = Number(flat[i + 1]);
    if (types.length === 0) return all as JobCounts;
    return Object.fromEntries(types.map((t) => [t, all[t] ?? 0])) as Partial<JobCounts>;
  }

  async getJobCountByTypes(...types: JobType[]): Promise<number> {
    const c = await this.getJobCounts(...(types.length ? types : ALL_TYPES));
    return Object.values(c).reduce((a, b) => a + (b ?? 0), 0);
  }

  /** Jobs that still wait to run: waiting, paused, delayed and prioritized. */
  count(): Promise<number> {
    return this.getJobCountByTypes('wait', 'paused', 'delayed', 'prioritized');
  }

  async getJobLogs(id: string, start = 0, end = -1): Promise<{ logs: string[]; count: number }> {
    await this.ready();
    const key = `${this.base}job:${id}:logs`;
    return { logs: await this.client.lrange(key, start, end), count: await this.client.llen(key) };
  }

  /** Completed or failed jobs per minute, newest last. */
  async getMetrics(type: 'completed' | 'failed', minutes = 60): Promise<{ count: number; data: number[]; minute: number }> {
    await this.ready();
    const minute = Math.floor(Date.now() / 60_000);
    const keys: string[] = [];
    for (let m = minute - minutes + 1; m <= minute; m++) keys.push(`${this.base}metrics:${type}:${m}`);
    const [vals, total] = await Promise.all([this.client.mget(...keys), this.client.get(`${this.base}metrics:${type}:total`)]);
    return { count: Number(total ?? 0), data: vals.map((v) => Number(v ?? 0)), minute };
  }

  // ----- control

  async pause(): Promise<void> {
    await this.fcall('snugq_pause');
  }

  async resume(): Promise<void> {
    await this.fcall('snugq_resume');
  }

  async isPaused(): Promise<boolean> {
    await this.ready();
    return (await this.client.hget(`${this.base}meta`, 'paused')) === '1';
  }

  /** Total jobs processed at the same time across all workers. */
  async setGlobalConcurrency(n: number): Promise<void> {
    await this.ready();
    if (!(n >= 1)) throw new Error('concurrency must be at least 1');
    await this.client.hset(`${this.base}meta`, { gconc: Math.floor(n) });
  }

  async removeGlobalConcurrency(): Promise<void> {
    await this.ready();
    await this.client.hdel(`${this.base}meta`, 'gconc');
  }

  async getGlobalConcurrency(): Promise<number | null> {
    await this.ready();
    const v = await this.client.hget(`${this.base}meta`, 'gconc');
    return v ? Number(v) : null;
  }

  /** At most `max` jobs start per `duration` ms across all workers. */
  async setRateLimit(limit: { max: number; duration: number }): Promise<void> {
    await this.ready();
    if (!(limit.max >= 1) || !(limit.duration >= 1)) throw new Error('max and duration must be positive');
    await this.client.hset(`${this.base}meta`, { max: Math.floor(limit.max), duration: Math.floor(limit.duration) });
  }

  async removeRateLimit(): Promise<void> {
    await this.ready();
    await this.client.hdel(`${this.base}meta`, 'max', 'duration');
    await this.client.del(`${this.base}limiter`);
  }

  /** Stop all workers from starting jobs for `ms` ms. */
  async rateLimit(ms: number): Promise<void> {
    await this.ready();
    await this.client.setOpts(`${this.base}limited`, '1', { pxMs: Math.max(1, Math.floor(ms)) });
  }

  async getRateLimitTtl(): Promise<number> {
    await this.ready();
    const [a, b] = await Promise.all([this.client.pttl(`${this.base}limited`), this.client.pttl(`${this.base}limiter`)]);
    return Math.max(a, b, 0);
  }

  async remove(id: string): Promise<boolean> {
    return ((await this.fcall('snugq_remove', id)) as number) === 1;
  }

  /** Removes waiting and prioritized (and delayed, if asked) jobs. Returns how many. */
  async drain(includeDelayed = false): Promise<number> {
    return (await this.fcall('snugq_drain', includeDelayed ? 1 : 0)) as number;
  }

  /** Removes jobs older than `graceMs`. Returns their ids. `limit` 0 means all. */
  async clean(graceMs: number, limit: number, type: Exclude<JobType, 'active'> = 'completed'): Promise<string[]> {
    return (await this.fcall('snugq_clean', type, Math.max(0, Math.floor(graceMs)), Math.max(0, Math.floor(limit)))) as string[];
  }

  /** Puts failed (or completed) jobs back in the queue. Returns how many. */
  async retryJobs(opts: { state?: 'failed' | 'completed'; count?: number; resetAttemptsMade?: boolean } = {}): Promise<number> {
    const state = opts.state ?? 'failed';
    const jobs = await this.getJobs(state, 0, (opts.count ?? 1000) - 1);
    let n = 0;
    for (const j of jobs) {
      try {
        await j.retry(state, { resetAttemptsMade: opts.resetAttemptsMade });
        n++;
      } catch {
        // gone or already moved
      }
    }
    return n;
  }

  /** Runs delayed jobs now. Returns how many. */
  async promoteJobs(count = 1000): Promise<number> {
    const jobs = await this.getJobs('delayed', 0, count - 1);
    let n = 0;
    for (const j of jobs) {
      try {
        await j.promote();
        n++;
      } catch {
        // already moved
      }
    }
    return n;
  }

  /** Deletes the queue and all its jobs. Refuses while jobs are active, unless `force`. */
  async obliterate(opts: { force?: boolean; count?: number } = {}): Promise<void> {
    for (;;) {
      const r = (await this.fcall('snugq_obliterate', opts.force ? 1 : 0, opts.count ?? 1000)) as number;
      if (r === -1) throw new Error('cannot obliterate a queue with active jobs; use force');
      if (r === 0) return;
    }
  }

  /** Names of all queues that have had jobs, under a prefix. */
  static async list(client: SnugKvClient, prefix = 'snugq'): Promise<string[]> {
    return (await client.smembers(`${prefix}:queues`)).sort();
  }

  async close(): Promise<void> {
    if (this.owns) this.client.close();
  }
}

export { toJson };
