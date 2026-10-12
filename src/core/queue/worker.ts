/* eslint-disable @typescript-eslint/no-explicit-any */
import { getTracer } from '../trace';
import { EventEmitter } from 'node:events';
import { SnugKvClient, SnugKvClientOptions } from '../client';
import type { SnugKvSubscriber } from '../pubsub';
import { errorMessage, uuid } from '../util';
import { nextCronTime } from './cron';
import { Job } from './job';
import { Queue } from './queue';
import { DelayedError, RateLimitError, toJson, UnrecoverableError } from './types';

export type Processor<D = any, R = any> = (job: Job<D, R>, token: string) => Promise<R> | R;

export interface WorkerOptions {
  client?: SnugKvClient;
  connection?: SnugKvClientOptions;
  prefix?: string;
  /** Jobs processed at the same time by this worker. Default 1. */
  concurrency?: number;
  /** A job's lock lasts this long unless renewed, ms. Default 30000. Keep it above long synchronous stretches. */
  lockDuration?: number;
  /** How often locks are renewed, ms. Default half the lock duration. */
  lockRenewTime?: number;
  /** How often to look for stalled jobs, ms. Default 30000. */
  stalledInterval?: number;
  /** A job may stall this many times before it fails. Default 1. */
  maxStalledCount?: number;
  skipStalledCheck?: boolean;
  /** Longest wait between checks when the queue is empty, ms. Default 5000. New jobs wake workers sooner. */
  drainDelay?: number;
  /** Start right away. Default true. */
  autorun?: boolean;
  /** Queue-wide rate limit, stored on the server when the worker starts: at most `max` jobs per `duration` ms. */
  limiter?: { max: number; duration: number };
  /** Delay for jobs with `backoff.type = 'custom'`. Return -1 to fail the job for good. */
  settings?: { backoffStrategy?: (attemptsMade: number, type: string, err: Error, job: Job) => number | Promise<number> };
}

/**
 * Takes jobs from a queue and runs the processor. Events: `active`, `completed`, `failed`
 * (once per failed attempt), `progress`, `stalled`, `drained`, `error`, `closed`.
 */
export class Worker<D = any, R = any> extends EventEmitter {
  readonly id = uuid().slice(0, 8);
  private readonly queue: Queue<D, R>;
  private readonly opts: Required<Pick<WorkerOptions, 'concurrency' | 'lockDuration' | 'stalledInterval' | 'maxStalledCount' | 'drainDelay'>> & WorkerOptions;
  private readonly active = new Map<string, string>();
  private readonly waiters = new Set<() => void>();
  private timers: NodeJS.Timeout[] = [];
  private sub?: SnugKvSubscriber;
  private unsub?: () => Promise<void>;
  private slots: Array<Promise<void>> = [];
  private running = false;
  private closing = false;
  private paused = false;
  private tokenCounter = 0;
  private pendingRateLimit = 0;
  private hadWork = false;
  private runPromise?: Promise<void>;

  constructor(readonly name: string, private readonly processor: Processor<D, R>, options: WorkerOptions = {}) {
    super();
    if (typeof processor !== 'function') throw new Error('Worker needs a processor function');
    this.opts = { concurrency: 1, lockDuration: 30_000, stalledInterval: 30_000, maxStalledCount: 1, drainDelay: 5_000, ...options };
    this.queue = new Queue<D, R>(name, { client: options.client, connection: options.connection, prefix: options.prefix });
    this.queue.on('error', (e: Error) => this.emitError(e));
    if (options.autorun !== false) void this.run();
  }

  get isRunning(): boolean {
    return this.running && !this.closing;
  }

  /** Starts processing. Resolves when the worker has closed. Called for you unless `autorun` is false. */
  run(): Promise<void> {
    this.runPromise ??= this.start().catch((e) => {
      this.emitError(e);
    });
    return this.runPromise;
  }

  waitUntilReady(): Promise<void> {
    return this.queue.ready();
  }

  private emitError(e: unknown): void {
    if (this.listenerCount('error') > 0) this.emit('error', e);
  }

  private async start(): Promise<void> {
    await this.queue.ready();
    if (this.opts.limiter) await this.queue.setRateLimit(this.opts.limiter);
    this.running = true;
    try {
      const owner = this.queue.client.name;
      // Named after the app and the queue, e.g. `ledger/<instance>:wake:chain-events`.
      this.sub = this.queue.client.subscriber(owner ? { name: `${owner}:wake:${this.queue.name}` } : {});
      this.sub.on('error', () => undefined);
      this.unsub = await this.sub.subscribe(`${this.queue.base}wake`, () => this.wake());
    } catch (e) {
      this.emitError(e); // polling still works
    }
    const renew = this.opts.lockRenewTime ?? Math.floor(this.opts.lockDuration / 2);
    this.timers.push(setInterval(() => void this.renewLocks(), renew));
    if (!this.opts.skipStalledCheck) {
      this.timers.push(setInterval(() => void this.checkStalled(), this.opts.stalledInterval));
      void this.checkStalled();
    }
    this.slots = Array.from({ length: this.opts.concurrency }, () => this.slot());
    await Promise.all(this.slots);
    await this.finish();
  }

  private wake(): void {
    for (const w of [...this.waiters]) w();
  }

  private idle(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const done = () => {
        clearTimeout(t);
        this.waiters.delete(done);
        resolve();
      };
      const t = setTimeout(done, Math.max(1, ms));
      this.waiters.add(done);
    });
  }

  private async slot(): Promise<void> {
    while (!this.closing) {
      if (this.paused) {
        await this.idle(500);
        continue;
      }
      let reply: any[];
      const token = `${this.id}:${++this.tokenCounter}`;
      try {
        reply = await this.queue.fcall('snugq_fetch', token, this.opts.lockDuration);
      } catch (e) {
        this.emitError(e);
        await this.idle(1000);
        continue;
      }
      switch (reply[0]) {
        case 'job': {
          this.hadWork = true;
          const job = Job.fromFlat(this.queue, String(reply[1]), reply[2]) as Job<D, R>;
          job.token = token;
          await this.process(job, token);
          break;
        }
        case 'empty': {
          if (this.hadWork && this.active.size === 0) {
            this.hadWork = false;
            this.emit('drained');
          }
          const next = Number(reply[1]);
          await this.idle(next < 0 ? this.opts.drainDelay : Math.min(next + 2, this.opts.drainDelay));
          break;
        }
        case 'limited':
          await this.idle(Math.min(Number(reply[1]) + 2, this.opts.drainDelay));
          break;
        case 'paused':
          await this.idle(Math.min(1000, this.opts.drainDelay));
          break;
        default: // 'busy': the queue's global concurrency is used up
          await this.idle(200);
      }
    }
  }

  private async process(job: Job<D, R>, token: string): Promise<void> {
    this.active.set(job.id, token);
    this.emit('active', job, 'waiting');
    try {
      if (job.schedulerId && job.raw.pat) await this.scheduleNext(job).catch((e) => this.emitError(e));
      const progress = (p: unknown) => this.emit('progress', job, p);
      const orig = job.updateProgress.bind(job);
      job.updateProgress = async (p) => {
        await orig(p);
        progress(p);
      };
      let result: R;
      try {
        const tracer = getTracer();
        result = tracer
          ? await tracer.continue(job.trace, `${this.queue.name}:${job.name}`, { queue: this.queue.name, job: job.name, jobId: job.id, attempt: job.attemptsMade + 1 }, () => this.processor(job, token))
          : await this.processor(job, token);
      } catch (err) {
        await this.onFailure(job, token, err instanceof Error ? err : new Error(errorMessage(err)));
        return;
      }
      const r = await this.queue.fcall('snugq_complete', job.id, token, toJson(result));
      if (r === 0) {
        job.returnvalue = result;
        job.finishedOn = Date.now();
        this.emit('completed', job, result, 'active');
      } else if (r === -2) {
        this.emitError(new Error(`lock of job ${job.id} was lost; its result was discarded`));
      }
    } catch (e) {
      this.emitError(e);
    } finally {
      this.active.delete(job.id);
    }
  }

  private async onFailure(job: Job<D, R>, token: string, err: Error): Promise<void> {
    if (err instanceof DelayedError) return;
    if (err instanceof RateLimitError) {
      const ms = this.pendingRateLimit;
      this.pendingRateLimit = 0;
      await this.queue.fcall('snugq_move_to_wait', job.id, token, ms);
      return;
    }
    const attempts = job.opts.attempts ?? 1;
    const made = job.attemptsMade;
    let final = err instanceof UnrecoverableError;
    let custom = -1;
    if (!final && made < attempts && job.opts.backoff && typeof job.opts.backoff === 'object' && job.opts.backoff.type === 'custom') {
      const strategy = this.opts.settings?.backoffStrategy;
      custom = strategy ? await strategy(made, 'custom', err, job as Job) : 0;
      if (custom < 0) final = true;
    }
    const limit = 10;
    const stack = toJson([...job.stacktrace, err.stack ?? err.message].slice(-limit));
    const r = await this.queue.fcall('snugq_fail', job.id, token, err.message || err.name, stack, final ? 1 : 0, final ? -1 : custom);
    if (r === -2) {
      this.emitError(new Error(`lock of job ${job.id} was lost while failing: ${err.message}`));
      return;
    }
    job.failedReason = err.message;
    job.attemptsMade = made;
    this.emit('failed', job, err, 'active');
  }

  private async scheduleNext(job: Job<D, R>): Promise<void> {
    const { p, tz } = JSON.parse(job.raw.pat!) as { p: string; tz: string };
    const own = Number(job.id.split(':').pop());
    const next = nextCronTime(p, Math.max(Number.isFinite(own) ? own : 0, Date.now()), tz);
    await this.queue.fcall('snugq_scheduler_next', job.schedulerId!, next);
  }

  /**
   * Call from the processor, then `throw new RateLimitError()`: no worker starts a job for `ms`,
   * and this job goes back to the queue without using an attempt.
   */
  async rateLimit(ms: number): Promise<void> {
    this.pendingRateLimit = Math.max(1, Math.floor(ms));
    await this.queue.rateLimit(ms);
  }

  private async renewLocks(): Promise<void> {
    for (const [id, token] of [...this.active]) {
      try {
        const ok = await this.queue.fcall('snugq_extend_lock', id, token, this.opts.lockDuration);
        if (ok === 0) this.emitError(new Error(`lost the lock of job ${id}`));
      } catch (e) {
        this.emitError(e);
      }
    }
  }

  private async checkStalled(): Promise<void> {
    if (this.closing) return;
    try {
      const [stalled, failed] = (await this.queue.fcall('snugq_stalled', this.opts.maxStalledCount, this.opts.stalledInterval)) as [string[], string[]];
      for (const id of stalled) this.emit('stalled', id, 'active');
      for (const id of failed) {
        const job = await this.queue.getJob(id);
        if (job) this.emit('failed', job, new Error('job stalled more than allowable limit'), 'active');
      }
    } catch (e) {
      this.emitError(e);
    }
  }

  /** Stops taking new jobs; running ones continue. */
  async pause(): Promise<void> {
    this.paused = true;
  }

  resume(): void {
    this.paused = false;
    this.wake();
  }

  /**
   * Stops the worker. Waits for running jobs unless `force`, in which case their locks expire and
   * another worker picks them up as stalled jobs.
   */
  async close(force = false): Promise<void> {
    if (this.closing) {
      if (force) await this.finish();
      else await this.runPromise;
      return;
    }
    this.closing = true;
    this.wake();
    if (force) {
      await this.finish();
      return;
    }
    if (this.runPromise) await this.runPromise;
    else await this.finish();
  }

  private finished = false;

  private async finish(): Promise<void> {
    if (this.finished) return;
    this.finished = true;
    for (const t of this.timers) clearInterval(t);
    this.timers = [];
    try {
      await this.unsub?.();
    } catch {
      // closing
    }
    this.sub?.close();
    await this.queue.close();
    this.running = false;
    this.emit('closed');
  }
}
