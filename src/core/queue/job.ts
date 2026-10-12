/* eslint-disable @typescript-eslint/no-explicit-any */
import { pairsToObject } from '../client';
import type { QueueEvents } from './events';
import { takeTrace, TraceContext } from '../trace';
import { JobsOptions, JobState, parseJson, QueueLike, queueNameOf, toJson } from './types';

/** A job as stored on the server. Fetched jobs are snapshots; call `getState()` or `Queue.getJob()` for fresh data. */
export class Job<D = any, R = any> {
  readonly id: string;
  readonly name: string;
  data: D;
  readonly opts: JobsOptions;
  readonly timestamp: number;
  readonly delay: number;
  readonly priority: number;
  progress: any;
  returnvalue: R | undefined;
  failedReason: string | undefined;
  stacktrace: string[];
  attemptsMade: number;
  stalledCounter: number;
  processedOn: number | undefined;
  finishedOn: number | undefined;
  /** Lock token while a worker holds the job. */
  token: string | undefined;
  /** Scheduler that created this job. */
  readonly schedulerId: string | undefined;
  readonly parent: { id: string; queue: string } | undefined;
  /** Children that have not finished. */
  readonly pendingChildren: number;
  /** Set by a worker. */
  readonly raw: Record<string, string>;
  /** Trace the job was added in, when tracing is on. */
  readonly trace: TraceContext | undefined;

  constructor(readonly queue: QueueLike, id: string, raw: Record<string, string>) {
    this.id = id;
    this.raw = raw;
    this.name = raw.name ?? '';
    this.data = parseJson(raw.data) as D;
    this.trace = takeTrace(this.data);
    this.timestamp = Number(raw.ts ?? 0);
    this.delay = Number(raw.delay ?? 0);
    this.priority = Number(raw.prio ?? 0);
    this.progress = raw.prg === undefined ? 0 : parseJson(raw.prg);
    this.returnvalue = parseJson(raw.rv) as R | undefined;
    this.failedReason = raw.fr || undefined;
    this.stacktrace = raw.st ? (parseJson(raw.st) as string[]) : [];
    if (!Array.isArray(this.stacktrace)) this.stacktrace = [String(raw.st)];
    this.attemptsMade = Number(raw.atm ?? 0);
    this.stalledCounter = Number(raw.stc ?? 0);
    this.processedOn = raw.po ? Number(raw.po) : undefined;
    this.finishedOn = raw.fo ? Number(raw.fo) : undefined;
    this.token = raw.tk || undefined;
    this.schedulerId = raw.rep || undefined;
    this.parent = raw.pq ? { id: raw.pid ?? '', queue: queueNameOf(raw.pq) } : undefined;
    this.pendingChildren = Number(raw.pend ?? 0);
    const bt = raw.bt as 'fixed' | 'exponential' | 'custom' | undefined;
    this.opts = {
      attempts: Number(raw.att ?? 1),
      delay: this.delay,
      priority: this.priority,
      lifo: raw.lifo === '1' || undefined,
      backoff: bt ? { type: bt, delay: Number(raw.bd ?? 0) } : undefined,
    };
  }

  static fromFlat(queue: QueueLike, id: string, flat: any[]): Job {
    return new Job(queue, id, pairsToObject(flat));
  }

  /** Where this job is now. */
  async getState(): Promise<JobState> {
    return (await this.queue.fcall('snugq_state', this.id)) as JobState;
  }

  async updateProgress(progress: number | string | boolean | object): Promise<void> {
    await this.queue.fcall('snugq_progress', this.id, toJson(progress));
    this.progress = progress;
  }

  /** Adds a line to the job log. Returns the log length. */
  async log(line: string): Promise<number> {
    return (await this.queue.fcall('snugq_log', this.id, line, this.opts.keepLogs ?? 0)) as number;
  }

  async updateData(data: D): Promise<void> {
    await this.queue.fcall('snugq_update_data', this.id, toJson(data));
    this.data = data;
  }

  /** Removes the job and its unfinished children. Returns false if a worker holds it. */
  async remove(): Promise<boolean> {
    return ((await this.queue.fcall('snugq_remove', this.id)) as number) === 1;
  }

  /** Puts a failed (or completed) job back in the queue. */
  async retry(state: 'failed' | 'completed' = 'failed', opts: { resetAttemptsMade?: boolean } = {}): Promise<void> {
    const r = (await this.queue.fcall('snugq_retry', this.id, state, opts.resetAttemptsMade ? 1 : 0)) as number;
    if (r === -1) throw new Error(`job ${this.id} does not exist`);
    if (r === -5) throw new Error(`job ${this.id} is not ${state}`);
  }

  /** Runs a delayed job now. */
  async promote(): Promise<void> {
    const r = (await this.queue.fcall('snugq_promote', this.id)) as number;
    if (r === -1) throw new Error(`job ${this.id} does not exist`);
    if (r === -3) throw new Error(`job ${this.id} is not delayed`);
  }

  async changeDelay(delayMs: number): Promise<void> {
    const r = (await this.queue.fcall('snugq_change_delay', this.id, Math.max(0, Math.floor(delayMs)))) as number;
    if (r === -1) throw new Error(`job ${this.id} does not exist`);
    if (r === -3) throw new Error(`job ${this.id} is not delayed`);
  }

  async changePriority(opts: { priority: number; lifo?: boolean }): Promise<void> {
    const r = (await this.queue.fcall('snugq_change_priority', this.id, opts.priority, opts.lifo ? 1 : 0)) as number;
    if (r === -1) throw new Error(`job ${this.id} does not exist`);
  }

  /**
   * From a processor: delay this job until `timestamp` (ms since epoch), then throw `new DelayedError()`.
   * It does not use up an attempt.
   */
  async moveToDelayed(timestamp: number, token?: string): Promise<void> {
    const t = token ?? this.token;
    if (!t) throw new Error('moveToDelayed needs the lock token');
    const r = (await this.queue.fcall('snugq_move_to_delayed', this.id, t, Math.max(0, Math.floor(timestamp - Date.now())))) as number;
    if (r === -2) throw new Error(`lock of job ${this.id} was lost`);
  }

  /** Log lines. */
  async getLogs(start = 0, end = -1): Promise<string[]> {
    await this.queue.ready();
    return this.queue.client.lrange(`${this.queue.base}job:${this.id}:logs`, start, end);
  }

  /** Results of finished children, keyed by `queue:jobId`. */
  async getChildrenValues(): Promise<Record<string, any>> {
    await this.queue.ready();
    const h = await this.queue.client.hgetall(`${this.queue.base}job:${this.id}:processed`);
    const out: Record<string, any> = {};
    for (const [ref, v] of Object.entries(h)) out[refToKey(ref)] = parseJson(v);
    return out;
  }

  /** Reasons of children that failed, keyed by `queue:jobId`. */
  async getFailedChildrenValues(): Promise<Record<string, string>> {
    await this.queue.ready();
    const h = await this.queue.client.hgetall(`${this.queue.base}job:${this.id}:failed`);
    const out: Record<string, string> = {};
    for (const [ref, v] of Object.entries(h)) out[refToKey(ref)] = v;
    return out;
  }

  /** Resolves with the return value when the job completes; rejects when it fails. Needs a started {@link QueueEvents}. */
  async waitUntilFinished(events: QueueEvents, ttlMs?: number): Promise<R> {
    await events.waitUntilReady();
    return new Promise<R>((resolve, reject) => {
      let timer: NodeJS.Timeout | undefined;
      const cleanup = () => {
        events.off('completed', onDone);
        events.off('failed', onFail);
        if (timer) clearTimeout(timer);
      };
      const onDone = (a: { jobId: string; returnvalue: R }) => {
        if (a.jobId !== this.id) return;
        cleanup();
        resolve(a.returnvalue);
      };
      const onFail = (a: { jobId: string; failedReason: string }) => {
        if (a.jobId !== this.id) return;
        cleanup();
        reject(new Error(a.failedReason));
      };
      events.on('completed', onDone);
      events.on('failed', onFail);
      if (ttlMs) {
        timer = setTimeout(() => {
          cleanup();
          reject(new Error(`job ${this.id} did not finish within ${ttlMs} ms`));
        }, ttlMs);
      }
      // Events may have happened before we listened: look at the stored state too.
      void this.queue.fcall('snugq_state', this.id).then(async (state) => {
        if (state !== 'completed' && state !== 'failed') return;
        const h = await this.queue.client.hmget(`${this.queue.base}job:${this.id}`, 'rv', 'fr');
        if (state === 'completed') onDone({ jobId: this.id, returnvalue: parseJson(h[0] ?? undefined) });
        else onFail({ jobId: this.id, failedReason: h[1] ?? 'failed' });
      }, reject);
    });
  }

  toJSON(): Record<string, unknown> {
    return {
      id: this.id,
      name: this.name,
      data: this.data,
      opts: this.opts,
      progress: this.progress,
      timestamp: this.timestamp,
      delay: this.delay,
      priority: this.priority,
      attemptsMade: this.attemptsMade,
      processedOn: this.processedOn,
      finishedOn: this.finishedOn,
      returnvalue: this.returnvalue,
      failedReason: this.failedReason,
      stacktrace: this.stacktrace,
      parent: this.parent,
      schedulerId: this.schedulerId,
    };
  }
}

function refToKey(ref: string): string {
  const i = ref.lastIndexOf('|');
  return i < 0 ? ref : `${queueNameOf(ref.slice(0, i))}:${ref.slice(i + 1)}`;
}
