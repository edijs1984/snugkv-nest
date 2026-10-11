/* eslint-disable @typescript-eslint/no-explicit-any */
import type { SnugKvClient } from '../client';
import type { Arg } from '../resp';

export interface BackoffOptions {
  /** `custom` asks the worker's `settings.backoffStrategy` for the delay. */
  type: 'fixed' | 'exponential' | 'custom';
  /** Base delay in ms. */
  delay?: number;
}

/** `true` removes at once, a number keeps that many, an object can also limit by age (seconds). */
export type KeepJobs = boolean | number | { count?: number; age?: number };

export interface JobsOptions {
  /** Your own id. Adding the same id twice is ignored. Must not be a plain integer or contain `|`. */
  jobId?: string;
  /** Wait this many ms before the job can run. */
  delay?: number;
  /** 1 is the most urgent. Jobs without a priority run first. Up to 2,097,152. */
  priority?: number;
  /** Run before the jobs already waiting. */
  lifo?: boolean;
  /** Total tries, including the first. Default 1. */
  attempts?: number;
  /** A number is a fixed delay in ms. */
  backoff?: number | BackoffOptions;
  removeOnComplete?: KeepJobs;
  removeOnFail?: KeepJobs;
  /** Skip adding while a job with the same dedup id exists (until it finishes, or for `ttl` ms). */
  deduplication?: { id: string; ttl?: number };
  /** Make this a child of an existing waiting-children job. */
  parent?: { id: string; queue: string };
  /** Fail the parent when this job fails for good. */
  failParentOnFailure?: boolean;
  /** Let the parent continue when this job fails for good (the failure is recorded). */
  ignoreDependencyOnFailure?: boolean;
  /** Let the parent continue when this job fails for good. */
  removeDependencyOnFailure?: boolean;
  /** Keep only the newest N log lines. */
  keepLogs?: number;
  /** Keep at most this many stack traces. Default 10. */
  stackTraceLimit?: number;
}

export type JobState = 'completed' | 'failed' | 'delayed' | 'prioritized' | 'waiting-children' | 'active' | 'wait' | 'paused' | 'unknown';
export type JobType = Exclude<JobState, 'unknown'>;

/** What a Job needs from the queue it belongs to. */
export interface QueueLike {
  readonly name: string;
  readonly base: string;
  readonly client: SnugKvClient;
  ready(): Promise<void>;
  fcall(fn: string, ...args: Arg[]): Promise<any>;
}

export class UnrecoverableError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = 'UnrecoverableError';
  }
}

/** Throw from a processor after `job.moveToDelayed(...)`. */
export class DelayedError extends Error {
  constructor(message = 'job moved to delayed') {
    super(message);
    this.name = 'DelayedError';
  }
}

/** Throw from a processor after `worker.rateLimit(ms)`. The job goes back to the queue without using an attempt. */
export class RateLimitError extends Error {
  constructor(message = 'queue rate limited') {
    super(message);
    this.name = 'RateLimitError';
  }
}

export function parseJson(raw: string | undefined | null): any {
  if (raw === undefined || raw === null || raw === '') return undefined;
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

export function toJson(value: unknown): string {
  const s = JSON.stringify(value === undefined ? null : value);
  return s === undefined ? 'null' : s;
}

function keep(k: KeepJobs | undefined): [number, number] {
  if (k === undefined || k === false) return [-1, 0];
  if (k === true) return [0, 0];
  if (typeof k === 'number') return [Math.max(0, Math.floor(k)), 0];
  return [k.count === undefined ? -1 : Math.max(0, Math.floor(k.count)), Math.max(0, Math.floor(k.age ?? 0))];
}

/** The 19 job arguments of `snugq_add`, in order. */
export function jobArgs(
  name: string,
  data: unknown,
  opts: JobsOptions,
  extra: { parentBase?: string; parentId?: string; waitChildren?: boolean } = {},
): Arg[] {
  const backoff: BackoffOptions | undefined = typeof opts.backoff === 'number' ? { type: 'fixed', delay: opts.backoff } : opts.backoff;
  const [rmc, rmca] = keep(opts.removeOnComplete);
  const [rmf, rmfa] = keep(opts.removeOnFail);
  const onFail = opts.failParentOnFailure ? 'fail' : opts.ignoreDependencyOnFailure ? 'ignore' : opts.removeDependencyOnFailure ? 'remove' : '';
  return [
    name,
    toJson(data),
    opts.jobId ?? '',
    Math.max(0, Math.floor(opts.delay ?? 0)),
    opts.priority ?? 0,
    opts.lifo ? 1 : 0,
    opts.attempts ?? 1,
    backoff?.type ?? '',
    Math.max(0, Math.floor(backoff?.delay ?? 0)),
    rmc,
    rmca,
    rmf,
    rmfa,
    extra.parentBase ?? '',
    extra.parentId ?? '',
    onFail,
    extra.waitChildren ? 1 : 0,
    opts.deduplication?.id ?? '',
    opts.deduplication?.ttl ?? 0,
  ];
}

export function queueNameOf(base: string): string {
  return /\{(.*)\}:$/.exec(base)?.[1] ?? base;
}
