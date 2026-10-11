/* eslint-disable @typescript-eslint/no-explicit-any */
import type { Job } from '../core/queue/job';
import type { WorkerOptions } from '../core/queue/worker';

export const SNUG_PROCESSOR = Symbol.for('snugkv-nest:processor');
export const SNUG_PROCESS = Symbol.for('snugkv-nest:process');
export const SNUG_WORKER_EVENT = Symbol.for('snugkv-nest:worker-event');

export type ProcessorOptions = Omit<WorkerOptions, 'client' | 'connection' | 'autorun'>;

export interface ProcessorMeta {
  queue: string;
  options: ProcessorOptions;
}

/**
 * Marks a provider as the worker of a queue. Either extend {@link WorkerHost} and implement `process`,
 * or mark methods with {@link SnugProcess}.
 *
 * ```ts
 * @SnugProcessor('emails', { concurrency: 5 })
 * export class EmailProcessor extends WorkerHost {
 *   async process(job: Job<{ to: string }>) { … }
 * }
 * ```
 */
export function SnugProcessor(queue: string, options: ProcessorOptions = {}): ClassDecorator {
  return (target) => {
    Object.defineProperty(target, SNUG_PROCESSOR, { value: { queue, options } satisfies ProcessorMeta, enumerable: false, configurable: true });
  };
}

function addToList(fn: any, symbol: symbol, value: unknown): void {
  if (typeof fn !== 'function') throw new Error('this decorator can only be used on methods');
  const list: unknown[] = Object.prototype.hasOwnProperty.call(fn, symbol) ? fn[symbol] : [];
  list.push(value);
  Object.defineProperty(fn, symbol, { value: list, enumerable: false, configurable: true });
}

function methodDecorator(symbol: symbol, value: unknown) {
  return (target: any, keyOrContext: any, descriptor?: PropertyDescriptor): void => {
    if (typeof keyOrContext === 'string' || typeof keyOrContext === 'symbol') addToList(descriptor?.value ?? target[keyOrContext], symbol, value);
    else addToList(target, symbol, value);
  };
}

/**
 * Handles jobs with this name (`*` or no name: every job without a more specific handler).
 * Use in a class marked with {@link SnugProcessor}.
 */
export function SnugProcess(jobName = '*') {
  return methodDecorator(SNUG_PROCESS, jobName);
}

export type WorkerEventName = 'active' | 'completed' | 'failed' | 'progress' | 'stalled' | 'drained' | 'error' | 'closed';

/** Calls the method for a worker event, with the event's arguments, e.g. `(job, result)` for `completed`. */
export function OnSnugWorkerEvent(event: WorkerEventName) {
  return methodDecorator(SNUG_WORKER_EVENT, event);
}

/** Base class for processors that implement a single `process` method. */
export abstract class WorkerHost<D = any, R = any> {
  abstract process(job: Job<D, R>, token: string): Promise<R> | R;
}
