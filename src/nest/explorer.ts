import { SNUG_SUBSCRIBE, SubscribeMeta, buildArgs, paramsFor } from './decorators';
import type { MessageHandler } from '../core/consumer';
import type { Job } from '../core/queue/job';
import { UnrecoverableError } from '../core/queue/types';
import type { Processor } from '../core/queue/worker';
import { ProcessorMeta, SNUG_PROCESS, SNUG_PROCESSOR, SNUG_WORKER_EVENT, WorkerEventName } from './queue-decorators';

/* eslint-disable @typescript-eslint/no-explicit-any */

export interface FoundSubscription {
  instance: object;
  method: string | symbol;
  meta: SubscribeMeta;
  handler: MessageHandler;
}

/** Finds `@SnugSubscribe` methods on the given objects (including inherited ones). */
export function collectSubscriptions(instances: Iterable<object>): FoundSubscription[] {
  const found: FoundSubscription[] = [];
  const seen = new Set<object>();
  for (const instance of instances) {
    if (!instance || typeof instance !== 'object' || seen.has(instance)) continue;
    seen.add(instance);
    const names = new Set<string | symbol>();
    for (let p: any = Object.getPrototypeOf(instance); p && p !== Object.prototype; p = Object.getPrototypeOf(p)) {
      for (const name of Reflect.ownKeys(p)) {
        if (name === 'constructor') continue;
        const d = Object.getOwnPropertyDescriptor(p, name);
        if (d && typeof d.value === 'function') names.add(name);
      }
    }
    for (const method of names) {
      const fn = (instance as any)[method];
      const metas: SubscribeMeta[] | undefined = fn?.[SNUG_SUBSCRIBE];
      if (!metas) continue;
      const params = paramsFor(instance, method);
      for (const meta of metas) {
        found.push({
          instance,
          method,
          meta,
          handler: (payload, ctx) => (instance as any)[method](...buildArgs(params, payload, ctx)),
        });
      }
    }
  }
  return found;
}

export interface FoundProcessor {
  instance: object;
  meta: ProcessorMeta;
  processor: Processor;
  events: Array<{ event: WorkerEventName; handler: (...args: any[]) => unknown }>;
}

function methodNames(instance: object): Array<string | symbol> {
  const names = new Set<string | symbol>();
  for (let p: any = Object.getPrototypeOf(instance); p && p !== Object.prototype; p = Object.getPrototypeOf(p)) {
    for (const name of Reflect.ownKeys(p)) {
      if (name === 'constructor') continue;
      const d = Object.getOwnPropertyDescriptor(p, name);
      if (d && typeof d.value === 'function') names.add(name);
    }
  }
  return [...names];
}

/** Finds providers marked with `@SnugProcessor` and builds their job handler and event handlers. */
export function collectProcessors(instances: Iterable<object>): FoundProcessor[] {
  const found: FoundProcessor[] = [];
  const seen = new Set<object>();
  for (const instance of instances) {
    if (!instance || typeof instance !== 'object' || seen.has(instance)) continue;
    seen.add(instance);
    const meta: ProcessorMeta | undefined = (instance.constructor as any)?.[SNUG_PROCESSOR];
    if (!meta) continue;
    const named = new Map<string, (job: Job, token: string) => unknown>();
    const events: FoundProcessor['events'] = [];
    for (const method of methodNames(instance)) {
      const fn = (instance as any)[method];
      for (const jobName of (fn?.[SNUG_PROCESS] as string[] | undefined) ?? []) named.set(jobName, (job, token) => (instance as any)[method](job, token));
      for (const event of (fn?.[SNUG_WORKER_EVENT] as WorkerEventName[] | undefined) ?? []) events.push({ event, handler: (...args) => (instance as any)[method](...args) });
    }
    const whole = typeof (instance as any).process === 'function' && !(((instance as any).process as any)?.[SNUG_PROCESS]);
    if (!whole && named.size === 0) throw new Error(`${instance.constructor.name} is a @SnugProcessor but has no process() method and no @SnugProcess handlers`);
    const processor: Processor = (job, token) => {
      const handler = named.get(job.name) ?? named.get('*');
      if (handler) return handler(job, token) as any;
      if (whole) return (instance as any).process(job, token);
      throw new UnrecoverableError(`${instance.constructor.name} has no handler for jobs named "${job.name}"`);
    };
    found.push({ instance, meta, processor, events });
  }
  return found;
}
