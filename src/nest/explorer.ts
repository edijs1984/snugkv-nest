import { SNUG_SUBSCRIBE, SubscribeMeta, buildArgs, paramsFor } from './decorators';
import type { MessageHandler } from '../core/consumer';

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
