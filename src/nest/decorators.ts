import type { MessageContext } from '../core/consumer';
import type { SubscribeOptions } from '../core/consumer';

export const SNUG_SUBSCRIBE = Symbol.for('snugkv-nest:subscribe');
export const SNUG_PARAMS = Symbol.for('snugkv-nest:params');

export type SubscribeMeta = SubscribeOptions;

/* eslint-disable @typescript-eslint/no-explicit-any */

function addMeta(fn: any, meta: SubscribeMeta): void {
  if (typeof fn !== 'function') throw new Error('@SnugSubscribe can only decorate methods');
  const list: SubscribeMeta[] = Object.prototype.hasOwnProperty.call(fn, SNUG_SUBSCRIBE) ? fn[SNUG_SUBSCRIBE] : [];
  list.push(meta);
  Object.defineProperty(fn, SNUG_SUBSCRIBE, { value: list, enumerable: false, configurable: true });
}

/**
 * Marks a method as the handler for a topic. Works with TypeScript's
 * `experimentalDecorators` (the Nest default) and with standard decorators.
 *
 * ```ts
 * @SnugSubscribe('orders.created', { concurrency: 4 })
 * async onOrder(order: Order, ctx: MessageContext) { … }
 * ```
 */
export function SnugSubscribe(topic: string, options: Omit<SubscribeOptions, 'topic'> = {}) {
  const meta: SubscribeMeta = { ...options, topic };
  return (target: any, keyOrContext: any, descriptor?: PropertyDescriptor): void => {
    if (typeof keyOrContext === 'string' || typeof keyOrContext === 'symbol') {
      addMeta(descriptor?.value ?? target[keyOrContext], meta);
    } else {
      addMeta(target, meta);
    }
  };
}

export type ParamKind = 'payload' | 'ctx' | 'header' | 'event' | 'id';
export interface ParamMeta {
  index: number;
  kind: ParamKind;
  name?: string;
}

function paramDecorator(kind: ParamKind, name?: string) {
  return (target: any, key: string | symbol | undefined, index: number): void => {
    if (key === undefined) return;
    const own = Object.prototype.hasOwnProperty.call(target, SNUG_PARAMS);
    const map: Map<string | symbol, ParamMeta[]> = own ? target[SNUG_PARAMS] : new Map();
    if (!own) Object.defineProperty(target, SNUG_PARAMS, { value: map, enumerable: false, configurable: true });
    const list = map.get(key) ?? [];
    list.push({ index, kind, name });
    map.set(key, list);
  };
}

/** The decoded message body. */
export const SnugPayload = () => paramDecorator('payload');
/** The {@link MessageContext}. */
export const SnugContext = () => paramDecorator('ctx');
/** One header value (`ctx.headers[name]`). */
export const SnugHeader = (name: string) => paramDecorator('header', name);
/** The event name (`ctx.event`). */
export const SnugEvent = () => paramDecorator('event');
/** The stream entry id (`ctx.id`). */
export const SnugMessageId = () => paramDecorator('id');

export function paramsFor(instance: object, method: string | symbol): ParamMeta[] {
  for (let p: any = Object.getPrototypeOf(instance); p && p !== Object.prototype; p = Object.getPrototypeOf(p)) {
    if (Object.prototype.hasOwnProperty.call(p, SNUG_PARAMS)) {
      const list = (p[SNUG_PARAMS] as Map<string | symbol, ParamMeta[]>).get(method);
      if (list) return list;
    }
  }
  return [];
}

/** Arguments for a handler call. Without parameter decorators: `(payload, ctx)`. */
export function buildArgs(params: ParamMeta[], payload: unknown, ctx: MessageContext): unknown[] {
  if (params.length === 0) return [payload, ctx];
  const args: unknown[] = [];
  for (const p of params) {
    switch (p.kind) {
      case 'payload':
        args[p.index] = payload;
        break;
      case 'ctx':
        args[p.index] = ctx;
        break;
      case 'header':
        args[p.index] = ctx.headers[p.name ?? ''];
        break;
      case 'event':
        args[p.index] = ctx.event;
        break;
      case 'id':
        args[p.index] = ctx.id;
        break;
    }
  }
  return args;
}
