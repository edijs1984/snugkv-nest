/* eslint-disable @typescript-eslint/no-explicit-any */
import * as dc from 'node:diagnostics_channel';
import { currentTrace, getTracer, Span } from './trace';

/**
 * Records outgoing HTTP calls (node:http / https, so axios and most SDKs, and
 * fetch) as steps of the current trace. Calls made outside a trace are not
 * recorded. Only the method, host, a masked path and the status are kept:
 * query strings are dropped and long path segments (API keys in RPC URLs,
 * tokens) are replaced with `…`.
 */

/** Path without query, long or secret-looking segments masked. */
export function maskPath(path: string): string {
  const p = (path || '/').split('?')[0]!.split('#')[0]!;
  return p
    .split('/')
    .map((seg) => (seg.length >= 16 && /^[A-Za-z0-9_\-=.~%]+$/.test(seg) && /\d/.test(seg) ? '…' : seg))
    .join('/')
    .slice(0, 200);
}

let installed = false;
const unsubs: Array<() => void> = [];

function sub(name: string, fn: (msg: any) => void): void {
  if (typeof (dc as any).subscribe === 'function') {
    (dc as any).subscribe(name, fn);
    unsubs.push(() => (dc as any).unsubscribe(name, fn));
  } else {
    const ch = dc.channel(name);
    ch.subscribe(fn);
    unsubs.push(() => ch.unsubscribe(fn));
  }
}

/** Starts recording outgoing HTTP calls. Safe to call more than once. */
export function instrumentHttpClient(): void {
  if (installed) return;
  installed = true;
  const spans = new WeakMap<object, Span>();

  const start = (key: object, method: string, host: string, path: string) => {
    const tracer = getTracer();
    if (!tracer || !currentTrace()) return;
    spans.set(key, tracer.start(`HTTP ${method} ${host}`, { method, host, path: maskPath(path) }));
  };
  const finish = (key: object, status?: number, error?: unknown) => {
    const span = spans.get(key);
    if (!span) return;
    spans.delete(key);
    if (status !== undefined) span.set({ statusCode: status });
    span.end(error ?? (status !== undefined && status >= 500 ? new Error(`HTTP ${status}`) : undefined));
  };

  // node:http / node:https (axios, got, most SDKs)
  sub('http.client.request.start', ({ request }: any) => {
    const host = String(request.host ?? request.getHeader?.('host') ?? '');
    start(request, String(request.method ?? 'GET'), host, String(request.path ?? '/'));
  });
  sub('http.client.response.finish', ({ request, response }: any) => finish(request, response?.statusCode));
  sub('http.client.request.error', ({ request, error }: any) => finish(request, undefined, error));

  // fetch (undici)
  sub('undici:request:create', ({ request }: any) => {
    let host = '';
    try {
      host = new URL(String(request.origin)).host;
    } catch {
      host = String(request.origin ?? '');
    }
    start(request, String(request.method ?? 'GET'), host, String(request.path ?? '/'));
  });
  sub('undici:request:headers', ({ request, response }: any) => finish(request, response?.statusCode));
  sub('undici:request:error', ({ request, error }: any) => finish(request, undefined, error));
}

/** Stops recording outgoing calls (tests). */
export function uninstrumentHttpClient(): void {
  for (const u of unsubs.splice(0)) u();
  installed = false;
}
