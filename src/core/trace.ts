/* eslint-disable @typescript-eslint/no-explicit-any */
import { AsyncLocalStorage } from 'node:async_hooks';
import { randomBytes } from 'node:crypto';
import type { SnugKvClient } from './client';
import type { Arg } from './resp';

/**
 * Flow tracing: every activity (a webhook, an HTTP request, a scheduled run)
 * is one trace, and every step inside it is a span. Spans are written to a
 * SnugKV stream in batches; a collector or a UI reads them from there.
 *
 * The current span lives in AsyncLocalStorage, so `step()` calls nest without
 * passing anything around. Queue jobs carry the context to the worker, which
 * continues the same trace in another service.
 */

/** What crosses a process boundary (a job, a message). */
export interface TraceContext {
  traceId: string;
  spanId: string;
}

export type SpanStatus = 'ok' | 'error';

/** One finished step, as written to the stream. */
export interface SpanRecord {
  traceId: string;
  spanId: string;
  /** Empty for the root span of a trace. */
  parentId: string;
  service: string;
  name: string;
  status: SpanStatus;
  /** Start time, ms since epoch. */
  start: number;
  durationMs: number;
  attrs: Record<string, unknown>;
  error?: string;
  stack?: string;
}

export interface TracerOptions {
  /** Name of this app, shown on every span. */
  service: string;
  /** Stream the spans go to (default `snug:traces`). */
  stream?: string;
  /** Approximate cap on the stream length (default 100,000). */
  maxLen?: number;
  /** Write after this many spans are buffered (default 100). */
  batchSize?: number;
  /** Write buffered spans at least this often, ms (default 250). */
  flushMs?: number;
  /** Spans kept while the server is unreachable; older ones are dropped (default 10,000). */
  maxBuffer?: number;
  /** Attribute keys matching this are replaced with `[redacted]`. */
  redact?: RegExp;
  /** Longest string kept in an attribute (default 2,000 characters). */
  maxAttrLength?: number;
  /** Called when spans cannot be written. Tracing never throws into your code. */
  onError?: (err: unknown) => void;
}

/** Keys that never belong in a trace: credentials, signatures, key material. */
export const DEFAULT_REDACT = /pass(word)?|secret|token|auth|cookie|signature|private|mnemonic|seed|api[-_]?key|^key$/i;

const storage = new AsyncLocalStorage<TraceContext>();
let globalTracer: Tracer | undefined;

/** The tracer used by queues and workers. Unset means tracing is off. */
export function setTracer(tracer: Tracer | undefined): void {
  globalTracer = tracer;
}

export function getTracer(): Tracer | undefined {
  return globalTracer;
}

/** The span the calling code runs in, if any. */
export function currentTrace(): TraceContext | undefined {
  return storage.getStore();
}

function newId(bytes: number): string {
  return randomBytes(bytes).toString('hex');
}

/** Turns a value into something safe and bounded to store. */
function clean(value: unknown, redact: RegExp, maxLen: number, depth = 0): unknown {
  if (value === null || value === undefined) return value;
  if (typeof value === 'string') return value.length > maxLen ? `${value.slice(0, maxLen)}…` : value;
  if (typeof value === 'number' || typeof value === 'boolean') return value;
  if (typeof value === 'bigint') return value.toString();
  if (value instanceof Date) return value.toISOString();
  if (depth >= 4) return '[nested]';
  if (Array.isArray(value)) return value.slice(0, 50).map((v) => clean(v, redact, maxLen, depth + 1));
  if (typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>).slice(0, 50)) {
      out[k] = redact.test(k) ? '[redacted]' : clean(v, redact, maxLen, depth + 1);
    }
    return out;
  }
  return String(value);
}

/** A running step. Add attributes while it runs; `step()` ends it for you. */
export class Span {
  readonly context: TraceContext;
  private readonly started = Date.now();
  private readonly attrs: Record<string, unknown>;
  private ended = false;

  constructor(
    private readonly tracer: Tracer,
    readonly name: string,
    readonly parentId: string,
    traceId: string,
    attrs: Record<string, unknown> = {},
  ) {
    this.context = { traceId, spanId: newId(8) };
    this.attrs = { ...attrs };
  }

  /** Adds or replaces attributes (shown in the step details). */
  set(attrs: Record<string, unknown>): this {
    Object.assign(this.attrs, attrs);
    return this;
  }

  end(error?: unknown): void {
    if (this.ended) return;
    this.ended = true;
    const err = error === undefined ? undefined : error instanceof Error ? error : new Error(String(error));
    this.tracer.record({
      traceId: this.context.traceId,
      spanId: this.context.spanId,
      parentId: this.parentId,
      service: this.tracer.service,
      name: this.name,
      status: err ? 'error' : 'ok',
      start: this.started,
      durationMs: Date.now() - this.started,
      attrs: this.attrs,
      error: err?.message,
      stack: err?.stack,
    });
  }
}

export class Tracer {
  readonly service: string;
  readonly stream: string;
  private readonly maxLen: number;
  private readonly batchSize: number;
  private readonly maxBuffer: number;
  private readonly redact: RegExp;
  private readonly maxAttrLength: number;
  private readonly onError: (err: unknown) => void;
  private buffer: SpanRecord[] = [];
  private timer: NodeJS.Timeout | undefined;
  private flushing: Promise<void> | undefined;

  constructor(
    private readonly client: Pick<SnugKvClient, 'pipeline'>,
    options: TracerOptions,
  ) {
    this.service = options.service;
    this.stream = options.stream ?? 'snug:traces';
    this.maxLen = options.maxLen ?? 100_000;
    this.batchSize = options.batchSize ?? 100;
    this.maxBuffer = options.maxBuffer ?? 10_000;
    this.redact = options.redact ?? DEFAULT_REDACT;
    this.maxAttrLength = options.maxAttrLength ?? 2_000;
    this.onError = options.onError ?? (() => undefined);
    this.timer = setInterval(() => void this.flush(), options.flushMs ?? 250);
    this.timer.unref?.();
  }

  /**
   * Runs `fn` as a step of the current trace (or starts a new trace when
   * there is none). The step records its duration, and its error if `fn`
   * throws; the error is rethrown unchanged.
   */
  async step<T>(name: string, attrs: Record<string, unknown> | undefined, fn: (span: Span) => Promise<T> | T): Promise<T> {
    return this.run(name, currentTrace(), attrs, fn);
  }

  /** Always starts a new trace, even inside another one. */
  async trace<T>(name: string, attrs: Record<string, unknown> | undefined, fn: (span: Span) => Promise<T> | T): Promise<T> {
    return this.run(name, undefined, attrs, fn);
  }

  /** Continues a trace that came from another process (a job, a message). */
  async continue<T>(parent: TraceContext | undefined, name: string, attrs: Record<string, unknown> | undefined, fn: (span: Span) => Promise<T> | T): Promise<T> {
    return this.run(name, parent, attrs, fn);
  }

  /** Records a step that already happened (no callback), e.g. "push sent". */
  event(name: string, attrs: Record<string, unknown> = {}, error?: unknown): void {
    const parent = currentTrace();
    const span = new Span(this, name, parent?.spanId ?? '', parent?.traceId ?? newId(16), attrs);
    span.end(error);
  }

  private async run<T>(name: string, parent: TraceContext | undefined, attrs: Record<string, unknown> | undefined, fn: (span: Span) => Promise<T> | T): Promise<T> {
    const span = new Span(this, name, parent?.spanId ?? '', parent?.traceId ?? newId(16), attrs);
    return storage.run(span.context, async () => {
      try {
        const out = await fn(span);
        span.end();
        return out;
      } catch (err) {
        span.end(err);
        throw err;
      }
    });
  }

  /** Called by {@link Span.end}. */
  record(span: SpanRecord): void {
    this.buffer.push({ ...span, attrs: clean(span.attrs, this.redact, this.maxAttrLength) as Record<string, unknown> });
    if (this.buffer.length > this.maxBuffer) this.buffer.splice(0, this.buffer.length - this.maxBuffer);
    if (this.buffer.length >= this.batchSize) void this.flush();
  }

  /** Writes buffered spans now. Resolves when they are written (or dropped on error). */
  async flush(): Promise<void> {
    if (this.flushing) return this.flushing;
    if (!this.buffer.length) return;
    const batch = this.buffer;
    this.buffer = [];
    const commands: Arg[][] = batch.map((s) => [
      'XADD', this.stream, 'MAXLEN', '~', this.maxLen, '*',
      'traceId', s.traceId,
      'spanId', s.spanId,
      'parentId', s.parentId,
      'service', s.service,
      'name', s.name,
      'status', s.status,
      'start', String(s.start),
      'durationMs', String(s.durationMs),
      'attrs', JSON.stringify(s.attrs ?? {}),
      ...(s.error ? ['error', s.error] : []),
      ...(s.stack ? ['stack', s.stack] : []),
    ]);
    this.flushing = this.client
      .pipeline(commands)
      .then(() => undefined)
      .catch((err) => {
        // Put the batch back (newest kept) so a short outage loses nothing.
        this.buffer = [...batch, ...this.buffer].slice(-this.maxBuffer);
        this.onError(err);
      })
      .finally(() => {
        this.flushing = undefined;
      });
    return this.flushing;
  }

  /** Flushes and stops the timer. */
  async close(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    await this.flush();
    if (this.buffer.length) await this.flush();
  }
}

// ----- carrying the context inside job data

/** Field added to plain-object job data while a trace is active. */
export const TRACE_FIELD = '__trace';

/** Adds the current trace to job data (plain objects only; other data is left as is). */
export function withTrace<D>(data: D): D {
  const ctx = currentTrace();
  if (!ctx || !globalTracer) return data;
  if (data === null || typeof data !== 'object' || Array.isArray(data)) return data;
  return { ...(data as any), [TRACE_FIELD]: { traceId: ctx.traceId, spanId: ctx.spanId } };
}

/** Removes the trace field from job data and returns it. */
export function takeTrace(data: any): TraceContext | undefined {
  if (data === null || typeof data !== 'object' || Array.isArray(data)) return undefined;
  const t = data[TRACE_FIELD];
  if (t === undefined) return undefined;
  delete data[TRACE_FIELD];
  return t && typeof t.traceId === 'string' && typeof t.spanId === 'string' ? { traceId: t.traceId, spanId: t.spanId } : undefined;
}
