import { randomUUID } from 'node:crypto';

export const uuid = (): string => randomUUID();

/** Sleep that resolves early (with `false`) when the signal aborts. */
export function sleep(ms: number, signal?: AbortSignal): Promise<boolean> {
  return new Promise((resolve) => {
    if (signal?.aborted) return resolve(false);
    const t = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve(true);
    }, ms);
    const onAbort = () => {
      clearTimeout(t);
      resolve(false);
    };
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

export function errorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  try {
    return typeof err === 'string' ? err : JSON.stringify(err);
  } catch {
    return String(err);
  }
}

export interface ParsedUrl {
  host: string;
  port: number;
  username?: string;
  password?: string;
  tls: boolean;
}

/** Accepts redis://, rediss://, snugkv:// and snugkvs:// URLs. */
export function parseUrl(raw: string): ParsedUrl {
  const u = new URL(raw);
  const scheme = u.protocol.replace(':', '');
  if (!['redis', 'rediss', 'snugkv', 'snugkvs'].includes(scheme)) {
    throw new Error(`unsupported URL scheme "${scheme}"`);
  }
  return {
    host: u.hostname || '127.0.0.1',
    port: u.port ? Number(u.port) : 6379,
    username: u.username ? decodeURIComponent(u.username) : undefined,
    password: u.password ? decodeURIComponent(u.password) : undefined,
    tls: scheme === 'rediss' || scheme === 'snugkvs',
  };
}

export interface BusLogger {
  debug?(message: string): void;
  log?(message: string): void;
  warn(message: string): void;
  error(message: string): void;
}

export const consoleLogger: BusLogger = {
  warn: (m) => console.warn(`[snugkv] ${m}`),
  error: (m) => console.error(`[snugkv] ${m}`),
};
