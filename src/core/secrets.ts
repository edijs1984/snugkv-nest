/**
 * Secrets from the SnugKV secrets module (SnugKV's -secrets-api-listen).
 *
 * Load them into process.env before the app starts, so ConfigModule,
 * TypeORM/Mongoose factories and plain process.env reads all see them:
 *
 *   await loadSecrets({ env: 'airsig/prod' });
 *   const app = await NestFactory.create(AppModule);
 *
 * The address and a read token come from SNUGKV_SECRETS_URL and
 * SNUGKV_SECRETS_TOKEN unless given.
 */

export interface SecretsOptions {
  /** Environment to read, "project/env". Default: SNUGKV_SECRETS_ENV. */
  env?: string;
  /** Secrets API address, e.g. http://snugkv:8092. Default: SNUGKV_SECRETS_URL. */
  url?: string;
  /** Read token (sks_…). Default: SNUGKV_SECRETS_TOKEN. */
  token?: string;
  /** Keep retrying this long (ms) while the API is unreachable or starting. Default 30000. */
  waitMs?: number;
  /** Per-request timeout (ms). Default 10000. */
  timeoutMs?: number;
}

export interface LoadSecretsOptions extends SecretsOptions {
  /** Let secrets replace variables that are already set. Default false: what is set wins. */
  override?: boolean;
  /** Where to put them. Default process.env. */
  target?: Record<string, string | undefined>;
}

export class SecretsError extends Error {
  constructor(
    message: string,
    readonly status?: number,
  ) {
    super(message);
    this.name = 'SecretsError';
  }
}

const ENV_RE = /^[a-z0-9][a-z0-9_-]{0,47}\/[a-z0-9][a-z0-9_-]{0,47}$/;

/** Reads every secret of an environment. Retries while the API is unreachable or answers 5xx; not on 401/403/404. */
export async function fetchSecrets(opts: SecretsOptions = {}): Promise<Record<string, string>> {
  const env = opts.env ?? process.env.SNUGKV_SECRETS_ENV ?? '';
  const url = (opts.url ?? process.env.SNUGKV_SECRETS_URL ?? '').replace(/\/+$/, '');
  const token = opts.token ?? process.env.SNUGKV_SECRETS_TOKEN ?? '';
  if (!ENV_RE.test(env)) throw new SecretsError('secrets: env must look like project/env, e.g. airsig/prod');
  if (!/^https?:\/\/[^/]+/.test(url)) throw new SecretsError('secrets: no API address (set SNUGKV_SECRETS_URL or pass url)');
  const deadline = Date.now() + (opts.waitMs ?? 30_000);
  let delay = 250;
  for (;;) {
    let status = 0;
    let detail = '';
    try {
      const res = await fetch(`${url}/v1/envs/${env}/export`, {
        headers: token ? { authorization: `Bearer ${token}` } : {},
        signal: AbortSignal.timeout(opts.timeoutMs ?? 10_000),
      });
      const text = await res.text();
      status = res.status;
      if (res.ok) {
        const body = JSON.parse(text) as { values?: Record<string, string> };
        if (!body.values || typeof body.values !== 'object') throw new SecretsError(`secrets: unexpected answer from ${url}`);
        return body.values;
      }
      try {
        detail = String((JSON.parse(text) as { error?: unknown }).error ?? '');
      } catch {
        detail = text.slice(0, 200);
      }
      if (status < 500) throw new SecretsError(`secrets: ${env}: HTTP ${status}${detail ? ` ${detail}` : ''}`, status);
    } catch (err) {
      if (err instanceof SecretsError) throw err;
      detail = err instanceof Error ? err.message : String(err);
    }
    if (Date.now() >= deadline) {
      throw new SecretsError(status ? `secrets: ${env}: HTTP ${status} ${detail}` : `secrets: cannot reach ${url}: ${detail}`, status || undefined);
    }
    await new Promise((r) => setTimeout(r, delay));
    delay = Math.min(delay * 2, 3000);
  }
}

/**
 * Reads the secrets and puts them in process.env (or `target`). Variables
 * already set win unless `override`, so a value in docker-compose still
 * overrides a secret. Returns the names that were set.
 */
export async function loadSecrets(opts: LoadSecretsOptions = {}): Promise<string[]> {
  const values = await fetchSecrets(opts);
  const target = opts.target ?? process.env;
  const set: string[] = [];
  for (const name of Object.keys(values).sort()) {
    if (!opts.override && target[name] !== undefined) continue;
    target[name] = values[name];
    set.push(name);
  }
  return set;
}
