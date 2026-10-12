import assert from 'node:assert/strict';
import { describe, test } from 'node:test';
import { HttpException } from '@nestjs/common';
import { lastValueFrom, of, throwError } from 'rxjs';
import { currentTrace, setTracer, Tracer } from '../src/core/trace';
import { TraceInterceptor } from '../src/nest/trace-interceptor';

function fakeClient() {
  const writes: any[][] = [];
  return { writes, async pipeline(cmds: any[][]) { writes.push(...cmds); return cmds.map(() => 'ok'); } };
}
const fields = (cmd: any[]) => {
  const out: Record<string, string> = {};
  for (let i = cmd.indexOf('*') + 1; i < cmd.length; i += 2) out[String(cmd[i])] = String(cmd[i + 1]);
  return out;
};

function ctx(url: string, status = 200) {
  const headers: Record<string, string> = {};
  const req = { method: 'POST', originalUrl: url, baseUrl: '/api/tx', route: { path: '/:id/send' } };
  const res = { statusCode: status, headersSent: false, setHeader: (k: string, v: string) => (headers[k] = v) };
  const context: any = {
    getType: () => 'http',
    switchToHttp: () => ({ getRequest: () => req, getResponse: () => res }),
    getClass: () => ({ name: 'TxController' }),
    getHandler: () => ({ name: 'send' }),
  };
  return { context, headers };
}

describe('TraceInterceptor', () => {
  test('a request becomes a trace named after its route, with the handler inside it', async () => {
    const c = fakeClient();
    const t = new Tracer(c as any, { service: 'api', flushMs: 60_000 });
    setTracer(t);
    try {
      const i = new TraceInterceptor({ tracing: true } as any);
      const { context, headers } = ctx('/api/tx/42/send?x=1', 201);
      let inside: string | undefined;
      const handler = { handle: () => { inside = currentTrace()?.traceId; return of({ ok: true }); } };
      assert.deepEqual(await lastValueFrom(i.intercept(context, handler)), { ok: true });
      await t.flush();
      const s = fields(c.writes[0]!);
      assert.equal(s.name, 'POST /api/tx/:id/send');
      assert.equal(s.status, 'ok');
      assert.equal(JSON.parse(s.attrs!).statusCode, 201);
      assert.equal(JSON.parse(s.attrs!).path, '/api/tx/42/send');
      assert.equal(inside, s.traceId);
      assert.equal(headers['x-trace-id'], s.traceId);
    } finally {
      setTracer(undefined);
      await t.close();
    }
  });

  test('a failed request is recorded with its status and error', async () => {
    const c = fakeClient();
    const t = new Tracer(c as any, { service: 'api', flushMs: 60_000 });
    setTracer(t);
    try {
      const i = new TraceInterceptor({ tracing: true } as any);
      const { context } = ctx('/api/tx/42/send');
      const err = new HttpException('insufficient funds', 400);
      await assert.rejects(lastValueFrom(i.intercept(context, { handle: () => throwError(() => err) })), (e) => e === err);
      await t.flush();
      const s = fields(c.writes[0]!);
      assert.equal(s.status, 'error');
      assert.equal(s.error, 'insufficient funds');
      assert.equal(JSON.parse(s.attrs!).statusCode, 400);
    } finally {
      setTracer(undefined);
      await t.close();
    }
  });

  test('health checks and disabled tracing pass straight through', async () => {
    const c = fakeClient();
    const t = new Tracer(c as any, { service: 'api', flushMs: 60_000 });
    setTracer(t);
    try {
      await lastValueFrom(new TraceInterceptor({ tracing: true } as any).intercept(ctx('/api/health').context, { handle: () => of(1) }));
      await lastValueFrom(new TraceInterceptor({ tracing: { http: false } } as any).intercept(ctx('/api/tx/1/send').context, { handle: () => of(1) }));
      await lastValueFrom(new TraceInterceptor({} as any).intercept(ctx('/api/tx/1/send').context, { handle: () => of(1) }));
      await t.flush();
      assert.equal(c.writes.length, 0);
    } finally {
      setTracer(undefined);
      await t.close();
    }
  });
});
