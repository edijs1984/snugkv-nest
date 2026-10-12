/* eslint-disable @typescript-eslint/no-explicit-any */
import { CallHandler, ExecutionContext, HttpException, Inject, Injectable, NestInterceptor } from '@nestjs/common';
import { Observable } from 'rxjs';
import { currentTrace, getTracer, runIn } from '../core/trace';
import { SNUG_OPTIONS } from './tokens';
import type { SnugKvModuleOptions } from './module';

/** Paths not traced unless `tracing.ignorePaths` says otherwise. */
export const DEFAULT_IGNORED_PATHS = ['/health', '/api/health', '/metrics', '/favicon.ico'];

/**
 * Makes every HTTP request a trace (or a step of the trace it belongs to):
 * `POST /api/transactions/:id/send` with the status code and the error of a
 * failed request. The trace id is returned in the `x-trace-id` header so a
 * client can show it. Request bodies and headers are not recorded.
 */
@Injectable()
export class TraceInterceptor implements NestInterceptor {
  private readonly ignored: string[];
  private readonly enabled: boolean;

  constructor(@Inject(SNUG_OPTIONS) options: SnugKvModuleOptions) {
    const t = typeof options.tracing === 'object' ? options.tracing : {};
    this.enabled = !!options.tracing && t.http !== false;
    this.ignored = t.ignorePaths ?? DEFAULT_IGNORED_PATHS;
  }

  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    const tracer = getTracer();
    if (!this.enabled || !tracer || context.getType() !== 'http') return next.handle();
    const http = context.switchToHttp();
    const req: any = http.getRequest();
    const res: any = http.getResponse();
    const url = String(req?.originalUrl ?? req?.url ?? '').split('?')[0]!;
    if (this.ignored.some((p) => url === p || url.startsWith(p + '/'))) return next.handle();

    const method = String(req?.method ?? 'GET');
    // The route pattern keeps names readable and groups calls to the same endpoint.
    const route = req?.route?.path ? `${req.baseUrl ?? ''}${req.route.path}` : url;
    const handler = `${context.getClass()?.name ?? ''}.${context.getHandler()?.name ?? ''}`;
    const span = tracer.start(`${method} ${route}`, { method, path: url, handler }, currentTrace());
    try {
      if (typeof res?.setHeader === 'function' && !res.headersSent) res.setHeader('x-trace-id', span.context.traceId);
      else if (typeof res?.header === 'function') res.header('x-trace-id', span.context.traceId);
    } catch {
      /* headers already sent */
    }

    return new Observable((subscriber) => {
      const sub = runIn(span.context, () =>
        next.handle().subscribe({
          next: (v) => subscriber.next(v),
          error: (err) => {
            const status = err instanceof HttpException ? err.getStatus() : 500;
            span.set({ statusCode: status });
            span.end(err);
            subscriber.error(err);
          },
          complete: () => {
            span.set({ statusCode: res?.statusCode });
            span.end();
            subscriber.complete();
          },
        }),
      );
      return () => sub.unsubscribe();
    });
  }
}
