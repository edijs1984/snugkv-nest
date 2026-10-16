import { DynamicModule, InjectionToken, Logger, Module, ModuleMetadata, OptionalFactoryDependency, Provider } from '@nestjs/common';
import { APP_INTERCEPTOR, DiscoveryModule } from '@nestjs/core';
import { SnugKvBus, SnugKvBusOptions } from '../core/bus';
import { CacheOptions, SnugKvCache } from '../core/cache';
import { SnugKvClient } from '../core/client';
import { SnugKvLocks } from '../core/lock';
import { FlowProducer } from '../core/queue/flow';
import { SnugKvSubscriber } from '../core/pubsub';
import { SnugKvRateLimiter } from '../core/ratelimit';
import { setTracer, Tracer, TracerOptions } from '../core/trace';
import { captureLogs, LogShipper, LogShipperOptions } from '../core/logs';
import { instrumentHttpClient } from '../core/trace-http';
import type { ServiceDependency } from '../core/service-map';
import { TraceInterceptor } from './trace-interceptor';
import { BusLogger } from '../core/util';
import { SnugKvRuntime } from './runtime';
import { SNUG_OPTIONS } from './tokens';

export interface SnugKvModuleOptions extends Omit<SnugKvBusOptions, 'service'> {
  /** Name of this app. Needed for messaging (default consumer group); defaults to `app`. */
  service?: string;
  /** Settings for the injectable {@link SnugKvCache}. */
  cache?: CacheOptions;
  /** Key prefix for {@link SnugKvLocks} (default `lock:`). */
  lockPrefix?: string;
  /** Key prefix for {@link SnugKvRateLimiter} (default `rl:`). */
  rateLimitPrefix?: string;
  /** Make the module global (default true). */
  isGlobal?: boolean;
  /** Start consumers for `@SnugSubscribe` methods automatically (default true). */
  autoSubscribe?: boolean;
  /** Start workers for `@SnugProcessor` classes automatically (default true). */
  autoProcess?: boolean;
  /** Key prefix for queues (default `snugq`). */
  queuePrefix?: string;
  /**
   * Flow tracing: queue jobs carry the trace to the worker, and an injectable
   * {@link Tracer} records your own steps. Spans go to the `snug:traces`
   * stream unless `stream` says otherwise. Off by default.
   */
  tracing?: boolean | SnugTracingOptions;
  /**
   * Describe this app in SnugKV (`snug:svc:<service>:<instance>`): its HTTP
   * routes, queues and topics, for the console's Map page. Default true.
   */
  serviceMap?: boolean | { dependencies?: ServiceDependency[] };
  /**
   * Log shipping: everything the app writes to stdout and stderr (Nest's
   * logger, winston, console.log) also goes to the `snug:logs` stream, where
   * the console shows the logs of all services together. Terminal output is
   * unchanged. Off by default.
   */
  logs?: boolean | SnugLogsOptions;
}

export type SnugLogsOptions = Omit<LogShipperOptions, 'service'>;

export interface SnugTracingOptions extends Omit<TracerOptions, 'service'> {
  /** Make every incoming HTTP request a trace (default true). */
  http?: boolean;
  /** Paths not traced (default `/health`, `/api/health`, `/metrics`, `/favicon.ico`). */
  ignorePaths?: string[];
  /** Record outgoing HTTP calls (http, https, fetch) made inside a trace (default true). */
  httpClient?: boolean;
}

export interface SnugKvModuleAsyncOptions extends Pick<ModuleMetadata, 'imports'> {
  isGlobal?: boolean;
  inject?: Array<InjectionToken | OptionalFactoryDependency>;
  useFactory: (...args: any[]) => SnugKvModuleOptions | Promise<SnugKvModuleOptions>;
}

function nestLogger(): BusLogger {
  const l = new Logger('SnugKV');
  return {
    debug: (m) => l.debug(m),
    log: (m) => l.log(m),
    warn: (m) => l.warn(m),
    error: (m) => l.error(m),
  };
}

const sharedProviders: Provider[] = [
  {
    provide: SnugKvBus,
    useFactory: (options: SnugKvModuleOptions) => {
      const { isGlobal, autoSubscribe, autoProcess, queuePrefix, cache, lockPrefix, rateLimitPrefix, service, tracing, serviceMap, logs, ...bus } = options;
      return new SnugKvBus({ logger: nestLogger(), service: service ?? 'app', ...bus });
    },
    inject: [SNUG_OPTIONS],
  },
  { provide: SnugKvClient, useFactory: (bus: SnugKvBus) => bus.client, inject: [SnugKvBus] },
  { provide: SnugKvSubscriber, useFactory: (bus: SnugKvBus) => bus.subscriber(), inject: [SnugKvBus] },
  {
    provide: SnugKvCache,
    useFactory: (client: SnugKvClient, o: SnugKvModuleOptions) => new SnugKvCache(client, o.cache),
    inject: [SnugKvClient, SNUG_OPTIONS],
  },
  {
    provide: SnugKvLocks,
    useFactory: (client: SnugKvClient, o: SnugKvModuleOptions) => new SnugKvLocks(client, o.lockPrefix),
    inject: [SnugKvClient, SNUG_OPTIONS],
  },
  {
    provide: SnugKvRateLimiter,
    useFactory: (client: SnugKvClient, o: SnugKvModuleOptions) => new SnugKvRateLimiter(client, o.rateLimitPrefix),
    inject: [SnugKvClient, SNUG_OPTIONS],
  },
  {
    provide: FlowProducer,
    useFactory: (client: SnugKvClient, o: SnugKvModuleOptions) => new FlowProducer({ client, prefix: o.queuePrefix }),
    inject: [SnugKvClient, SNUG_OPTIONS],
  },
  {
    // Null when tracing is off; inject it with @Optional().
    provide: Tracer,
    useFactory: (client: SnugKvClient, o: SnugKvModuleOptions) => {
      if (!o.tracing) return null;
      const log = new Logger('SnugKV');
      let warned = 0;
      const tracer = new Tracer(client, {
        onError: (e) => {
          if (Date.now() - warned > 60_000) {
            warned = Date.now();
            log.warn(`trace spans not written: ${e instanceof Error ? e.message : String(e)}`);
          }
        },
        ...(o.tracing === true ? {} : o.tracing),
        service: o.service ?? 'app',
      });
      setTracer(tracer);
      if (o.tracing === true || o.tracing.httpClient !== false) instrumentHttpClient();
      return tracer;
    },
    inject: [SnugKvClient, SNUG_OPTIONS],
  },
  {
    // Null when log shipping is off; inject it with @Optional().
    provide: LogShipper,
    useFactory: (client: SnugKvClient, o: SnugKvModuleOptions) => {
      if (!o.logs) return null;
      const out = process.stderr.write.bind(process.stderr);
      let warned = 0;
      const shipper = new LogShipper(client, {
        // Written past the capture, so a failing write cannot loop.
        onError: (e) => {
          if (Date.now() - warned > 60_000) {
            warned = Date.now();
            out(`[SnugKV] log lines not written: ${e instanceof Error ? e.message : String(e)}\n`);
          }
        },
        ...(o.logs === true ? {} : o.logs),
        service: o.service ?? 'app',
      });
      captureLogs(shipper);
      return shipper;
    },
    inject: [SnugKvClient, SNUG_OPTIONS],
  },
  // Passes requests straight through unless tracing (and its `http` option) is on.
  { provide: APP_INTERCEPTOR, useClass: TraceInterceptor },
  SnugKvRuntime,
];

const exported = [SnugKvBus, SnugKvClient, SnugKvSubscriber, SnugKvCache, SnugKvLocks, SnugKvRateLimiter, FlowProducer, Tracer, LogShipper];

@Module({})
export class SnugKvModule {
  static forRoot(options: SnugKvModuleOptions): DynamicModule {
    return {
      module: SnugKvModule,
      global: options.isGlobal ?? true,
      imports: [DiscoveryModule],
      providers: [{ provide: SNUG_OPTIONS, useValue: options }, ...sharedProviders],
      exports: exported,
    };
  }

  static forRootAsync(options: SnugKvModuleAsyncOptions): DynamicModule {
    return {
      module: SnugKvModule,
      global: options.isGlobal ?? true,
      imports: [DiscoveryModule, ...(options.imports ?? [])],
      providers: [{ provide: SNUG_OPTIONS, useFactory: options.useFactory, inject: options.inject ?? [] }, ...sharedProviders],
      exports: exported,
    };
  }
}
