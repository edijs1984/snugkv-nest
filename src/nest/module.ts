import { DynamicModule, InjectionToken, Logger, Module, ModuleMetadata, OptionalFactoryDependency, Provider } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { SnugKvBus, SnugKvBusOptions } from '../core/bus';
import { CacheOptions, SnugKvCache } from '../core/cache';
import { SnugKvClient } from '../core/client';
import { SnugKvLocks } from '../core/lock';
import { FlowProducer } from '../core/queue/flow';
import { SnugKvSubscriber } from '../core/pubsub';
import { SnugKvRateLimiter } from '../core/ratelimit';
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
      const { isGlobal, autoSubscribe, autoProcess, queuePrefix, cache, lockPrefix, rateLimitPrefix, service, ...bus } = options;
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
  SnugKvRuntime,
];

const exported = [SnugKvBus, SnugKvClient, SnugKvSubscriber, SnugKvCache, SnugKvLocks, SnugKvRateLimiter, FlowProducer];

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
