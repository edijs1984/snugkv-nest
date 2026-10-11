import { DynamicModule, InjectionToken, Logger, Module, ModuleMetadata, OptionalFactoryDependency, Provider } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { SnugKvBus, SnugKvBusOptions } from '../core/bus';
import { SnugKvClient } from '../core/client';
import { BusLogger } from '../core/util';
import { SnugKvRuntime } from './runtime';
import { SNUG_OPTIONS } from './tokens';

export interface SnugKvModuleOptions extends SnugKvBusOptions {
  /** Make the module global (default true). */
  isGlobal?: boolean;
  /** Start consumers for `@SnugSubscribe` methods automatically (default true). */
  autoSubscribe?: boolean;
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
      const { isGlobal, autoSubscribe, ...bus } = options;
      return new SnugKvBus({ logger: nestLogger(), ...bus });
    },
    inject: [SNUG_OPTIONS],
  },
  { provide: SnugKvClient, useFactory: (bus: SnugKvBus) => bus.client, inject: [SnugKvBus] },
  SnugKvRuntime,
];

@Module({})
export class SnugKvModule {
  static forRoot(options: SnugKvModuleOptions): DynamicModule {
    return {
      module: SnugKvModule,
      global: options.isGlobal ?? true,
      imports: [DiscoveryModule],
      providers: [{ provide: SNUG_OPTIONS, useValue: options }, ...sharedProviders],
      exports: [SnugKvBus, SnugKvClient],
    };
  }

  static forRootAsync(options: SnugKvModuleAsyncOptions): DynamicModule {
    return {
      module: SnugKvModule,
      global: options.isGlobal ?? true,
      imports: [DiscoveryModule, ...(options.imports ?? [])],
      providers: [{ provide: SNUG_OPTIONS, useFactory: options.useFactory, inject: options.inject ?? [] }, ...sharedProviders],
      exports: [SnugKvBus, SnugKvClient],
    };
  }
}
