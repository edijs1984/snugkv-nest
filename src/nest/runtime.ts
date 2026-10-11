import { Inject, Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { DiscoveryService } from '@nestjs/core';
import { SnugKvBus, Subscription } from '../core/bus';
import { collectSubscriptions } from './explorer';
import { SNUG_OPTIONS } from './tokens';
import type { SnugKvModuleOptions } from './module';

/**
 * Connects when the app starts, starts a consumer for every `@SnugSubscribe`
 * method it finds, and stops everything cleanly when the app closes.
 */
@Injectable()
export class SnugKvRuntime implements OnModuleInit, OnApplicationBootstrap, OnModuleDestroy {
  private readonly log = new Logger('SnugKV');
  private subscriptions: Subscription[] = [];

  constructor(
    private readonly bus: SnugKvBus,
    private readonly discovery: DiscoveryService,
    @Inject(SNUG_OPTIONS) private readonly options: SnugKvModuleOptions,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.bus.connect();
  }

  async onApplicationBootstrap(): Promise<void> {
    if (this.options.autoSubscribe === false) return;
    const wrappers = [...this.discovery.getProviders(), ...this.discovery.getControllers()];
    const instances: object[] = [];
    for (const w of wrappers) {
      const inst = w.instance as unknown;
      if (!inst || typeof inst !== 'object') continue;
      if (!w.isDependencyTreeStatic()) {
        // Request-scoped and transient providers have no single instance to call.
        continue;
      }
      instances.push(inst);
    }
    for (const s of collectSubscriptions(instances)) {
      const sub = await this.bus.subscribe(s.meta, s.handler);
      this.subscriptions.push(sub);
      this.log.log(`${s.instance.constructor.name}.${String(s.method)} ← ${s.meta.topic} (group ${sub.group}, consumer ${sub.consumer})`);
    }
  }

  async onModuleDestroy(): Promise<void> {
    await this.bus.close();
    this.subscriptions = [];
  }
}
