import { Inject, Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { DiscoveryService } from '@nestjs/core';
import { SnugKvBus, Subscription } from '../core/bus';
import { Worker } from '../core/queue/worker';
import { getTracer, setTracer } from '../core/trace';
import { collectProcessors, collectSubscriptions } from './explorer';
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
  private workers: Worker[] = [];

  constructor(
    private readonly bus: SnugKvBus,
    private readonly discovery: DiscoveryService,
    @Inject(SNUG_OPTIONS) private readonly options: SnugKvModuleOptions,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.bus.connect();
  }

  async onApplicationBootstrap(): Promise<void> {
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
    if (this.options.autoProcess !== false) {
      for (const p of collectProcessors(instances)) {
        const worker = new Worker(p.meta.queue, p.processor, { ...p.meta.options, client: this.bus.client, prefix: this.options.queuePrefix });
        worker.on('error', (e: Error) => this.log.warn(`worker ${p.meta.queue}: ${e.message}`));
        for (const e of p.events) worker.on(e.event, (...args: unknown[]) => void Promise.resolve(e.handler(...args)).catch((err: Error) => this.log.warn(`${p.instance.constructor.name} ${e.event} handler: ${err.message}`)));
        this.workers.push(worker);
        this.log.log(`${p.instance.constructor.name} processes queue ${p.meta.queue} (concurrency ${p.meta.options.concurrency ?? 1})`);
      }
    }
    if (this.options.autoSubscribe === false) return;
    for (const s of collectSubscriptions(instances)) {
      const sub = await this.bus.subscribe(s.meta, s.handler);
      this.subscriptions.push(sub);
      this.log.log(`${s.instance.constructor.name}.${String(s.method)} ← ${s.meta.topic} (group ${sub.group}, consumer ${sub.consumer})`);
    }
  }

  async onModuleDestroy(): Promise<void> {
    await Promise.allSettled(this.workers.splice(0).map((w) => w.close()));
    const tracer = getTracer();
    if (tracer) {
      await tracer.close().catch(() => undefined);
      setTracer(undefined);
    }
    await this.bus.close();
    this.subscriptions = [];
  }
}
