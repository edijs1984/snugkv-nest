import { Inject, Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy, OnModuleInit, RequestMethod } from '@nestjs/common';
import { METHOD_METADATA, PATH_METADATA } from '@nestjs/common/constants';
import { ApplicationConfig, DiscoveryService, ModuleRef } from '@nestjs/core';
import { declareDependency, ServiceCardReporter, ServiceRoute } from '../core/service-map';
import { parseUrl } from '../core/util';
import { SnugKvBus, Subscription } from '../core/bus';
import { Worker } from '../core/queue/worker';
import { getTracer, setTracer } from '../core/trace';
import { getLogShipper } from '../core/logs';
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
  private card?: ServiceCardReporter;

  constructor(
    private readonly bus: SnugKvBus,
    private readonly discovery: DiscoveryService,
    @Inject(SNUG_OPTIONS) private readonly options: SnugKvModuleOptions,
    private readonly moduleRef: ModuleRef,
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
    if (this.options.autoSubscribe !== false) {
      for (const s of collectSubscriptions(instances)) {
        const sub = await this.bus.subscribe(s.meta, s.handler);
        this.subscriptions.push(sub);
        this.log.log(`${s.instance.constructor.name}.${String(s.method)} ← ${s.meta.topic} (group ${sub.group}, consumer ${sub.consumer})`);
      }
    }
    if (this.options.serviceMap !== false) {
      // SnugKV itself is always a dependency.
      let address: string | undefined;
      try {
        if (this.options.url) {
          const u = parseUrl(this.options.url);
          address = `${u.host}:${u.port}`;
        } else if (this.options.host) address = `${this.options.host}:${this.options.port ?? 6379}`;
      } catch {
        /* unusual URL: leave the address out */
      }
      declareDependency({ kind: 'snugkv', name: 'SnugKV', address });
      for (const d of (typeof this.options.serviceMap === 'object' ? this.options.serviceMap.dependencies : undefined) ?? []) declareDependency(d);
      this.card = new ServiceCardReporter(this.bus.client, { service: this.bus.service, instance: this.bus.instance, http: this.routes() });
      this.card.start();
    }
  }

  /** HTTP routes of the app's controllers, with the global prefix when it can be read. */
  private routes(): ServiceRoute[] {
    let prefix = '';
    try {
      prefix = this.moduleRef.get(ApplicationConfig, { strict: false })?.getGlobalPrefix() ?? '';
    } catch {
      /* not an HTTP app, or the config is not reachable */
    }
    const join = (...parts: string[]) => '/' + parts.map((p) => p.replace(/^\/+|\/+$/g, '')).filter(Boolean).join('/');
    const asList = (v: unknown): string[] => (Array.isArray(v) ? v.map(String) : [v === undefined ? '' : String(v)]);
    const out: ServiceRoute[] = [];
    for (const w of this.discovery.getControllers()) {
      const ctrl = w.metatype as (new (...a: unknown[]) => unknown) | undefined;
      if (!ctrl?.prototype) continue;
      const bases = asList(Reflect.getMetadata(PATH_METADATA, ctrl));
      const proto = ctrl.prototype as Record<string, unknown>;
      for (const name of Object.getOwnPropertyNames(proto)) {
        if (name === 'constructor') continue;
        const fn = Object.getOwnPropertyDescriptor(proto, name)?.value;
        if (typeof fn !== 'function') continue;
        const path = Reflect.getMetadata(PATH_METADATA, fn);
        if (path === undefined) continue;
        const method = RequestMethod[Reflect.getMetadata(METHOD_METADATA, fn) as RequestMethod] ?? 'GET';
        for (const b of bases) for (const p of asList(path)) out.push({ method, path: join(prefix, b, p), handler: `${ctrl.name}.${name}` });
      }
    }
    return out.sort((a, b) => a.path.localeCompare(b.path) || a.method.localeCompare(b.method));
  }

  async onModuleDestroy(): Promise<void> {
    await this.card?.stop();
    await Promise.allSettled(this.workers.splice(0).map((w) => w.close()));
    const tracer = getTracer();
    if (tracer) {
      await tracer.close().catch(() => undefined);
      setTracer(undefined);
    }
    // Last, so the shutdown lines above are shipped too. Capture stays on
    // until the process exits; lines after this are dropped quietly.
    await getLogShipper()?.close().catch(() => undefined);
    await this.bus.close();
    this.subscriptions = [];
  }
}
