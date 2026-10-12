import { noteQueueProducer } from '../core/service-map';
import { DynamicModule, Inject, Module, Provider } from '@nestjs/common';
import { SnugKvClient } from '../core/client';
import type { JobsOptions } from '../core/queue/types';
import { Queue } from '../core/queue/queue';

export interface RegisterQueueOptions {
  name: string;
  /** Overrides the module's `queuePrefix`. */
  prefix?: string;
  defaultJobOptions?: JobsOptions;
}

/** Injection token of a queue registered with {@link SnugKvQueueModule.registerQueue}. */
export const getQueueToken = (name: string): string => `SnugKvQueue_${name}`;

/** `constructor(@InjectSnugQueue('emails') private emails: Queue) {}` */
export const InjectSnugQueue = (name: string): ParameterDecorator => Inject(getQueueToken(name));

/**
 * Makes queues injectable. Import after `SnugKvModule`; workers for `@SnugProcessor` classes start by themselves.
 *
 * ```ts
 * SnugKvQueueModule.registerQueue({ name: 'emails', defaultJobOptions: { attempts: 3 } })
 * ```
 */
@Module({})
export class SnugKvQueueModule {
  static registerQueue(...configs: RegisterQueueOptions[]): DynamicModule {
    const providers: Provider[] = configs.map((c) => ({
      provide: getQueueToken(c.name),
      useFactory: (client: SnugKvClient) => {
        noteQueueProducer(c.name);
        return new Queue(c.name, { client, prefix: c.prefix, defaultJobOptions: c.defaultJobOptions });
      },
      inject: [SnugKvClient],
    }));
    return { module: SnugKvQueueModule, providers, exports: providers.map((p) => (p as { provide: string }).provide) };
  }
}
