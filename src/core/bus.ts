import { noteSubscribe } from './service-map';
import { hostname } from 'node:os';
import { SnugKvClient, SnugKvClientOptions } from './client';
import { Consumer, MessageHandler, SubscribeOptions, ConsumerStats } from './consumer';
import { SnugKvSubscriber } from './pubsub';
import { PresenceOptions, PresenceReporter } from './presence';
import { BatchMessage, jsonSerializer, PublishOptions, Publisher, Serializer } from './publisher';
import { BusLogger, consoleLogger, uuid } from './util';

export interface SnugKvBusOptions extends SnugKvClientOptions {
  /** Name of this microservice, e.g. `billing`. Default consumer group. */
  service: string;
  /** Unique id of this running copy. Default `<hostname>-<random>`. Use a stable id (e.g. the pod name) for faster crash recovery. */
  instance?: string;
  /** Prepended to every topic to form the stream key. Default none. */
  keyPrefix?: string;
  /** Trim published streams to about this many entries. Default: no trimming. */
  maxLen?: number;
  /** Report this process to tools like the stream UI. Default true. */
  presence?: boolean | PresenceOptions;
  serializer?: Serializer;
  logger?: BusLogger;
  /** Wait this long for the first connection in `connect()`. Default 10 s. */
  connectTimeoutMs?: number;
}

export interface Subscription {
  readonly topic: string;
  readonly group: string;
  readonly consumer: string;
  stats(): ConsumerStats;
  stop(): Promise<void>;
}

/** Everything in one object: publish, subscribe, presence, shutdown. */
export class SnugKvBus {
  readonly client: SnugKvClient;
  readonly service: string;
  readonly instance: string;
  readonly publisher: Publisher;
  private readonly presence?: PresenceReporter;
  private readonly consumers: Consumer[] = [];
  private readonly serializer: Serializer;
  private readonly logger: BusLogger;
  private readonly keyPrefix: string;
  private closed = false;
  private pubsub?: SnugKvSubscriber;

  constructor(private readonly options: SnugKvBusOptions) {
    if (!options.service) throw new Error('snugkv: "service" is required');
    this.service = options.service;
    this.instance = options.instance ?? `${hostname()}-${uuid().slice(0, 8)}`;
    this.serializer = options.serializer ?? jsonSerializer;
    this.logger = options.logger ?? consoleLogger;
    this.keyPrefix = options.keyPrefix ?? '';
    const { service, instance, keyPrefix, maxLen, presence, serializer, logger, connectTimeoutMs, ...conn } = options;
    this.client = new SnugKvClient({ name: `${this.service}/${this.instance}`, ...conn });
    this.client.on('error', (e: Error) => this.logger.warn(`connection error: ${e.message}`));
    if (options.presence !== false) {
      this.presence = new PresenceReporter(this.client, this.logger, typeof options.presence === 'object' ? options.presence : {});
    }
    this.publisher = new Publisher(
      this.client,
      { service: this.service, instance: this.instance, keyPrefix: this.keyPrefix, defaultMaxLen: options.maxLen, serializer: this.serializer },
      this.presence,
    );
  }

  /** Connects the main connection. Rejects if the server is not reachable in time. */
  async connect(): Promise<void> {
    await this.client.connect(this.options.connectTimeoutMs ?? 10_000);
  }

  publish<T = unknown>(topic: string, payload: T, options?: PublishOptions): Promise<string> {
    return this.publisher.publish(topic, payload, options);
  }

  publishBatch<T = unknown>(topic: string, messages: Array<BatchMessage<T>>, options?: { maxLen?: number }): Promise<string[]> {
    return this.publisher.publishBatch(topic, messages, options);
  }

  /** Starts a consumer. Resolves when it is reading. */
  async subscribe<T = unknown>(options: SubscribeOptions, handler: MessageHandler<T>): Promise<Subscription> {
    if (this.closed) throw new Error('snugkv: bus is closed');
    await this.connect();
    const read = this.client.duplicate({ name: `${this.service}/${this.instance}:read:${options.topic}` });
    read.on('error', (e: Error) => this.logger.warn(`read connection error: ${e.message}`));
    const consumer = new Consumer(options, handler as MessageHandler, {
      cmd: this.client,
      read,
      service: this.service,
      instance: this.instance,
      keyPrefix: this.keyPrefix,
      serializer: this.serializer,
      logger: this.logger,
      presence: this.presence,
    });
    this.consumers.push(consumer);
    noteSubscribe(options.topic, options.group ?? this.service);
    try {
      await consumer.start();
    } catch (err) {
      this.consumers.splice(this.consumers.indexOf(consumer), 1);
      read.close();
      throw err;
    }
    return {
      topic: consumer.topic,
      group: consumer.group,
      consumer: consumer.consumer,
      stats: () => consumer.stats(),
      stop: async () => {
        const i = this.consumers.indexOf(consumer);
        if (i >= 0) this.consumers.splice(i, 1);
        await consumer.stop();
      },
    };
  }

  /** Shared pub/sub subscriber (own connection, opened on first use, closed with the bus). */
  subscriber(): SnugKvSubscriber {
    this.pubsub ??= this.client.subscriber();
    return this.pubsub;
  }

  stats(): { published: Record<string, number>; consumers: ConsumerStats[] } {
    return { published: this.publisher.stats(), consumers: this.consumers.map((c) => c.stats()) };
  }

  /** Stops consumers (letting running handlers finish), removes presence records and closes connections. */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    await Promise.allSettled(this.consumers.splice(0).map((c) => c.stop()));
    await this.presence?.stop();
    this.pubsub?.close();
    this.client.close();
  }
}
