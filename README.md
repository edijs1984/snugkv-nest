# snugkv-nest

The NestJS module and TypeScript client for [SnugKV](https://github.com/edijs1984/snugkv): strings, hashes, lists, sets, sorted sets, expiry, Lua, pub/sub and transactions, plus cache, lock and rate-limit helpers and a durable message bus on streams.

- [Using SnugKV as a data store](#using-snugkv-as-a-data-store)
- [Messaging (streams)](#messaging-streams)

- **Durable topics.** Messages are stream entries. They survive restarts and are replayable.
- **Consumer groups.** Instances of one service share the work; every different service gets every message.
- **Safe delivery.** At-least-once with acknowledgements, retries with backoff, dead-letter topics, and automatic takeover of messages a crashed instance left behind.
- **No dependencies.** A small built-in RESP client (reconnect, AUTH, TLS, pipelining). Only `@nestjs/common` and `@nestjs/core` as peers.
- **Visible.** Every publisher and consumer reports itself, so [snugkv-stream-ui](https://github.com/edijs1984/snugkv-stream-ui) can show who publishes and who listens to each topic.

It also works as a plain Redis-protocol streams client, so it should run against Redis 6.2+ too, but SnugKV is what it is tested with.

## Install

```bash
npm install snugkv-nest
```

Needs Node 18+ and NestJS 10 or 11.

## Using SnugKV as a data store

Import the module once; everything below can be injected.

```ts
import { Injectable } from '@nestjs/common';
import { SnugKvCache, SnugKvClient, SnugKvLocks, SnugKvModule, SnugKvRateLimiter, SnugKvSubscriber } from 'snugkv-nest';

// app.module.ts
@Module({ imports: [SnugKvModule.forRoot({ url: process.env.SNUGKV_URL, cache: { prefix: 'app:', ttlSeconds: 120 } })] })
export class AppModule {}

@Injectable()
export class UsersService {
  constructor(
    private readonly kv: SnugKvClient,
    private readonly cache: SnugKvCache,
    private readonly locks: SnugKvLocks,
    private readonly limiter: SnugKvRateLimiter,
    private readonly pubsub: SnugKvSubscriber,
  ) {}

  profile(id: string) {
    return this.cache.wrap(`user:${id}`, () => this.loadFromDb(id)); // loader runs once, result cached 120 s
  }

  async rebuild() {
    const r = await this.locks.run('rebuild', () => this.doRebuild(), { ttlMs: 60_000 });
    return r.acquired;
  }

  async allow(ip: string) {
    return (await this.limiter.hit(ip, 100, 60_000)).allowed; // 100 per minute
  }
}
```

`service` is only needed for messaging; without it the module uses `app`. Outside Nest, use `new SnugKvClient({ url })` from `snugkv-nest/core`.

### Client

```ts
await kv.set('a', '1');  await kv.setOpts('a', '1', { nx: true, exSeconds: 30 });
await kv.incr('visits');  await kv.setJson('cfg', { a: 1 });  await kv.getJson('cfg');
await kv.hset('user:1', { name: 'Ann', age: 31 });  await kv.hgetall('user:1');
await kv.rpush('queue', 'a', 'b');  await kv.lrange('queue');
await kv.sadd('tags', 'x', 'y');  await kv.smembers('tags');
await kv.zadd('board', [{ member: 'ann', score: 10 }]);  await kv.zrange('board', 0, 9, { reverse: true });
await kv.expire('a', 60);  await kv.ttl('a');
for await (const key of kv.scanIterator({ match: 'user:*' })) { /* … */ }
await kv.eval("return redis.call('get', KEYS[1])", ['a']);
await kv.tx().set('a', '1').incr('n').hset('h', { f: 1 }).exec(); // MULTI/EXEC in one write
await kv.call('ANYTHING', 'else');                                 // raw command
```

### Pub/sub

Fire-and-forget broadcast. Use the streams below when a message must not be lost.

```ts
const off = await pubsub.subscribe('news', (message, channel) => console.log(channel, message));
await pubsub.psubscribe('events.*', (message, channel, pattern) => { /* … */ });
await kv.publish('news', 'hello');
await off();
```

Subscriptions are restored after a reconnect; messages sent while disconnected are not replayed.

### Cache, locks, rate limits

- `SnugKvCache`: `get`, `set`, `del`, `wrap(key, loader, ttl)` (concurrent callers share one load), `clear(pattern)`.
- `SnugKvLocks`: `acquire(name, { ttlMs, waitMs })`, `run(name, fn)`. A lock expires on its own, and only its holder can release or extend it. It protects against crashed holders, not against the server itself failing, so keep critical sections idempotent.
- `SnugKvRateLimiter`: `hit(key, limit, windowMs)` is a fixed window; it returns `{ allowed, count, remaining, resetMs }`.

## Messaging (streams)

## Quick start

```ts
// app.module.ts
import { Module } from '@nestjs/common';
import { SnugKvModule } from 'snugkv-nest';

@Module({
  imports: [
    SnugKvModule.forRoot({
      url: 'redis://localhost:6379',
      service: 'billing', // this microservice's name
    }),
  ],
})
export class AppModule {}
```

Publish:

```ts
import { Injectable } from '@nestjs/common';
import { SnugKvBus } from 'snugkv-nest';

@Injectable()
export class OrdersService {
  constructor(private readonly bus: SnugKvBus) {}

  async create(order: Order) {
    await this.bus.publish('orders.created', order, { event: 'order.created', headers: { tenant: 'acme' } });
  }
}
```

Consume:

```ts
import { Injectable } from '@nestjs/common';
import { SnugSubscribe, MessageContext } from 'snugkv-nest';

@Injectable()
export class BillingHandlers {
  @SnugSubscribe('orders.created', { concurrency: 4 })
  async onOrder(order: Order, ctx: MessageContext) {
    // throw to retry; after maxAttempts the message goes to "orders.created.dlq"
  }
}
```

Any provider or controller with `@SnugSubscribe` methods is picked up when the app starts. Consumers stop cleanly when the app closes (`app.enableShutdownHooks()` makes this work on SIGTERM).

Prefer parameter decorators?

```ts
@SnugSubscribe('orders.created')
onOrder(@SnugPayload() order: Order, @SnugHeader('tenant') tenant: string, @SnugContext() ctx: MessageContext) {}
```

## How delivery works

| You want | Do this |
| --- | --- |
| Run 3 copies of `billing`, each message handled once | Same `service` name on all copies |
| `mailer` and `billing` both react to the same event | Different `service` names |
| Two handlers in one service on one topic | Give one a different `group` |

The consumer group defaults to the `service` name. Each running copy is a consumer named `<service>-<instance>`.

**Retries.** A handler that throws is called again after a delay (200 ms doubling to 10 s) up to `maxAttempts` (3). Then the message is copied to `<topic>.dlq` with `dlq_error`, `dlq_attempts`, `dlq_source`, `dlq_group` and `dlq_id`, and acknowledged. `ctx.attempt` tells the handler which try it is.

**Crashes.** Messages a consumer read but never acknowledged stay pending. Every consumer checks for pending messages idle longer than `claimMinIdleMs` (60 s) and takes them over. A message that has already been delivered `maxAttempts` times goes straight to the dead-letter topic, so a message that kills the process cannot loop forever. Set `claimMinIdleMs` above your slowest handler.

**Order.** `concurrency: 1` (the default) handles messages one at a time in stream order. Higher values trade order for throughput.

**At-least-once.** After a crash a message can be handled twice. Make handlers idempotent; `ctx.messageId` is stable across redeliveries.

**Manual ack.** With `autoAck: false` the message stays pending until the handler calls `await ctx.ack()`.

## Options

### `SnugKvModule.forRoot(options)` / `SnugKvBus`

| Option | Default | |
| --- | --- | --- |
| `service` | required | Name of this microservice. Default consumer group. |
| `url` | | `redis://user:pass@host:port`, `rediss://` for TLS. `snugkv://` and `snugkvs://` also work. |
| `host`, `port`, `username`, `password`, `tls` | `127.0.0.1`, `6379` | Instead of `url`. |
| `instance` | `<hostname>-<random>` | Id of this running copy. Use the pod name for fast recovery after a restart. |
| `keyPrefix` | none | Prepended to every topic to form the stream key. |
| `maxLen` | none | Trim published streams to about this many entries. |
| `presence` | `true` | Report publishers and consumers for the UI. `false` turns it off. |
| `isGlobal` | `true` | Make the module global. |
| `autoSubscribe` | `true` | Start `@SnugSubscribe` consumers automatically. |

`forRootAsync({ imports, inject, useFactory })` works as usual:

```ts
SnugKvModule.forRootAsync({
  inject: [ConfigService],
  useFactory: (config: ConfigService) => ({ url: config.get('SNUGKV_URL')!, service: 'billing' }),
});
```

### `@SnugSubscribe(topic, options)`

| Option | Default | |
| --- | --- | --- |
| `group` | `service` | Consumer group. |
| `consumer` | `<service>-<instance>` | Consumer name. |
| `startFrom` | `'$'` | Where a new group starts: `'$'` new messages only, `'0'` from the oldest retained. |
| `concurrency` | `1` | Messages handled at once. |
| `prefetch` | `max(10, 2×concurrency)` | Messages read ahead. |
| `autoAck` | `true` | Acknowledge when the handler returns. |
| `maxAttempts` | `3` | Handler runs before dead-lettering. |
| `retryDelayMs` | 200 ms × 2ⁿ, max 10 s | Number, or `(failedAttempt) => ms`. |
| `deadLetterTopic` | `<topic>.dlq` | `false` drops failed messages after logging. |
| `claimMinIdleMs` | `60000` | Take over messages idle this long. |
| `claimIntervalMs` | `15000` | How often to look. |
| `shutdownTimeoutMs` | `10000` | How long to wait for running handlers on stop. |

### Publishing

```ts
bus.publish(topic, payload, { event?, headers?, maxLen? }): Promise<string>   // entry id
bus.publishBatch(topic, [{ payload, event?, headers? }, …]): Promise<string[]> // one round trip
```

Each entry has the fields `payload` (JSON), `event`, `mid` (message uuid), `ts` (ms), `producer` (`service/instance`) and optionally `headers` (JSON). Entries written by other tools are delivered too; without a `payload` field the handler gets an object of all fields.

### Raw commands and health

`SnugKvClient` is injectable for anything else:

```ts
constructor(private readonly kv: SnugKvClient) {}
await this.kv.call('XLEN', 'orders.created');
await this.kv.ping(); // use in a health check
```

## Without Nest

```ts
import { SnugKvBus } from 'snugkv-nest/core';

const bus = new SnugKvBus({ url: 'redis://localhost:6379', service: 'worker' });
const sub = await bus.subscribe({ topic: 'jobs' }, async (job) => { /* … */ });
await bus.publish('jobs', { id: 1 });
await bus.close(); // stops consumers, removes presence records
```

## Presence protocol

For tools that want to show who is connected: each publisher and consumer keeps a JSON string at
`snugkv:presence:<role>:<topic>:<service-or-group>:<instance>` with a 15 s expiry, refreshed every 5 s. Fields:
`v, role, service, instance, topic, group?, consumer?, host, pid, startedAt, updatedAt` and counters
`published?, processed?, failed?, deadLettered?, inflight?, concurrency?`.

## Development

```bash
git clone https://github.com/edijs1984/snugkv && (cd snugkv && go build -o /tmp/snugkv ./cmd/snugkv)
npm install
npm run build
SNUGKV_BIN=/tmp/snugkv npm test   # tests start real servers on free ports
```

Tests that need a server are skipped when `SNUGKV_BIN` is not set.

## License

MIT
