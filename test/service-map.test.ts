import assert from 'node:assert/strict';
import { describe, test } from 'node:test';
import { declareDependency, noteQueueProducer, noteQueueWorker, notePublish, noteSubscribe, resetServiceMap, ServiceCardReporter } from '../src/core/service-map';

describe('service cards', () => {
  test('a card lists routes, queues and topics, and is removed on stop', async () => {
    resetServiceMap();
    const calls: unknown[][] = [];
    const client = { call: async (...a: unknown[]) => (calls.push(a), 'OK') };
    const r = new ServiceCardReporter(client as any, {
      service: 'ledger',
      instance: 'h-1',
      http: [{ method: 'GET', path: '/api/balance', handler: 'BalanceController.get' }],
      intervalMs: 60_000,
    });
    noteQueueWorker('chain-events', 4);
    noteQueueProducer('notify');
    noteQueueProducer('notify');
    notePublish('wallet.created');
    noteSubscribe('orders', 'ledger');
    await r.write();
    const [cmd, key, json, ex, ttl] = calls[0] as [string, string, string, string, number];
    assert.equal(cmd, 'SET');
    assert.equal(key, 'snug:svc:ledger:h-1');
    assert.equal(ex, 'EX');
    assert.equal(ttl, 180);
    const card = JSON.parse(json);
    assert.equal(card.service, 'ledger');
    assert.deepEqual(card.http, [{ method: 'GET', path: '/api/balance', handler: 'BalanceController.get' }]);
    assert.deepEqual(card.queues, { produces: ['notify'], processes: [{ queue: 'chain-events', concurrency: 4 }] });
    assert.deepEqual(card.topics, { publishes: ['wallet.created'], subscribes: [{ topic: 'orders', group: 'ledger' }] });
    assert.deepEqual(card.dependencies, []);
    declareDependency({ kind: 'mongodb', name: 'MongoDB', address: 'mongodb://user:secret@mongo:27017/airsig' });
    declareDependency({ kind: 'http', name: 'Alchemy', direction: 'in', routes: ['/api/evm/alchemy-webhook'] });
    declareDependency({ kind: 'mongodb', name: 'MongoDB', address: 'mongo:27017' }); // replaces the first
    const deps = r.card().dependencies;
    assert.equal(deps.length, 2);
    assert.deepEqual(deps.find((d) => d.kind === 'mongodb'), { kind: 'mongodb', name: 'MongoDB', address: 'mongo:27017' });
    assert.ok(!JSON.stringify(deps).includes('secret'));
    declareDependency({ kind: 'redis', name: 'Cache', address: 'redis://u:pw@cache:6379' });
    assert.equal(r.card().dependencies.find((d) => d.kind === 'redis')!.address, 'redis://cache:6379');
    await r.stop();
    assert.deepEqual(calls[1], ['DEL', 'snug:svc:ledger:h-1']);
    resetServiceMap();
  });
});
