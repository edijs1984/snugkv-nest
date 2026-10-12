import assert from 'node:assert/strict';
import { describe, test } from 'node:test';
import { noteQueueProducer, noteQueueWorker, notePublish, noteSubscribe, resetServiceMap, ServiceCardReporter } from '../src/core/service-map';

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
    await r.stop();
    assert.deepEqual(calls[1], ['DEL', 'snug:svc:ledger:h-1']);
    resetServiceMap();
  });
});
