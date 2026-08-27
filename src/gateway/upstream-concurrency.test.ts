import { afterEach, describe, expect, it, vi } from 'vitest';
import { parseGatewayConfigFromRaw } from '../config';
import type { ProviderConfig } from '../types';
import {
  acquireProviderConcurrencySlot,
  closeProviderConcurrencyStore,
  setProviderConcurrencyRedisCommandExecutorForTests,
  resetProviderConcurrencyForTests
} from './upstream-concurrency';

describe('gateway upstream concurrency', () => {
  afterEach(async () => {
    vi.useRealTimers();
    resetProviderConcurrencyForTests();
    await closeProviderConcurrencyStore();
  });

  it('times out queued requests for the same provider when the provider slot is occupied', async () => {
    const config = parseGatewayConfigFromRaw({
      upstreamConcurrency: {
        enabled: true,
        maxInFlightPerProvider: 1,
        queueTimeoutMs: 1
      }
    });
    const provider = createProviderConfig('openai-main');
    const first = await acquireProviderConcurrencySlot(config, 'openai', provider);
    expect(first.ok).toBe(true);
    if (!first.ok) {
      return;
    }

    const second = await acquireProviderConcurrencySlot(config, 'openai', provider);

    expect(second).toMatchObject({
      ok: false,
      status: 429,
      message: 'Provider upstream concurrency limit exceeded.',
      details: {
        provider: 'openai',
        providerName: 'openai-main',
        maxInFlight: 1,
        queueTimeoutMs: 1
      }
    });
    first.release();
  });

  it('keeps named providers isolated from each other', async () => {
    const config = parseGatewayConfigFromRaw({
      upstreamConcurrency: {
        enabled: true,
        maxInFlightPerProvider: 1,
        queueTimeoutMs: 1
      }
    });
    const first = await acquireProviderConcurrencySlot(config, 'openai', createProviderConfig('openai-a'));
    const second = await acquireProviderConcurrencySlot(config, 'openai', createProviderConfig('openai-b'));

    expect(first.ok).toBe(true);
    expect(second.ok).toBe(true);
    if (first.ok) {
      first.release();
    }
    if (second.ok) {
      second.release();
    }
  });

  it('aborts queued requests when the client disconnect signal fires', async () => {
    const config = parseGatewayConfigFromRaw({
      upstreamConcurrency: {
        enabled: true,
        maxInFlightPerProvider: 1,
        queueTimeoutMs: 1000
      }
    });
    const provider = createProviderConfig('openai-main');
    const first = await acquireProviderConcurrencySlot(config, 'openai', provider);
    expect(first.ok).toBe(true);
    if (!first.ok) {
      return;
    }

    const controller = new AbortController();
    const queued = acquireProviderConcurrencySlot(config, 'openai', provider, controller.signal);
    controller.abort(new Error('client disconnected'));
    const result = await queued;

    expect(result).toMatchObject({
      ok: false,
      status: 499,
      aborted: true,
      message: 'Client connection closed before acquiring provider concurrency slot.',
      details: {
        provider: 'openai',
        providerName: 'openai-main',
        maxInFlight: 1,
        queueTimeoutMs: 1000
      }
    });

    first.release();
    const next = await acquireProviderConcurrencySlot(config, 'openai', provider);
    expect(next.ok).toBe(true);
    if (next.ok) {
      next.release();
    }
  });

  it('uses Redis storage to enforce provider concurrency across instances', async () => {
    const store = installFakeRedisConcurrencyStore();
    const config = parseGatewayConfigFromRaw({
      upstreamConcurrency: {
        enabled: true,
        maxInFlightPerProvider: 1,
        queueTimeoutMs: 1,
        storage: {
          type: 'redis',
          url: 'redis://127.0.0.1:6379/0',
          keyPrefix: 'test:upstream-concurrency',
          leaseTtlMs: 1000,
          pollIntervalMs: 1
        }
      }
    });
    const provider = createProviderConfig('openai-main');
    const first = await acquireProviderConcurrencySlot(config, 'openai', provider);
    expect(first.ok).toBe(true);
    if (!first.ok) {
      return;
    }

    const second = await acquireProviderConcurrencySlot(config, 'openai', provider);
    expect(second).toMatchObject({
      ok: false,
      status: 429,
      message: 'Provider upstream concurrency limit exceeded.'
    });
    first.release();
    await delay(1);

    const third = await acquireProviderConcurrencySlot(config, 'openai', provider);
    expect(third.ok).toBe(true);
    if (third.ok) {
      third.release();
    }
    expect(Array.from(store.counts.values()).every((count) => count <= 0)).toBe(true);
  });

  it('renews Redis provider concurrency leases while a slot is held', async () => {
    vi.useFakeTimers();
    const commands: string[][] = [];
    installFakeRedisConcurrencyStore((args) => {
      commands.push(args);
    });
    const config = parseGatewayConfigFromRaw({
      upstreamConcurrency: {
        enabled: true,
        maxInFlightPerProvider: 1,
        queueTimeoutMs: 1,
        storage: {
          type: 'redis',
          url: 'redis://127.0.0.1:6379/0',
          keyPrefix: 'test:upstream-concurrency',
          leaseTtlMs: 20,
          pollIntervalMs: 1
        }
      }
    });
    const provider = createProviderConfig('openai-main');
    const first = await acquireProviderConcurrencySlot(config, 'openai', provider);
    expect(first.ok).toBe(true);
    if (!first.ok) {
      return;
    }

    await vi.advanceTimersByTimeAsync(10);

    const renewalsBeforeRelease = commands.filter(
      (args) => args[0] === 'EVAL' && (args[1] || '').includes('ZSCORE')
    );
    expect(renewalsBeforeRelease.length).toBeGreaterThan(0);
    expect(renewalsBeforeRelease[0]?.[6]).toBe('20');

    first.release();
    const renewalCountAtRelease = commands.filter(
      (args) => args[0] === 'EVAL' && (args[1] || '').includes('ZSCORE')
    ).length;
    await vi.advanceTimersByTimeAsync(40);

    expect(commands.filter(
      (args) => args[0] === 'EVAL' && (args[1] || '').includes('ZSCORE')
    )).toHaveLength(renewalCountAtRelease);
  });

  it('does not let a stale Redis release remove a newer provider concurrency lease', async () => {
    const store = installFakeRedisConcurrencyStore();
    const config = parseGatewayConfigFromRaw({
      upstreamConcurrency: {
        enabled: true,
        maxInFlightPerProvider: 1,
        queueTimeoutMs: 0,
        storage: {
          type: 'redis',
          url: 'redis://127.0.0.1:6379/0',
          keyPrefix: 'test:upstream-concurrency',
          leaseTtlMs: 1000,
          pollIntervalMs: 1
        }
      }
    });
    const provider = createProviderConfig('openai-main');
    const first = await acquireProviderConcurrencySlot(config, 'openai', provider);
    expect(first.ok).toBe(true);
    if (!first.ok) {
      return;
    }

    for (const slots of store.slots.values()) {
      slots.clear();
    }
    store.refreshCounts();
    const second = await acquireProviderConcurrencySlot(config, 'openai', provider);
    expect(second.ok).toBe(true);
    if (!second.ok) {
      return;
    }

    first.release();
    await Promise.resolve();

    const third = await acquireProviderConcurrencySlot(config, 'openai', provider);
    expect(third).toMatchObject({
      ok: false,
      status: 429,
      message: 'Provider upstream concurrency limit exceeded.'
    });

    second.release();
    await Promise.resolve();
    const fourth = await acquireProviderConcurrencySlot(config, 'openai', provider);
    expect(fourth.ok).toBe(true);
    if (fourth.ok) {
      fourth.release();
    }
  });
});

function installFakeRedisConcurrencyStore(onCommand?: (args: string[]) => void): {
  counts: Map<string, number>;
  slots: Map<string, Map<string, number>>;
  refreshCounts: () => void;
} {
  const counts = new Map<string, number>();
  const slots = new Map<string, Map<string, number>>();
  const getSlots = (key: string) => {
    let current = slots.get(key);
    if (!current) {
      current = new Map<string, number>();
      slots.set(key, current);
    }
    return current;
  };
  const refreshCounts = () => {
    for (const [key, current] of slots) {
      counts.set(key, current.size);
    }
  };
  const pruneExpired = (current: Map<string, number>, now: number) => {
    for (const [token, expiresAt] of current) {
      if (expiresAt <= now) {
        current.delete(token);
      }
    }
  };
  setProviderConcurrencyRedisCommandExecutorForTests(async (_storage, args) => {
    onCommand?.(args);
    if (args[0] !== 'EVAL') {
      throw new Error(`Unexpected Redis command in test: ${args.join(' ')}`);
    }
    const script = args[1] || '';
    const key = args[3] || '';
    const current = getSlots(key);
    if (script.includes('ZADD') && script.includes('ZCARD')) {
      const token = args[4] || '';
      const now = Number(args[5]) || Date.now();
      const ttlMs = Number(args[6]) || 1000;
      const limit = Number(args[7]) || 1;
      pruneExpired(current, now);
      if (current.size < limit) {
        current.set(token, now + ttlMs);
        refreshCounts();
        return [1, current.size, now + ttlMs];
      }
      refreshCounts();
      return [0, current.size, 0];
    }
    if (script.includes('ZSCORE')) {
      const token = args[4] || '';
      const now = Number(args[5]) || Date.now();
      const ttlMs = Number(args[6]) || 1000;
      pruneExpired(current, now);
      if (!current.has(token)) {
        refreshCounts();
        return 0;
      }
      current.set(token, now + ttlMs);
      refreshCounts();
      return 1;
    }
    if (script.includes('ZREM')) {
      const token = args[4] || '';
      const removed = current.delete(token) ? 1 : 0;
      refreshCounts();
      return removed;
    }
    throw new Error(`Unexpected Redis script in test: ${script}`);
  });
  return {
    counts,
    slots,
    refreshCounts
  };
}

function createProviderConfig(name: string): ProviderConfig {
  return {
    name,
    type: 'openai_responses',
    models: ['gpt-test'],
    extraHeaders: {
      default: {},
      byModel: {}
    },
    extraBody: {
      default: {},
      byModel: {}
    },
    billing: {
      byModel: {}
    }
  };
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
