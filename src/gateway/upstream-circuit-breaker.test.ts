import { afterEach, describe, expect, it, vi } from 'vitest';
import { parseGatewayConfigFromRaw } from '../config';
import type { ProviderConfig } from '../types';
import {
  checkProviderCircuitBreaker,
  closeProviderCircuitBreakerStore,
  recordProviderCircuitBreakerFailure,
  recordProviderCircuitBreakerResponse,
  resetProviderCircuitBreakerForTests,
  setProviderCircuitBreakerRedisCommandExecutorForTests
} from './upstream-circuit-breaker';

describe('gateway upstream circuit breaker', () => {
  afterEach(async () => {
    vi.useRealTimers();
    resetProviderCircuitBreakerForTests();
    await closeProviderCircuitBreakerStore();
  });

  it('opens after consecutive failures and closes after cooldown', async () => {
    vi.useFakeTimers();
    const config = parseGatewayConfigFromRaw({
      upstreamCircuitBreaker: {
        enabled: true,
        failureThreshold: 2,
        cooldownMs: 1000,
        failureStatusCodes: [500]
      }
    });
    const provider = createProviderConfig('openai-main');

    expect((await checkProviderCircuitBreaker(config, 'openai', provider)).ok).toBe(true);
    await recordProviderCircuitBreakerFailure(config, 'openai', provider);
    expect((await checkProviderCircuitBreaker(config, 'openai', provider)).ok).toBe(true);

    await recordProviderCircuitBreakerResponse(config, 'openai', provider, 500);
    const open = await checkProviderCircuitBreaker(config, 'openai', provider);
    expect(open).toMatchObject({
      ok: false,
      status: 503,
      message: 'Provider upstream circuit breaker is open.',
      details: {
        provider: 'openai',
        providerName: 'openai-main',
        failureThreshold: 2,
        cooldownMs: 1000
      }
    });

    vi.advanceTimersByTime(1001);
    expect((await checkProviderCircuitBreaker(config, 'openai', provider)).ok).toBe(true);
  });

  it('resets consecutive failures after a non-failure response', async () => {
    const config = parseGatewayConfigFromRaw({
      upstreamCircuitBreaker: {
        enabled: true,
        failureThreshold: 2,
        cooldownMs: 1000,
        failureStatusCodes: [500]
      }
    });
    const provider = createProviderConfig('openai-main');

    await recordProviderCircuitBreakerResponse(config, 'openai', provider, 500);
    await recordProviderCircuitBreakerResponse(config, 'openai', provider, 200);
    await recordProviderCircuitBreakerResponse(config, 'openai', provider, 500);

    expect((await checkProviderCircuitBreaker(config, 'openai', provider)).ok).toBe(true);
  });

  it('keeps named providers isolated', async () => {
    const config = parseGatewayConfigFromRaw({
      upstreamCircuitBreaker: {
        enabled: true,
        failureThreshold: 1,
        cooldownMs: 1000,
        failureStatusCodes: [500]
      }
    });

    await recordProviderCircuitBreakerResponse(config, 'openai', createProviderConfig('openai-a'), 500);

    expect((await checkProviderCircuitBreaker(config, 'openai', createProviderConfig('openai-a'))).ok).toBe(false);
    expect((await checkProviderCircuitBreaker(config, 'openai', createProviderConfig('openai-b'))).ok).toBe(true);
  });

  it('shares open state through redis storage', async () => {
    vi.useFakeTimers();
    installFakeRedisCircuitBreakerExecutor();
    const configA = parseGatewayConfigFromRaw({
      upstreamCircuitBreaker: {
        enabled: true,
        failureThreshold: 2,
        cooldownMs: 1000,
        failureStatusCodes: [500],
        storage: {
          type: 'redis',
          url: 'redis://redis.example:6379/0',
          keyPrefix: 'test:circuit'
        }
      }
    });
    const configB = parseGatewayConfigFromRaw({
      upstreamCircuitBreaker: {
        enabled: true,
        failureThreshold: 2,
        cooldownMs: 1000,
        failureStatusCodes: [500],
        storage: {
          type: 'redis',
          url: 'redis://redis.example:6379/0',
          keyPrefix: 'test:circuit'
        }
      }
    });
    const provider = createProviderConfig('openai-main');

    await recordProviderCircuitBreakerResponse(configA, 'openai', provider, 500);
    expect((await checkProviderCircuitBreaker(configB, 'openai', provider)).ok).toBe(true);

    await recordProviderCircuitBreakerResponse(configB, 'openai', provider, 500);
    expect(await checkProviderCircuitBreaker(configA, 'openai', provider)).toMatchObject({
      ok: false,
      status: 503,
      details: {
        provider: 'openai',
        providerName: 'openai-main',
        failureThreshold: 2,
        cooldownMs: 1000
      }
    });

    vi.advanceTimersByTime(1001);
    expect((await checkProviderCircuitBreaker(configB, 'openai', provider)).ok).toBe(true);
  });
});

function installFakeRedisCircuitBreakerExecutor(): void {
  const states = new Map<string, { consecutiveFailures: number; openedUntil: number }>();
  setProviderCircuitBreakerRedisCommandExecutorForTests(async (_storage, args) => {
    const script = args[1] || '';
    const key = args[3] || '';
    if (script.includes('return {1, 0, 0}')) {
      const now = Number(args[4]);
      const state = states.get(key);
      if (!state) {
        return [1, 0, 0];
      }
      if (state.openedUntil > now) {
        return [0, state.consecutiveFailures, state.openedUntil];
      }
      if (state.openedUntil > 0) {
        states.delete(key);
        return [1, 0, 0];
      }
      return [1, state.consecutiveFailures, 0];
    }

    if (script.includes('failures = failures + 1')) {
      const now = Number(args[4]);
      const threshold = Number(args[5]);
      const cooldownMs = Number(args[6]);
      const current = states.get(key);
      if (current && current.openedUntil > now) {
        return [current.consecutiveFailures, current.openedUntil];
      }
      const consecutiveFailures = (current?.openedUntil && current.openedUntil <= now
        ? 0
        : current?.consecutiveFailures || 0) + 1;
      const openedUntil = consecutiveFailures >= threshold ? now + cooldownMs : 0;
      states.set(key, { consecutiveFailures, openedUntil });
      return [consecutiveFailures, openedUntil];
    }

    if (script.trim().startsWith("redis.call('DEL', KEYS[1])")) {
      states.delete(key);
      return 1;
    }

    throw new Error(`Unexpected Redis circuit breaker command: ${args.join(' ')}`);
  });
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
