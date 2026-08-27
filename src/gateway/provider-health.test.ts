import { afterEach, describe, expect, it } from 'vitest';
import type { GatewayConfig, ProviderConfig } from '../types';
import {
  closeProviderHealthStore,
  hydrateProviderHealthFromStore,
  recordProviderHealthFailure,
  recordProviderHealthResponse,
  setProviderHealthRedisCommandExecutorForTests
} from './provider-health';

describe('provider health recording', () => {
  afterEach(async () => {
    await closeProviderHealthStore();
  });

  it('marks reachable non-transient responses healthy', () => {
    const provider = createProviderConfig();

    recordProviderHealthResponse(provider, 400, 12.4, new Date('2026-06-08T00:00:00.000Z'));

    expect(provider.health).toEqual({
      status: 'healthy',
      available: true,
      priority: 3,
      latencyMs: 12,
      checkedAt: '2026-06-08T00:00:00.000Z'
    });
  });

  it('marks rate limits and server errors degraded while keeping provider available', () => {
    const provider = createProviderConfig();

    recordProviderHealthResponse(provider, 429, 28.7, new Date('2026-06-08T00:00:01.000Z'));
    expect(provider.health).toMatchObject({
      status: 'degraded',
      available: true,
      priority: 3,
      latencyMs: 29
    });

    recordProviderHealthResponse(provider, 503, 44.2, new Date('2026-06-08T00:00:02.000Z'));
    expect(provider.health).toMatchObject({
      status: 'degraded',
      available: true,
      priority: 3,
      latencyMs: 44,
      checkedAt: '2026-06-08T00:00:02.000Z'
    });
  });

  it('marks authentication failures down and unavailable', () => {
    const provider = createProviderConfig();

    recordProviderHealthResponse(provider, 401, 9, new Date('2026-06-08T00:00:02.500Z'));

    expect(provider.health).toMatchObject({
      status: 'down',
      available: false,
      checkedAt: '2026-06-08T00:00:02.500Z'
    });
  });

  it('marks connection failures down and unavailable', () => {
    const provider = createProviderConfig();

    recordProviderHealthFailure(provider, 101.8, new Date('2026-06-08T00:00:03.000Z'));

    expect(provider.health).toEqual({
      status: 'down',
      available: false,
      priority: 3,
      latencyMs: 102,
      checkedAt: '2026-06-08T00:00:03.000Z'
    });
  });

  it('writes provider health to redis storage when configured', async () => {
    const commands: string[][] = [];
    setProviderHealthRedisCommandExecutorForTests(async (_storage, args) => {
      commands.push(args);
      return 'OK';
    });
    const provider = createProviderConfig();

    recordProviderHealthResponse(
      provider,
      200,
      17.4,
      new Date('2026-06-08T00:00:04.000Z'),
      {
        type: 'redis',
        url: 'redis://redis.example:6379/0',
        keyPrefix: 'test:provider-health',
        connectTimeoutMs: 100,
        commandTimeoutMs: 100,
        stateTtlMs: 300000
      }
    );
    await Promise.resolve();

    expect(commands).toHaveLength(1);
    expect(commands[0]?.[0]).toBe('EVAL');
    expect(commands[0]?.[2]).toBe('1');
    expect(commands[0]?.[3]).toMatch(/^test:provider-health:/);
    expect(JSON.parse(commands[0]?.[4] || '{}')).toMatchObject({
      checkedAtMs: Date.parse('2026-06-08T00:00:04.000Z'),
      health: {
        status: 'healthy',
        available: true,
        priority: 3,
        latencyMs: 17,
        checkedAt: '2026-06-08T00:00:04.000Z'
      }
    });
    expect(commands[0]?.slice(5)).toEqual([
      String(Date.parse('2026-06-08T00:00:04.000Z')),
      '300000',
      '2026-06-08T00:00:04.000Z'
    ]);
  });

  it('does not overwrite newer local health with stale shared state', async () => {
    setProviderHealthRedisCommandExecutorForTests(async (_storage, args) => {
      if (args[0] !== 'MGET') {
        return null;
      }
      return args.slice(1).map(() => JSON.stringify({
        checkedAtMs: Date.parse('2026-06-08T00:00:04.000Z'),
        health: {
          status: 'down',
          available: false,
          latencyMs: 100,
          checkedAt: '2026-06-08T00:00:04.000Z'
        }
      }));
    });
    const provider = createProviderConfig();
    provider.health = {
      status: 'healthy',
      available: true,
      priority: 3,
      latencyMs: 12,
      checkedAt: '2026-06-08T00:00:05.000Z'
    };
    const config = {
      providerHealthCheck: {
        storage: {
          type: 'redis',
          url: 'redis://redis.example:6379/0',
          keyPrefix: 'test:provider-health',
          connectTimeoutMs: 100,
          commandTimeoutMs: 100,
          stateTtlMs: 300000
        }
      }
    } as GatewayConfig;

    await hydrateProviderHealthFromStore(config, [provider]);

    expect(provider.health).toMatchObject({
      status: 'healthy',
      available: true,
      latencyMs: 12,
      checkedAt: '2026-06-08T00:00:05.000Z'
    });
  });
});

function createProviderConfig(): ProviderConfig {
  return {
    name: 'openai-main',
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
    },
    health: {
      status: 'unknown',
      priority: 3
    }
  };
}
