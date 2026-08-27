import { afterEach, describe, expect, it } from 'vitest';
import type { GatewayConfig, ProviderConfig } from '../types';
import { applyHealthAwareRouting, type HealthAwareProviderRoute } from './health-routing';
import {
  closeProviderHealthStore,
  setProviderHealthRedisCommandExecutorForTests
} from './provider-health';

describe('applyHealthAwareRouting', () => {
  afterEach(async () => {
    await closeProviderHealthStore();
  });

  it('skips unavailable providers when a usable alternative exists', async () => {
    const openai = createProviderConfig('openai-main', 'openai_responses', {
      status: 'down',
      available: false
    });
    const anthropic = createProviderConfig('anthropic-main', 'anthropic_messages', {
      status: 'healthy',
      available: true
    });

    const routes = await applyHealthAwareRouting(
      [
        { provider: 'openai', providerConfig: openai },
        { provider: 'anthropic', providerConfig: anthropic }
      ],
      createConfig([openai, anthropic])
    );

    expect(routes.map(formatRoute)).toEqual(['anthropic-main']);
  });

  it('keeps explicit candidates when every provider is unavailable', async () => {
    const openai = createProviderConfig('openai-main', 'openai_responses', {
      status: 'down',
      available: false
    });
    const anthropic = createProviderConfig('anthropic-main', 'anthropic_messages', {
      status: 'down',
      available: false
    });

    const routes = await applyHealthAwareRouting(
      [
        { provider: 'openai', providerConfig: openai },
        { provider: 'anthropic', providerConfig: anthropic }
      ],
      createConfig([openai, anthropic])
    );

    expect(routes.map(formatRoute)).toEqual(['openai-main', 'anthropic-main']);
  });

  it('prefers healthier and lower-latency providers', async () => {
    const slowHealthy = createProviderConfig('slow-openai', 'openai_responses', {
      status: 'healthy',
      available: true,
      latencyMs: 300
    });
    const fastHealthy = createProviderConfig('fast-openai', 'openai_responses', {
      status: 'healthy',
      available: true,
      latencyMs: 50
    });
    const degraded = createProviderConfig('degraded-openai', 'openai_responses', {
      status: 'degraded',
      available: true,
      latencyMs: 10
    });

    const routes = await applyHealthAwareRouting(
      [
        { provider: 'openai', providerConfig: degraded },
        { provider: 'openai', providerConfig: slowHealthy },
        { provider: 'openai', providerConfig: fastHealthy }
      ],
      createConfig([degraded, slowHealthy, fastHealthy])
    );

    expect(routes.map(formatRoute)).toEqual([
      'fast-openai',
      'slow-openai',
      'degraded-openai'
    ]);
  });

  it('uses redis provider health state when configured', async () => {
    setProviderHealthRedisCommandExecutorForTests(async (_storage, args) => {
      if (args[0] !== 'MGET') {
        throw new Error(`Unexpected Redis provider health command: ${args.join(' ')}`);
      }
      return args.slice(1).map((_, index) => JSON.stringify(index === 0
        ? {
            status: 'down',
            available: false,
            latencyMs: 200,
            checkedAt: '2026-08-25T00:00:00.000Z'
          }
        : {
            status: 'healthy',
            available: true,
            latencyMs: 20,
            checkedAt: '2026-08-25T00:00:00.000Z'
          }));
    });
    const openai = createProviderConfig('openai-main', 'openai_responses', {
      status: 'healthy',
      available: true
    });
    const anthropic = createProviderConfig('anthropic-main', 'anthropic_messages', {
      status: 'healthy',
      available: true
    });

    const routes = await applyHealthAwareRouting(
      [
        { provider: 'openai', providerConfig: openai },
        { provider: 'anthropic', providerConfig: anthropic }
      ],
      createConfig([openai, anthropic], {
        type: 'redis',
        url: 'redis://redis.example:6379/0',
        keyPrefix: 'test:provider-health',
        connectTimeoutMs: 100,
        commandTimeoutMs: 100,
        stateTtlMs: 300000
      })
    );

    expect(routes.map(formatRoute)).toEqual(['anthropic-main']);
  });

  it('falls back to base provider Redis health for scheduled credential routes', async () => {
    let requestedKeys: string[] = [];
    setProviderHealthRedisCommandExecutorForTests(async (_storage, args) => {
      if (args[0] !== 'MGET') {
        throw new Error(`Unexpected Redis provider health command: ${args.join(' ')}`);
      }
      requestedKeys = args.slice(1);
      return requestedKeys.map((_, index) => {
        if (index === 1) {
          return JSON.stringify({
            status: 'down',
            available: false,
            latencyMs: 200,
            checkedAt: '2026-08-25T00:00:00.000Z'
          });
        }
        if (index === 3) {
          return JSON.stringify({
            status: 'healthy',
            available: true,
            latencyMs: 20,
            checkedAt: '2026-08-25T00:00:00.000Z'
          });
        }
        return null;
      });
    });
    const openaiBase = createProviderConfig('openai-main', 'openai_responses', {
      status: 'healthy',
      available: true
    });
    const anthropicBase = createProviderConfig('anthropic-main', 'anthropic_messages', {
      status: 'healthy',
      available: true
    });
    const openaiCredential = createScheduledCredentialConfig(openaiBase, 'primary');
    const anthropicCredential = createScheduledCredentialConfig(anthropicBase, 'primary');

    const routes = await applyHealthAwareRouting(
      [
        { provider: 'openai', providerConfig: openaiCredential },
        { provider: 'anthropic', providerConfig: anthropicCredential }
      ],
      createConfig([openaiBase, anthropicBase], {
        type: 'redis',
        url: 'redis://redis.example:6379/0',
        keyPrefix: 'test:provider-health',
        connectTimeoutMs: 100,
        commandTimeoutMs: 100,
        stateTtlMs: 300000
      })
    );

    expect(requestedKeys).toHaveLength(4);
    expect(routes.map(formatRoute)).toEqual(['anthropic-main::credential:primary']);
    expect(openaiCredential.health?.status).toBe('down');
    expect(anthropicCredential.health?.status).toBe('healthy');
  });
});

function formatRoute(route: HealthAwareProviderRoute): string {
  return route.providerConfig?.name || route.provider;
}

function createConfig(
  providers: ProviderConfig[],
  storage: GatewayConfig['providerHealthCheck']['storage'] = { type: 'memory' }
): GatewayConfig {
  return {
    providers,
    providerHealthCheck: {
      enabled: false,
      intervalMs: 60000,
      timeoutMs: 5000,
      initialDelayMs: 0,
      storage
    },
    healthAwareRouting: {
      enabled: true,
      skipUnavailable: true,
      unhealthyStatuses: ['down'],
      preferHealthy: true,
      preferLowerLatency: true
    }
  } as GatewayConfig;
}

function createProviderConfig(
  name: string,
  type: ProviderConfig['type'],
  health: ProviderConfig['health']
): ProviderConfig {
  return {
    name,
    type,
    models: ['test-model'],
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
    health
  };
}

function createScheduledCredentialConfig(
  base: ProviderConfig,
  credentialId: string
): ProviderConfig {
  return {
    ...base,
    name: `${base.name}::credential:${credentialId}`,
    credentialId,
    credentialSourceProviderName: base.name,
    health: {
      status: 'healthy',
      available: true
    }
  };
}
