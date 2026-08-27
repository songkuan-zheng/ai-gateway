import { describe, expect, it } from 'vitest';
import type { GatewayConfig } from '../types';
import type { GatewayRuntimePluginHealthSummary } from './runtime';
import { buildGatewayReadinessSnapshot } from './readiness';

const healthyPlugins: GatewayRuntimePluginHealthSummary = {
  status: 'ok',
  total: 0,
  degraded: 0,
  unhealthy: 0
};

describe('gateway readiness', () => {
  it('is ready when at least one configured provider is available', async () => {
    const config = createConfig([
      { name: 'down', health: { status: 'down', available: false } },
      { name: 'healthy', health: { status: 'healthy', available: true } }
    ]);

    await expect(buildGatewayReadinessSnapshot(config, healthyPlugins)).resolves.toMatchObject({
      ready: true,
      status: 'ready',
      reasons: [],
      dependencies: []
    });
  });

  it('is not ready when every configured provider is unavailable', async () => {
    const config = createConfig([
      { name: 'down', health: { status: 'down', available: false } }
    ]);

    await expect(buildGatewayReadinessSnapshot(config, healthyPlugins)).resolves.toMatchObject({
      ready: false,
      status: 'not_ready',
      reasons: ['no_available_provider']
    });
  });

  it('is not ready when no provider is configured', async () => {
    await expect(buildGatewayReadinessSnapshot(createConfig([]), healthyPlugins)).resolves.toMatchObject({
      ready: false,
      status: 'not_ready',
      reasons: ['no_provider_configured']
    });
  });

  it('is not ready when a plugin reports unhealthy', async () => {
    const plugins: GatewayRuntimePluginHealthSummary = {
      status: 'unhealthy',
      total: 1,
      degraded: 0,
      unhealthy: 1
    };

    await expect(buildGatewayReadinessSnapshot(createConfig([
      { name: 'healthy', health: { status: 'healthy', available: true } }
    ]), plugins)).resolves.toMatchObject({
      ready: false,
      reasons: ['plugin_unhealthy']
    });
  });

  it('checks enabled fail-closed Redis dependencies and reuses a shared probe', async () => {
    const config = createConfig([
      { name: 'healthy', health: { status: 'healthy', available: true } }
    ]);
    const storage = {
      type: 'redis' as const,
      url: 'redis://redis:6379/0',
      keyPrefix: 'gateway:test',
      connectTimeoutMs: 100,
      commandTimeoutMs: 100
    };
    config.precheck = {
      enabled: true,
      storage,
      rateLimit: { enabled: false },
      quota: { enabled: false },
      budget: { enabled: false },
      estimation: { charsPerToken: 4, defaultMaxOutputTokens: 1024 }
    } as GatewayConfig['precheck'];
    config.idempotency = {
      enabled: true,
      storage,
      headerName: 'idempotency-key'
    } as GatewayConfig['idempotency'];
    let probes = 0;

    await expect(buildGatewayReadinessSnapshot(config, healthyPlugins, {
      redisProbe: async () => {
        probes += 1;
        return false;
      }
    })).resolves.toMatchObject({
      ready: false,
      reasons: [
        'dependency_unavailable:precheck',
        'dependency_unavailable:idempotency'
      ],
      dependencies: [
        { name: 'precheck', type: 'redis', ready: false },
        { name: 'idempotency', type: 'redis', ready: false }
      ]
    });
    expect(probes).toBe(1);
  });
});

function createConfig(
  providers: Array<{ name: string; health: { status: 'healthy' | 'down'; available: boolean } }>
): GatewayConfig {
  return {
    providers: providers.map((provider) => ({
      ...provider,
      type: 'openai_responses',
      models: ['gpt-test'],
      extraHeaders: { default: {}, byModel: {} },
      extraBody: { default: {}, byModel: {} },
      billing: { byModel: {} }
    }))
  } as GatewayConfig;
}
