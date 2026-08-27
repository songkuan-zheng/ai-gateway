import type { GatewayConfig, ProviderHealthStatus } from '../types';
import { SimpleRedisClient } from '../redis-client';
import type { GatewayRuntimePluginHealthSummary } from './runtime';

export interface GatewayReadinessProvider {
  name: string;
  status: ProviderHealthStatus;
  available: boolean;
  checkedAt?: string;
}

export interface GatewayReadinessSnapshot {
  ready: boolean;
  status: 'ready' | 'not_ready';
  reasons: string[];
  providers: GatewayReadinessProvider[];
  dependencies: GatewayReadinessDependency[];
  plugins: GatewayRuntimePluginHealthSummary;
}

export type GatewayReadinessDependencyName =
  | 'precheck'
  | 'idempotency'
  | 'upstream_concurrency'
  | 'upstream_circuit_breaker';

export interface GatewayReadinessDependency {
  name: GatewayReadinessDependencyName;
  type: 'redis';
  ready: boolean;
}

interface GatewayReadinessRedisStorage {
  type: 'redis';
  url: string;
  connectTimeoutMs: number;
  commandTimeoutMs: number;
}

export interface GatewayReadinessOptions {
  redisProbe?: (storage: GatewayReadinessRedisStorage) => Promise<boolean>;
}

interface RequiredRedisDependency {
  name: GatewayReadinessDependencyName;
  storage: GatewayReadinessRedisStorage;
}

export async function buildGatewayReadinessSnapshot(
  config: GatewayConfig,
  plugins: GatewayRuntimePluginHealthSummary,
  options: GatewayReadinessOptions = {}
): Promise<GatewayReadinessSnapshot> {
  const providers = (config.providers || []).map((provider): GatewayReadinessProvider => {
    const status = provider.health?.status || 'unknown';
    return {
      name: provider.name,
      status,
      available: provider.health?.available !== false && status !== 'down',
      checkedAt: provider.health?.checkedAt
    };
  });
  const dependencies = await probeRequiredRedisDependencies(
    collectRequiredRedisDependencies(config),
    options.redisProbe || probeRedis
  );
  const reasons: string[] = [];

  if (plugins.unhealthy > 0) {
    reasons.push('plugin_unhealthy');
  }
  if (providers.length === 0) {
    reasons.push('no_provider_configured');
  } else if (!providers.some((provider) => provider.available)) {
    reasons.push('no_available_provider');
  }
  for (const dependency of dependencies) {
    if (!dependency.ready) {
      reasons.push(`dependency_unavailable:${dependency.name}`);
    }
  }

  const ready = reasons.length === 0;
  return {
    ready,
    status: ready ? 'ready' : 'not_ready',
    reasons,
    providers,
    dependencies,
    plugins
  };
}

function collectRequiredRedisDependencies(config: GatewayConfig): RequiredRedisDependency[] {
  const dependencies: RequiredRedisDependency[] = [];
  if (config.precheck?.enabled && config.precheck.storage?.type === 'redis') {
    dependencies.push({ name: 'precheck', storage: config.precheck.storage });
  }
  if (config.idempotency?.enabled && config.idempotency.storage?.type === 'redis') {
    dependencies.push({ name: 'idempotency', storage: config.idempotency.storage });
  }
  if (
    config.upstreamConcurrency?.enabled &&
    config.upstreamConcurrency.storage?.type === 'redis'
  ) {
    dependencies.push({
      name: 'upstream_concurrency',
      storage: config.upstreamConcurrency.storage
    });
  }
  if (
    config.upstreamCircuitBreaker?.enabled &&
    config.upstreamCircuitBreaker.storage?.type === 'redis'
  ) {
    dependencies.push({
      name: 'upstream_circuit_breaker',
      storage: config.upstreamCircuitBreaker.storage
    });
  }
  return dependencies;
}

async function probeRequiredRedisDependencies(
  required: RequiredRedisDependency[],
  redisProbe: (storage: GatewayReadinessRedisStorage) => Promise<boolean>
): Promise<GatewayReadinessDependency[]> {
  const probes = new Map<string, Promise<boolean>>();
  for (const dependency of required) {
    const key = JSON.stringify({
      url: dependency.storage.url,
      connectTimeoutMs: dependency.storage.connectTimeoutMs,
      commandTimeoutMs: dependency.storage.commandTimeoutMs
    });
    if (!probes.has(key)) {
      probes.set(key, safelyProbeRedis(redisProbe, dependency.storage));
    }
  }

  return Promise.all(required.map(async (dependency): Promise<GatewayReadinessDependency> => {
    const key = JSON.stringify({
      url: dependency.storage.url,
      connectTimeoutMs: dependency.storage.connectTimeoutMs,
      commandTimeoutMs: dependency.storage.commandTimeoutMs
    });
    return {
      name: dependency.name,
      type: 'redis',
      ready: await probes.get(key)!
    };
  }));
}

async function safelyProbeRedis(
  redisProbe: (storage: GatewayReadinessRedisStorage) => Promise<boolean>,
  storage: GatewayReadinessRedisStorage
): Promise<boolean> {
  try {
    return await redisProbe(storage);
  } catch {
    return false;
  }
}

async function probeRedis(storage: GatewayReadinessRedisStorage): Promise<boolean> {
  const client = new SimpleRedisClient({
    url: storage.url,
    connectTimeoutMs: normalizeProbeTimeout(storage.connectTimeoutMs),
    commandTimeoutMs: normalizeProbeTimeout(storage.commandTimeoutMs),
    errorPrefix: 'Redis readiness check'
  });
  let overallTimeout: NodeJS.Timeout | undefined;
  try {
    const reply = await Promise.race([
      client.command(['PING']),
      new Promise<undefined>((resolve) => {
        overallTimeout = setTimeout(() => {
          void client.close();
          resolve(undefined);
        }, 3000);
        overallTimeout.unref?.();
      })
    ]);
    return typeof reply === 'string' && reply.toUpperCase() === 'PONG';
  } catch {
    return false;
  } finally {
    if (overallTimeout) {
      clearTimeout(overallTimeout);
    }
    await client.close();
  }
}

function normalizeProbeTimeout(value: number): number {
  return Number.isFinite(value) && value > 0 ? Math.min(Math.trunc(value), 2000) : 1000;
}
