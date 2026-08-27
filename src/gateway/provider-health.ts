import { createHash } from 'node:crypto';
import { SimpleRedisClient, type RedisReply } from '../redis-client';
import type {
  GatewayConfig,
  ProviderConfig,
  ProviderHealthCheckRedisStorageConfig,
  ProviderHealthCheckStorageConfig,
  ProviderHealthConfig,
  ProviderHealthStatus
} from '../types';

export function recordProviderHealthResponse(
  providerConfig: ProviderConfig | undefined,
  statusCode: number,
  latencyMs: number,
  checkedAt = new Date(),
  storage?: ProviderHealthCheckStorageConfig
): void {
  if (!providerConfig) {
    return;
  }

  const status = classifyProviderResponseStatus(statusCode);
  const health = updateProviderHealth(providerConfig, {
    status,
    available: status !== 'down',
    latencyMs,
    checkedAt
  });
  writeProviderHealthToStore(storage, providerConfig, health);
}

export function recordProviderHealthFailure(
  providerConfig: ProviderConfig | undefined,
  latencyMs: number,
  checkedAt = new Date(),
  storage?: ProviderHealthCheckStorageConfig
): void {
  if (!providerConfig) {
    return;
  }

  const health = updateProviderHealth(providerConfig, {
    status: 'down',
    available: false,
    latencyMs,
    checkedAt
  });
  writeProviderHealthToStore(storage, providerConfig, health);
}

const redisProviderHealthClients = new Map<string, SimpleRedisClient>();
let redisProviderHealthCommandExecutorForTests:
  | ((storage: ProviderHealthCheckRedisStorageConfig, args: string[]) => Promise<RedisReply>)
  | undefined;

const redisWriteProviderHealthScript = `
local existingRaw = redis.call('GET', KEYS[1])
if existingRaw then
  local decoded, existing = pcall(cjson.decode, existingRaw)
  if decoded and type(existing) == 'table' then
    local existingCheckedAtMs = tonumber(existing.checkedAtMs)
    if existingCheckedAtMs and existingCheckedAtMs >= tonumber(ARGV[2]) then
      redis.call('PEXPIRE', KEYS[1], ARGV[3])
      return 0
    end
    local existingHealth = existing.health
    local existingCheckedAt = existing.checkedAt
    if type(existingHealth) == 'table' then
      existingCheckedAt = existingHealth.checkedAt
    end
    if not existingCheckedAtMs and type(existingCheckedAt) == 'string' and existingCheckedAt >= ARGV[4] then
      redis.call('PEXPIRE', KEYS[1], ARGV[3])
      return 0
    end
  end
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
return 1
`;

export function setProviderHealthRedisCommandExecutorForTests(
  executor:
    | ((storage: ProviderHealthCheckRedisStorageConfig, args: string[]) => Promise<RedisReply>)
    | undefined
): void {
  redisProviderHealthCommandExecutorForTests = executor;
}

export async function closeProviderHealthStore(): Promise<void> {
  const clients = Array.from(redisProviderHealthClients.values());
  redisProviderHealthClients.clear();
  redisProviderHealthCommandExecutorForTests = undefined;
  await Promise.allSettled(clients.map((client) => client.close()));
}

export async function hydrateProviderHealthFromStore(
  config: GatewayConfig,
  providerConfigs: ProviderConfig[]
): Promise<void> {
  const storage = config.providerHealthCheck?.storage;
  if (storage?.type !== 'redis' || providerConfigs.length === 0) {
    return;
  }

  const healthKeysByProvider = providerConfigs.map((providerConfig) => ({
    providerConfig,
    primaryKey: buildRedisProviderHealthKey(storage, providerConfig),
    fallbackKey: buildRedisProviderHealthBaseKey(storage, providerConfig)
  }));
  const keySet = new Set<string>();
  for (const item of healthKeysByProvider) {
    keySet.add(item.primaryKey);
    keySet.add(item.fallbackKey);
  }

  const keys = Array.from(keySet);
  const healthByKey = new Map<string, ProviderHealthConfig>();

  try {
    const reply = await commandRedisProviderHealth(storage, ['MGET', ...keys]);
    if (!Array.isArray(reply)) {
      return;
    }
    for (let index = 0; index < keys.length; index += 1) {
      const health = parseStoredProviderHealth(reply[index]);
      if (!health) {
        continue;
      }

      healthByKey.set(keys[index], health);
    }

    for (const item of healthKeysByProvider) {
      const health = healthByKey.get(item.primaryKey) || healthByKey.get(item.fallbackKey);
      if (!health) {
        continue;
      }
      mergeStoredProviderHealth(item.providerConfig, health);
    }
  } catch {
    // Shared health is an optimization for better routing; stale local health remains usable.
  }
}

function mergeStoredProviderHealth(
  providerConfig: ProviderConfig,
  health: ProviderHealthConfig
): void {
  const current = providerConfig.health;
  const currentCheckedAtMs = parseCheckedAtMs(current?.checkedAt);
  const storedCheckedAtMs = parseCheckedAtMs(health.checkedAt);
  if (
    currentCheckedAtMs !== undefined &&
    (storedCheckedAtMs === undefined || storedCheckedAtMs <= currentCheckedAtMs)
  ) {
    return;
  }
  const merged: ProviderHealthConfig = {
    ...(current || { status: health.status }),
    status: health.status
  };

  if (current?.priority !== undefined) {
    merged.priority = current.priority;
  } else if (health.priority !== undefined) {
    merged.priority = health.priority;
  }
  if (health.available !== undefined) {
    merged.available = health.available;
  }
  if (health.latencyMs !== undefined) {
    merged.latencyMs = health.latencyMs;
  }
  if (health.checkedAt !== undefined) {
    merged.checkedAt = health.checkedAt;
  }
  providerConfig.health = merged;
}

function classifyProviderResponseStatus(statusCode: number): ProviderHealthStatus {
  if (!Number.isFinite(statusCode)) {
    return 'unknown';
  }

  if (statusCode === 401 || statusCode === 403) {
    return 'down';
  }

  if (statusCode === 408 || statusCode === 429 || statusCode >= 500) {
    return 'degraded';
  }

  return 'healthy';
}

function updateProviderHealth(
  providerConfig: ProviderConfig,
  next: {
    status: ProviderHealthStatus;
    available: boolean;
    latencyMs: number;
    checkedAt: Date;
  }
): ProviderHealthConfig {
  providerConfig.health = {
    ...providerConfig.health,
    status: next.status,
    available: next.available,
    latencyMs: normalizeLatencyMs(next.latencyMs),
    checkedAt: next.checkedAt.toISOString()
  };
  return providerConfig.health;
}

function normalizeLatencyMs(value: number): number {
  if (!Number.isFinite(value) || value < 0) {
    return 0;
  }

  return Math.round(value);
}

function writeProviderHealthToStore(
  storage: ProviderHealthCheckStorageConfig | undefined,
  providerConfig: ProviderConfig,
  health: ProviderHealthConfig
): void {
  if (storage?.type !== 'redis') {
    return;
  }

  const checkedAtMs = parseCheckedAtMs(health.checkedAt) ?? Date.now();
  void commandRedisProviderHealth(storage, [
    'EVAL',
    redisWriteProviderHealthScript,
    '1',
    buildRedisProviderHealthKey(storage, providerConfig),
    JSON.stringify({ checkedAtMs, health }),
    String(checkedAtMs),
    String(normalizePositiveInteger(storage.stateTtlMs, 300000)),
    health.checkedAt || new Date(checkedAtMs).toISOString()
  ]).catch(() => {
    // Health writes are best-effort; request success/failure handling should not fail on Redis.
  });
}

async function commandRedisProviderHealth(
  storage: ProviderHealthCheckRedisStorageConfig,
  args: string[]
): Promise<RedisReply> {
  if (redisProviderHealthCommandExecutorForTests) {
    return redisProviderHealthCommandExecutorForTests(storage, args);
  }

  return getRedisProviderHealthClient(storage).command(args);
}

function getRedisProviderHealthClient(storage: ProviderHealthCheckRedisStorageConfig): SimpleRedisClient {
  const cacheKey = JSON.stringify({
    url: storage.url || 'redis://127.0.0.1:6379/0',
    keyPrefix: storage.keyPrefix || 'next-ai:gateway:provider-health',
    connectTimeoutMs: normalizePositiveInteger(storage.connectTimeoutMs, 1000),
    commandTimeoutMs: normalizePositiveInteger(storage.commandTimeoutMs, 1000)
  });
  const existing = redisProviderHealthClients.get(cacheKey);
  if (existing) {
    return existing;
  }

  const client = new SimpleRedisClient({
    url: storage.url || 'redis://127.0.0.1:6379/0',
    connectTimeoutMs: normalizePositiveInteger(storage.connectTimeoutMs, 1000),
    commandTimeoutMs: normalizePositiveInteger(storage.commandTimeoutMs, 1000),
    errorPrefix: 'Redis provider health'
  });
  redisProviderHealthClients.set(cacheKey, client);
  return client;
}

function buildRedisProviderHealthKey(
  storage: ProviderHealthCheckRedisStorageConfig,
  providerConfig: ProviderConfig
): string {
  const prefix = (storage.keyPrefix || 'next-ai:gateway:provider-health').replace(/:+$/, '') ||
    'next-ai:gateway:provider-health';
  return `${prefix}:${createHash('sha256').update(providerHealthKey(providerConfig)).digest('hex')}`;
}

function buildRedisProviderHealthBaseKey(
  storage: ProviderHealthCheckRedisStorageConfig,
  providerConfig: ProviderConfig
): string {
  const prefix = (storage.keyPrefix || 'next-ai:gateway:provider-health').replace(/:+$/, '') ||
    'next-ai:gateway:provider-health';
  return `${prefix}:${createHash('sha256').update(providerHealthBaseKey(providerConfig)).digest('hex')}`;
}

function providerHealthKey(providerConfig: ProviderConfig): string {
  return [
    providerConfig.type,
    providerConfig.credentialSourceProviderName || providerConfig.name,
    providerConfig.credentialId || 'default'
  ].join(':');
}

function providerHealthBaseKey(providerConfig: ProviderConfig): string {
  return [
    providerConfig.type,
    providerConfig.credentialSourceProviderName || providerConfig.name,
    'default'
  ].join(':');
}

function parseStoredProviderHealth(reply: RedisReply | undefined): ProviderHealthConfig | undefined {
  if (typeof reply !== 'string' || !reply) {
    return undefined;
  }

  try {
    const parsed = JSON.parse(reply) as {
      health?: Partial<ProviderHealthConfig>;
    } & Partial<ProviderHealthConfig>;
    const raw = parsed?.health && typeof parsed.health === 'object'
      ? parsed.health
      : parsed;
    if (!raw || !isProviderHealthStatus(raw.status)) {
      return undefined;
    }
    return {
      status: raw.status,
      available: typeof raw.available === 'boolean' ? raw.available : undefined,
      priority: normalizeOptionalNumber(raw.priority),
      latencyMs: normalizeOptionalNumber(raw.latencyMs),
      checkedAt: typeof raw.checkedAt === 'string' ? raw.checkedAt : undefined
    };
  } catch {
    return undefined;
  }
}

function parseCheckedAtMs(value: string | undefined): number | undefined {
  if (!value) {
    return undefined;
  }
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function isProviderHealthStatus(value: unknown): value is ProviderHealthStatus {
  return value === 'healthy' || value === 'degraded' || value === 'unknown' || value === 'down';
}

function normalizeOptionalNumber(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined;
}

function normalizePositiveInteger(value: number, fallback: number): number {
  if (!Number.isFinite(value) || value <= 0) {
    return fallback;
  }

  return Math.trunc(value);
}
