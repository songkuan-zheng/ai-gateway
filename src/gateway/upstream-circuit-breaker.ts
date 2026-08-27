import { createHash } from 'node:crypto';
import { SimpleRedisClient, type RedisReply } from '../redis-client';
import type {
  GatewayConfig,
  GatewayUpstreamCircuitBreakerRedisStorageConfig,
  Provider,
  ProviderConfig
} from '../types';

interface ProviderCircuitBreakerState {
  consecutiveFailures: number;
  openedUntil?: number;
}

export type ProviderCircuitBreakerCheckResult =
  | { ok: true }
  | {
      ok: false;
      status: 503;
      message: string;
      details: {
        provider: Provider;
        providerName?: string;
        failureThreshold: number;
        cooldownMs: number;
        openedUntil: string;
        error?: string;
      };
    };

const circuitBreakerStates = new Map<string, ProviderCircuitBreakerState>();
const redisCircuitBreakerClients = new Map<string, SimpleRedisClient>();
let redisCircuitBreakerCommandExecutorForTests:
  | ((storage: GatewayUpstreamCircuitBreakerRedisStorageConfig, args: string[]) => Promise<RedisReply>)
  | undefined;

const redisCheckCircuitBreakerScript = `
local raw = redis.call('GET', KEYS[1])
if not raw then
  return {1, 0, 0}
end
local ok, state = pcall(cjson.decode, raw)
if not ok or type(state) ~= 'table' then
  redis.call('DEL', KEYS[1])
  return {1, 0, 0}
end
local now = tonumber(ARGV[1])
local openedUntil = tonumber(state.openedUntil or 0)
if openedUntil > now then
  return {0, tonumber(state.consecutiveFailures or 0), openedUntil}
end
if openedUntil > 0 then
  redis.call('DEL', KEYS[1])
  return {1, 0, 0}
end
return {1, tonumber(state.consecutiveFailures or 0), 0}
`;

const redisRecordCircuitBreakerFailureScript = `
local raw = redis.call('GET', KEYS[1])
local now = tonumber(ARGV[1])
local threshold = tonumber(ARGV[2])
local cooldownMs = tonumber(ARGV[3])
local ttlMs = tonumber(ARGV[4])
local failures = 0
local openedUntil = 0
if raw then
  local ok, state = pcall(cjson.decode, raw)
  if ok and type(state) == 'table' then
    openedUntil = tonumber(state.openedUntil or 0)
    if openedUntil > now then
      redis.call('PEXPIRE', KEYS[1], ttlMs)
      return {tonumber(state.consecutiveFailures or threshold), openedUntil}
    end
    if openedUntil <= now then
      openedUntil = 0
    end
    failures = tonumber(state.consecutiveFailures or 0)
  end
end
failures = failures + 1
if failures >= threshold then
  openedUntil = now + cooldownMs
end
redis.call('SET', KEYS[1], cjson.encode({ consecutiveFailures = failures, openedUntil = openedUntil }), 'PX', ttlMs)
return {failures, openedUntil}
`;

const redisRecordCircuitBreakerSuccessScript = `
redis.call('DEL', KEYS[1])
return 1
`;

export async function checkProviderCircuitBreaker(
  config: GatewayConfig,
  provider: Provider,
  providerConfig?: ProviderConfig
): Promise<ProviderCircuitBreakerCheckResult> {
  const breaker = config.upstreamCircuitBreaker;
  if (!breaker?.enabled) {
    return { ok: true };
  }

  if (breaker.storage?.type === 'redis') {
    return checkRedisProviderCircuitBreaker(config, breaker.storage, provider, providerConfig);
  }

  const now = Date.now();
  const state = getProviderCircuitBreakerState(provider, providerConfig);
  if (state.openedUntil && state.openedUntil > now) {
    return {
      ok: false,
      status: 503,
      message: 'Provider upstream circuit breaker is open.',
      details: {
        provider,
        providerName: providerConfig?.name,
        failureThreshold: normalizePositiveInteger(breaker.failureThreshold, 1),
        cooldownMs: normalizePositiveInteger(breaker.cooldownMs, 1),
        openedUntil: new Date(state.openedUntil).toISOString()
      }
    };
  }

  if (state.openedUntil && state.openedUntil <= now) {
    state.openedUntil = undefined;
    state.consecutiveFailures = 0;
  }

  return { ok: true };
}

export function recordProviderCircuitBreakerResponse(
  config: GatewayConfig,
  provider: Provider,
  providerConfig: ProviderConfig | undefined,
  statusCode: number
): Promise<void> {
  const breaker = config.upstreamCircuitBreaker;
  if (!breaker?.enabled) {
    return Promise.resolve();
  }

  if (breaker.failureStatusCodes.includes(statusCode)) {
    return recordProviderCircuitBreakerFailure(config, provider, providerConfig);
  }

  return recordProviderCircuitBreakerSuccess(config, provider, providerConfig);
}

export async function recordProviderCircuitBreakerFailure(
  config: GatewayConfig,
  provider: Provider,
  providerConfig?: ProviderConfig
): Promise<void> {
  const breaker = config.upstreamCircuitBreaker;
  if (!breaker?.enabled) {
    return;
  }

  if (breaker.storage?.type === 'redis') {
    await recordRedisProviderCircuitBreakerFailure(config, breaker.storage, provider, providerConfig);
    return;
  }

  const state = getProviderCircuitBreakerState(provider, providerConfig);
  state.consecutiveFailures += 1;
  if (state.consecutiveFailures >= normalizePositiveInteger(breaker.failureThreshold, 1)) {
    state.openedUntil = Date.now() + normalizePositiveInteger(breaker.cooldownMs, 1);
  }
}

export async function recordProviderCircuitBreakerSuccess(
  config: GatewayConfig,
  provider: Provider,
  providerConfig?: ProviderConfig
): Promise<void> {
  if (!config.upstreamCircuitBreaker?.enabled) {
    return;
  }

  const breaker = config.upstreamCircuitBreaker;
  if (breaker.storage?.type === 'redis') {
    await recordRedisProviderCircuitBreakerSuccess(breaker.storage, provider, providerConfig);
    return;
  }

  const state = getProviderCircuitBreakerState(provider, providerConfig);
  state.consecutiveFailures = 0;
  state.openedUntil = undefined;
}

export function resetProviderCircuitBreakerForTests(): void {
  circuitBreakerStates.clear();
  redisCircuitBreakerCommandExecutorForTests = undefined;
}

export function setProviderCircuitBreakerRedisCommandExecutorForTests(
  executor:
    | ((storage: GatewayUpstreamCircuitBreakerRedisStorageConfig, args: string[]) => Promise<RedisReply>)
    | undefined
): void {
  redisCircuitBreakerCommandExecutorForTests = executor;
}

export async function closeProviderCircuitBreakerStore(): Promise<void> {
  const clients = Array.from(redisCircuitBreakerClients.values());
  redisCircuitBreakerClients.clear();
  await Promise.allSettled(clients.map((client) => client.close()));
}

async function checkRedisProviderCircuitBreaker(
  config: GatewayConfig,
  storage: GatewayUpstreamCircuitBreakerRedisStorageConfig,
  provider: Provider,
  providerConfig?: ProviderConfig
): Promise<ProviderCircuitBreakerCheckResult> {
  const breaker = config.upstreamCircuitBreaker;
  const failureThreshold = normalizePositiveInteger(breaker.failureThreshold, 1);
  const cooldownMs = normalizePositiveInteger(breaker.cooldownMs, 1);
  try {
    const result = parseRedisCircuitBreakerStateReply(
      await commandRedisCircuitBreaker(storage, [
        'EVAL',
        redisCheckCircuitBreakerScript,
        '1',
        buildRedisCircuitBreakerKey(storage, provider, providerConfig),
        String(Date.now())
      ])
    );
    if (!result) {
      throw new Error('Redis circuit breaker check returned an invalid response.');
    }
    if (result.ok) {
      return { ok: true };
    }

    return {
      ok: false,
      status: 503,
      message: 'Provider upstream circuit breaker is open.',
      details: {
        provider,
        providerName: providerConfig?.name,
        failureThreshold,
        cooldownMs,
        openedUntil: new Date(result.openedUntil).toISOString()
      }
    };
  } catch (error) {
    return {
      ok: false,
      status: 503,
      message: 'Provider upstream circuit breaker store is unavailable.',
      details: {
        provider,
        providerName: providerConfig?.name,
        failureThreshold,
        cooldownMs,
        openedUntil: new Date(Date.now() + cooldownMs).toISOString(),
        error: error instanceof Error ? error.message : String(error)
      }
    };
  }
}

async function recordRedisProviderCircuitBreakerFailure(
  config: GatewayConfig,
  storage: GatewayUpstreamCircuitBreakerRedisStorageConfig,
  provider: Provider,
  providerConfig?: ProviderConfig
): Promise<void> {
  try {
    await commandRedisCircuitBreaker(storage, [
      'EVAL',
      redisRecordCircuitBreakerFailureScript,
      '1',
      buildRedisCircuitBreakerKey(storage, provider, providerConfig),
      String(Date.now()),
      String(normalizePositiveInteger(config.upstreamCircuitBreaker.failureThreshold, 1)),
      String(normalizePositiveInteger(config.upstreamCircuitBreaker.cooldownMs, 1)),
      String(resolveCircuitBreakerStateTtlMs(storage, config.upstreamCircuitBreaker.cooldownMs))
    ]);
  } catch {
    // Recording failures must not turn an already-failed upstream attempt into a gateway failure.
  }
}

async function recordRedisProviderCircuitBreakerSuccess(
  storage: GatewayUpstreamCircuitBreakerRedisStorageConfig,
  provider: Provider,
  providerConfig?: ProviderConfig
): Promise<void> {
  try {
    await commandRedisCircuitBreaker(storage, [
      'EVAL',
      redisRecordCircuitBreakerSuccessScript,
      '1',
      buildRedisCircuitBreakerKey(storage, provider, providerConfig)
    ]);
  } catch {
    // Success recording is best-effort; the next check will expire stale open windows.
  }
}

function parseRedisCircuitBreakerStateReply(
  reply: RedisReply
): { ok: true } | { ok: false; openedUntil: number } | undefined {
  if (!Array.isArray(reply) || reply.length < 3) {
    return undefined;
  }
  if (Number(reply[0]) === 1) {
    return { ok: true };
  }
  const openedUntil = Number(reply[2]);
  if (!Number.isFinite(openedUntil) || openedUntil <= 0) {
    return undefined;
  }

  return { ok: false, openedUntil };
}

async function commandRedisCircuitBreaker(
  storage: GatewayUpstreamCircuitBreakerRedisStorageConfig,
  args: string[]
): Promise<RedisReply> {
  if (redisCircuitBreakerCommandExecutorForTests) {
    return redisCircuitBreakerCommandExecutorForTests(storage, args);
  }

  return getRedisCircuitBreakerClient(storage).command(args);
}

function getRedisCircuitBreakerClient(
  storage: GatewayUpstreamCircuitBreakerRedisStorageConfig
): SimpleRedisClient {
  const cacheKey = JSON.stringify({
    url: storage.url || 'redis://127.0.0.1:6379/0',
    keyPrefix: storage.keyPrefix || 'next-ai:gateway:upstream-circuit-breaker',
    connectTimeoutMs: normalizePositiveInteger(storage.connectTimeoutMs, 1000),
    commandTimeoutMs: normalizePositiveInteger(storage.commandTimeoutMs, 1000)
  });
  const existing = redisCircuitBreakerClients.get(cacheKey);
  if (existing) {
    return existing;
  }

  const client = new SimpleRedisClient({
    url: storage.url || 'redis://127.0.0.1:6379/0',
    connectTimeoutMs: normalizePositiveInteger(storage.connectTimeoutMs, 1000),
    commandTimeoutMs: normalizePositiveInteger(storage.commandTimeoutMs, 1000),
    errorPrefix: 'Redis upstream circuit breaker'
  });
  redisCircuitBreakerClients.set(cacheKey, client);
  return client;
}

function buildRedisCircuitBreakerKey(
  storage: GatewayUpstreamCircuitBreakerRedisStorageConfig,
  provider: Provider,
  providerConfig?: ProviderConfig
): string {
  const prefix = (storage.keyPrefix || 'next-ai:gateway:upstream-circuit-breaker').replace(/:+$/, '') ||
    'next-ai:gateway:upstream-circuit-breaker';
  return `${prefix}:${createHash('sha256').update(providerCircuitBreakerKey(provider, providerConfig)).digest('hex')}`;
}

function resolveCircuitBreakerStateTtlMs(
  storage: GatewayUpstreamCircuitBreakerRedisStorageConfig,
  cooldownMs: number
): number {
  return Math.max(
    normalizePositiveInteger(storage.stateTtlMs, 86400000),
    normalizePositiveInteger(cooldownMs, 1) * 2
  );
}

function getProviderCircuitBreakerState(
  provider: Provider,
  providerConfig?: ProviderConfig
): ProviderCircuitBreakerState {
  const key = providerCircuitBreakerKey(provider, providerConfig);
  let state = circuitBreakerStates.get(key);
  if (!state) {
    state = {
      consecutiveFailures: 0
    };
    circuitBreakerStates.set(key, state);
  }

  return state;
}

function providerCircuitBreakerKey(provider: Provider, providerConfig?: ProviderConfig): string {
  return providerConfig?.name ? `${provider}:${providerConfig.name}` : provider;
}

function normalizePositiveInteger(value: number, fallback: number): number {
  if (!Number.isFinite(value) || value <= 0) {
    return fallback;
  }

  return Math.trunc(value);
}
