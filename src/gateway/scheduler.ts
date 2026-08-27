import { createHash } from 'node:crypto';
import type { FastifyReply, FastifyRequest } from 'fastify';
import { SimpleRedisClient, type RedisReply } from '../redis-client';
import type {
  GatewayConfig,
  GatewaySchedulingRedisStorageConfig,
  Provider,
  ProviderCacheConfig,
  ProviderConfig,
  ProviderCredentialConfig,
  ProviderCredentialLimitConfig,
  StandardUsage
} from '../types';
import { findDefaultProviderConfig, readHeader } from '../utils';

export interface GatewaySchedulingRoute {
  provider: Provider;
  providerConfig?: ProviderConfig;
}

interface CredentialRuntimeState {
  cooldownUntil?: number;
  consecutiveFailures: number;
  currentWeight: number;
  counters: Map<string, WindowCounter>;
}

interface WindowCounter {
  expiresAt: number;
  value: number;
}

interface CredentialCandidate<T extends GatewaySchedulingRoute> {
  route: T;
  providerConfig: ProviderConfig;
  credential?: ProviderCredentialConfig;
  cacheScopeKey: string;
  cacheAffinityHit: boolean;
  blocked: boolean;
  cooldown: boolean;
  utilization: number;
  priority: number;
  weight: number;
  originalIndex: number;
}

interface CacheAffinityBinding {
  key: string;
  providerName: string;
  model?: string;
  credentialId?: string;
  cacheScopeKey: string;
  expiresAt: number;
  lastHitAt: number;
}

interface GatewaySchedulingRequestEstimate {
  body: unknown;
  imageCount?: number;
}

const credentialStates = new Map<string, CredentialRuntimeState>();
const cacheAffinityBindings = new Map<string, CacheAffinityBinding>();
const redisSchedulingClients = new Map<string, SimpleRedisClient>();
let requestEstimates = new WeakMap<FastifyRequest, GatewaySchedulingRequestEstimate>();
let redisSchedulingCommandExecutorForTests:
  | ((storage: GatewaySchedulingRedisStorageConfig, args: string[]) => Promise<RedisReply>)
  | undefined;
const defaultProviderCacheConfig: ProviderCacheConfig = {
  enabled: true,
  scope: 'credential_model',
  ttlMs: 600000,
  minPrefixTokens: 1024,
  maxWaitMs: 3000
};

export async function applyGatewayScheduling<T extends GatewaySchedulingRoute>(
  routes: T[],
  input: {
    config: GatewayConfig;
    request: FastifyRequest;
    requestModel?: string;
  }
): Promise<T[]> {
  const scheduling = input.config.scheduling;
  if (!scheduling?.enabled || routes.length === 0) {
    return routes;
  }

  await hydrateGatewaySchedulingState(routes, input);

  const candidates: CredentialCandidate<T>[] = [];
  for (const [index, route] of routes.entries()) {
    candidates.push(...expandSchedulingRoute(route, index, input));
  }

  if (candidates.length === 0) {
    return routes;
  }

  const rankedCandidates = rankCredentialCandidates(candidates);
  writeGatewaySchedulingCandidateStatesToStore(scheduling.storage, rankedCandidates);
  const ranked = rankedCandidates.map((candidate) => candidate.route);
  const maxAttempts = resolveSchedulingMaxAttempts(input.config);
  return maxAttempts > 0 ? ranked.slice(0, maxAttempts) : ranked;
}

export function recordGatewaySchedulingResponse(input: {
  config: GatewayConfig;
  request: FastifyRequest;
  providerConfig?: ProviderConfig;
  model?: string;
  statusCode?: number;
  error?: boolean;
  usage?: StandardUsage;
}): void {
  const scheduling = input.config.scheduling;
  if (!scheduling?.enabled || !input.providerConfig?.credentialId) {
    return;
  }

  const providerConfig = input.providerConfig;
  const state = getCredentialRuntimeState(providerConfig);
  const statusCode = input.statusCode;
  if (statusCode !== undefined && statusCode >= 200 && statusCode < 400) {
    state.consecutiveFailures = 0;
    state.cooldownUntil = undefined;
    if (providerConfig.credentialLimits) {
      incrementCredentialCounters(providerConfig, estimateRequestUsage(input.request, input.config));
    }
    const binding = updateCacheAffinity(input.config, input.request, providerConfig, input.model, input.usage);
    writeGatewaySchedulingStateToStore(scheduling.storage, providerConfig, state);
    writeGatewaySchedulingCacheAffinityToStore(scheduling.storage, binding);
    return;
  }

  if (statusCode !== undefined && statusCode < 500 && statusCode !== 401 && statusCode !== 403 && statusCode !== 429) {
    state.consecutiveFailures = 0;
    state.cooldownUntil = undefined;
    writeGatewaySchedulingStateToStore(scheduling.storage, providerConfig, state);
    return;
  }

  if (input.error || statusCode === 401 || statusCode === 403 || statusCode === 429 || (statusCode !== undefined && statusCode >= 500)) {
    state.consecutiveFailures += 1;
    const cooldownMs = resolveCredentialCooldownMs(scheduling.credentialScheduler.cooldownMs, statusCode, input.error);
    state.cooldownUntil = Date.now() + cooldownMs;
    writeGatewaySchedulingStateToStore(scheduling.storage, providerConfig, state);
  }
}

export function recordGatewaySchedulingUsage(input: {
  config: GatewayConfig;
  request: FastifyRequest;
  providerConfig?: ProviderConfig;
  model?: string;
  usage?: StandardUsage;
}): void {
  if (!input.config.scheduling?.enabled || !input.providerConfig?.credentialId || !input.usage) {
    return;
  }

  const binding = updateCacheAffinity(input.config, input.request, input.providerConfig, input.model, input.usage);
  writeGatewaySchedulingCacheAffinityToStore(input.config.scheduling.storage, binding);
}

export function attachGatewaySchedulingHeaders(
  reply: FastifyReply,
  providerConfig?: ProviderConfig
): void {
  if (!providerConfig?.credentialId) {
    return;
  }

  reply.header('x-gateway-scheduled-provider-name', providerConfig.credentialSourceProviderName || providerConfig.name);
  reply.header('x-gateway-scheduled-credential-id', providerConfig.credentialId);
}

export function resolveGatewayScheduledCredential(
  providerConfig: ProviderConfig,
  credentialId: string | undefined
): ProviderConfig | undefined {
  if (!credentialId) {
    return providerConfig;
  }

  const credential = providerConfig.credentials?.find(
    (item) =>
      item.id === credentialId &&
      item.enabled !== false &&
      Boolean(item.apikey)
  );
  return credential ? providerConfigForCredential(providerConfig, credential) : undefined;
}

export function setGatewaySchedulingRequestEstimate(
  request: FastifyRequest,
  estimate: GatewaySchedulingRequestEstimate
): void {
  requestEstimates.set(request, estimate);
}

export function resetGatewaySchedulingStateForTests(): void {
  credentialStates.clear();
  cacheAffinityBindings.clear();
  requestEstimates = new WeakMap<FastifyRequest, GatewaySchedulingRequestEstimate>();
  redisSchedulingCommandExecutorForTests = undefined;
}

export function setGatewaySchedulingRedisCommandExecutorForTests(
  executor:
    | ((storage: GatewaySchedulingRedisStorageConfig, args: string[]) => Promise<RedisReply>)
    | undefined
): void {
  redisSchedulingCommandExecutorForTests = executor;
}

export async function closeGatewaySchedulingStore(): Promise<void> {
  const clients = Array.from(redisSchedulingClients.values());
  redisSchedulingClients.clear();
  await Promise.allSettled(clients.map((client) => client.close()));
}

async function hydrateGatewaySchedulingState<T extends GatewaySchedulingRoute>(
  routes: T[],
  input: {
    config: GatewayConfig;
    request: FastifyRequest;
    requestModel?: string;
  }
): Promise<void> {
  const storage = input.config.scheduling.storage;
  if (storage?.type !== 'redis') {
    return;
  }

  const stateProviderConfigs = collectSchedulingStateProviderConfigs(routes, input);
  const stateKeys = Array.from(
    new Set(stateProviderConfigs.map((providerConfig) => credentialStateKey(providerConfig)))
  );
  const cacheAffinityKey = buildCacheAffinityKey(input.config, input.request, input.requestModel);
  const redisStateKeys = stateKeys.map((key) => buildRedisSchedulingStateKey(storage, key));
  const redisCacheKey = cacheAffinityKey
    ? buildRedisSchedulingCacheAffinityKey(storage, cacheAffinityKey)
    : undefined;
  const redisKeys = redisCacheKey ? [...redisStateKeys, redisCacheKey] : redisStateKeys;
  if (redisKeys.length === 0) {
    return;
  }

  try {
    const reply = await commandRedisScheduling(storage, ['MGET', ...redisKeys]);
    if (!Array.isArray(reply)) {
      return;
    }
    for (let index = 0; index < stateKeys.length; index += 1) {
      const state = parseStoredCredentialRuntimeState(reply[index]);
      if (state) {
        credentialStates.set(stateKeys[index], state);
      }
    }

    if (cacheAffinityKey && redisCacheKey) {
      const binding = parseStoredCacheAffinityBinding(reply[stateKeys.length], cacheAffinityKey);
      if (binding) {
        cacheAffinityBindings.set(cacheAffinityKey, binding);
      }
    }
  } catch {
    // Scheduling Redis state is an optimization; local state remains available on Redis errors.
  }
}

function collectSchedulingStateProviderConfigs<T extends GatewaySchedulingRoute>(
  routes: T[],
  input: {
    config: GatewayConfig;
    request: FastifyRequest;
    requestModel?: string;
  }
): ProviderConfig[] {
  const providerConfigs: ProviderConfig[] = [];
  for (const route of routes) {
    const providerConfig = resolveRouteProviderConfig(input.config, route);
    if (!providerConfig) {
      continue;
    }
    const activeCredentials = input.config.scheduling.credentialScheduler.enabled
      ? (providerConfig.credentials || []).filter((credential) => credential.enabled !== false && Boolean(credential.apikey))
      : [];
    if (activeCredentials.length === 0 || providerConfig.credentialId) {
      providerConfigs.push(providerConfig);
      continue;
    }
    for (const credential of activeCredentials) {
      providerConfigs.push(providerConfigForCredential(providerConfig, credential));
    }
  }

  return providerConfigs;
}

function expandSchedulingRoute<T extends GatewaySchedulingRoute>(
  route: T,
  originalIndex: number,
  input: {
    config: GatewayConfig;
    request: FastifyRequest;
    requestModel?: string;
  }
): CredentialCandidate<T>[] {
  const providerConfig = resolveRouteProviderConfig(input.config, route);
  if (!providerConfig) {
    return [];
  }

  const activeCredentials = input.config.scheduling.credentialScheduler.enabled
    ? (providerConfig.credentials || []).filter((credential) => credential.enabled !== false && Boolean(credential.apikey))
    : [];
  if (activeCredentials.length === 0 || providerConfig.credentialId) {
    const cacheScopeKey = buildCacheScopeKey(providerConfig, undefined, input.requestModel, input.config);
    return [{
      route,
      providerConfig,
      cacheScopeKey,
      cacheAffinityHit: cacheAffinityMatches(input.config, input.request, cacheScopeKey),
      blocked: false,
      cooldown: false,
      utilization: 0,
      priority: originalIndex + 1,
      weight: 1,
      originalIndex
    }];
  }

  return activeCredentials.map((credential) => {
    const credentialProviderConfig = providerConfigForCredential(providerConfig, credential);
    const limitState = credentialLimitState(credentialProviderConfig, credential, input.request, input.config);
    const state = getCredentialRuntimeState(credentialProviderConfig);
    const cooldown = Boolean(state.cooldownUntil && state.cooldownUntil > Date.now());
    const cacheScopeKey = buildCacheScopeKey(
      credentialProviderConfig,
      credential,
      input.requestModel,
      input.config
    );

    return {
      route: {
        ...route,
        providerConfig: credentialProviderConfig
      },
      providerConfig: credentialProviderConfig,
      credential,
      cacheScopeKey,
      cacheAffinityHit: cacheAffinityMatches(input.config, input.request, cacheScopeKey),
      blocked: limitState.blocked,
      cooldown,
      utilization: limitState.utilization,
      priority: credential.priority,
      weight: Math.max(1, credential.weight || 1),
      originalIndex
    } as CredentialCandidate<T>;
  });
}

function rankCredentialCandidates<T extends GatewaySchedulingRoute>(
  candidates: CredentialCandidate<T>[]
): CredentialCandidate<T>[] {
  const usable = candidates.filter((candidate) => !candidate.blocked && !candidate.cooldown);
  const pool = usable.length > 0 ? usable : candidates;
  const selected = selectWeightedCandidate(pool);
  return [...pool].sort((left, right) => {
    if (left.cacheAffinityHit !== right.cacheAffinityHit) {
      return left.cacheAffinityHit ? -1 : 1;
    }
    if (left === selected) return -1;
    if (right === selected) return 1;
    if (left.blocked !== right.blocked) {
      return left.blocked ? 1 : -1;
    }
    if (left.cooldown !== right.cooldown) {
      return left.cooldown ? 1 : -1;
    }
    return (
      left.priority - right.priority ||
      left.utilization - right.utilization ||
      right.weight - left.weight ||
      left.originalIndex - right.originalIndex ||
      left.providerConfig.name.localeCompare(right.providerConfig.name)
    );
  });
}

function resolveSchedulingMaxAttempts(config: GatewayConfig): number {
  const fallback = config.scheduling.fallback;
  if (fallback.mode === 'off') {
    return 1;
  }

  return Math.max(1, Math.trunc(fallback.maxAttempts || 1));
}

function selectWeightedCandidate<T extends GatewaySchedulingRoute>(
  candidates: CredentialCandidate<T>[]
): CredentialCandidate<T> | undefined {
  const primaryPriority = Math.min(...candidates.map((candidate) => candidate.priority));
  const primary = candidates.filter((candidate) => candidate.priority === primaryPriority);
  if (primary.length === 0) {
    return undefined;
  }

  const totalWeight = primary.reduce((sum, candidate) => sum + candidate.weight, 0);
  let selected = primary[0];
  for (const candidate of primary) {
    const state = getCredentialRuntimeState(candidate.providerConfig);
    state.currentWeight += candidate.weight;
    const selectedState = getCredentialRuntimeState(selected.providerConfig);
    if (state.currentWeight > selectedState.currentWeight) {
      selected = candidate;
    }
  }

  getCredentialRuntimeState(selected.providerConfig).currentWeight -= totalWeight;
  return selected;
}

function providerConfigForCredential(
  providerConfig: ProviderConfig,
  credential: ProviderCredentialConfig
): ProviderConfig {
  return {
    ...providerConfig,
    name: `${providerConfig.name}::cred:${sanitizeCredentialId(credential.id)}`,
    apikey: credential.apikey,
    apiKeyEnv: credential.apiKeyEnv,
    cache: credential.cache || providerConfig.cache,
    credentialId: credential.id,
    credentialLimits: credential.limits,
    credentialSourceProviderName: providerConfig.credentialSourceProviderName || providerConfig.name,
    credentials: undefined
  };
}

function resolveRouteProviderConfig(
  config: GatewayConfig,
  route: GatewaySchedulingRoute
): ProviderConfig | undefined {
  if (route.providerConfig) {
    return route.providerConfig;
  }

  return findDefaultProviderConfig(config.providers, route.provider);
}

function credentialLimitState(
  providerConfig: ProviderConfig,
  credential: ProviderCredentialConfig,
  request: FastifyRequest,
  config: GatewayConfig
): { blocked: boolean; utilization: number } {
  const limits = credential.limits;
  if (!limits) {
    return { blocked: false, utilization: 0 };
  }

  const usage = estimateRequestUsage(request, config);
  const checks = credentialLimitChecks(limits, usage);
  if (checks.length === 0) {
    return { blocked: false, utilization: 0 };
  }

  const state = getCredentialRuntimeState(providerConfig);
  const now = Date.now();
  let blocked = false;
  let utilization = 0;
  for (const check of checks) {
    const counter = readWindowCounter(state, check.name, check.windowMs, now);
    blocked = blocked || counter.value + check.requested > check.limit;
    utilization = Math.max(utilization, (counter.value + check.requested) / check.limit);
  }

  return { blocked, utilization };
}

function credentialLimitChecks(
  limits: ProviderCredentialLimitConfig,
  usage: { imageCount: number; totalTokens: number }
): Array<{ name: string; limit: number; requested: number; windowMs: number }> {
  const checks: Array<{ name: string; limit: number; requested: number; windowMs: number }> = [];
  addLimitCheck(checks, 'rpm', limits.rpm, 1, 60000);
  addLimitCheck(checks, 'rpd', limits.rpd, 1, 86400000);
  addLimitCheck(checks, 'tpm', limits.tpm, usage.totalTokens, 60000);
  addLimitCheck(checks, 'tpd', limits.tpd, usage.totalTokens, 86400000);
  addLimitCheck(checks, 'ipm', limits.ipm, usage.imageCount, 60000);
  return checks;
}

function addLimitCheck(
  checks: Array<{ name: string; limit: number; requested: number; windowMs: number }>,
  name: string,
  limit: number | undefined,
  requested: number,
  windowMs: number
): void {
  if (!limit || limit <= 0) {
    return;
  }

  checks.push({ name, limit, requested, windowMs });
}

function incrementCredentialCounters(
  providerConfig: ProviderConfig,
  usage: { imageCount: number; totalTokens: number }
): void {
  const credential = findCredentialConfig(providerConfig);
  if (!credential?.limits) {
    return;
  }

  const state = getCredentialRuntimeState(providerConfig);
  const now = Date.now();
  for (const check of credentialLimitChecks(credential.limits, usage)) {
    readWindowCounter(state, check.name, check.windowMs, now).value += check.requested;
  }
}

function findCredentialConfig(providerConfig: ProviderConfig): ProviderCredentialConfig | undefined {
  if (!providerConfig.credentialId) {
    return undefined;
  }

  return {
    id: providerConfig.credentialId,
    apikey: providerConfig.apikey,
    apiKeyEnv: providerConfig.apiKeyEnv,
    enabled: true,
    priority: 1,
    weight: 1,
    limits: providerConfig.credentialLimits
  };
}

function readWindowCounter(
  state: CredentialRuntimeState,
  name: string,
  windowMs: number,
  now: number
): WindowCounter {
  const windowStart = Math.floor(now / windowMs) * windowMs;
  const key = `${name}:${windowMs}:${windowStart}`;
  const existing = state.counters.get(key);
  if (existing && existing.expiresAt > now) {
    return existing;
  }

  const next = { expiresAt: windowStart + windowMs * 2, value: 0 };
  state.counters.set(key, next);
  for (const [counterKey, counter] of state.counters) {
    if (counter.expiresAt <= now) {
      state.counters.delete(counterKey);
    }
  }
  return next;
}

function getCredentialRuntimeState(providerConfig: ProviderConfig): CredentialRuntimeState {
  const key = credentialStateKey(providerConfig);
  let state = credentialStates.get(key);
  if (!state) {
    state = {
      consecutiveFailures: 0,
      currentWeight: 0,
      counters: new Map()
    };
    credentialStates.set(key, state);
  }
  return state;
}

function writeGatewaySchedulingCandidateStatesToStore<T extends GatewaySchedulingRoute>(
  storage: GatewayConfig['scheduling']['storage'],
  candidates: CredentialCandidate<T>[]
): void {
  if (storage?.type !== 'redis') {
    return;
  }

  const providerConfigs = new Map<string, ProviderConfig>();
  for (const candidate of candidates) {
    providerConfigs.set(credentialStateKey(candidate.providerConfig), candidate.providerConfig);
  }
  for (const providerConfig of providerConfigs.values()) {
    writeGatewaySchedulingStateToStore(
      storage,
      providerConfig,
      getCredentialRuntimeState(providerConfig)
    );
  }
}

function writeGatewaySchedulingStateToStore(
  storage: GatewayConfig['scheduling']['storage'],
  providerConfig: ProviderConfig,
  state: CredentialRuntimeState
): void {
  if (storage?.type !== 'redis') {
    return;
  }

  const stateKey = credentialStateKey(providerConfig);
  void commandRedisScheduling(storage, [
    'SET',
    buildRedisSchedulingStateKey(storage, stateKey),
    JSON.stringify(serializeCredentialRuntimeState(state)),
    'PX',
    String(resolveSchedulingStateTtlMs(storage))
  ]).catch(() => {
    // Scheduling state is advisory; failed Redis writes should not affect the upstream call.
  });
}

function writeGatewaySchedulingCacheAffinityToStore(
  storage: GatewayConfig['scheduling']['storage'],
  binding: CacheAffinityBinding | undefined
): void {
  if (storage?.type !== 'redis' || !binding) {
    return;
  }

  void commandRedisScheduling(storage, [
    'SET',
    buildRedisSchedulingCacheAffinityKey(storage, binding.key),
    JSON.stringify(binding),
    'PX',
    String(Math.max(resolveSchedulingStateTtlMs(storage), Math.max(1, binding.expiresAt - Date.now())))
  ]).catch(() => {
    // Cache affinity is a preference, not a hard routing requirement.
  });
}

function credentialStateKey(providerConfig: ProviderConfig): string {
  return `${providerConfig.credentialSourceProviderName || providerConfig.name}:${providerConfig.credentialId || 'default'}`;
}

function serializeCredentialRuntimeState(
  state: CredentialRuntimeState
): {
  cooldownUntil?: number;
  consecutiveFailures: number;
  currentWeight: number;
  counters: Record<string, WindowCounter>;
} {
  const now = Date.now();
  const counters: Record<string, WindowCounter> = {};
  for (const [key, counter] of state.counters) {
    if (counter.expiresAt > now) {
      counters[key] = {
        expiresAt: counter.expiresAt,
        value: counter.value
      };
    }
  }

  return {
    cooldownUntil: state.cooldownUntil,
    consecutiveFailures: normalizeNonNegativeInteger(state.consecutiveFailures, 0),
    currentWeight: normalizeFiniteNumber(state.currentWeight, 0),
    counters
  };
}

function parseStoredCredentialRuntimeState(reply: RedisReply | undefined): CredentialRuntimeState | undefined {
  if (typeof reply !== 'string' || !reply) {
    return undefined;
  }

  try {
    const raw = JSON.parse(reply) as {
      cooldownUntil?: unknown;
      consecutiveFailures?: unknown;
      currentWeight?: unknown;
      counters?: unknown;
    };
    const now = Date.now();
    const counters = new Map<string, WindowCounter>();
    if (raw.counters && typeof raw.counters === 'object' && !Array.isArray(raw.counters)) {
      for (const [key, value] of Object.entries(raw.counters as Record<string, unknown>)) {
        if (!value || typeof value !== 'object') {
          continue;
        }
        const counter = value as Partial<WindowCounter>;
        const expiresAt = normalizeFiniteNumber(counter.expiresAt, 0);
        const count = normalizeFiniteNumber(counter.value, 0);
        if (expiresAt > now && count >= 0) {
          counters.set(key, {
            expiresAt,
            value: count
          });
        }
      }
    }

    return {
      cooldownUntil: normalizeOptionalFutureTimestamp(raw.cooldownUntil),
      consecutiveFailures: normalizeNonNegativeInteger(raw.consecutiveFailures, 0),
      currentWeight: normalizeFiniteNumber(raw.currentWeight, 0),
      counters
    };
  } catch {
    return undefined;
  }
}

function parseStoredCacheAffinityBinding(
  reply: RedisReply | undefined,
  expectedKey: string
): CacheAffinityBinding | undefined {
  if (typeof reply !== 'string' || !reply) {
    return undefined;
  }

  try {
    const raw = JSON.parse(reply) as Partial<CacheAffinityBinding>;
    if (
      raw.key !== expectedKey ||
      typeof raw.providerName !== 'string' ||
      typeof raw.cacheScopeKey !== 'string'
    ) {
      return undefined;
    }
    const expiresAt = normalizeFiniteNumber(raw.expiresAt, 0);
    if (expiresAt <= Date.now()) {
      return undefined;
    }

    return {
      key: raw.key,
      providerName: raw.providerName,
      model: typeof raw.model === 'string' ? raw.model : undefined,
      credentialId: typeof raw.credentialId === 'string' ? raw.credentialId : undefined,
      cacheScopeKey: raw.cacheScopeKey,
      expiresAt,
      lastHitAt: normalizeFiniteNumber(raw.lastHitAt, 0)
    };
  } catch {
    return undefined;
  }
}

async function commandRedisScheduling(
  storage: GatewaySchedulingRedisStorageConfig,
  args: string[]
): Promise<RedisReply> {
  if (redisSchedulingCommandExecutorForTests) {
    return redisSchedulingCommandExecutorForTests(storage, args);
  }

  return getRedisSchedulingClient(storage).command(args);
}

function getRedisSchedulingClient(storage: GatewaySchedulingRedisStorageConfig): SimpleRedisClient {
  const cacheKey = JSON.stringify({
    url: storage.url || 'redis://127.0.0.1:6379/0',
    keyPrefix: storage.keyPrefix || 'next-ai:gateway:scheduling',
    connectTimeoutMs: normalizePositiveInteger(storage.connectTimeoutMs, 1000),
    commandTimeoutMs: normalizePositiveInteger(storage.commandTimeoutMs, 1000)
  });
  const existing = redisSchedulingClients.get(cacheKey);
  if (existing) {
    return existing;
  }

  const client = new SimpleRedisClient({
    url: storage.url || 'redis://127.0.0.1:6379/0',
    connectTimeoutMs: normalizePositiveInteger(storage.connectTimeoutMs, 1000),
    commandTimeoutMs: normalizePositiveInteger(storage.commandTimeoutMs, 1000),
    errorPrefix: 'Redis gateway scheduling'
  });
  redisSchedulingClients.set(cacheKey, client);
  return client;
}

function buildRedisSchedulingStateKey(
  storage: GatewaySchedulingRedisStorageConfig,
  stateKey: string
): string {
  return `${redisSchedulingKeyPrefix(storage)}:state:${stableHash(stateKey)}`;
}

function buildRedisSchedulingCacheAffinityKey(
  storage: GatewaySchedulingRedisStorageConfig,
  affinityKey: string
): string {
  return `${redisSchedulingKeyPrefix(storage)}:cache:${stableHash(affinityKey)}`;
}

function redisSchedulingKeyPrefix(storage: GatewaySchedulingRedisStorageConfig): string {
  return (storage.keyPrefix || 'next-ai:gateway:scheduling').replace(/:+$/, '') ||
    'next-ai:gateway:scheduling';
}

function resolveSchedulingStateTtlMs(storage: GatewaySchedulingRedisStorageConfig): number {
  return normalizePositiveInteger(storage.stateTtlMs, 86400000);
}

function normalizeOptionalFutureTimestamp(value: unknown): number | undefined {
  const timestamp = normalizeFiniteNumber(value, 0);
  return timestamp > Date.now() ? timestamp : undefined;
}

function normalizeNonNegativeInteger(value: unknown, fallback: number): number {
  const parsed = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return fallback;
  }

  return Math.trunc(parsed);
}

function normalizeFiniteNumber(value: unknown, fallback: number): number {
  const parsed = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function normalizePositiveInteger(value: number, fallback: number): number {
  if (!Number.isFinite(value) || value <= 0) {
    return fallback;
  }

  return Math.trunc(value);
}

function estimateRequestUsage(
  request: FastifyRequest,
  config: GatewayConfig
): { imageCount: number; totalTokens: number } {
  const requestEstimate = requestEstimates.get(request);
  const body = requestEstimate?.body ?? request.body;
  const binary = binaryPayloadBytes(body);
  const estimatedCharacterCount = binary?.byteLength ?? stableStringify(body).length;
  const charsPerToken = Math.max(1, config.precheck?.estimation?.charsPerToken || 4);
  return {
    imageCount: requestEstimate?.imageCount ?? countImageMarkers(body),
    totalTokens: Math.max(1, Math.ceil(estimatedCharacterCount / charsPerToken))
  };
}

function countImageMarkers(value: unknown): number {
  if (binaryPayloadBytes(value)) {
    return 0;
  }
  if (Array.isArray(value)) {
    return value.reduce((sum, item) => sum + countImageMarkers(item), 0);
  }
  if (!value || typeof value !== 'object') {
    return 0;
  }

  let count = 0;
  for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
    if (key === 'image_url' || key === 'input_image' || key === 'source') {
      count += 1;
    }
    count += countImageMarkers(entry);
  }
  return count;
}

function updateCacheAffinity(
  config: GatewayConfig,
  request: FastifyRequest,
  providerConfig: ProviderConfig,
  model: string | undefined,
  usage: StandardUsage | undefined
): CacheAffinityBinding | undefined {
  if (!config.scheduling.cacheAffinity.enabled) {
    return undefined;
  }

  const providerCache = resolveProviderCacheConfig(config, providerConfig);
  if (!providerCache.enabled) {
    return undefined;
  }

  const estimated = estimateRequestUsage(request, config);
  const cacheUsageObserved = Boolean(
    (usage?.cache_read_tokens || 0) > 0 ||
      (usage?.cache_write_tokens || 0) > 0
  );
  if (!cacheUsageObserved && estimated.totalTokens < providerCache.minPrefixTokens) {
    return undefined;
  }

  const key = buildCacheAffinityKey(config, request, model);
  if (!key) {
    return undefined;
  }

  const now = Date.now();
  const binding = {
    key,
    providerName: providerConfig.credentialSourceProviderName || providerConfig.name,
    model,
    credentialId: providerConfig.credentialId,
    cacheScopeKey: buildCacheScopeKey(providerConfig, undefined, model, config),
    expiresAt: now + providerCache.ttlMs,
    lastHitAt: now
  };
  cacheAffinityBindings.set(key, binding);
  return binding;
}

function cacheAffinityMatches(
  config: GatewayConfig,
  request: FastifyRequest,
  cacheScopeKey: string
): boolean {
  if (!config.scheduling.cacheAffinity.enabled || !cacheScopeKey) {
    return false;
  }

  const key = buildCacheAffinityKey(config, request, undefined);
  if (!key) {
    return false;
  }

  const binding = cacheAffinityBindings.get(key);
  if (!binding) {
    return false;
  }

  if (binding.expiresAt <= Date.now()) {
    cacheAffinityBindings.delete(key);
    return false;
  }

  return binding.cacheScopeKey === cacheScopeKey;
}

function buildCacheAffinityKey(
  config: GatewayConfig,
  request: FastifyRequest,
  model: string | undefined
): string | undefined {
  const explicit = readHeader(request.headers['x-gateway-cache-affinity-key']);
  const identity = request.gatewayIdentity;
  const subject =
    explicit ||
    identity?.billingSubjectKey ||
    identity?.userId ||
    identity?.tenantId ||
    readHeader(request.headers['x-session-id']);
  if (!subject) {
    return undefined;
  }

  const prefixHash = stableHash({
    body: request.body
  });
  return stableHash({
    subject,
    prefixHash,
    version: 1,
    minPrefixTokens: config.scheduling.cacheAffinity.minPrefixTokens
  });
}

function buildCacheScopeKey(
  providerConfig: ProviderConfig,
  credential: ProviderCredentialConfig | undefined,
  model: string | undefined,
  config: GatewayConfig
): string {
  const cacheConfig = resolveProviderCacheConfig(config, providerConfig);
  const providerName = providerConfig.credentialSourceProviderName || providerConfig.name;
  const credentialId = credential?.id || providerConfig.credentialId || 'default';
  if (cacheConfig.scope === 'provider') {
    return providerName;
  }
  if (cacheConfig.scope === 'provider_model') {
    return `${providerName}:${model || '*'}`;
  }
  if (cacheConfig.scope === 'credential') {
    return `${providerName}:${credentialId}`;
  }
  return `${providerName}:${credentialId}:${model || '*'}`;
}

function resolveProviderCacheConfig(
  config: GatewayConfig,
  providerConfig: ProviderConfig
): ProviderCacheConfig {
  const providerCache = providerConfig.cache;
  return {
    ...defaultProviderCacheConfig,
    enabled: providerCache?.enabled ?? config.scheduling.cacheAffinity.enabled,
    scope: providerCache?.scope || config.scheduling.cacheAffinity.defaultScope,
    ttlMs: providerCache?.ttlMs || config.scheduling.cacheAffinity.ttlMs,
    minPrefixTokens:
      providerCache?.minPrefixTokens ?? config.scheduling.cacheAffinity.minPrefixTokens,
    maxWaitMs: providerCache?.maxWaitMs ?? config.scheduling.cacheAffinity.maxWaitMs
  };
}

function resolveCredentialCooldownMs(
  cooldowns: GatewayConfig['scheduling']['credentialScheduler']['cooldownMs'],
  statusCode: number | undefined,
  error: boolean | undefined
): number {
  if (error) {
    return cooldowns.network;
  }
  if (statusCode === 401 || statusCode === 403) {
    return cooldowns.auth;
  }
  if (statusCode === 429) {
    return cooldowns.rateLimit;
  }
  return cooldowns.serverError;
}

function stableHash(value: unknown): string {
  return createHash('sha256').update(stableStringify(value)).digest('hex');
}

function stableStringify(value: unknown): string {
  if (value === null || value === undefined) {
    return '';
  }
  if (typeof value !== 'object') {
    return JSON.stringify(value);
  }
  const binary = binaryPayloadBytes(value);
  if (binary) {
    const sha256 = createHash('sha256').update(binary).digest('hex');
    return `{"$binary":{"byteLength":${binary.byteLength},"sha256":${JSON.stringify(sha256)}}}`;
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(',')}]`;
  }

  const object = value as Record<string, unknown>;
  return `{${Object.keys(object)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableStringify(object[key])}`)
    .join(',')}}`;
}

function binaryPayloadBytes(value: unknown): Uint8Array | undefined {
  if (Buffer.isBuffer(value)) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  return undefined;
}

function sanitizeCredentialId(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_.-]+/g, '-')
    .replace(/^-+|-+$/g, '') || 'key';
}
