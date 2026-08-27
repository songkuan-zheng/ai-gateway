import { createHash } from 'node:crypto';
import type { FastifyRequest } from 'fastify';
import { calculateUsageBilling } from '../billing';
import { formatRedisNumber, SimpleRedisClient, type RedisReply } from '../redis-client';
import type {
  BillingRate,
  GatewayConfig,
  GatewayPrecheckRuleBaseConfig,
  GatewayPrecheckRedisStorageConfig,
  GatewayPrecheckStorageConfig,
  GatewayPrecheckScope,
  GatewayPrecheckSubject,
  GatewayApiKeyRestrictions,
  GatewayRateLimitDimensionConfig,
  GatewayRateLimitMetric,
  GatewayRateLimitPrecheckConfig,
  GatewayRequestIdentity,
  Provider,
  ProviderConfig,
  StandardRequest,
  StandardRequestInputContent,
  StandardRequestInputMessage,
  StandardUsage
} from '../types';
import { findDefaultProviderConfig, isObject, readHeader } from '../utils';

type PrecheckKind = 'rate_limit' | 'quota' | 'budget';

export interface GatewayPrecheckInput {
  request: FastifyRequest;
  config: GatewayConfig;
  targetProvider: Provider;
  targetProviderConfig?: ProviderConfig;
  model?: string;
  standardRequest?: StandardRequest;
  requestBody?: unknown;
  imageCount?: number;
  videoSeconds?: number;
  videoSize?: string;
}

export interface GatewayPrecheckEstimate {
  inputTokens: number;
  outputTokens: number;
  totalTokens: number;
  imageCount: number;
  videoSeconds: number;
  estimatedCostUsd: number;
}

export interface GatewayPrecheckFailure {
  ok: false;
  kind: PrecheckKind;
  statusCode: number;
  code: string;
  message: string;
  details: {
    subject: string;
    scope: string;
    window_ms: number;
    limit: number;
    used: number;
    requested: number;
    metric: string;
    limit_name?: string;
    estimated?: GatewayPrecheckEstimate;
  };
}

export type GatewayPrecheckResult =
  | { ok: true; estimate?: GatewayPrecheckEstimate }
  | GatewayPrecheckFailure;

interface WindowCounter {
  windowStart: number;
  value: number;
}

interface PendingCheck {
  kind: PrecheckKind;
  key: string;
  subjectKey: string;
  scopeKey: string;
  metric: GatewayRateLimitMetric | 'cost_usd';
  limitName?: string;
  windowMs: number;
  limit: number;
  requested: number;
  windowStart: number;
}

interface ApiKeyRestrictionBudgetLimit extends GatewayPrecheckRuleBaseConfig {
  name: string;
  maxCostUsd: number;
}

type RedisReservationResult =
  | { ok: true }
  | { ok: false; failedIndex: number; used: number };

type RedisReservationExecutor = (
  storage: GatewayPrecheckRedisStorageConfig,
  checks: PendingCheck[]
) => Promise<RedisReservationResult>;

const counters = new Map<string, WindowCounter>();
const redisClients = new Map<string, RedisPrecheckClient>();
let redisReservationExecutorForTests: RedisReservationExecutor | undefined;

export async function evaluateGatewayPrecheck(input: GatewayPrecheckInput): Promise<GatewayPrecheckResult> {
  const precheck = input.config.precheck;
  const apiKeyRestrictionRateLimits = resolveApiKeyRestrictionRateLimits(input.request);
  const apiKeyRestrictionBudgetLimits = resolveApiKeyRestrictionBudgetLimits(input.request);
  if (
    !precheck?.enabled &&
    apiKeyRestrictionRateLimits.length === 0 &&
    apiKeyRestrictionBudgetLimits.length === 0
  ) {
    return { ok: true };
  }

  const staticPrecheckEnabled = precheck.enabled === true;
  const rateLimitRules = staticPrecheckEnabled && precheck.rateLimit.enabled
    ? [...resolveRateLimitRules(precheck.rateLimit), ...apiKeyRestrictionRateLimits]
    : apiKeyRestrictionRateLimits;
  const hasQuota = staticPrecheckEnabled && precheck.quota.enabled && precheck.quota.maxTokens > 0;
  const hasBudget = staticPrecheckEnabled && precheck.budget.enabled && precheck.budget.maxCostUsd > 0;
  const hasApiKeyBudget = apiKeyRestrictionBudgetLimits.length > 0;

  if (rateLimitRules.length === 0 && !hasQuota && !hasBudget && !hasApiKeyBudget) {
    return { ok: true };
  }

  const needsEstimate =
    hasQuota ||
    hasBudget ||
    hasApiKeyBudget ||
    rateLimitRules.some((limit) => limit.metric === 'tokens' || limit.metric === 'images');
  const estimate =
    needsEstimate
      ? estimateGatewayRequestUsage(input)
      : undefined;
  const now = Date.now();
  const checks: PendingCheck[] = [];

  for (const limit of rateLimitRules) {
    checks.push(
      buildRateLimitPendingCheck(
        'rate_limit',
        input,
        limit,
        resolveRateLimitRequestedValue(limit.metric, estimate),
        now
      )
    );
  }

  if (hasQuota && estimate) {
    checks.push(
      buildPendingCheck(
        'quota',
        input,
        precheck.quota,
        'tokens',
        undefined,
        precheck.quota.maxTokens,
        estimate.totalTokens,
        now
      )
    );
  }

  if (hasBudget && estimate) {
    checks.push(
      buildPendingCheck(
        'budget',
        input,
        precheck.budget,
        'cost_usd',
        undefined,
        precheck.budget.maxCostUsd,
        estimate.estimatedCostUsd,
        now
      )
    );
  }

  if (hasApiKeyBudget && estimate) {
    for (const limit of apiKeyRestrictionBudgetLimits) {
      checks.push(
        buildPendingCheck(
          'budget',
          input,
          limit,
          'cost_usd',
          limit.name,
          limit.maxCostUsd,
          estimate.estimatedCostUsd,
          now
        )
      );
    }
  }

  return reserveChecks(input.config.precheck.storage, checks, estimate);
}

async function reserveChecks(
  storage: GatewayPrecheckStorageConfig,
  checks: PendingCheck[],
  estimate: GatewayPrecheckEstimate | undefined
): Promise<GatewayPrecheckResult> {
  if (storage.type === 'redis') {
    return reserveRedisChecks(storage, checks, estimate);
  }

  return reserveMemoryChecks(checks, estimate);
}

function reserveMemoryChecks(
  checks: PendingCheck[],
  estimate: GatewayPrecheckEstimate | undefined
): GatewayPrecheckResult {
  for (const check of checks) {
    const counter = readWindowCounter(check.key, check.windowStart);
    const used = counter.value;
    if (used + check.requested > check.limit) {
      return buildPrecheckFailure(check, used, estimate);
    }
  }

  for (const check of checks) {
    const counter = readWindowCounter(check.key, check.windowStart);
    counter.value += check.requested;
  }

  return { ok: true, estimate };
}

async function reserveRedisChecks(
  storage: GatewayPrecheckRedisStorageConfig,
  checks: PendingCheck[],
  estimate: GatewayPrecheckEstimate | undefined
): Promise<GatewayPrecheckResult> {
  if (checks.length === 0) {
    return { ok: true, estimate };
  }

  try {
    const reservation = redisReservationExecutorForTests
      ? await redisReservationExecutorForTests(storage, checks)
      : await getRedisPrecheckClient(storage).reserve(checks);
    if (reservation.ok) {
      return { ok: true, estimate };
    }

    const failedCheck = checks[Math.max(0, reservation.failedIndex - 1)] || checks[0];
    return buildPrecheckFailure(failedCheck, reservation.used, estimate);
  } catch (error) {
    return buildPrecheckStoreFailure(checks[0], estimate, error);
  }
}

function getRedisPrecheckClient(storage: GatewayPrecheckRedisStorageConfig): RedisPrecheckClient {
  const cacheKey = [
    storage.url,
    storage.keyPrefix,
    storage.connectTimeoutMs,
    storage.commandTimeoutMs
  ].join('|');
  const existing = redisClients.get(cacheKey);
  if (existing) {
    return existing;
  }

  const created = new RedisPrecheckClient(storage);
  redisClients.set(cacheKey, created);
  return created;
}

export async function closeGatewayPrecheckStore(): Promise<void> {
  const clients = [...redisClients.values()];
  redisClients.clear();
  await Promise.allSettled(clients.map((client) => client.close()));
}

export function resetGatewayPrecheckStateForTests(): void {
  counters.clear();
  redisReservationExecutorForTests = undefined;
  for (const client of redisClients.values()) {
    void client.close();
  }
  redisClients.clear();
}

export function setGatewayPrecheckRedisReservationExecutorForTests(
  executor: RedisReservationExecutor | undefined
): void {
  redisReservationExecutorForTests = executor;
}

function resolveRateLimitRules(
  rateLimit: GatewayRateLimitPrecheckConfig
): GatewayRateLimitDimensionConfig[] {
  const configuredLimits = Array.isArray(rateLimit.limits)
    ? rateLimit.limits.filter((limit) => limit.enabled && limit.max > 0)
    : [];
  if (configuredLimits.length > 0 || rateLimit.maxRequests <= 0) {
    return configuredLimits;
  }

  return [
    {
      enabled: true,
      name: 'requests',
      metric: 'requests',
      windowMs: rateLimit.windowMs,
      max: rateLimit.maxRequests,
      subject: rateLimit.subject,
      scope: rateLimit.scope,
      headerName: rateLimit.headerName
    }
  ];
}

function resolveApiKeyRestrictionRateLimits(
  request: FastifyRequest
): GatewayRateLimitDimensionConfig[] {
  const restrictions = readRequestApiKeyRestrictions(request);
  if (!restrictions) {
    return [];
  }

  const limits: GatewayRateLimitDimensionConfig[] = [];
  const maxRequests = normalizePositiveInteger(
    restrictions.rateLimit ?? restrictions.requestsPerMinute ?? restrictions.rpm
  );
  if (maxRequests) {
    limits.push({
      enabled: true,
      name: 'api_key_restriction',
      metric: 'requests',
      windowMs: resolveRestrictionWindowMs(restrictions.rateLimitWindowSeconds),
      max: maxRequests,
      subject: 'api_key',
      scope: 'global',
    });
  }

  const maxTokens = normalizePositiveInteger(
    restrictions.tokensPerMinute ?? restrictions.tpm
  );
  if (maxTokens) {
    limits.push({
      enabled: true,
      name: 'api_key_tpm',
      metric: 'tokens',
      windowMs: resolveRestrictionWindowMs(restrictions.tokenLimitWindowSeconds),
      max: maxTokens,
      subject: 'api_key',
      scope: 'global',
    });
  }

  return limits;
}

function resolveApiKeyRestrictionBudgetLimits(
  request: FastifyRequest
): ApiKeyRestrictionBudgetLimit[] {
  const restrictions = readRequestApiKeyRestrictions(request);
  if (!restrictions) {
    return [];
  }

  const maxCostUsd = normalizePositiveNumber(
    restrictions.costLimitUsd ??
      restrictions.costLimit ??
      restrictions.maxCostUsd ??
      restrictions.costPerMinuteUsd
  );
  if (!maxCostUsd) {
    return [];
  }

  return [
    {
      enabled: true,
      name: 'api_key_cost',
      windowMs: resolveRestrictionWindowMs(restrictions.costLimitWindowSeconds),
      maxCostUsd,
      subject: 'api_key',
      scope: 'global',
    },
  ];
}

function readRequestApiKeyRestrictions(
  request: FastifyRequest
): GatewayApiKeyRestrictions | undefined {
  return (
    request as FastifyRequest & {
      gatewayApiKeyRestrictions?: GatewayApiKeyRestrictions;
    }
  ).gatewayApiKeyRestrictions;
}

function resolveRestrictionWindowMs(value: unknown): number {
  const windowSeconds = normalizePositiveInteger(value) || 60;
  return Math.min(Math.max(windowSeconds, 1), 3600) * 1000;
}

function normalizePositiveInteger(value: unknown): number | undefined {
  const numeric =
    typeof value === 'number'
      ? value
      : typeof value === 'string'
        ? Number(value)
        : NaN;
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return undefined;
  }

  return Math.floor(numeric);
}

function normalizePositiveNumber(value: unknown): number | undefined {
  const numeric =
    typeof value === 'number'
      ? value
      : typeof value === 'string'
        ? Number(value)
        : NaN;
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return undefined;
  }

  return numeric;
}

const redisReserveScript = `
local count = tonumber(ARGV[1])
for i = 1, count do
  local offset = 1 + ((i - 1) * 3)
  local requested = tonumber(ARGV[offset + 1])
  local limit = tonumber(ARGV[offset + 2])
  local used = tonumber(redis.call("GET", KEYS[i]) or "0")
  if used + requested > limit then
    return {0, i, used}
  end
end
for i = 1, count do
  local offset = 1 + ((i - 1) * 3)
  local requested = tonumber(ARGV[offset + 1])
  local ttl = tonumber(ARGV[offset + 3])
  redis.call("INCRBYFLOAT", KEYS[i], requested)
  redis.call("PEXPIRE", KEYS[i], ttl)
end
return {1, 0, 0}
`.trim();

class RedisPrecheckClient {
  private readonly client: SimpleRedisClient;

  constructor(private readonly storage: GatewayPrecheckRedisStorageConfig) {
    this.client = new SimpleRedisClient({
      url: storage.url,
      connectTimeoutMs: storage.connectTimeoutMs,
      commandTimeoutMs: storage.commandTimeoutMs,
      errorPrefix: 'Redis precheck'
    });
  }

  async reserve(checks: PendingCheck[]): Promise<RedisReservationResult> {
    const now = Date.now();
    const keys = checks.map((check) => this.buildRedisKey(check));
    const args = checks.flatMap((check) => [
      formatRedisNumber(check.requested),
      formatRedisNumber(check.limit),
      String(Math.max(check.windowStart + check.windowMs - now, 1) + 1000)
    ]);
    const reply = await this.command([
      'EVAL',
      redisReserveScript,
      String(keys.length),
      ...keys,
      String(keys.length),
      ...args
    ]);
    const parsed = parseRedisReservationReply(reply);
    if (!parsed) {
      throw new Error('Redis precheck reservation returned an invalid response.');
    }

    return parsed;
  }

  async close(): Promise<void> {
    await this.client.close();
  }

  private buildRedisKey(check: PendingCheck): string {
    const prefix = this.storage.keyPrefix.replace(/:+$/, '') || 'next-ai:gateway:precheck';
    const hash = createHash('sha256').update(check.key).digest('hex');
    return `${prefix}:${hash}:${check.windowStart}`;
  }

  private async command(args: string[]): Promise<RedisReply> {
    return this.client.command(args);
  }
}

function parseRedisReservationReply(reply: RedisReply): RedisReservationResult | undefined {
  if (!Array.isArray(reply) || reply.length < 3) {
    return undefined;
  }

  const allowed = Number(reply[0]);
  if (allowed === 1) {
    return { ok: true };
  }

  return {
    ok: false,
    failedIndex: Math.max(Number(reply[1]) || 1, 1),
    used: Number(reply[2]) || 0
  };
}

function buildRateLimitPendingCheck(
  kind: PrecheckKind,
  input: GatewayPrecheckInput,
  rule: GatewayRateLimitDimensionConfig,
  requested: number,
  now: number
): PendingCheck {
  return buildPendingCheck(
    kind,
    input,
    rule,
    rule.metric,
    rule.name,
    rule.max,
    requested,
    now
  );
}

function buildPendingCheck(
  kind: PrecheckKind,
  input: GatewayPrecheckInput,
  rule: GatewayPrecheckRuleBaseConfig,
  metric: GatewayRateLimitMetric | 'cost_usd',
  limitName: string | undefined,
  limit: number,
  requested: number,
  now: number
): PendingCheck {
  const subjectKey = resolveSubjectKey(input.request, rule.subject, rule.headerName);
  const scopeKey = resolveScopeKey(rule.scope, input.targetProvider, input.model);
  const key = [
    kind,
    metric,
    limitName || '',
    rule.windowMs,
    rule.subject,
    subjectKey,
    rule.scope,
    scopeKey
  ].join('|');

  return {
    kind,
    key,
    subjectKey,
    scopeKey,
    metric,
    limitName,
    windowMs: rule.windowMs,
    limit,
    requested,
    windowStart: calculateWindowStart(rule.windowMs, now)
  };
}

function readWindowCounter(key: string, windowStart: number): WindowCounter {
  const existing = counters.get(key);
  if (existing && existing.windowStart === windowStart) {
    return existing;
  }

  const fresh: WindowCounter = {
    windowStart,
    value: 0
  };
  counters.set(key, fresh);
  return fresh;
}

function calculateWindowStart(windowMs: number, now: number): number {
  return Math.floor(now / windowMs) * windowMs;
}

function buildPrecheckFailure(
  check: PendingCheck,
  used: number,
  estimate: GatewayPrecheckEstimate | undefined
): GatewayPrecheckFailure {
  const code =
    check.kind === 'rate_limit'
      ? 'rate_limit_exceeded'
      : check.kind === 'quota'
        ? 'quota_exceeded'
        : 'budget_exceeded';
  const statusCode = check.kind === 'budget' ? 402 : 429;
  const limitLabel =
    check.kind === 'rate_limit'
      ? `${check.limitName || check.metric} rate limit`
      : check.kind === 'quota'
        ? 'token quota'
        : 'budget';

  return {
    ok: false,
    kind: check.kind,
    statusCode,
    code,
    message: `Gateway ${limitLabel} precheck failed.`,
    details: {
      subject: check.subjectKey,
      scope: check.scopeKey,
      window_ms: check.windowMs,
      limit: check.limit,
      used,
      requested: check.requested,
      metric: check.metric,
      limit_name: check.limitName,
      estimated: estimate
    }
  };
}

function buildPrecheckStoreFailure(
  check: PendingCheck,
  estimate: GatewayPrecheckEstimate | undefined,
  error: unknown
): GatewayPrecheckFailure {
  void error;
  return {
    ok: false,
    kind: check.kind,
    statusCode: 503,
    code: 'precheck_store_unavailable',
    message: 'Gateway precheck store is unavailable.',
    details: {
      subject: check.subjectKey,
      scope: check.scopeKey,
      window_ms: check.windowMs,
      limit: check.limit,
      used: 0,
      requested: check.requested,
      metric: check.metric,
      limit_name: check.limitName,
      estimated: estimate
    }
  };
}

function resolveRateLimitRequestedValue(
  metric: GatewayRateLimitMetric,
  estimate: GatewayPrecheckEstimate | undefined
): number {
  if (metric === 'requests') {
    return 1;
  }

  if (metric === 'tokens') {
    return estimate?.totalTokens || 0;
  }

  return estimate?.imageCount || 0;
}

function estimateGatewayRequestUsage(input: GatewayPrecheckInput): GatewayPrecheckEstimate {
  const charsPerToken = Math.max(input.config.precheck.estimation.charsPerToken, 1);
  const inputCharacters = input.standardRequest
    ? countStandardRequestInputCharacters(input.standardRequest)
    : countUnknownCharacters(input.requestBody);
  const inputTokens = Math.ceil(inputCharacters / charsPerToken);
  const outputTokens = resolveMaxOutputTokens(input);
  const totalTokens = inputTokens + outputTokens;
  const usage: StandardUsage = {
    input_tokens: inputTokens,
    output_tokens: outputTokens,
    total_tokens: totalTokens,
    video_seconds: input.videoSeconds,
    video_size: input.videoSize
  };
  const billing = calculateUsageBilling(
    input.targetProvider,
    usage,
    input.config.billing,
    resolveProviderBillingRate(input.config, input.targetProvider, input.model, input.targetProviderConfig)
  );

  return {
    inputTokens,
    outputTokens,
    totalTokens,
    imageCount: input.imageCount ?? countImageInputs(input.requestBody),
    videoSeconds: input.videoSeconds ?? 0,
    estimatedCostUsd: billing.cost.total
  };
}

function countStandardRequestInputCharacters(request: StandardRequest): number {
  let count = 0;
  count += request.model?.length || 0;
  count += request.instructions?.length || 0;
  count += countStandardInputCharacters(request.input);
  count += countUnknownCharacters(request.tools);
  count += countUnknownCharacters(request.tool_choice);
  count += countUnknownCharacters(request.reasoning);
  count += countUnknownCharacters(request.thinking);
  count += countUnknownCharacters(request.output_config);
  count += countUnknownCharacters(request.text);
  return count;
}

function countStandardInputCharacters(input: StandardRequest['input']): number {
  if (typeof input === 'string') {
    return input.length;
  }

  return input.reduce((sum, message) => sum + countMessageCharacters(message), 0);
}

function countMessageCharacters(message: StandardRequestInputMessage): number {
  return (
    message.role.length +
    message.content.reduce((sum, item) => sum + countContentCharacters(item), 0)
  );
}

function countContentCharacters(item: StandardRequestInputContent): number {
  if (item.type === 'input_text') {
    return item.text.length;
  }

  if (item.type === 'tool_result') {
    return item.content.length;
  }

  if (item.type === 'reasoning') {
    return (
      (item.text?.length || 0) +
      (item.summary?.length || 0) +
      countUnknownCharacters(item.reasoning_details)
    );
  }

  if (item.type === 'tool_search_call') {
    return 'ToolSearch'.length + countUnknownCharacters(item.arguments);
  }

  if (item.type === 'tool_search_output') {
    return countUnknownCharacters(item.tools);
  }

  return item.name.length + countUnknownCharacters(item.input);
}

function countUnknownCharacters(value: unknown): number {
  if (value === undefined || value === null) {
    return 0;
  }

  if (typeof value === 'string') {
    return value.length;
  }

  if (isBinaryValue(value)) {
    return 0;
  }

  try {
    return JSON.stringify(value)?.length || 0;
  } catch {
    return String(value).length;
  }
}

function countImageInputs(value: unknown): number {
  if (Array.isArray(value)) {
    return value.reduce((sum, item) => sum + countImageInputs(item), 0);
  }

  if (isBinaryValue(value)) {
    return 0;
  }

  if (!isObject(value)) {
    return 0;
  }

  if (isImageBlock(value)) {
    return 1;
  }

  return Object.values(value).reduce<number>((sum, item) => sum + countImageInputs(item), 0);
}

function isBinaryValue(value: unknown): boolean {
  return (
    Buffer.isBuffer(value) ||
    value instanceof ArrayBuffer ||
    (typeof ArrayBuffer !== 'undefined' && ArrayBuffer.isView(value))
  );
}

function isImageBlock(value: Record<string, unknown>): boolean {
  const type = readObjectString(value, 'type')?.toLowerCase();
  if (type === 'image_url' || type === 'input_image') {
    return true;
  }

  if (type === 'image') {
    return true;
  }

  if (value.image_url !== undefined || value.input_image !== undefined) {
    return true;
  }

  const inlineData = isObject(value.inlineData)
    ? value.inlineData
    : isObject(value.inline_data)
      ? value.inline_data
      : undefined;
  const inlineMimeType =
    readObjectString(inlineData, 'mimeType') || readObjectString(inlineData, 'mime_type');
  if (inlineMimeType?.toLowerCase().startsWith('image/')) {
    return true;
  }

  const source = isObject(value.source) ? value.source : undefined;
  const sourceMediaType = readObjectString(source, 'media_type');
  return sourceMediaType?.toLowerCase().startsWith('image/') === true;
}

function readObjectString(
  value: Record<string, unknown> | undefined,
  key: string
): string | undefined {
  const item = value?.[key];
  return typeof item === 'string' && item.trim() ? item.trim() : undefined;
}

function resolveMaxOutputTokens(input: GatewayPrecheckInput): number {
  const fromStandard = input.standardRequest?.max_output_tokens;
  if (typeof fromStandard === 'number' && Number.isFinite(fromStandard) && fromStandard >= 0) {
    return Math.max(0, Math.ceil(fromStandard));
  }

  if (isObject(input.requestBody)) {
    const raw =
      input.requestBody.max_output_tokens ??
      input.requestBody.max_tokens ??
      input.requestBody.max_completion_tokens;
    if (typeof raw === 'number' && Number.isFinite(raw) && raw > 0) {
      return Math.ceil(raw);
    }
  }

  return input.config.precheck.estimation.defaultMaxOutputTokens;
}

function resolveSubjectKey(
  request: FastifyRequest,
  subject: GatewayPrecheckSubject,
  headerName?: string
): string {
  const identity = (request as FastifyRequest & { gatewayIdentity?: GatewayRequestIdentity })
    .gatewayIdentity;

  if (subject === 'global') {
    return 'global';
  }

  if (subject === 'identity') {
    return (
      identity?.billingSubjectKey ||
      identity?.userId ||
      identity?.tenantId ||
      identity?.organizationId ||
      identity?.apiKeyId ||
      resolveClientIp(request)
    );
  }

  if (subject === 'user') {
    return identity?.userId || identity?.subject || resolveClientIp(request);
  }

  if (subject === 'tenant') {
    return identity?.tenantId || resolveClientIp(request);
  }

  if (subject === 'organization') {
    return identity?.organizationId || resolveClientIp(request);
  }

  if (subject === 'api_key') {
    return identity?.apiKeyId || resolveApiKeySubject(request) || resolveClientIp(request);
  }

  if (subject === 'header') {
    const headerValue = headerName ? readHeader(request.headers[headerName]) : undefined;
    return headerValue ? `header:${hashSubjectValue(headerValue)}` : resolveClientIp(request);
  }

  return resolveClientIp(request);
}

function resolveApiKeySubject(request: FastifyRequest): string | undefined {
  const value =
    readHeader(request.headers['x-api-key']) ||
    readHeader(request.headers['api-key']) ||
    readHeader(request.headers.authorization);
  return value ? `api_key:${hashSubjectValue(value)}` : undefined;
}

function resolveClientIp(request: FastifyRequest): string {
  return `ip:${request.ip || request.socket.remoteAddress || 'unknown'}`;
}

function resolveScopeKey(
  scope: GatewayPrecheckScope,
  provider: Provider,
  model: string | undefined
): string {
  if (scope === 'provider') {
    return `provider:${provider}`;
  }

  if (scope === 'model') {
    return `model:${model || 'unknown'}`;
  }

  if (scope === 'provider_model') {
    return `provider:${provider}:model:${model || 'unknown'}`;
  }

  return 'global';
}

function hashSubjectValue(value: string): string {
  return createHash('sha256').update(value).digest('hex').slice(0, 24);
}

function resolveProviderBillingRate(
  config: GatewayConfig,
  provider: Provider,
  model: string | undefined,
  targetProviderConfig?: ProviderConfig
): BillingRate | undefined {
  const providerConfig = targetProviderConfig || findProviderConfigByType(config.providers, provider);
  if (!providerConfig) {
    return undefined;
  }

  if (model && providerConfig.billing.byModel[model]) {
    return providerConfig.billing.byModel[model];
  }

  return providerConfig.billing.default;
}

function findProviderConfigByType(
  providers: ProviderConfig[],
  provider: Provider
): ProviderConfig | undefined {
  return findDefaultProviderConfig(providers, provider);
}
