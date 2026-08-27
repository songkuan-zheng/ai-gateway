import { createHash, randomUUID } from 'node:crypto';
import { PassThrough, Readable } from 'node:stream';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { SimpleRedisClient, type RedisReply } from '../redis-client';
import type {
  GatewayConfig,
  GatewayIdempotencyRedisStorageConfig,
  GatewayRequestIdentity
} from '../types';
import { isObject, readHeader } from '../utils';

type CachedPayload = string | Buffer;
type CachedHeaderValue = string | number | string[];

interface CachedIdempotencyResponse {
  statusCode: number;
  headers: Record<string, CachedHeaderValue>;
  payload: CachedPayload;
}

interface PendingIdempotencyEntry {
  state: 'pending';
  requestHash: string;
  expiresAt: number;
  promise: Promise<CachedIdempotencyResponse | undefined>;
  resolve: (response: CachedIdempotencyResponse | undefined) => void;
}

interface CompletedIdempotencyEntry {
  state: 'completed';
  requestHash: string;
  expiresAt: number;
  sizeBytes: number;
  response: CachedIdempotencyResponse;
}

type IdempotencyEntry = PendingIdempotencyEntry | CompletedIdempotencyEntry;

interface SerializedRedisCachedPayload {
  type: 'string' | 'buffer';
  data: string;
}

interface SerializedRedisCachedResponse {
  statusCode: number;
  headers: Record<string, CachedHeaderValue>;
  payload: SerializedRedisCachedPayload;
}

type RedisIdempotencyEntry =
  | {
      state: 'pending';
      requestHash: string;
      ownerToken?: string;
    }
  | {
      state: 'completed';
      requestHash: string;
      response: SerializedRedisCachedResponse;
    };

interface IdempotencyRequestContext {
  storage: 'memory' | 'redis';
  storeKey: string;
  redisKey?: string;
  requestHash: string;
  ownerToken?: string;
  servedFromCache: boolean;
}

interface DeferredIdempotencyRequest {
  key: string;
}

interface GatewayIdempotencyPreHandlerOptions {
  defer?: boolean | ((request: FastifyRequest) => boolean);
}

const idempotencyStore = new Map<string, IdempotencyEntry>();
let idempotencyStoreBytes = 0;
const requestContexts = new WeakMap<FastifyRequest, IdempotencyRequestContext>();
let deferredRequestContexts = new WeakMap<FastifyRequest, DeferredIdempotencyRequest>();
const redisIdempotencyClients = new Map<string, SimpleRedisClient>();
let redisIdempotencyCommandExecutorForTests:
  | ((storage: GatewayIdempotencyRedisStorageConfig, args: string[]) => Promise<RedisReply>)
  | undefined;

const redisClaimIdempotencyScript = `
local existing = redis.call('GET', KEYS[1])
if existing then
  return {0, existing}
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return {1, ARGV[1]}
`;

const redisCompleteIdempotencyScript = `
local existing = redis.call('GET', KEYS[1])
if not existing then
  return 0
end
local ok, entry = pcall(cjson.decode, existing)
if not ok or type(entry) ~= 'table' then
  return -1
end
if entry.state ~= 'pending' or entry.requestHash ~= ARGV[1] or entry.ownerToken ~= ARGV[2] then
  return -1
end
redis.call('SET', KEYS[1], ARGV[3], 'PX', ARGV[4])
return 1
`;

const redisDeleteIdempotencyScript = `
local existing = redis.call('GET', KEYS[1])
if not existing then
  return 0
end
local ok, entry = pcall(cjson.decode, existing)
if not ok or type(entry) ~= 'table' then
  return -1
end
if entry.state ~= 'pending' or entry.requestHash ~= ARGV[1] or entry.ownerToken ~= ARGV[2] then
  return -1
end
return redis.call('DEL', KEYS[1])
`;

const routeSensitiveHeaders = [
  'content-type',
  'authorization',
  'x-api-key',
  'api-key',
  'x-goog-api-key',
  'x-mcp-key',
  'x-codex-access-token',
  'x-codex-refresh-token',
  'x-codex-account-id',
  'x-target-provider',
  'x-target-providers',
  'x-target-model',
  'x-auth-user-id',
  'x-auth-tenant-id',
  'x-auth-sub',
  'x-auth-organization-id',
  'x-auth-plan',
  'openai-organization',
  'openai-project',
  'anthropic-version',
  'anthropic-beta'
];

const hopByHopHeaders = new Set([
  'connection',
  'content-encoding',
  'content-length',
  'date',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade'
]);

export function createGatewayIdempotencyPreHandler(
  config: GatewayConfig,
  options: GatewayIdempotencyPreHandlerOptions = {}
) {
  return async (request: FastifyRequest, reply: FastifyReply) => {
    if (!isIdempotencyEligible(request, config)) {
      return;
    }

    const key = readIdempotencyKey(request, config);
    if (!key) {
      return;
    }

    const defer =
      typeof options.defer === 'function'
        ? options.defer(request)
        : options.defer === true;
    if (defer) {
      deferredRequestContexts.set(request, { key });
      return;
    }

    return applyGatewayIdempotencyPrecheck(request, reply, config, key);
  };
}

export async function applyDeferredGatewayIdempotency(
  request: FastifyRequest,
  reply: FastifyReply,
  config: GatewayConfig,
  extraFingerprint?: unknown
): Promise<boolean> {
  const deferred = deferredRequestContexts.get(request);
  if (!deferred) {
    return false;
  }

  deferredRequestContexts.delete(request);
  if (!isIdempotencyEligible(request, config)) {
    return false;
  }

  await applyGatewayIdempotencyPrecheck(request, reply, config, deferred.key, extraFingerprint);
  return reply.sent;
}

function applyGatewayIdempotencyPrecheck(
  request: FastifyRequest,
  reply: FastifyReply,
  config: GatewayConfig,
  key: string,
  extraFingerprint?: unknown
) {
    const storeKey = buildIdempotencyStoreKey(request, key);
    const requestHash = hashIdempotencyRequest(request, config, extraFingerprint);
    if (config.idempotency.storage?.type === 'redis') {
      return handleRedisIdempotencyPrecheck(
        request,
        reply,
        config,
        storeKey,
        requestHash,
        config.idempotency.storage
      );
    }

    return handleMemoryIdempotencyPrecheck(request, reply, config, storeKey, requestHash);
}

async function handleMemoryIdempotencyPrecheck(
  request: FastifyRequest,
  reply: FastifyReply,
  config: GatewayConfig,
  storeKey: string,
  requestHash: string
) {
  const now = Date.now();
  pruneIdempotencyStore(config, now);
  const existing = idempotencyStore.get(storeKey);

  if (existing && existing.expiresAt > now) {
    if (existing.requestHash !== requestHash) {
      return sendIdempotencyConflict(reply);
    }

    if (existing.state === 'completed') {
      requestContexts.set(request, {
        storage: 'memory',
        storeKey,
        requestHash,
        servedFromCache: true
      });
      return sendCachedResponse(reply, existing.response);
    }

    const waitResult = await waitForMemoryIdempotencyCompletion(
      existing.promise,
      config.idempotency.pendingWaitTimeoutMs
    );
    if (waitResult.status === 'in_progress') {
      return sendIdempotencyInProgress(reply);
    }
    const response = waitResult.response;
    if (response) {
      requestContexts.set(request, {
        storage: 'memory',
        storeKey,
        requestHash,
        servedFromCache: true
      });
      return sendCachedResponse(reply, response);
    }

    return reply
      .code(409)
      .header('x-gateway-idempotency-status', 'not-cacheable')
      .send({
        error: {
          message: 'Original request did not produce a cacheable idempotency response.',
          code: 'idempotency_response_not_cacheable'
        }
      });
  }

  if (existing) {
    deleteMemoryIdempotencyEntry(storeKey);
  }

  const pending = createPendingEntry(requestHash, now + config.idempotency.ttlMs);
  idempotencyStore.set(storeKey, pending);
  requestContexts.set(request, {
    storage: 'memory',
    storeKey,
    requestHash,
    servedFromCache: false
  });
  pruneIdempotencyStore(config, now);
}

async function handleRedisIdempotencyPrecheck(
  request: FastifyRequest,
  reply: FastifyReply,
  config: GatewayConfig,
  storeKey: string,
  requestHash: string,
  storage: GatewayIdempotencyRedisStorageConfig
) {
  const redisKey = buildRedisIdempotencyKey(storage, storeKey);
  const ownerToken = randomUUID();
  try {
    const pendingEntry = serializeRedisIdempotencyEntry({
      state: 'pending',
      requestHash,
      ownerToken
    });
    const result = parseRedisClaimResult(
      await commandRedisIdempotency(storage, [
        'EVAL',
        redisClaimIdempotencyScript,
        '1',
        redisKey,
        pendingEntry,
        String(resolvePositiveInteger(config.idempotency.ttlMs, 86400000))
      ])
    );
    if (!result) {
      throw new Error('Redis idempotency claim returned an invalid response.');
    }

    if (result.claimed) {
      requestContexts.set(request, {
        storage: 'redis',
        storeKey,
        redisKey,
        requestHash,
        ownerToken,
        servedFromCache: false
      });
      return;
    }

    const existing = parseRedisIdempotencyEntry(result.entry);
    if (!existing || existing.requestHash !== requestHash) {
      return sendIdempotencyConflict(reply);
    }

    if (existing.state === 'completed') {
      requestContexts.set(request, {
        storage: 'redis',
        storeKey,
        redisKey,
        requestHash,
        servedFromCache: true
      });
      return sendCachedResponse(reply, deserializeRedisCachedResponse(existing.response));
    }

    const waitResult = await waitForRedisIdempotencyCompletion(storage, redisKey, requestHash, config);
    if (waitResult.status === 'completed') {
      requestContexts.set(request, {
        storage: 'redis',
        storeKey,
        redisKey,
        requestHash,
        servedFromCache: true
      });
      return sendCachedResponse(reply, waitResult.response);
    }
    if (waitResult.status === 'conflict') {
      return sendIdempotencyConflict(reply);
    }
    if (waitResult.status === 'in_progress') {
      return sendIdempotencyInProgress(reply);
    }

    return sendIdempotencyResponseNotCacheable(reply);
  } catch (error) {
    request.log.warn(
      {
        details: error instanceof Error ? error.message : String(error)
      },
      'Gateway idempotency Redis store failed.'
    );
    return sendIdempotencyStoreUnavailable(reply);
  }
}

export function registerGatewayIdempotencyHooks(
  fastify: FastifyInstance,
  config: GatewayConfig
): void {
  fastify.addHook('onSend', async (request, reply, payload) => {
    const context = requestContexts.get(request);
    if (!context || context.servedFromCache) {
      return payload;
    }

    if (context.storage === 'redis') {
      const cached = buildCachedResponse(reply, payload, config);
      if (cached) {
        try {
          await completeRedisIdempotencyEntry(config, context, cached);
          reply.header('x-gateway-idempotency-status', 'stored');
        } catch (error) {
          request.log.warn(
            {
              details: error instanceof Error ? error.message : String(error)
            },
            'Failed to store Redis idempotency response.'
          );
        }
        return payload;
      }

      const cacheableStream = buildRedisCacheableStreamResponse(reply, payload, context, config, request);
      if (cacheableStream) {
        reply.header('x-gateway-idempotency-status', 'stored');
        return cacheableStream;
      }

      try {
        await deleteRedisIdempotencyEntry(config, context);
      } catch (error) {
        request.log.warn(
          {
            details: error instanceof Error ? error.message : String(error)
          },
          'Failed to clear Redis idempotency pending entry.'
        );
      }
      return payload;
    }

    const entry = idempotencyStore.get(context.storeKey);
    if (!entry || entry.state !== 'pending' || entry.requestHash !== context.requestHash) {
      return payload;
    }

    const cached = buildCachedResponse(reply, payload, config);
    if (cached) {
      completePendingIdempotencyEntry(context, entry, cached, config);
      reply.header('x-gateway-idempotency-status', 'stored');
      return payload;
    }

    const cacheableStream = buildCacheableStreamResponse(reply, payload, context, entry, config);
    if (cacheableStream) {
      reply.header('x-gateway-idempotency-status', 'stored');
      return cacheableStream;
    }

    failPendingIdempotencyEntry(context, entry);
    return payload;
  });
}

function completePendingIdempotencyEntry(
  context: IdempotencyRequestContext,
  entry: PendingIdempotencyEntry,
  cached: CachedIdempotencyResponse,
  config: GatewayConfig
): void {
  const current = idempotencyStore.get(context.storeKey);
  if (current !== entry || current.state !== 'pending' || current.requestHash !== context.requestHash) {
    return;
  }

  const completed: CompletedIdempotencyEntry = {
    state: 'completed',
    requestHash: entry.requestHash,
    expiresAt: entry.expiresAt,
    sizeBytes: cachedResponseSize(cached),
    response: cached
  };
  setCompletedMemoryIdempotencyEntry(context.storeKey, completed);
  entry.resolve(cached);
  pruneIdempotencyStore(config, Date.now());
}

function failPendingIdempotencyEntry(
  context: IdempotencyRequestContext,
  entry: PendingIdempotencyEntry
): void {
  const current = idempotencyStore.get(context.storeKey);
  if (current !== entry || current.state !== 'pending' || current.requestHash !== context.requestHash) {
    return;
  }

  deleteMemoryIdempotencyEntry(context.storeKey);
  entry.resolve(undefined);
}

async function completeRedisIdempotencyEntry(
  config: GatewayConfig,
  context: IdempotencyRequestContext,
  cached: CachedIdempotencyResponse
): Promise<void> {
  if (!context.redisKey || config.idempotency.storage?.type !== 'redis') {
    return;
  }
  if (!context.ownerToken) {
    throw new Error('Redis idempotency owner token is missing for pending entry completion.');
  }

  const result = await commandRedisIdempotency(config.idempotency.storage, [
    'EVAL',
    redisCompleteIdempotencyScript,
    '1',
    context.redisKey,
    context.requestHash,
    context.ownerToken,
    serializeRedisIdempotencyEntry({
      state: 'completed',
      requestHash: context.requestHash,
      response: serializeRedisCachedResponse(cached)
    }),
    String(resolvePositiveInteger(config.idempotency.ttlMs, 86400000))
  ]);
  const resultCode = Number(result);
  if (resultCode < 0) {
    throw new Error('Redis idempotency entry changed before response could be stored.');
  }
  if (resultCode !== 1) {
    throw new Error('Redis idempotency pending entry was unavailable before response could be stored.');
  }
}

async function deleteRedisIdempotencyEntry(
  config: GatewayConfig,
  context: IdempotencyRequestContext
): Promise<void> {
  if (!context.redisKey || config.idempotency.storage?.type !== 'redis') {
    return;
  }
  if (!context.ownerToken) {
    return;
  }

  await commandRedisIdempotency(config.idempotency.storage, [
    'EVAL',
    redisDeleteIdempotencyScript,
    '1',
    context.redisKey,
    context.requestHash,
    context.ownerToken
  ]);
}

function buildCacheableStreamResponse(
  reply: FastifyReply,
  payload: unknown,
  context: IdempotencyRequestContext,
  entry: PendingIdempotencyEntry,
  config: GatewayConfig
): Readable | undefined {
  if (!isCacheableStatus(reply.statusCode, config)) {
    return undefined;
  }

  if (isEventStreamResponse(reply)) {
    return undefined;
  }

  if (!isReadablePayload(payload)) {
    return undefined;
  }

  const stream = new PassThrough();
  const chunks: Buffer[] = [];
  const responseSnapshot = {
    statusCode: reply.statusCode,
    headers: sanitizeCachedHeaders(reply.getHeaders())
  };
  let settled = false;
  let streamCacheable = true;

  const settle = (cachedPayload: Buffer | undefined): void => {
    if (settled) {
      return;
    }
    settled = true;
    if (!cachedPayload) {
      failPendingIdempotencyEntry(context, entry);
      return;
    }

    const completed: CompletedIdempotencyEntry = {
      state: 'completed',
      requestHash: entry.requestHash,
      expiresAt: entry.expiresAt,
      sizeBytes: cachedPayload.byteLength,
      response: {
        ...responseSnapshot,
        payload: cachedPayload
      }
    };
    const current = idempotencyStore.get(context.storeKey);
    if (current !== entry || current.state !== 'pending' || current.requestHash !== context.requestHash) {
      return;
    }

    setCompletedMemoryIdempotencyEntry(context.storeKey, completed);
    entry.resolve(completed.response);
    pruneIdempotencyStore(config, Date.now());
  };

  let bufferedBytes = 0;
  payload.on('data', (chunk) => {
    const buffer = normalizeStreamChunk(chunk);
    if (buffer && streamCacheable) {
      bufferedBytes += buffer.byteLength;
      if (bufferedBytes > config.idempotency.maxResponseBytes) {
        streamCacheable = false;
        chunks.length = 0;
        return;
      }
      chunks.push(buffer);
      return;
    }

    if (!buffer) {
      streamCacheable = false;
      chunks.length = 0;
    }
  });
  payload.once('end', () => {
    settle(streamCacheable ? Buffer.concat(chunks) : undefined);
  });
  payload.once('error', (error) => {
    settle(undefined);
    stream.destroy(error instanceof Error ? error : new Error(String(error)));
  });
  payload.once('close', () => {
    if (!settled && !payload.readableEnded) {
      settle(undefined);
    }
  });

  payload.pipe(stream);
  return stream;
}

function buildRedisCacheableStreamResponse(
  reply: FastifyReply,
  payload: unknown,
  context: IdempotencyRequestContext,
  config: GatewayConfig,
  request: FastifyRequest
): Readable | undefined {
  if (!isCacheableStatus(reply.statusCode, config)) {
    return undefined;
  }

  if (isEventStreamResponse(reply)) {
    return undefined;
  }

  if (!isReadablePayload(payload)) {
    return undefined;
  }

  const stream = new PassThrough();
  const chunks: Buffer[] = [];
  const responseSnapshot = {
    statusCode: reply.statusCode,
    headers: sanitizeCachedHeaders(reply.getHeaders())
  };
  let settled = false;
  let streamCacheable = true;
  let bufferedBytes = 0;

  const logFailure = (error: unknown, message: string): void => {
    request.log.warn(
      {
        details: error instanceof Error ? error.message : String(error)
      },
      message
    );
  };

  const settle = (cachedPayload: Buffer | undefined): void => {
    if (settled) {
      return;
    }
    settled = true;
    if (!cachedPayload) {
      void deleteRedisIdempotencyEntry(config, context).catch((error) => {
        logFailure(error, 'Failed to clear Redis idempotency pending entry.');
      });
      return;
    }

    void completeRedisIdempotencyEntry(config, context, {
      ...responseSnapshot,
      payload: cachedPayload
    }).catch((error) => {
      logFailure(error, 'Failed to store Redis idempotency streamed response.');
    });
  };

  payload.on('data', (chunk) => {
    const buffer = normalizeStreamChunk(chunk);
    if (buffer && streamCacheable) {
      bufferedBytes += buffer.byteLength;
      if (bufferedBytes > config.idempotency.maxResponseBytes) {
        streamCacheable = false;
        chunks.length = 0;
        return;
      }
      chunks.push(buffer);
      return;
    }

    if (!buffer) {
      streamCacheable = false;
      chunks.length = 0;
    }
  });
  payload.once('end', () => {
    settle(streamCacheable ? Buffer.concat(chunks) : undefined);
  });
  payload.once('error', (error) => {
    settle(undefined);
    stream.destroy(error instanceof Error ? error : new Error(String(error)));
  });
  payload.once('close', () => {
    if (!settled && !payload.readableEnded) {
      settle(undefined);
    }
  });

  payload.pipe(stream);
  return stream;
}

export function resetGatewayIdempotencyForTests(): void {
  idempotencyStore.clear();
  idempotencyStoreBytes = 0;
  deferredRequestContexts = new WeakMap();
  redisIdempotencyCommandExecutorForTests = undefined;
}

export function setGatewayIdempotencyRedisCommandExecutorForTests(
  executor:
    | ((storage: GatewayIdempotencyRedisStorageConfig, args: string[]) => Promise<RedisReply>)
    | undefined
): void {
  redisIdempotencyCommandExecutorForTests = executor;
}

export async function closeGatewayIdempotencyStore(): Promise<void> {
  const clients = Array.from(redisIdempotencyClients.values());
  redisIdempotencyClients.clear();
  await Promise.allSettled(clients.map((client) => client.close()));
}

function isIdempotencyEligible(request: FastifyRequest, config: GatewayConfig): boolean {
  if (!config.idempotency?.enabled) {
    return false;
  }

  if (request.method.toUpperCase() !== 'POST') {
    return false;
  }

  const path = request.url.split('?')[0] || '';
  return path.startsWith('/v1/') || path.startsWith('/v1beta/');
}

function readIdempotencyKey(request: FastifyRequest, config: GatewayConfig): string | undefined {
  const headerName = config.idempotency.headerName.trim().toLowerCase();
  if (!headerName) {
    return undefined;
  }

  const value = readHeader(request.headers[headerName]);
  const normalized = value?.trim();
  return normalized || undefined;
}

function createPendingEntry(requestHash: string, expiresAt: number): PendingIdempotencyEntry {
  let resolve!: (response: CachedIdempotencyResponse | undefined) => void;
  const promise = new Promise<CachedIdempotencyResponse | undefined>((innerResolve) => {
    resolve = innerResolve;
  });

  return {
    state: 'pending',
    requestHash,
    expiresAt,
    promise,
    resolve
  };
}

function sendCachedResponse(reply: FastifyReply, response: CachedIdempotencyResponse) {
  for (const [name, value] of Object.entries(response.headers)) {
    reply.header(name, value);
  }

  return reply
    .code(response.statusCode)
    .header('x-gateway-idempotency-status', 'replayed')
    .send(Buffer.isBuffer(response.payload) ? Buffer.from(response.payload) : response.payload);
}

function sendIdempotencyConflict(reply: FastifyReply) {
  return reply
    .code(409)
    .header('x-gateway-idempotency-status', 'conflict')
    .send({
      error: {
        message: 'Idempotency key was reused with a different request.',
        code: 'idempotency_key_conflict'
      }
    });
}

function sendIdempotencyResponseNotCacheable(reply: FastifyReply) {
  return reply
    .code(409)
    .header('x-gateway-idempotency-status', 'not-cacheable')
    .send({
      error: {
        message: 'Original request did not produce a cacheable idempotency response.',
        code: 'idempotency_response_not_cacheable'
      }
    });
}

function sendIdempotencyInProgress(reply: FastifyReply) {
  return reply
    .code(409)
    .header('x-gateway-idempotency-status', 'in-progress')
    .send({
      error: {
        message: 'Original request is still in progress.',
        code: 'idempotency_request_in_progress'
      }
    });
}

function sendIdempotencyStoreUnavailable(reply: FastifyReply) {
  return reply
    .code(503)
    .header('x-gateway-idempotency-status', 'store-unavailable')
    .send({
      error: {
        message: 'Gateway idempotency store is unavailable.',
        code: 'idempotency_store_unavailable'
      }
    });
}

function buildCachedResponse(
  reply: FastifyReply,
  payload: unknown,
  config: GatewayConfig
): CachedIdempotencyResponse | undefined {
  if (!isCacheableStatus(reply.statusCode, config)) {
    return undefined;
  }

  if (isEventStreamResponse(reply)) {
    return undefined;
  }

  if (typeof payload !== 'string' && !Buffer.isBuffer(payload)) {
    return undefined;
  }

  if (cachedPayloadSize(payload) > config.idempotency.maxResponseBytes) {
    return undefined;
  }

  return {
    statusCode: reply.statusCode,
    headers: sanitizeCachedHeaders(reply.getHeaders()),
    payload: Buffer.isBuffer(payload) ? Buffer.from(payload) : payload
  };
}

function isReadablePayload(payload: unknown): payload is Readable {
  if (payload instanceof Readable) {
    return true;
  }

  if (!isObject(payload)) {
    return false;
  }

  const candidate = payload as {
    pipe?: unknown;
    on?: unknown;
    once?: unknown;
  };
  return (
    typeof candidate.pipe === 'function' &&
    typeof candidate.on === 'function' &&
    typeof candidate.once === 'function'
  );
}

function normalizeStreamChunk(chunk: unknown): Buffer | undefined {
  if (Buffer.isBuffer(chunk)) {
    return Buffer.from(chunk);
  }

  if (typeof chunk === 'string') {
    return Buffer.from(chunk, 'utf8');
  }

  if (chunk instanceof Uint8Array) {
    return Buffer.from(chunk);
  }

  return undefined;
}

function isCacheableStatus(statusCode: number, config: GatewayConfig): boolean {
  if (statusCode >= 200 && statusCode < 300) {
    return true;
  }

  return config.idempotency.cacheErrorResponses && statusCode >= 400 && statusCode < 500;
}

function isEventStreamResponse(reply: FastifyReply): boolean {
  const contentType = String(reply.getHeader('content-type') || '').toLowerCase();
  return contentType.includes('text/event-stream');
}

function sanitizeCachedHeaders(headers: Record<string, unknown>): Record<string, CachedHeaderValue> {
  const cached: Record<string, CachedHeaderValue> = {};

  for (const [rawName, rawValue] of Object.entries(headers)) {
    const name = rawName.toLowerCase();
    if (hopByHopHeaders.has(name) || name === 'x-gateway-idempotency-status') {
      continue;
    }

    if (typeof rawValue === 'string' || typeof rawValue === 'number') {
      cached[name] = rawValue;
    } else if (
      Array.isArray(rawValue) &&
      rawValue.every((item): item is string => typeof item === 'string')
    ) {
      cached[name] = rawValue;
    }
  }

  return cached;
}

async function waitForRedisIdempotencyCompletion(
  storage: GatewayIdempotencyRedisStorageConfig,
  redisKey: string,
  requestHash: string,
  config: GatewayConfig
): Promise<
  | { status: 'completed'; response: CachedIdempotencyResponse }
  | { status: 'conflict' | 'in_progress' | 'not_cacheable' }
> {
  const deadline = Date.now() + resolvePositiveInteger(config.idempotency.pendingWaitTimeoutMs, 30000);
  while (Date.now() < deadline) {
    await sleep(resolvePositiveInteger(config.idempotency.pollIntervalMs, 100));
    const entry = parseRedisIdempotencyEntry(
      await commandRedisIdempotency(storage, ['GET', redisKey])
    );
    if (!entry) {
      return { status: 'not_cacheable' };
    }
    if (entry.requestHash !== requestHash) {
      return { status: 'conflict' };
    }
    if (entry.state === 'completed') {
      return {
        status: 'completed',
        response: deserializeRedisCachedResponse(entry.response)
      };
    }
  }

  return { status: 'in_progress' };
}

function serializeRedisIdempotencyEntry(entry: RedisIdempotencyEntry): string {
  return JSON.stringify(entry);
}

function parseRedisIdempotencyEntry(value: RedisReply): RedisIdempotencyEntry | undefined {
  if (typeof value !== 'string' || !value.trim()) {
    return undefined;
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    return undefined;
  }
  if (!isObject(parsed)) {
    return undefined;
  }
  const requestHash = typeof parsed.requestHash === 'string' ? parsed.requestHash : undefined;
  if (!requestHash) {
    return undefined;
  }
  if (parsed.state === 'pending') {
    const ownerToken = typeof parsed.ownerToken === 'string' ? parsed.ownerToken : undefined;
    return {
      state: 'pending',
      requestHash,
      ...(ownerToken ? { ownerToken } : {})
    };
  }
  if (parsed.state === 'completed') {
    const response = parseRedisCachedResponse(parsed.response);
    if (!response) {
      return undefined;
    }
    return {
      state: 'completed',
      requestHash,
      response
    };
  }

  return undefined;
}

function serializeRedisCachedResponse(response: CachedIdempotencyResponse): SerializedRedisCachedResponse {
  return {
    statusCode: response.statusCode,
    headers: response.headers,
    payload: Buffer.isBuffer(response.payload)
      ? {
          type: 'buffer',
          data: response.payload.toString('base64')
        }
      : {
          type: 'string',
          data: response.payload
        }
  };
}

function parseRedisCachedResponse(value: unknown): SerializedRedisCachedResponse | undefined {
  if (!isObject(value)) {
    return undefined;
  }
  const statusCode = Number(value.statusCode);
  if (!Number.isInteger(statusCode) || statusCode < 100 || statusCode > 599) {
    return undefined;
  }
  const headers = parseRedisCachedHeaders(value.headers);
  const payload = isObject(value.payload)
    ? parseRedisCachedPayload(value.payload)
    : undefined;
  if (!payload) {
    return undefined;
  }

  return {
    statusCode,
    headers,
    payload
  };
}

function parseRedisCachedHeaders(value: unknown): Record<string, CachedHeaderValue> {
  const headers: Record<string, CachedHeaderValue> = {};
  if (!isObject(value)) {
    return headers;
  }
  for (const [name, headerValue] of Object.entries(value)) {
    if (typeof headerValue === 'string' || typeof headerValue === 'number') {
      headers[name] = headerValue;
    } else if (
      Array.isArray(headerValue) &&
      headerValue.every((item): item is string => typeof item === 'string')
    ) {
      headers[name] = headerValue;
    }
  }
  return headers;
}

function parseRedisCachedPayload(value: Record<string, unknown>): SerializedRedisCachedPayload | undefined {
  if (
    (value.type === 'string' || value.type === 'buffer') &&
    typeof value.data === 'string'
  ) {
    return {
      type: value.type,
      data: value.data
    };
  }

  return undefined;
}

function deserializeRedisCachedResponse(response: SerializedRedisCachedResponse): CachedIdempotencyResponse {
  return {
    statusCode: response.statusCode,
    headers: response.headers,
    payload: response.payload.type === 'buffer'
      ? Buffer.from(response.payload.data, 'base64')
      : response.payload.data
  };
}

function parseRedisClaimResult(value: RedisReply): { claimed: boolean; entry: RedisReply } | undefined {
  if (!Array.isArray(value) || value.length < 2) {
    return undefined;
  }

  return {
    claimed: Number(value[0]) === 1,
    entry: value[1]
  };
}

async function commandRedisIdempotency(
  storage: GatewayIdempotencyRedisStorageConfig,
  args: string[]
): Promise<RedisReply> {
  if (redisIdempotencyCommandExecutorForTests) {
    return redisIdempotencyCommandExecutorForTests(storage, args);
  }

  return getRedisIdempotencyClient(storage).command(args);
}

function getRedisIdempotencyClient(
  storage: GatewayIdempotencyRedisStorageConfig
): SimpleRedisClient {
  const cacheKey = JSON.stringify({
    url: storage.url || 'redis://127.0.0.1:6379/0',
    keyPrefix: storage.keyPrefix || 'next-ai:gateway:idempotency',
    connectTimeoutMs: resolvePositiveInteger(storage.connectTimeoutMs, 1000),
    commandTimeoutMs: resolvePositiveInteger(storage.commandTimeoutMs, 1000)
  });
  const existing = redisIdempotencyClients.get(cacheKey);
  if (existing) {
    return existing;
  }

  const client = new SimpleRedisClient({
    url: storage.url || 'redis://127.0.0.1:6379/0',
    connectTimeoutMs: resolvePositiveInteger(storage.connectTimeoutMs, 1000),
    commandTimeoutMs: resolvePositiveInteger(storage.commandTimeoutMs, 1000),
    errorPrefix: 'Redis idempotency'
  });
  redisIdempotencyClients.set(cacheKey, client);
  return client;
}

function buildRedisIdempotencyKey(
  storage: GatewayIdempotencyRedisStorageConfig,
  storeKey: string
): string {
  const prefix = (storage.keyPrefix || 'next-ai:gateway:idempotency').replace(/:+$/, '') || 'next-ai:gateway:idempotency';
  return `${prefix}:${hashStableValue(storeKey)}`;
}

function pruneIdempotencyStore(config: GatewayConfig, now: number): void {
  for (const [key, entry] of idempotencyStore) {
    if (entry.expiresAt <= now) {
      deleteMemoryIdempotencyEntry(key);
      if (entry.state === 'pending') {
        entry.resolve(undefined);
      }
    }
  }

  while (
    idempotencyStore.size > config.idempotency.maxEntries ||
    idempotencyStoreBytes > config.idempotency.maxTotalBytes
  ) {
    const oldestCompleted = Array.from(idempotencyStore.entries()).find(
      ([, entry]) => entry.state === 'completed'
    );
    const [key, entry] = oldestCompleted || idempotencyStore.entries().next().value || [];
    if (!key || !entry) {
      return;
    }

    deleteMemoryIdempotencyEntry(key);
    if (entry.state === 'pending') {
      entry.resolve(undefined);
    }
  }
}

function setCompletedMemoryIdempotencyEntry(
  key: string,
  entry: CompletedIdempotencyEntry
): void {
  deleteMemoryIdempotencyEntry(key);
  idempotencyStore.set(key, entry);
  idempotencyStoreBytes += entry.sizeBytes;
}

function deleteMemoryIdempotencyEntry(key: string): IdempotencyEntry | undefined {
  const entry = idempotencyStore.get(key);
  if (!entry) {
    return undefined;
  }

  idempotencyStore.delete(key);
  if (entry.state === 'completed') {
    idempotencyStoreBytes = Math.max(0, idempotencyStoreBytes - entry.sizeBytes);
  }
  return entry;
}

function cachedResponseSize(response: CachedIdempotencyResponse): number {
  return cachedPayloadSize(response.payload);
}

function cachedPayloadSize(payload: CachedPayload): number {
  return Buffer.isBuffer(payload) ? payload.byteLength : Buffer.byteLength(payload);
}

async function waitForMemoryIdempotencyCompletion(
  promise: Promise<CachedIdempotencyResponse | undefined>,
  timeoutMs: number
): Promise<
  | { status: 'completed'; response: CachedIdempotencyResponse | undefined }
  | { status: 'in_progress' }
> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise.then((response) => ({ status: 'completed' as const, response })),
      new Promise<{ status: 'in_progress' }>((resolve) => {
        timer = setTimeout(
          () => resolve({ status: 'in_progress' }),
          resolvePositiveInteger(timeoutMs, 30000)
        );
      })
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

function hashIdempotencyRequest(
  request: FastifyRequest,
  config: GatewayConfig,
  extraFingerprint?: unknown
): string {
  const hash = createHash('sha256');
  hash.update(request.method.toUpperCase());
  hash.update('\n');
  hash.update(request.url);
  hash.update('\n');
  hash.update(config.idempotency.headerName.trim().toLowerCase());
  hash.update('\n');
  hash.update(stableStringify(normalizeGatewayIdentityForFingerprint(request.gatewayIdentity)));
  hash.update('\n');
  hash.update(stableStringify(selectFingerprintHeaders(request)));
  hash.update('\n');
  updateIdempotencyBodyHash(hash, request.body);
  if (extraFingerprint !== undefined) {
    hash.update('\n');
    hash.update(stableStringify(extraFingerprint));
  }
  return hash.digest('hex');
}

function updateIdempotencyBodyHash(hash: ReturnType<typeof createHash>, body: unknown): void {
  if (Buffer.isBuffer(body)) {
    hash.update('Buffer\n');
    hash.update(body);
    return;
  }

  if (body instanceof ArrayBuffer) {
    hash.update('ArrayBuffer\n');
    hash.update(Buffer.from(body));
    return;
  }

  if (typeof ArrayBuffer !== 'undefined' && ArrayBuffer.isView(body)) {
    hash.update(`${body.constructor.name || 'ArrayBufferView'}\n`);
    hash.update(Buffer.from(body.buffer, body.byteOffset, body.byteLength));
    return;
  }

  hash.update(stableStringify(body));
}

function buildIdempotencyStoreKey(request: FastifyRequest, idempotencyKey: string): string {
  const identity = request.gatewayIdentity;
  if (identity?.billingSubjectKey) {
    return `identity:${identity.source}:${hashStableValue(identity.billingSubjectKey)}:${idempotencyKey}`;
  }

  const fingerprintHeaders = selectFingerprintHeaders(request);
  if (Object.keys(fingerprintHeaders).length > 0) {
    return `headers:${hashStableValue(fingerprintHeaders)}:${idempotencyKey}`;
  }

  return `anonymous:${idempotencyKey}`;
}

function selectFingerprintHeaders(request: FastifyRequest): Record<string, string | string[]> {
  const selected: Record<string, string | string[]> = {};
  for (const headerName of routeSensitiveHeaders) {
    const value = request.headers[headerName];
    if (typeof value === 'string') {
      selected[headerName] = value;
    } else if (Array.isArray(value)) {
      selected[headerName] = value.map(String);
    }
  }

  return selected;
}

function normalizeGatewayIdentityForFingerprint(
  identity: GatewayRequestIdentity | undefined
): Record<string, string> | undefined {
  if (!identity) {
    return undefined;
  }

  const normalized: Record<string, string> = {
    source: identity.source,
    billingSubjectKey: identity.billingSubjectKey
  };

  for (const key of ['userId', 'tenantId', 'subject', 'organizationId', 'plan', 'apiKeyId'] as const) {
    const value = identity[key];
    if (typeof value === 'string') {
      normalized[key] = value;
    }
  }

  return normalized;
}

function hashStableValue(value: unknown): string {
  return createHash('sha256').update(stableStringify(value)).digest('hex');
}

function stableStringify(value: unknown): string {
  return JSON.stringify(sortStable(value)) ?? 'undefined';
}

function sortStable(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(sortStable);
  }

  if (!isObject(value)) {
    return value;
  }

  const sorted: Record<string, unknown> = {};
  for (const key of Object.keys(value).sort()) {
    sorted[key] = sortStable(value[key]);
  }
  return sorted;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, Math.max(ms, 1)));
}

function resolvePositiveInteger(value: number | undefined, fallback: number): number {
  if (!Number.isFinite(value) || value === undefined || value <= 0) {
    return fallback;
  }

  return Math.trunc(value);
}
