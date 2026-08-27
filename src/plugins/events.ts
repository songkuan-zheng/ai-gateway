import type { GatewayConfig, GatewayPluginConfig } from '../types';
import { recordGatewayPluginDelivery } from '../gateway/metrics';

export interface GatewayPluginLifecycleLogger {
  debug?: (payload: unknown, message?: string) => void;
  info?: (payload: unknown, message?: string) => void;
  warn?: (payload: unknown, message?: string) => void;
  error?: (payload: unknown, message?: string) => void;
}

export interface GatewayPluginLifecycleContext {
  config?: GatewayConfig;
  plugin?: GatewayPluginConfig;
  logger?: GatewayPluginLifecycleLogger;
}

export type GatewayPluginHealthStatus = 'healthy' | 'degraded' | 'unhealthy' | 'unknown';

export interface GatewayPluginHealth {
  status: GatewayPluginHealthStatus;
  message?: string;
  details?: unknown;
  checkedAt?: string;
}

export interface GatewayPluginDeliveryOptions {
  timeoutMs?: number;
  maxAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  backoffMultiplier?: number;
  concurrency?: number;
  maxQueueSize?: number;
  dedupe?: boolean;
  dedupeTtlMs?: number;
  deadLetter?: boolean | GatewayPluginDeadLetterOptions;
}

export interface GatewayPluginDeliveryContext {
  signal: AbortSignal;
  attempt: number;
  maxAttempts: number;
  eventId?: string;
  idempotencyKey?: string;
}

export interface GatewayPluginDeadLetterOptions {
  enabled?: boolean;
  maxEntries?: number;
}

export interface GatewayPluginDeadLetter<TEvent = unknown> {
  id: string;
  extensionKey: string;
  transport?: string;
  eventId?: string;
  failedAt: string;
  attempts: number;
  error: string;
  event: TEvent;
}

export interface GatewayPluginDeliveryStateStore extends GatewayPluginExtension {
  claimDelivery?(idempotencyKey: string, ttlMs: number): boolean | Promise<boolean>;
  releaseDeliveryClaim?(idempotencyKey: string): void | Promise<void>;
  writeDeadLetter?(
    entry: GatewayPluginDeadLetter,
    options: GatewayPluginDeadLetterOptions
  ): void | Promise<void>;
  listDeadLetters?(extensionKey?: string): GatewayPluginDeadLetter[] | Promise<GatewayPluginDeadLetter[]>;
  clearDeadLetters?(extensionKey?: string): number | Promise<number>;
}

export interface GatewayPluginExtension {
  key: string;
  delivery?: GatewayPluginDeliveryOptions;
  init?(context: GatewayPluginLifecycleContext): void | Promise<void>;
  ready?(): boolean | Promise<boolean>;
  health?(): GatewayPluginHealth | Promise<GatewayPluginHealth>;
  flush?(): void | Promise<void>;
  drain?(): void | Promise<void>;
  deadLetter?(entry: GatewayPluginDeadLetter): void | Promise<void>;
  close?(): void | Promise<void>;
}

export interface GatewayPluginEventPublisher<TEvent = unknown> extends GatewayPluginExtension {
  transport?: string;
  publish(event: TEvent, context?: GatewayPluginDeliveryContext): boolean | void | Promise<boolean | void>;
}

export interface GatewayPluginOutbox<TEvent = unknown> extends GatewayPluginExtension {
  transport?: string;
  append(event: TEvent, context?: GatewayPluginDeliveryContext): boolean | void | Promise<boolean | void>;
}

interface RegisterOptions {
  overwrite?: boolean;
}

const closedExtensions = new WeakSet<GatewayPluginExtension>();
const initializedExtensions = new WeakSet<GatewayPluginExtension>();
const deliveryLimiters = new WeakMap<GatewayPluginExtension, GatewayPluginDeliveryLimiter>();
const deliveredEventKeys = new Map<string, number>();
const deadLettersByExtension = new Map<string, GatewayPluginDeadLetter[]>();
let deliveryStateStores: GatewayPluginDeliveryStateStore[] = [];
const defaultDeadLetterMaxEntries = 1000;
const memoryDeliveryStateStore: GatewayPluginDeliveryStateStore = {
  key: 'memory',
  claimDelivery(idempotencyKey, ttlMs) {
    return claimGatewayPluginEventInMemory(idempotencyKey, ttlMs);
  },
  releaseDeliveryClaim(idempotencyKey) {
    deliveredEventKeys.delete(idempotencyKey);
  },
  writeDeadLetter(entry, options) {
    writeGatewayPluginDeadLetterInMemory(entry, options);
  },
  listDeadLetters(extensionKey) {
    return listGatewayPluginDeadLettersInMemory(extensionKey);
  },
  clearDeadLetters(extensionKey) {
    return clearGatewayPluginDeadLettersInMemory(extensionKey);
  }
};

export class GatewayPluginExtensionRegistry<T extends GatewayPluginExtension> {
  private readonly extensions = new Map<string, T>();

  register(extension: T, options?: RegisterOptions): void {
    const exists = this.extensions.has(extension.key);
    if (exists && !options?.overwrite) {
      throw new Error(`Gateway plugin extension already registered: ${extension.key}`);
    }

    closedExtensions.delete(extension);
    this.extensions.set(extension.key, extension);
  }

  get(key: string): T | undefined {
    return this.extensions.get(key);
  }

  unregister(key: string): T | undefined {
    const existing = this.extensions.get(key);
    this.extensions.delete(key);
    return existing;
  }

  list(): T[] {
    return Array.from(this.extensions.values());
  }

  clear(): T[] {
    const existing = this.list();
    this.extensions.clear();
    return existing;
  }
}

export async function closeGatewayPluginExtensions(
  extensions: Iterable<GatewayPluginExtension>
): Promise<void> {
  await Promise.allSettled(
    Array.from(uniqueGatewayPluginExtensions(extensions), closeGatewayPluginExtension)
  );
}

export async function initializeGatewayPluginExtensions(
  extensions: Iterable<GatewayPluginExtension>,
  context: GatewayPluginLifecycleContext = {}
): Promise<void> {
  for (const extension of uniqueGatewayPluginExtensions(extensions)) {
    if (initializedExtensions.has(extension) && !closedExtensions.has(extension)) {
      continue;
    }

    closedExtensions.delete(extension);
    try {
      await extension.init?.(context);
      const ready = await extension.ready?.();
      if (ready === false) {
        throw new Error(`Gateway plugin extension is not ready: ${extension.key}`);
      }
      initializedExtensions.add(extension);
    } catch (error) {
      await closeGatewayPluginExtensions([extension]);
      throw error;
    }
  }
}

export async function collectGatewayPluginExtensionHealth(
  extensions: Iterable<GatewayPluginExtension>
): Promise<Array<GatewayPluginHealth & { key: string }>> {
  const results: Array<GatewayPluginHealth & { key: string }> = [];
  for (const extension of uniqueGatewayPluginExtensions(extensions)) {
    try {
      const health: GatewayPluginHealth = extension.health
        ? await extension.health()
        : {
            status: initializedExtensions.has(extension) && !closedExtensions.has(extension)
              ? 'healthy'
              : 'unknown'
          };
      results.push({
        key: extension.key,
        ...health
      });
    } catch (error) {
      results.push({
        key: extension.key,
        status: 'unhealthy',
        message: error instanceof Error ? error.message : String(error),
        checkedAt: new Date().toISOString()
      });
    }
  }

  return results;
}

export async function executeGatewayPluginDelivery(
  extension: GatewayPluginExtension,
  operation: (context: GatewayPluginDeliveryContext) => boolean | void | Promise<boolean | void>
): Promise<boolean> {
  return executeGatewayPluginEventDelivery(extension, undefined, operation);
}

export async function executeGatewayPluginEventDelivery<TEvent>(
  extension: GatewayPluginExtension,
  event: TEvent,
  operation: (context: GatewayPluginDeliveryContext) => boolean | void | Promise<boolean | void>
): Promise<boolean> {
  const options = normalizeDeliveryOptions(extension.delivery);
  const eventId = readGatewayPluginEventId(event);
  const idempotencyKey = eventId ? `${extension.key}:${eventId}` : undefined;
  let deliveryClaimed = false;

  try {
    if (idempotencyKey && options.dedupe) {
      deliveryClaimed = await claimGatewayPluginEvent(idempotencyKey, options.dedupeTtlMs);
      if (!deliveryClaimed) {
        recordGatewayPluginDelivery({
          extensionKey: extension.key,
          transport: readGatewayPluginExtensionTransport(extension),
          outcome: 'not_delivered'
        });
        return false;
      }
    }

    const delivered = await runWithGatewayPluginDeliveryLimiter(extension, options, () =>
      createGatewayPluginDeliveryWithRetryExecution(
        extension,
        eventId,
        idempotencyKey,
        operation,
        options
      )
    );
    if (!delivered && deliveryClaimed && idempotencyKey) {
      await releaseGatewayPluginEventClaim(idempotencyKey);
      deliveryClaimed = false;
    }
    recordGatewayPluginDelivery({
      extensionKey: extension.key,
      transport: readGatewayPluginExtensionTransport(extension),
      outcome: delivered ? 'delivered' : 'not_delivered'
    });
    return delivered;
  } catch (error) {
    if (
      deliveryClaimed &&
      idempotencyKey &&
      !isGatewayPluginDeliveryTimeoutError(error)
    ) {
      await releaseGatewayPluginEventClaim(idempotencyKey);
      deliveryClaimed = false;
    }
    recordGatewayPluginDelivery({
      extensionKey: extension.key,
      transport: readGatewayPluginExtensionTransport(extension),
      outcome: isGatewayPluginDeliveryQueueFullError(error)
        ? 'queue_full'
        : isGatewayPluginDeliveryTimeoutError(error)
          ? 'timeout'
          : 'failed'
    });
    if (event !== undefined && shouldDeadLetterGatewayPluginDelivery(extension, options)) {
      await writeGatewayPluginDeadLetter(extension, event, options, error);
    }
    throw error;
  }
}

export function configureGatewayPluginDeliveryStateStores(
  stores: GatewayPluginDeliveryStateStore[] = []
): void {
  deliveryStateStores = stores;
}

export async function listGatewayPluginDeadLetters(
  extensionKey?: string
): Promise<GatewayPluginDeadLetter[]> {
  return await resolveGatewayPluginDeadLetterStateStore()
    .listDeadLetters?.(extensionKey) || [];
}

export async function clearGatewayPluginDeadLetters(extensionKey?: string): Promise<number> {
  return await resolveGatewayPluginDeadLetterStateStore()
    .clearDeadLetters?.(extensionKey) || 0;
}

async function closeGatewayPluginExtension(extension: GatewayPluginExtension): Promise<void> {
  if (closedExtensions.has(extension)) {
    return;
  }

  closedExtensions.add(extension);
  initializedExtensions.delete(extension);

  const errors: unknown[] = [];
  for (const method of [extension.drain, extension.flush, extension.close]) {
    if (!method) {
      continue;
    }

    try {
      await method.call(extension);
    } catch (error) {
      errors.push(error);
    }
  }

  if (errors.length > 0) {
    throw errors[0];
  }
}

function uniqueGatewayPluginExtensions(
  extensions: Iterable<GatewayPluginExtension>
): GatewayPluginExtension[] {
  return Array.from(new Set(extensions));
}

function readGatewayPluginExtensionTransport(extension: GatewayPluginExtension): string | undefined {
  return (extension as { transport?: string }).transport;
}

function readGatewayPluginEventId(event: unknown): string | undefined {
  if (!event || typeof event !== 'object') {
    return undefined;
  }

  const source = event as Record<string, unknown>;
  for (const key of ['eventId', 'id', 'requestId']) {
    const value = source[key];
    if (typeof value === 'string' && value.trim()) {
      return value.trim();
    }
  }

  return undefined;
}

interface NormalizedDeliveryOptions {
  timeoutMs?: number;
  maxAttempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
  backoffMultiplier: number;
  concurrency?: number;
  maxQueueSize?: number;
  dedupe: boolean;
  dedupeTtlMs: number;
  deadLetter: GatewayPluginDeadLetterOptions;
}

function normalizeDeliveryOptions(options: GatewayPluginDeliveryOptions | undefined): NormalizedDeliveryOptions {
  const baseDelayMs = normalizeNonNegativeInteger(options?.baseDelayMs, 100);
  return {
    timeoutMs: normalizeOptionalPositiveInteger(options?.timeoutMs),
    maxAttempts: normalizePositiveInteger(options?.maxAttempts, 1),
    baseDelayMs,
    maxDelayMs: Math.max(normalizeNonNegativeInteger(options?.maxDelayMs, baseDelayMs), baseDelayMs),
    backoffMultiplier: normalizePositiveNumber(options?.backoffMultiplier, 2),
    concurrency: normalizeOptionalPositiveInteger(options?.concurrency),
    maxQueueSize: normalizeOptionalNonNegativeInteger(options?.maxQueueSize),
    dedupe: options?.dedupe === true,
    dedupeTtlMs: normalizePositiveInteger(options?.dedupeTtlMs, 300000),
    deadLetter: normalizeDeadLetterOptions(options?.deadLetter)
  };
}

function normalizeDeadLetterOptions(
  value: GatewayPluginDeliveryOptions['deadLetter']
): GatewayPluginDeadLetterOptions {
  if (value === true) {
    return {
      enabled: true,
      maxEntries: defaultDeadLetterMaxEntries
    };
  }
  if (!value || typeof value !== 'object') {
    return {
      enabled: false,
      maxEntries: defaultDeadLetterMaxEntries
    };
  }

  return {
    enabled: value.enabled !== false,
    maxEntries: normalizePositiveInteger(value.maxEntries, defaultDeadLetterMaxEntries)
  };
}

async function claimGatewayPluginEvent(key: string, ttlMs: number): Promise<boolean> {
  return Boolean(await resolveGatewayPluginDedupeStateStore().claimDelivery?.(key, ttlMs));
}

async function releaseGatewayPluginEventClaim(key: string): Promise<void> {
  await resolveGatewayPluginDedupeStateStore().releaseDeliveryClaim?.(key);
}

function claimGatewayPluginEventInMemory(key: string, ttlMs: number): boolean {
  const now = Date.now();
  const expiresAt = deliveredEventKeys.get(key);
  if (expiresAt && expiresAt > now) {
    return false;
  }
  if (expiresAt) {
    deliveredEventKeys.delete(key);
  }
  deliveredEventKeys.set(key, now + ttlMs);
  for (const [eventKey, expiresAt] of deliveredEventKeys) {
    if (expiresAt <= now) {
      deliveredEventKeys.delete(eventKey);
    }
  }
  return true;
}

function shouldDeadLetterGatewayPluginDelivery(
  extension: GatewayPluginExtension,
  options: NormalizedDeliveryOptions
): boolean {
  return options.deadLetter.enabled === true || typeof extension.deadLetter === 'function';
}

async function writeGatewayPluginDeadLetter<TEvent>(
  extension: GatewayPluginExtension,
  event: TEvent,
  options: NormalizedDeliveryOptions,
  error: unknown
): Promise<void> {
  const entry: GatewayPluginDeadLetter<TEvent> = {
    id: `${extension.key}:${Date.now()}:${Math.random().toString(36).slice(2)}`,
    extensionKey: extension.key,
    transport: readGatewayPluginExtensionTransport(extension),
    eventId: readGatewayPluginEventId(event),
    failedAt: new Date().toISOString(),
    attempts: options.maxAttempts,
    error: error instanceof Error ? error.message : String(error),
    event
  };

  if (options.deadLetter.enabled) {
    await resolveGatewayPluginDeadLetterStateStore()
      .writeDeadLetter?.(entry, options.deadLetter);
  }

  await extension.deadLetter?.(entry);
}

function resolveGatewayPluginDedupeStateStore(): GatewayPluginDeliveryStateStore {
  return deliveryStateStores.find(hasGatewayPluginDedupeStateStoreMethods) ||
    memoryDeliveryStateStore;
}

function resolveGatewayPluginDeadLetterStateStore(): GatewayPluginDeliveryStateStore {
  return deliveryStateStores.find(hasGatewayPluginDeadLetterStateStoreMethods) ||
    memoryDeliveryStateStore;
}

function hasGatewayPluginDedupeStateStoreMethods(store: GatewayPluginDeliveryStateStore): boolean {
  return (
    typeof store.claimDelivery === 'function' &&
    typeof store.releaseDeliveryClaim === 'function'
  );
}

function hasGatewayPluginDeadLetterStateStoreMethods(store: GatewayPluginDeliveryStateStore): boolean {
  return (
    typeof store.writeDeadLetter === 'function' &&
    typeof store.listDeadLetters === 'function' &&
    typeof store.clearDeadLetters === 'function'
  );
}

function listGatewayPluginDeadLettersInMemory(extensionKey?: string): GatewayPluginDeadLetter[] {
  if (extensionKey) {
    return [...(deadLettersByExtension.get(extensionKey) || [])];
  }

  return Array.from(deadLettersByExtension.values()).flatMap((entries) => entries);
}

function clearGatewayPluginDeadLettersInMemory(extensionKey?: string): number {
  if (extensionKey) {
    const count = deadLettersByExtension.get(extensionKey)?.length || 0;
    deadLettersByExtension.delete(extensionKey);
    return count;
  }

  let count = 0;
  for (const entries of deadLettersByExtension.values()) {
    count += entries.length;
  }
  deadLettersByExtension.clear();
  return count;
}

function writeGatewayPluginDeadLetterInMemory(
  entry: GatewayPluginDeadLetter,
  options: GatewayPluginDeadLetterOptions
): void {
  const entries = deadLettersByExtension.get(entry.extensionKey) || [];
  entries.push(entry);
  const maxEntries = options.maxEntries || defaultDeadLetterMaxEntries;
  if (entries.length > maxEntries) {
    entries.splice(0, entries.length - maxEntries);
  }
  deadLettersByExtension.set(entry.extensionKey, entries);
}

interface GatewayPluginDeliveryExecution<T> {
  result: Promise<T>;
  completion: Promise<void>;
}

function createGatewayPluginDeliveryWithRetryExecution(
  extension: GatewayPluginExtension,
  eventId: string | undefined,
  idempotencyKey: string | undefined,
  operation: (context: GatewayPluginDeliveryContext) => boolean | void | Promise<boolean | void>,
  options: NormalizedDeliveryOptions
): GatewayPluginDeliveryExecution<boolean> {
  let currentAttemptCompletion = Promise.resolve();
  const result = (async (): Promise<boolean> => {
    let lastError: unknown;
    for (let attempt = 1; attempt <= options.maxAttempts; attempt += 1) {
      const execution = createGatewayPluginDeliveryAttemptExecution(
        extension,
        eventId,
        idempotencyKey,
        operation,
        options,
        attempt
      );
      currentAttemptCompletion = execution.completion;
      try {
        const attemptResult = await execution.result;
        return attemptResult !== false;
      } catch (error) {
        lastError = error;
        if (isGatewayPluginDeliveryTimeoutError(error) || attempt >= options.maxAttempts) {
          break;
        }
        await sleep(resolveGatewayPluginRetryDelayMs(attempt, options));
      }
    }

    throw lastError instanceof Error ? lastError : new Error(String(lastError));
  })();
  const completion = result.then(
    () => currentAttemptCompletion,
    () => currentAttemptCompletion
  ).then(
    () => undefined,
    () => undefined
  );
  return { result, completion };
}

function createGatewayPluginDeliveryAttemptExecution(
  extension: GatewayPluginExtension,
  eventId: string | undefined,
  idempotencyKey: string | undefined,
  operation: (context: GatewayPluginDeliveryContext) => boolean | void | Promise<boolean | void>,
  options: NormalizedDeliveryOptions,
  attempt: number
): GatewayPluginDeliveryExecution<boolean | void> {
  const abortController = new AbortController();
  const deliveryContext: GatewayPluginDeliveryContext = {
    signal: abortController.signal,
    attempt,
    maxAttempts: options.maxAttempts,
    eventId,
    idempotencyKey
  };
  const promise = Promise.resolve().then(() => operation(deliveryContext));
  const completion = promise.then(
    () => undefined,
    () => undefined
  );
  if (!options.timeoutMs) {
    return { result: promise, completion };
  }

  let timeout: ReturnType<typeof setTimeout>;
  const result = Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(() => {
          abortController.abort(new Error(`Gateway plugin delivery timed out: ${extension.key}`));
          reject(new GatewayPluginDeliveryTimeoutError(`Gateway plugin delivery timed out: ${extension.key}`));
        }, options.timeoutMs);
      })
    ]).finally(() => {
      clearTimeout(timeout);
    });
  return { result, completion };
}

function resolveGatewayPluginRetryDelayMs(
  failedAttempt: number,
  options: NormalizedDeliveryOptions
): number {
  const delay = options.baseDelayMs * Math.pow(options.backoffMultiplier, failedAttempt - 1);
  return Math.min(options.maxDelayMs, delay);
}

function runWithGatewayPluginDeliveryLimiter<T>(
  extension: GatewayPluginExtension,
  options: NormalizedDeliveryOptions,
  task: () => GatewayPluginDeliveryExecution<T>
): Promise<T> {
  if (!options.concurrency) {
    return task().result;
  }

  let limiter = deliveryLimiters.get(extension);
  if (!limiter) {
    limiter = new GatewayPluginDeliveryLimiter(options.concurrency, options.maxQueueSize);
    deliveryLimiters.set(extension, limiter);
  } else {
    limiter.configure(options.concurrency, options.maxQueueSize);
  }

  return limiter.run(task);
}

class GatewayPluginDeliveryLimiter {
  private active = 0;
  private readonly queue: Array<() => void> = [];

  constructor(
    private concurrency: number,
    private maxQueueSize: number | undefined
  ) {}

  configure(concurrency: number, maxQueueSize: number | undefined): void {
    this.concurrency = concurrency;
    this.maxQueueSize = maxQueueSize;
    this.drain();
  }

  run<T>(task: () => GatewayPluginDeliveryExecution<T>): Promise<T> {
    if (this.active < this.concurrency) {
      return this.start(task);
    }

    if (this.maxQueueSize !== undefined && this.queue.length >= this.maxQueueSize) {
      return Promise.reject(new GatewayPluginDeliveryQueueFullError('Gateway plugin delivery queue is full.'));
    }

    return new Promise<T>((resolve, reject) => {
      this.queue.push(() => {
        this.start(task).then(resolve, reject);
      });
    });
  }

  private start<T>(task: () => GatewayPluginDeliveryExecution<T>): Promise<T> {
    this.active += 1;
    const execution = task();
    void execution.completion.finally(() => {
      this.active -= 1;
      this.drain();
    });
    return execution.result;
  }

  private drain(): void {
    while (this.active < this.concurrency) {
      const next = this.queue.shift();
      if (!next) {
        return;
      }
      next();
    }
  }
}

class GatewayPluginDeliveryTimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'GatewayPluginDeliveryTimeoutError';
  }
}

class GatewayPluginDeliveryQueueFullError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'GatewayPluginDeliveryQueueFullError';
  }
}

function isGatewayPluginDeliveryTimeoutError(error: unknown): boolean {
  return error instanceof GatewayPluginDeliveryTimeoutError;
}

function isGatewayPluginDeliveryQueueFullError(error: unknown): boolean {
  return error instanceof GatewayPluginDeliveryQueueFullError;
}

function normalizePositiveInteger(value: number | undefined, fallback: number): number {
  const normalized = normalizeOptionalPositiveInteger(value);
  return normalized ?? fallback;
}

function normalizeNonNegativeInteger(value: number | undefined, fallback: number): number {
  const normalized = normalizeOptionalNonNegativeInteger(value);
  return normalized ?? fallback;
}

function normalizeOptionalPositiveInteger(value: number | undefined): number | undefined {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) {
    return undefined;
  }

  return Math.trunc(value);
}

function normalizeOptionalNonNegativeInteger(value: number | undefined): number | undefined {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
    return undefined;
  }

  return Math.trunc(value);
}

function normalizePositiveNumber(value: number | undefined, fallback: number): number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 1) {
    return fallback;
  }

  return value;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
