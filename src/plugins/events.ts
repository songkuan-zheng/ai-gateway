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
const defaultDeadLetterMaxEntries = 1000;

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
  if (idempotencyKey && options.dedupe && hasDeliveredGatewayPluginEvent(idempotencyKey, options.dedupeTtlMs)) {
    recordGatewayPluginDelivery({
      extensionKey: extension.key,
      transport: readGatewayPluginExtensionTransport(extension),
      outcome: 'not_delivered'
    });
    return false;
  }

  try {
    const delivered = await runWithGatewayPluginDeliveryLimiter(extension, options, () =>
      runGatewayPluginDeliveryWithRetry(extension, eventId, idempotencyKey, operation, options)
    );
    if (delivered && idempotencyKey && options.dedupe) {
      rememberDeliveredGatewayPluginEvent(idempotencyKey, options.dedupeTtlMs);
    }
    recordGatewayPluginDelivery({
      extensionKey: extension.key,
      transport: readGatewayPluginExtensionTransport(extension),
      outcome: delivered ? 'delivered' : 'not_delivered'
    });
    return delivered;
  } catch (error) {
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

export function listGatewayPluginDeadLetters(extensionKey?: string): GatewayPluginDeadLetter[] {
  if (extensionKey) {
    return [...(deadLettersByExtension.get(extensionKey) || [])];
  }

  return Array.from(deadLettersByExtension.values()).flatMap((entries) => entries);
}

export function clearGatewayPluginDeadLetters(extensionKey?: string): number {
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

function hasDeliveredGatewayPluginEvent(key: string, ttlMs: number): boolean {
  const expiresAt = deliveredEventKeys.get(key);
  if (!expiresAt) {
    return false;
  }
  if (expiresAt <= Date.now()) {
    deliveredEventKeys.delete(key);
    return false;
  }
  return true;
}

function rememberDeliveredGatewayPluginEvent(key: string, ttlMs: number): void {
  const now = Date.now();
  deliveredEventKeys.set(key, now + ttlMs);
  for (const [eventKey, expiresAt] of deliveredEventKeys) {
    if (expiresAt <= now) {
      deliveredEventKeys.delete(eventKey);
    }
  }
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
    const entries = deadLettersByExtension.get(extension.key) || [];
    entries.push(entry);
    const maxEntries = options.deadLetter.maxEntries || defaultDeadLetterMaxEntries;
    if (entries.length > maxEntries) {
      entries.splice(0, entries.length - maxEntries);
    }
    deadLettersByExtension.set(extension.key, entries);
  }

  await extension.deadLetter?.(entry);
}

async function runGatewayPluginDeliveryWithRetry(
  extension: GatewayPluginExtension,
  eventId: string | undefined,
  idempotencyKey: string | undefined,
  operation: (context: GatewayPluginDeliveryContext) => boolean | void | Promise<boolean | void>,
  options: NormalizedDeliveryOptions
): Promise<boolean> {
  let lastError: unknown;
  for (let attempt = 1; attempt <= options.maxAttempts; attempt += 1) {
    try {
      const result = await runGatewayPluginDeliveryAttempt(
        extension,
        eventId,
        idempotencyKey,
        operation,
        options,
        attempt
      );
      return result !== false;
    } catch (error) {
      lastError = error;
      if (attempt >= options.maxAttempts) {
        break;
      }
      await sleep(resolveGatewayPluginRetryDelayMs(attempt, options));
    }
  }

  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

async function runGatewayPluginDeliveryAttempt(
  extension: GatewayPluginExtension,
  eventId: string | undefined,
  idempotencyKey: string | undefined,
  operation: (context: GatewayPluginDeliveryContext) => boolean | void | Promise<boolean | void>,
  options: NormalizedDeliveryOptions,
  attempt: number
): Promise<boolean | void> {
  const abortController = new AbortController();
  const deliveryContext: GatewayPluginDeliveryContext = {
    signal: abortController.signal,
    attempt,
    maxAttempts: options.maxAttempts,
    eventId,
    idempotencyKey
  };
  const promise = Promise.resolve().then(() => operation(deliveryContext));
  if (!options.timeoutMs) {
    return promise;
  }

  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(() => {
          abortController.abort(new Error(`Gateway plugin delivery timed out: ${extension.key}`));
          reject(new GatewayPluginDeliveryTimeoutError(`Gateway plugin delivery timed out: ${extension.key}`));
        }, options.timeoutMs);
      })
    ]);
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
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
  task: () => Promise<T>
): Promise<T> {
  if (!options.concurrency) {
    return task();
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

  run<T>(task: () => Promise<T>): Promise<T> {
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

  private start<T>(task: () => Promise<T>): Promise<T> {
    this.active += 1;
    return task().finally(() => {
      this.active -= 1;
      this.drain();
    });
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
