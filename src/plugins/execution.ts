import type { GatewayPluginExecutionConfig } from '../types';

export type GatewayPluginExecutionFailureReason =
  | 'error'
  | 'timeout'
  | 'queue_full'
  | 'circuit_open';

export type GatewayPluginExecutionMetricOutcome =
  | 'error'
  | 'timeout'
  | 'queue_full'
  | 'circuit_open';

export type GatewayPluginProtectedExecutionResult<T> =
  | { ok: true; value: T }
  | { ok: true; skipped: true; reason: GatewayPluginExecutionFailureReason; error: string }
  | { ok: false; reason: GatewayPluginExecutionFailureReason; error: string };

interface GatewayPluginExecutionState {
  active: number;
  queue: Array<{
    start: () => void;
    reject: (error: Error) => void;
  }>;
  timedOutActive: number;
  timedOutError?: GatewayPluginExecutionTimeoutError;
  failures: number;
  openUntil?: number;
}

interface GatewayPluginTimedExecution<T> {
  result: Promise<T>;
  completion: Promise<unknown>;
}

interface NormalizedGatewayPluginExecutionConfig {
  timeoutMs?: number;
  concurrency?: number;
  maxQueueSize?: number;
  failureThreshold?: number;
  cooldownMs: number;
  failureMode: 'fail_closed' | 'fail_open';
}

const executionStates = new Map<string, GatewayPluginExecutionState>();
const defaultCooldownMs = 30000;

export async function runGatewayPluginProtectedOperation<T>(input: {
  pluginKey: string;
  kind: string;
  hook: string;
  execution?: GatewayPluginExecutionConfig;
  operation: () => T | Promise<T>;
}): Promise<GatewayPluginProtectedExecutionResult<T>> {
  const options = normalizeGatewayPluginExecution(input.execution);
  const stateKey = buildGatewayPluginExecutionStateKey(input);
  const state = getGatewayPluginExecutionState(stateKey);
  const now = Date.now();

  if (options.failureThreshold && state.openUntil && state.openUntil > now) {
    return handleGatewayPluginExecutionFailure(
      options,
      'circuit_open',
      `Gateway plugin "${input.pluginKey}" ${input.hook} circuit is open.`
    );
  }
  if (state.openUntil && state.openUntil <= now) {
    state.openUntil = undefined;
  }

  try {
    const value = await runWithGatewayPluginExecutionLimiter(state, options, () =>
      createGatewayPluginTimedExecution(input.operation, input.pluginKey, input.hook, options.timeoutMs)
    );
    state.failures = 0;
    state.openUntil = undefined;
    return {
      ok: true,
      value
    };
  } catch (error) {
    const reason = classifyGatewayPluginExecutionFailure(error);
    recordGatewayPluginExecutionFailure(state, options);
    return handleGatewayPluginExecutionFailure(options, reason, formatGatewayPluginExecutionError(error));
  }
}

export function resetGatewayPluginExecutionStateForTests(): void {
  executionStates.clear();
}

function normalizeGatewayPluginExecution(
  execution: GatewayPluginExecutionConfig | undefined
): NormalizedGatewayPluginExecutionConfig {
  return {
    timeoutMs: normalizeOptionalPositiveInteger(execution?.timeoutMs),
    concurrency: normalizeOptionalPositiveInteger(execution?.concurrency),
    maxQueueSize: normalizeOptionalNonNegativeInteger(execution?.maxQueueSize),
    failureThreshold: normalizeOptionalPositiveInteger(execution?.failureThreshold),
    cooldownMs: normalizePositiveInteger(execution?.cooldownMs, defaultCooldownMs),
    failureMode: execution?.failureMode === 'fail_open' ? 'fail_open' : 'fail_closed'
  };
}

function buildGatewayPluginExecutionStateKey(input: {
  pluginKey: string;
  kind: string;
  hook: string;
}): string {
  return `${input.pluginKey}\n${input.kind}\n${input.hook}`;
}

function getGatewayPluginExecutionState(key: string): GatewayPluginExecutionState {
  let state = executionStates.get(key);
  if (!state) {
    state = {
      active: 0,
      queue: [],
      timedOutActive: 0,
      failures: 0
    };
    executionStates.set(key, state);
  }
  return state;
}

function handleGatewayPluginExecutionFailure<T>(
  options: NormalizedGatewayPluginExecutionConfig,
  reason: GatewayPluginExecutionFailureReason,
  error: string
): GatewayPluginProtectedExecutionResult<T> {
  if (options.failureMode === 'fail_open') {
    return {
      ok: true,
      skipped: true,
      reason,
      error
    };
  }

  return {
    ok: false,
    reason,
    error
  };
}

function recordGatewayPluginExecutionFailure(
  state: GatewayPluginExecutionState,
  options: NormalizedGatewayPluginExecutionConfig
): void {
  if (!options.failureThreshold) {
    return;
  }

  state.failures += 1;
  if (state.failures >= options.failureThreshold) {
    state.openUntil = Date.now() + options.cooldownMs;
    state.failures = 0;
  }
}

function createGatewayPluginTimedExecution<T>(
  operation: () => T | Promise<T>,
  pluginKey: string,
  hook: string,
  timeoutMs: number | undefined
): GatewayPluginTimedExecution<T> {
  const promise = Promise.resolve().then(operation);
  if (!timeoutMs) {
    return {
      result: promise,
      completion: promise.catch(() => undefined)
    };
  }

  let timeout: ReturnType<typeof setTimeout> | undefined;
  const result = Promise.race([
    promise,
    new Promise<never>((_resolve, reject) => {
      timeout = setTimeout(() => {
        reject(new GatewayPluginExecutionTimeoutError(
          `Gateway plugin "${pluginKey}" ${hook} timed out.`
        ));
      }, timeoutMs);
    })
  ]).finally(() => {
    if (timeout) {
      clearTimeout(timeout);
    }
  });

  return {
    result,
    completion: promise.catch(() => undefined)
  };
}

function runWithGatewayPluginExecutionLimiter<T>(
  state: GatewayPluginExecutionState,
  options: NormalizedGatewayPluginExecutionConfig,
  task: () => GatewayPluginTimedExecution<T>
): Promise<T> {
  if (!options.concurrency) {
    return task().result;
  }

  if (state.active < options.concurrency) {
    return startGatewayPluginExecutionTask(state, options.concurrency, task);
  }

  if (state.timedOutActive > 0) {
    return Promise.reject(
      state.timedOutError || new GatewayPluginExecutionTimeoutError('Gateway plugin execution timed out.')
    );
  }

  if (options.maxQueueSize !== undefined && state.queue.length >= options.maxQueueSize) {
    return Promise.reject(new GatewayPluginExecutionQueueFullError('Gateway plugin execution queue is full.'));
  }

  return new Promise<T>((resolve, reject) => {
    state.queue.push({
      start: () => {
        startGatewayPluginExecutionTask(state, options.concurrency || 1, task).then(resolve, reject);
      },
      reject
    });
  });
}

function startGatewayPluginExecutionTask<T>(
  state: GatewayPluginExecutionState,
  concurrency: number,
  task: () => GatewayPluginTimedExecution<T>
): Promise<T> {
  state.active += 1;
  let execution: GatewayPluginTimedExecution<T>;
  try {
    execution = task();
  } catch (error) {
    state.active -= 1;
    drainGatewayPluginExecutionQueue(state, concurrency);
    return Promise.reject(error);
  }

  let timedOut = false;
  const result = execution.result.catch((error) => {
    if (error instanceof GatewayPluginExecutionTimeoutError) {
      timedOut = true;
      state.timedOutActive += 1;
      state.timedOutError = error;
      rejectGatewayPluginExecutionQueue(state, error);
    }
    throw error;
  });

  void execution.completion.finally(() => {
    if (timedOut) {
      state.timedOutActive = Math.max(0, state.timedOutActive - 1);
      if (state.timedOutActive === 0) {
        state.timedOutError = undefined;
      }
    }
    state.active -= 1;
    drainGatewayPluginExecutionQueue(state, concurrency);
  });
  return result;
}

function drainGatewayPluginExecutionQueue(state: GatewayPluginExecutionState, concurrency: number): void {
  while (state.active < concurrency && state.queue.length > 0) {
    const next = state.queue.shift();
    if (!next) {
      return;
    }
    next.start();
  }
}

function rejectGatewayPluginExecutionQueue(state: GatewayPluginExecutionState, error: Error): void {
  const queued = state.queue.splice(0);
  for (const item of queued) {
    item.reject(error);
  }
}

function classifyGatewayPluginExecutionFailure(error: unknown): GatewayPluginExecutionFailureReason {
  if (error instanceof GatewayPluginExecutionTimeoutError) {
    return 'timeout';
  }
  if (error instanceof GatewayPluginExecutionQueueFullError) {
    return 'queue_full';
  }
  return 'error';
}

function formatGatewayPluginExecutionError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

class GatewayPluginExecutionTimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'GatewayPluginExecutionTimeoutError';
  }
}

class GatewayPluginExecutionQueueFullError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'GatewayPluginExecutionQueueFullError';
  }
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

function normalizePositiveInteger(value: number | undefined, fallback: number): number {
  return normalizeOptionalPositiveInteger(value) ?? fallback;
}
