import { recordGatewayPluginHookExecution } from '../gateway/metrics';
import { matchesAnyPattern } from '../shared/pattern';
import { runGatewayPluginProtectedOperation } from './execution';
import type {
  GatewayPluginHookFailure,
  GatewayPluginHookResult,
  GatewayPluginMatchable,
  GatewayPluginRequestHeaderMutations,
  GatewayPluginRequestHook,
  GatewayPluginRequestHookInput,
  GatewayPluginRequestTransform,
  GatewayPluginRequestTransformInput,
  GatewayPluginRequestTransformValue,
  GatewayPluginResponseHook,
  GatewayPluginResponseHookInput,
  GatewayPluginResponseTransformValue,
  GatewayPluginRouteResolution,
  GatewayPluginRouteResolver,
  GatewayPluginStreamHook,
  GatewayPluginStreamHookInput
} from '../types';

export interface GatewayPluginHookFailureResult extends GatewayPluginHookFailure {
  pluginKey: string;
}

export interface GatewayPluginHookSuccessResult<TValue = void> {
  ok: true;
  pluginKey?: string;
  value?: TValue;
}

export type GatewayPluginRequestHookStage =
  | 'beforeAuth'
  | 'beforeRouting'
  | 'beforePrecheck'
  | 'afterPrecheck';

export interface GatewayPluginAppliedRequestTransform {
  requestBody?: unknown;
  standardRequest?: GatewayPluginRequestTransformInput['standardRequest'];
  standardRequestTransformed: boolean;
  model?: string;
  source?: GatewayPluginRequestTransformInput['source'];
}

export interface GatewayPluginAppliedResponseTransform {
  responsePayload: unknown;
  statusCode: number;
  responseHeaders: Record<string, string>;
  removeHeaders: string[];
}

export async function executeGatewayPluginRequestHookStage<TValue = void>(
  hooks: GatewayPluginRequestHook[],
  stage: GatewayPluginRequestHookStage,
  input: GatewayPluginRequestHookInput
): Promise<GatewayPluginHookSuccessResult<TValue> | GatewayPluginHookFailureResult> {
  for (const hook of hooks) {
    const method = hook[stage] as
      | ((hookInput: GatewayPluginRequestHookInput) => GatewayPluginHookResult<TValue> | Promise<GatewayPluginHookResult<TValue>>)
      | undefined;
    if (!method) {
      continue;
    }

    if (!shouldRunGatewayPluginMatchable(hook, input)) {
      recordGatewayPluginHookExecution({
        pluginKey: hook.key,
        kind: 'request',
        hook: stage,
        outcome: 'skipped'
      });
      continue;
    }

    const startedAt = process.hrtime.bigint();
    const executionResult = await runGatewayPluginProtectedOperation({
      pluginKey: hook.key,
      kind: 'request',
      hook: stage,
      execution: hook.execution,
      operation: () => method(input)
    });
    if (!executionResult.ok) {
      recordGatewayPluginHookExecution({
        pluginKey: hook.key,
        kind: 'request',
        hook: stage,
        outcome: executionResult.reason,
        durationMs: elapsedMs(startedAt)
      });
      return {
        ok: false,
        pluginKey: hook.key,
        error: executionResult.error
      };
    }

    if ('skipped' in executionResult) {
      recordGatewayPluginHookExecution({
        pluginKey: hook.key,
        kind: 'request',
        hook: stage,
        outcome: executionResult.reason,
        durationMs: elapsedMs(startedAt)
      });
      continue;
    }

    const result = normalizeGatewayPluginHookResult<TValue>(executionResult.value);
    recordGatewayPluginHookExecution({
      pluginKey: hook.key,
      kind: 'request',
      hook: stage,
      outcome: result.ok ? 'success' : 'blocked',
      durationMs: elapsedMs(startedAt)
    });
    if (!result.ok) {
      return {
        ...result,
        pluginKey: hook.key
      };
    }
    if (result.value !== undefined) {
      return {
        ...result,
        pluginKey: hook.key
      };
    }
  }

  return { ok: true };
}

export async function applyGatewayPluginRequestTransforms(
  transforms: GatewayPluginRequestTransform[],
  input: GatewayPluginRequestTransformInput
): Promise<{ ok: true; value: GatewayPluginAppliedRequestTransform } | GatewayPluginHookFailureResult> {
  const applied: GatewayPluginAppliedRequestTransform = {
    requestBody: input.requestBody,
    standardRequest: input.standardRequest,
    standardRequestTransformed: false,
    model: input.model,
    source: input.source
  };
  let currentInput: GatewayPluginRequestTransformInput = input;

  for (const transform of transforms) {
    if (transform.stage && transform.stage !== input.stage) {
      recordGatewayPluginHookExecution({
        pluginKey: transform.key,
        kind: 'request_transform',
        hook: input.stage,
        outcome: 'skipped'
      });
      continue;
    }

    if (!shouldRunGatewayPluginMatchable(transform, currentInput)) {
      recordGatewayPluginHookExecution({
        pluginKey: transform.key,
        kind: 'request_transform',
        hook: input.stage,
        outcome: 'skipped'
      });
      continue;
    }

    const startedAt = process.hrtime.bigint();
    const executionResult = await runGatewayPluginProtectedOperation({
      pluginKey: transform.key,
      kind: 'request_transform',
      hook: input.stage,
      execution: transform.execution,
      operation: () => transform.transform(currentInput)
    });
    if (!executionResult.ok) {
      recordGatewayPluginHookExecution({
        pluginKey: transform.key,
        kind: 'request_transform',
        hook: input.stage,
        outcome: executionResult.reason,
        durationMs: elapsedMs(startedAt)
      });
      return {
        ok: false,
        pluginKey: transform.key,
        error: executionResult.error
      };
    }

    if ('skipped' in executionResult) {
      recordGatewayPluginHookExecution({
        pluginKey: transform.key,
        kind: 'request_transform',
        hook: input.stage,
        outcome: executionResult.reason,
        durationMs: elapsedMs(startedAt)
      });
      continue;
    }

    const result = normalizeGatewayPluginHookResult<GatewayPluginRequestTransformValue | void>(
      executionResult.value
    );
    recordGatewayPluginHookExecution({
      pluginKey: transform.key,
      kind: 'request_transform',
      hook: input.stage,
      outcome: result.ok ? 'success' : 'blocked',
      durationMs: elapsedMs(startedAt)
    });
    if (!result.ok) {
      return {
        ...result,
        pluginKey: transform.key
      };
    }

    if (!result.value) {
      continue;
    }

    applyRequestTransformValue(applied, currentInput, result.value);
    currentInput = {
      ...currentInput,
      requestBody: applied.requestBody,
      standardRequest: applied.standardRequest,
      model: applied.model,
      source: applied.source,
      sourceAdapterKey: applied.source?.adapterKey || currentInput.sourceAdapterKey
    };
  }

  return {
    ok: true,
    value: applied
  };
}

export async function resolveGatewayPluginRoute(
  resolvers: GatewayPluginRouteResolver[],
  input: GatewayPluginRequestHookInput
): Promise<{ ok: true; pluginKey?: string; value?: GatewayPluginRouteResolution } | GatewayPluginHookFailureResult> {
  for (const resolver of resolvers) {
    if (!shouldRunGatewayPluginMatchable(resolver, input)) {
      recordGatewayPluginHookExecution({
        pluginKey: resolver.key,
        kind: 'route_resolver',
        hook: 'resolve',
        outcome: 'skipped'
      });
      continue;
    }

    const startedAt = process.hrtime.bigint();
    const executionResult = await runGatewayPluginProtectedOperation({
      pluginKey: resolver.key,
      kind: 'route_resolver',
      hook: 'resolve',
      execution: resolver.execution,
      operation: () => resolver.resolve(input)
    });
    if (!executionResult.ok) {
      recordGatewayPluginHookExecution({
        pluginKey: resolver.key,
        kind: 'route_resolver',
        hook: 'resolve',
        outcome: executionResult.reason,
        durationMs: elapsedMs(startedAt)
      });
      return {
        ok: false,
        pluginKey: resolver.key,
        error: executionResult.error
      };
    }

    if ('skipped' in executionResult) {
      recordGatewayPluginHookExecution({
        pluginKey: resolver.key,
        kind: 'route_resolver',
        hook: 'resolve',
        outcome: executionResult.reason,
        durationMs: elapsedMs(startedAt)
      });
      continue;
    }

    const result = normalizeGatewayPluginHookResult<GatewayPluginRouteResolution | void>(
      executionResult.value
    );
    recordGatewayPluginHookExecution({
      pluginKey: resolver.key,
      kind: 'route_resolver',
      hook: 'resolve',
      outcome: result.ok ? 'success' : 'blocked',
      durationMs: elapsedMs(startedAt)
    });
    if (!result.ok) {
      return {
        ...result,
        pluginKey: resolver.key
      };
    }
    if (result.value) {
      return {
        ok: true,
        pluginKey: resolver.key,
        value: result.value
      };
    }
  }

  return { ok: true };
}

export async function applyGatewayPluginResponseHooks(
  hooks: GatewayPluginResponseHook[],
  input: GatewayPluginResponseHookInput
): Promise<{ ok: true; value: GatewayPluginAppliedResponseTransform } | GatewayPluginHookFailureResult> {
  const applied: GatewayPluginAppliedResponseTransform = {
    responsePayload: input.responsePayload,
    statusCode: input.statusCode,
    responseHeaders: {
      ...input.responseHeaders
    },
    removeHeaders: []
  };
  let currentInput: GatewayPluginResponseHookInput = input;

  for (const hook of hooks) {
    if (!shouldRunGatewayPluginMatchable(hook, currentInput)) {
      recordGatewayPluginHookExecution({
        pluginKey: hook.key,
        kind: 'response',
        hook: 'transformResponse',
        outcome: 'skipped'
      });
      continue;
    }

    const startedAt = process.hrtime.bigint();
    const executionResult = await runGatewayPluginProtectedOperation({
      pluginKey: hook.key,
      kind: 'response',
      hook: 'transformResponse',
      execution: hook.execution,
      operation: () => hook.transformResponse(currentInput)
    });
    if (!executionResult.ok) {
      recordGatewayPluginHookExecution({
        pluginKey: hook.key,
        kind: 'response',
        hook: 'transformResponse',
        outcome: executionResult.reason,
        durationMs: elapsedMs(startedAt)
      });
      return {
        ok: false,
        pluginKey: hook.key,
        error: executionResult.error
      };
    }

    if ('skipped' in executionResult) {
      recordGatewayPluginHookExecution({
        pluginKey: hook.key,
        kind: 'response',
        hook: 'transformResponse',
        outcome: executionResult.reason,
        durationMs: elapsedMs(startedAt)
      });
      continue;
    }

    const result = normalizeGatewayPluginHookResult<GatewayPluginResponseTransformValue | void>(
      executionResult.value
    );
    recordGatewayPluginHookExecution({
      pluginKey: hook.key,
      kind: 'response',
      hook: 'transformResponse',
      outcome: result.ok ? 'success' : 'blocked',
      durationMs: elapsedMs(startedAt)
    });
    if (!result.ok) {
      return {
        ...result,
        pluginKey: hook.key
      };
    }

    if (!result.value) {
      continue;
    }

    applyResponseTransformValue(applied, result.value);
    currentInput = {
      ...currentInput,
      responsePayload: applied.responsePayload,
      statusCode: applied.statusCode,
      responseHeaders: {
        ...applied.responseHeaders
      }
    };
  }

  return {
    ok: true,
    value: applied
  };
}

export async function applyGatewayPluginStreamResponseHooks(
  hooks: GatewayPluginStreamHook[],
  input: GatewayPluginStreamHookInput
): Promise<{ ok: true; value: Response } | GatewayPluginHookFailureResult> {
  let upstreamResponse = input.upstreamResponse;
  for (const hook of hooks) {
    if (!hook.transformResponse) {
      continue;
    }

    if (!shouldRunGatewayPluginMatchable(hook, input)) {
      recordGatewayPluginHookExecution({
        pluginKey: hook.key,
        kind: 'stream',
        hook: 'transformResponse',
        outcome: 'skipped'
      });
      continue;
    }

    const startedAt = process.hrtime.bigint();
    const executionResult = await runGatewayPluginProtectedOperation({
      pluginKey: hook.key,
      kind: 'stream',
      hook: 'transformResponse',
      execution: hook.execution,
      operation: () => hook.transformResponse?.({
        ...input,
        upstreamResponse
      })
    });
    if (!executionResult.ok) {
      recordGatewayPluginHookExecution({
        pluginKey: hook.key,
        kind: 'stream',
        hook: 'transformResponse',
        outcome: executionResult.reason,
        durationMs: elapsedMs(startedAt)
      });
      return {
        ok: false,
        pluginKey: hook.key,
        error: executionResult.error
      };
    }

    if ('skipped' in executionResult) {
      recordGatewayPluginHookExecution({
        pluginKey: hook.key,
        kind: 'stream',
        hook: 'transformResponse',
        outcome: executionResult.reason,
        durationMs: elapsedMs(startedAt)
      });
      continue;
    }

    const normalized = executionResult.value instanceof Response
      ? ({ ok: true, value: executionResult.value } satisfies GatewayPluginHookResult<Response>)
      : normalizeGatewayPluginHookResult<Response>(executionResult.value);
    recordGatewayPluginHookExecution({
      pluginKey: hook.key,
      kind: 'stream',
      hook: 'transformResponse',
      outcome: normalized.ok ? 'success' : 'blocked',
      durationMs: elapsedMs(startedAt)
    });
    if (!normalized.ok) {
      return {
        ...normalized,
        pluginKey: hook.key
      };
    }
    if (normalized.value instanceof Response) {
      upstreamResponse = normalized.value;
    }
  }

  return {
    ok: true,
    value: upstreamResponse
  };
}

export function shouldRunGatewayPluginMatchable(
  matchable: GatewayPluginMatchable,
  input: Pick<
    GatewayPluginRequestHookInput,
    'source' | 'sourceAdapterKey' | 'sourceProvider' | 'targetProvider' | 'targetProviderConfig' | 'model'
  >
): boolean {
  if (matchable.provider && matchable.provider !== input.targetProvider) {
    return false;
  }

  if (matchable.providerName) {
    const targetProviderName = input.targetProviderConfig?.name?.trim().toLowerCase();
    if (!targetProviderName || targetProviderName !== matchable.providerName.trim().toLowerCase()) {
      return false;
    }
  }

  if (matchable.models && matchable.models.length > 0) {
    const model = input.model?.trim();
    if (!model || !matchesAnyPattern(model, matchable.models)) {
      return false;
    }
  }

  if (matchable.sourceAdapters && matchable.sourceAdapters.length > 0) {
    const sourceAdapterKey = input.sourceAdapterKey || input.source?.adapterKey;
    if (!sourceAdapterKey || !matchesAnyPattern(sourceAdapterKey, matchable.sourceAdapters)) {
      return false;
    }
  }

  if (matchable.sourceRoutes && matchable.sourceRoutes.length > 0) {
    const sourceRoute = input.source?.metadata?.sourceRoute;
    if (!sourceRoute || !matchesAnyPattern(sourceRoute, matchable.sourceRoutes)) {
      return false;
    }
  }

  return true;
}

function normalizeGatewayPluginHookResult<TValue>(
  value: GatewayPluginHookResult<TValue> | TValue | undefined | void
): GatewayPluginHookResult<TValue> {
  if (value && typeof value === 'object' && 'ok' in value) {
    return value as GatewayPluginHookResult<TValue>;
  }

  return {
    ok: true,
    value: value as TValue | undefined
  };
}

function applyRequestTransformValue(
  applied: GatewayPluginAppliedRequestTransform,
  input: GatewayPluginRequestTransformInput,
  value: GatewayPluginRequestTransformValue
): void {
  if ('requestBody' in value) {
    applied.requestBody = value.requestBody;
    (input.request as unknown as { body?: unknown }).body = value.requestBody;
    if (!('model' in value)) {
      applied.model = resolveModelFromTransformedRequestBody(value.requestBody, applied.source);
    }
  }

  if (value.standardRequest !== undefined) {
    applied.standardRequest = value.standardRequest;
    applied.standardRequestTransformed = true;
  }

  if ('model' in value) {
    applied.model = typeof value.model === 'string' && value.model.trim()
      ? value.model.trim()
      : undefined;
  }

  if (value.source) {
    applied.source = value.source;
  }

  if (value.metadata && applied.source) {
    applied.source = {
      ...applied.source,
      metadata: applyMetadataMutations(applied.source.metadata, value.metadata)
    };
  }

  applyRequestHeaderMutations(input, value.headers);
}

function applyResponseTransformValue(
  applied: GatewayPluginAppliedResponseTransform,
  value: GatewayPluginResponseTransformValue
): void {
  if ('responsePayload' in value) {
    applied.responsePayload = value.responsePayload;
  }

  if (typeof value.statusCode === 'number' && Number.isInteger(value.statusCode)) {
    applied.statusCode = value.statusCode;
  }

  for (const headerName of value.removeHeaders || []) {
    removeResponseHeader(applied, headerName);
  }

  for (const [headerName, headerValue] of Object.entries(value.headers || {})) {
    const normalizedName = headerName.trim().toLowerCase();
    if (!normalizedName) {
      continue;
    }
    if (headerValue === null || headerValue === undefined) {
      removeResponseHeader(applied, normalizedName);
      continue;
    }
    applied.removeHeaders = applied.removeHeaders.filter((item) => item !== normalizedName);
    applied.responseHeaders[normalizedName] = String(headerValue);
  }
}

function removeResponseHeader(
  applied: GatewayPluginAppliedResponseTransform,
  headerName: string
): void {
  const normalizedName = headerName.trim().toLowerCase();
  if (!normalizedName) {
    return;
  }
  delete applied.responseHeaders[normalizedName];
  if (!applied.removeHeaders.includes(normalizedName)) {
    applied.removeHeaders.push(normalizedName);
  }
}

function applyMetadataMutations(
  current: Record<string, string> | undefined,
  mutations: Record<string, string | null | undefined>
): Record<string, string> | undefined {
  const next = {
    ...(current || {})
  };

  for (const [key, value] of Object.entries(mutations)) {
    if (!key) {
      continue;
    }
    if (value === null || value === undefined) {
      delete next[key];
      continue;
    }
    next[key] = value;
  }

  return Object.keys(next).length > 0 ? next : undefined;
}

function applyRequestHeaderMutations(
  input: GatewayPluginRequestTransformInput,
  mutations: GatewayPluginRequestTransformValue['headers']
): void {
  if (!mutations) {
    return;
  }

  if (isGatewayPluginRequestHeaderMutations(mutations)) {
    for (const headerName of mutations.remove || []) {
      delete input.request.headers[headerName.toLowerCase()];
    }
    for (const [headerName, headerValue] of Object.entries(mutations.set || {})) {
      applyRequestHeaderMutation(input, headerName, headerValue);
    }
    return;
  }

  for (const [headerName, headerValue] of Object.entries(mutations)) {
    applyRequestHeaderMutation(input, headerName, headerValue);
  }
}

function applyRequestHeaderMutation(
  input: GatewayPluginRequestTransformInput,
  headerName: string,
  headerValue: string | number | boolean | null | undefined
): void {
  const normalizedName = headerName.trim().toLowerCase();
  if (!normalizedName) {
    return;
  }

  if (headerValue === null || headerValue === undefined) {
    delete input.request.headers[normalizedName];
    return;
  }

  input.request.headers[normalizedName] = String(headerValue);
}

function resolveModelFromTransformedRequestBody(
  body: unknown,
  source: GatewayPluginAppliedRequestTransform['source']
): string | undefined {
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    return undefined;
  }
  const record = body as Record<string, unknown>;
  if (typeof record.model === 'string' && record.model.trim()) {
    return record.model.trim();
  }
  if (source?.adapterKey === 'gemini_interactions' && typeof record.agent === 'string' && record.agent.trim()) {
    return record.agent.trim();
  }
  return undefined;
}

function isGatewayPluginRequestHeaderMutations(
  value: GatewayPluginRequestTransformValue['headers']
): value is GatewayPluginRequestHeaderMutations {
  if (!value || typeof value !== 'object') {
    return false;
  }
  const candidate = value as GatewayPluginRequestHeaderMutations;
  return candidate.set !== undefined || candidate.remove !== undefined;
}

function elapsedMs(startedAt: bigint): number {
  return Number(process.hrtime.bigint() - startedAt) / 1_000_000;
}
