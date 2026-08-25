import { recordGatewayPluginHookExecution } from '../gateway/metrics';
import { matchesAnyPattern } from '../shared/pattern';
import type {
  GatewayPluginHookFailure,
  GatewayPluginHookResult,
  GatewayPluginMatchable,
  GatewayPluginRequestHook,
  GatewayPluginRequestHookInput,
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
    try {
      const result = normalizeGatewayPluginHookResult<TValue>(await method(input));
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
    } catch (error) {
      recordGatewayPluginHookExecution({
        pluginKey: hook.key,
        kind: 'request',
        hook: stage,
        outcome: 'error',
        durationMs: elapsedMs(startedAt)
      });
      return {
        ok: false,
        pluginKey: hook.key,
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }

  return { ok: true };
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
    try {
      const result = await hook.transformResponse({
        ...input,
        upstreamResponse
      });
      const normalized = result instanceof Response
        ? ({ ok: true, value: result } satisfies GatewayPluginHookResult<Response>)
        : normalizeGatewayPluginHookResult<Response>(result);
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
    } catch (error) {
      recordGatewayPluginHookExecution({
        pluginKey: hook.key,
        kind: 'stream',
        hook: 'transformResponse',
        outcome: 'error',
        durationMs: elapsedMs(startedAt)
      });
      return {
        ok: false,
        pluginKey: hook.key,
        error: error instanceof Error ? error.message : String(error)
      };
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

function elapsedMs(startedAt: bigint): number {
  return Number(process.hrtime.bigint() - startedAt) / 1_000_000;
}
