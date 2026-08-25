import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import {
  applyGatewayConfigInPlace,
  parseGatewayConfigFromRaw,
  resolveGatewayConfigPath
} from '../config';
import { checkProviderHealth } from '../gateway/provider-health-check';
import {
  collectGatewayRuntimePluginHealth,
  summarizeGatewayRuntimePluginHealth,
  type GatewayRuntime
} from '../gateway/runtime';
import {
  clearGatewayPluginDeadLetters,
  listGatewayPluginDeadLetters
} from '../plugins/events';
import { isProviderExternalSourceEnabled } from '../provider/external';
import type { GatewayConfig, ProviderConfig } from '../types';
import { isObject, parseProvider, providerFromProviderType, readBearerToken, readHeader } from '../utils';

interface ManagerRouteOptions {
  config: GatewayConfig;
  runtime?: GatewayRuntime;
  beforeApplyConfig?: (nextConfig: GatewayConfig) => Promise<void>;
  onConfigReload?: (nextConfig: GatewayConfig) => Promise<void>;
}

interface ManagerConfigQuery {
  revealSecrets?: string;
  reveal_secrets?: string;
}

interface ManagerPluginParams {
  key: string;
}

interface ManagerPluginStateBody {
  enabled?: unknown;
}

const PROVIDER_MANAGEMENT_DISABLED_MESSAGE =
  'Provider management API is disabled because provider data source is configured as external API.';
const redactedSecretValue = '[REDACTED]';

export function registerManagerRoutes(fastify: FastifyInstance, options: ManagerRouteOptions): void {
  const preHandler = async (request: FastifyRequest, reply: FastifyReply) => {
    const authResult = authenticateManagerRequest(request, options.config);
    if (authResult.ok) {
      return;
    }

    return reply.code(authResult.statusCode).send({
      error: {
        message: authResult.message
      }
    });
  };

  const applyConfigUpdate = async (
    request: FastifyRequest,
    reply: FastifyReply,
    bodyForApply: Record<string, unknown>
  ) => {
    const path = resolveGatewayConfigPath();
    const next = parseGatewayConfigFromRaw(bodyForApply);
    if (containsProviderPayload(bodyForApply) && isProviderExternalSourceEnabled(next)) {
      return sendProviderManagementDisabled(reply);
    }

    if (options.beforeApplyConfig) {
      await options.beforeApplyConfig(next);
    }

    const before = cloneJsonObject(options.config as unknown as Record<string, unknown>);
    writeJsonFile(path, bodyForApply);
    applyGatewayConfigInPlace(options.config, next);
    if (options.onConfigReload) {
      await options.onConfigReload(options.config);
    }

    return {
      ok: true,
      path,
      reloadedAt: new Date().toISOString(),
      warnings: collectReloadWarnings(before, options.config, bodyForApply)
    };
  };

  fastify.get<{ Querystring: ManagerConfigQuery }>('/manager/config', { preHandler }, async (request, reply) => {
    try {
      const path = resolveGatewayConfigPath();
      const fileConfig = readGatewayConfigFile(path);
      const revealSecrets = shouldRevealSecrets(request.query);

      return {
        path,
        secretsRedacted: !revealSecrets,
        fileConfig: maybeRedactSecrets(fileConfig, revealSecrets),
        effectiveConfig: maybeRedactSecrets(options.config, revealSecrets)
      };
    } catch (error) {
      request.log.warn(
        {
          details: error instanceof Error ? error.message : String(error)
        },
        'Failed to read gateway config file.'
      );

      return reply.code(500).send({
        error: {
          message: error instanceof Error ? error.message : String(error)
        }
      });
    }
  });

  fastify.get<{ Querystring: ManagerConfigQuery }>('/manager/providers', { preHandler }, async (request, reply) => {
    if (isProviderExternalSourceEnabled(options.config)) {
      return sendProviderManagementDisabled(reply);
    }

    try {
      const revealSecrets = shouldRevealSecrets(request.query);
      return {
        secretsRedacted: !revealSecrets,
        providers: maybeRedactSecrets(options.config.providers, revealSecrets)
      };
    } catch (error) {
      request.log.warn(
        {
          details: error instanceof Error ? error.message : String(error)
        },
        'Failed to get providers.'
      );

      return reply.code(500).send({
        error: {
          message: error instanceof Error ? error.message : String(error)
        }
      });
    }
  });

  fastify.get('/manager/providers/health', { preHandler }, async () => {
    return {
      providers: buildProviderHealthSnapshots(options.config.providers)
    };
  });

  fastify.get('/manager/plugins', { preHandler }, async () => {
    return {
      configured: {
        plugins: options.config.plugins || [],
        providerPlugins: options.config.providerPlugins || []
      },
      runtime: options.runtime
        ? {
            sourceAdapters: options.runtime.sourceAdapters.list().map((adapter) => ({
              key: adapter.key,
              provider: adapter.provider,
              routes: adapter.routes || []
            })),
            targetAdapters: options.runtime.targetAdapters.list().map((adapter) => ({
              key: adapter.key,
              provider: adapter.provider,
              providerTypes: adapter.providerTypes || []
            })),
            providerPlugins: options.runtime.providerPlugins.list().map((plugin) => ({
              key: plugin.key,
              provider: plugin.provider,
              providerName: plugin.providerName,
              models: plugin.models,
              sourceAdapters: plugin.sourceAdapters,
              sourceRoutes: plugin.sourceRoutes
            })),
            requestHooks: options.runtime.requestHooks.list().map((hook) => ({
              key: hook.key,
              provider: hook.provider,
              providerName: hook.providerName,
              models: hook.models,
              sourceAdapters: hook.sourceAdapters,
              sourceRoutes: hook.sourceRoutes
            })),
            streamHooks: options.runtime.streamHooks.list().map((hook) => ({
              key: hook.key,
              provider: hook.provider,
              providerName: hook.providerName,
              models: hook.models,
              sourceAdapters: hook.sourceAdapters,
              sourceRoutes: hook.sourceRoutes
            })),
            billingEventHooks: options.runtime.billingEventHooks.list().map((hook) => ({
              key: hook.key
            })),
            agentEventHooks: options.runtime.agentEventHooks.list().map((hook) => ({
              key: hook.key
            })),
            virtualModelProfiles: options.runtime.virtualModelProfiles.list().map((profile) => ({
              id: profile.id,
              key: profile.key,
              displayName: profile.displayName,
              match: profile.match
            })),
            billingPublishers: options.runtime.billingPublishers.list().map(formatPluginExtensionSummary),
            billingOutboxes: options.runtime.billingOutboxes.list().map(formatPluginExtensionSummary),
            agentEventPublishers: options.runtime.agentEventPublishers.list().map(formatPluginExtensionSummary),
            agentEventOutboxes: options.runtime.agentEventOutboxes.list().map(formatPluginExtensionSummary)
          }
        : undefined
    };
  });

  fastify.get('/manager/plugins/health', { preHandler }, async () => {
    if (!options.runtime) {
      return {
        summary: {
          status: 'ok',
          total: 0,
          degraded: 0,
          unhealthy: 0
        },
        groups: []
      };
    }

    const groups = await collectGatewayRuntimePluginHealth(options.runtime);
    return {
      summary: summarizeGatewayRuntimePluginHealth(groups),
      groups
    };
  });

  fastify.get('/manager/plugins/catalog', { preHandler }, async () => {
    return {
      configured: (options.config.plugins || []).map((plugin) => ({
        key: plugin.key,
        enabled: plugin.enabled,
        modulePath: plugin.modulePath,
        manifest: plugin.manifest,
        capabilities: plugin.manifest?.capabilities || [],
        providerHooks: plugin.providerHooks.length
      })),
      runtime: options.runtime
        ? {
            providers: options.config.providers.map((provider) => ({
              name: provider.name,
              type: provider.type,
              models: provider.models
            })),
            sourceAdapters: options.runtime.sourceAdapters.list().map((adapter) => adapter.key),
            targetAdapters: options.runtime.targetAdapters.list().map((adapter) => ({
              key: adapter.key,
              provider: adapter.provider,
              providerTypes: adapter.providerTypes || []
            })),
            requestHooks: options.runtime.requestHooks.list().map((hook) => hook.key),
            streamHooks: options.runtime.streamHooks.list().map((hook) => hook.key),
            billingEventHooks: options.runtime.billingEventHooks.list().map((hook) => hook.key),
            agentEventHooks: options.runtime.agentEventHooks.list().map((hook) => hook.key)
          }
        : undefined
    };
  });

  fastify.post('/manager/plugins/reload', { preHandler }, async (request, reply) => {
    try {
      const path = resolveGatewayConfigPath();
      const fileConfig = readGatewayConfigFile(path);
      return await applyConfigUpdate(request, reply, fileConfig);
    } catch (error) {
      request.log.warn(
        {
          details: error instanceof Error ? error.message : String(error)
        },
        'Failed to reload gateway plugins.'
      );
      return reply.code(400).send({
        error: {
          message: error instanceof Error ? error.message : String(error)
        }
      });
    }
  });

  fastify.post('/manager/plugins', { preHandler }, async (request, reply) => {
    if (!isObject(request.body)) {
      return reply.code(400).send({
        error: {
          message: 'Request body must be a plugin object.'
        }
      });
    }

    const plugin = request.body as Record<string, unknown>;
    const key = readStringValue(plugin.key);
    if (!key) {
      return reply.code(400).send({
        error: {
          message: 'Plugin key is required.'
        }
      });
    }

    try {
      const path = resolveGatewayConfigPath();
      const fileConfig = readGatewayConfigFile(path);
      const plugins = Array.isArray(fileConfig.plugins) ? [...fileConfig.plugins] : [];
      const index = plugins.findIndex((item) => isObject(item) && readStringValue((item as Record<string, unknown>).key) === key);
      if (index >= 0) {
        plugins[index] = plugin;
      } else {
        plugins.push(plugin);
      }
      const bodyForApply = {
        ...fileConfig,
        plugins
      };
      return await applyConfigUpdate(request, reply, bodyForApply);
    } catch (error) {
      request.log.warn(
        {
          plugin: key,
          details: error instanceof Error ? error.message : String(error)
        },
        'Failed to add or update gateway plugin.'
      );
      return reply.code(400).send({
        error: {
          message: error instanceof Error ? error.message : String(error)
        }
      });
    }
  });

  fastify.patch<{ Params: ManagerPluginParams; Body: ManagerPluginStateBody }>(
    '/manager/plugins/:key',
    { preHandler },
    async (request, reply) => {
      const enabled = readBooleanValue(request.body?.enabled);
      if (enabled === undefined) {
        return reply.code(400).send({
          error: {
            message: 'enabled must be a boolean.'
          }
        });
      }

      return updateConfiguredPluginEnabled(request.params.key, enabled, request, reply, applyConfigUpdate);
    }
  );

  fastify.post<{ Params: ManagerPluginParams }>(
    '/manager/plugins/:key/enable',
    { preHandler },
    async (request, reply) =>
      updateConfiguredPluginEnabled(request.params.key, true, request, reply, applyConfigUpdate)
  );

  fastify.post<{ Params: ManagerPluginParams }>(
    '/manager/plugins/:key/disable',
    { preHandler },
    async (request, reply) =>
      updateConfiguredPluginEnabled(request.params.key, false, request, reply, applyConfigUpdate)
  );

  fastify.get('/manager/plugins/dead-letters', { preHandler }, async () => ({
    deadLetters: listGatewayPluginDeadLetters()
  }));

  fastify.delete<{ Params: ManagerPluginParams }>(
    '/manager/plugins/:key/dead-letters',
    { preHandler },
    async (request) => ({
      cleared: clearGatewayPluginDeadLetters(request.params.key)
    })
  );

  fastify.post<{ Querystring: ManagerConfigQuery }>('/manager/config/validate', { preHandler }, async (request, reply) => {
    if (!isObject(request.body)) {
      return reply.code(400).send({
        error: {
          message: 'Request body must be a JSON object.'
        }
      });
    }

    const body = request.body as Record<string, unknown>;
    const revealSecrets = shouldRevealSecrets(request.query);

    try {
      const path = resolveGatewayConfigPath();
      const bodyForApply = preserveRedactedSecrets(body, readGatewayConfigFile(path));
      const next = parseGatewayConfigFromRaw(bodyForApply);
      if (containsProviderPayload(bodyForApply) && isProviderExternalSourceEnabled(next)) {
        return sendProviderManagementDisabled(reply);
      }

      const before = cloneJsonObject(options.config as unknown as Record<string, unknown>);
      const warnings = collectReloadWarnings(before, next, bodyForApply);

      return {
        ok: true,
        valid: true,
        secretsRedacted: !revealSecrets,
        warnings,
        effectiveConfig: maybeRedactSecrets(next, revealSecrets)
      };
    } catch (error) {
      return reply.code(400).send({
        ok: false,
        valid: false,
        error: {
          message: error instanceof Error ? error.message : String(error)
        }
      });
    }
  });

  fastify.post('/manager/providers/health/check', { preHandler }, async (request, reply) => {
    const body = request.body === undefined ? {} : request.body;
    if (!isObject(body)) {
      return reply.code(400).send({
        error: {
          message: 'Request body must be a JSON object.'
        }
      });
    }

    const selector = readHealthCheckProviderSelector(body);
    const providers = selectProvidersForHealthCheck(options.config.providers, selector);
    if (selector && providers.length === 0) {
      return reply.code(404).send({
        error: {
          message: `Provider not found: ${selector}`
        }
      });
    }

    const timeoutMs = readNumberValue(body.timeoutMs) || options.config.upstreamTimeoutMs;
    const results = await Promise.all(
      providers.map((providerConfig) => checkProviderHealth(providerConfig, options.config, { timeoutMs }))
    );

    return {
      checkedAt: new Date().toISOString(),
      results,
      providers: buildProviderHealthSnapshots(options.config.providers)
    };
  });

  fastify.put('/manager/config', { preHandler }, async (request, reply) => {
    if (!isObject(request.body)) {
      return reply.code(400).send({
        error: {
          message: 'Request body must be a JSON object.'
        }
      });
    }

    const body = request.body as Record<string, unknown>;
    const path = resolveGatewayConfigPath();

    try {
      const bodyForApply = preserveRedactedSecrets(body, readGatewayConfigFile(path));
      const next = parseGatewayConfigFromRaw(bodyForApply);
      if (containsProviderPayload(bodyForApply) && isProviderExternalSourceEnabled(next)) {
        return sendProviderManagementDisabled(reply);
      }

      if (options.beforeApplyConfig) {
        await options.beforeApplyConfig(next);
      }

      const before = cloneJsonObject(options.config as unknown as Record<string, unknown>);
      writeJsonFile(path, bodyForApply);
      applyGatewayConfigInPlace(options.config, next);
      if (options.onConfigReload) {
        await options.onConfigReload(options.config);
      }

      const warnings = collectReloadWarnings(before, options.config, bodyForApply);

      return {
        ok: true,
        path,
        reloadedAt: new Date().toISOString(),
        warnings
      };
    } catch (error) {
      request.log.warn(
        {
          details: error instanceof Error ? error.message : String(error)
        },
        'Failed to update and reload gateway config.'
      );

      return reply.code(400).send({
        error: {
          message: error instanceof Error ? error.message : String(error)
        }
      });
    }
  });
}

async function updateConfiguredPluginEnabled(
  pluginKey: string,
  enabled: boolean,
  request: FastifyRequest,
  reply: FastifyReply,
  applyConfigUpdate: (
    request: FastifyRequest,
    reply: FastifyReply,
    bodyForApply: Record<string, unknown>
  ) => Promise<unknown>
) {
  try {
    const path = resolveGatewayConfigPath();
    const fileConfig = readGatewayConfigFile(path);
    const plugins = Array.isArray(fileConfig.plugins) ? [...fileConfig.plugins] : [];
    const index = plugins.findIndex((item) => isObject(item) && readStringValue((item as Record<string, unknown>).key) === pluginKey);
    if (index < 0) {
      return reply.code(404).send({
        error: {
          message: `Plugin not found: ${pluginKey}`
        }
      });
    }

    const plugin = plugins[index];
    if (!isObject(plugin)) {
      return reply.code(400).send({
        error: {
          message: `Plugin entry is invalid: ${pluginKey}`
        }
      });
    }

    plugins[index] = {
      ...(plugin as Record<string, unknown>),
      enabled
    };
    return await applyConfigUpdate(request, reply, {
      ...fileConfig,
      plugins
    });
  } catch (error) {
    request.log.warn(
      {
        plugin: pluginKey,
        enabled,
        details: error instanceof Error ? error.message : String(error)
      },
      'Failed to update gateway plugin state.'
    );
    return reply.code(400).send({
      error: {
        message: error instanceof Error ? error.message : String(error)
      }
    });
  }
}

function shouldRevealSecrets(query: ManagerConfigQuery | undefined): boolean {
  return readBooleanQueryValue(query?.revealSecrets) || readBooleanQueryValue(query?.reveal_secrets);
}

function readBooleanValue(value: unknown): boolean | undefined {
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value !== 'string') {
    return undefined;
  }

  const normalized = value.trim().toLowerCase();
  if (normalized === 'true' || normalized === '1' || normalized === 'yes' || normalized === 'on') {
    return true;
  }
  if (normalized === 'false' || normalized === '0' || normalized === 'no' || normalized === 'off') {
    return false;
  }
  return undefined;
}

function readBooleanQueryValue(value: string | undefined): boolean {
  const normalized = value?.trim().toLowerCase();
  return normalized === 'true' || normalized === '1' || normalized === 'yes';
}

function maybeRedactSecrets<T>(value: T, revealSecrets: boolean): T {
  if (revealSecrets) {
    return value;
  }

  return redactSecrets(value, []) as T;
}

function preserveRedactedSecrets<T>(submitted: T, existing: unknown): T {
  return replaceRedactedSecretPlaceholders(submitted, existing, []) as T;
}

function replaceRedactedSecretPlaceholders(submitted: unknown, existing: unknown, path: string[]): unknown {
  if (Array.isArray(submitted)) {
    const existingItems = Array.isArray(existing) ? existing : [];
    return submitted.map((item, index) =>
      replaceRedactedSecretPlaceholders(item, findExistingArrayItem(item, existingItems, index), path)
    );
  }

  if (!isObject(submitted)) {
    return submitted;
  }

  if (isReferenceObject(submitted)) {
    return clonePlainObject(submitted as Record<string, unknown>);
  }

  const submittedSource = submitted as Record<string, unknown>;
  const existingSource = isObject(existing) ? (existing as Record<string, unknown>) : {};
  const preserved: Record<string, unknown> = {};
  for (const [key, nested] of Object.entries(submittedSource)) {
    if (
      nested === redactedSecretValue &&
      shouldRedactConfigValue(key, nested, path) &&
      Object.prototype.hasOwnProperty.call(existingSource, key)
    ) {
      preserved[key] = cloneJsonValue(existingSource[key]);
      continue;
    }

    preserved[key] = replaceRedactedSecretPlaceholders(nested, existingSource[key], [...path, key]);
  }

  return preserved;
}

function findExistingArrayItem(submitted: unknown, existingItems: unknown[], index: number): unknown {
  const identity = readArrayItemIdentity(submitted);
  if (identity) {
    const matched = existingItems.find((item) => {
      const candidate = readArrayItemIdentity(item);
      return candidate?.key === identity.key && candidate.value === identity.value;
    });
    if (matched !== undefined) {
      return matched;
    }
  }

  return existingItems[index];
}

function readArrayItemIdentity(value: unknown): { key: string; value: string } | undefined {
  if (!isObject(value)) {
    return undefined;
  }

  const source = value as Record<string, unknown>;
  for (const key of ['name', 'key']) {
    const identity = readStringValue(source[key]);
    if (identity) {
      return {
        key,
        value: identity.trim().toLowerCase()
      };
    }
  }

  return undefined;
}

function redactSecrets(value: unknown, path: string[]): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => redactSecrets(item, path));
  }

  if (!isObject(value)) {
    return value;
  }

  if (isReferenceObject(value)) {
    return clonePlainObject(value as Record<string, unknown>);
  }

  const source = value as Record<string, unknown>;
  const redacted: Record<string, unknown> = {};
  for (const [key, nested] of Object.entries(source)) {
    if (shouldRedactConfigValue(key, nested, path)) {
      redacted[key] = redactedSecretValue;
      continue;
    }

    redacted[key] = redactSecrets(nested, [...path, key]);
  }

  return redacted;
}

function shouldRedactConfigValue(key: string, value: unknown, path: string[]): boolean {
  if (value === undefined || value === null) {
    return false;
  }

  if (isReferenceObject(value)) {
    return false;
  }

  if (isSensitiveHeaderValue(key, path)) {
    return true;
  }

  const normalized = normalizeSecretKey(key);
  if (
    normalized === 'apikey' ||
    normalized === 'password' ||
    normalized === 'secret' ||
    normalized === 'sharedsecret' ||
    normalized === 'credential' ||
    normalized === 'authorization' ||
    normalized === 'accesstoken' ||
    normalized === 'refreshtoken' ||
    normalized === 'idtoken' ||
    normalized === 'clientsecret' ||
    normalized === 'bearertoken'
  ) {
    return true;
  }

  if (normalized.endsWith('apikey') && !isSafeApiKeyMetadataKey(normalized)) {
    return true;
  }

  if (normalized.endsWith('secret') && normalized !== 'secretenv') {
    return true;
  }

  if (normalized.endsWith('token') && !isSafeTokenMetadataKey(normalized)) {
    return true;
  }

  if (normalized === 'key' && path.some((item) => normalizeSecretKey(item) === 'principals')) {
    return true;
  }

  return false;
}

function isReferenceObject(value: unknown): boolean {
  if (!isObject(value)) {
    return false;
  }

  const entries = Object.entries(value as Record<string, unknown>);
  return entries.length === 1 && entries[0]?.[0] === 'from' && typeof entries[0]?.[1] === 'string';
}

function clonePlainObject(value: Record<string, unknown>): Record<string, unknown> {
  return { ...value };
}

function isSensitiveHeaderValue(key: string, path: string[]): boolean {
  const parentKey = normalizeSecretKey(path[path.length - 1] || '');
  if (!parentKey.endsWith('headers')) {
    return false;
  }

  const normalizedHeader = key.trim().toLowerCase();
  return (
    normalizedHeader === 'authorization' ||
    normalizedHeader === 'proxy-authorization' ||
    normalizedHeader === 'cookie' ||
    normalizedHeader === 'set-cookie' ||
    normalizedHeader.includes('api-key') ||
    normalizedHeader.includes('apikey') ||
    normalizedHeader.includes('token') ||
    normalizedHeader.includes('secret') ||
    normalizedHeader.includes('signature')
  );
}

function isSafeTokenMetadataKey(normalized: string): boolean {
  return (
    normalized === 'tokenendpoint' ||
    normalized === 'tokenheader' ||
    normalized === 'tokenbeareronly' ||
    normalized === 'requesttokenfield' ||
    normalized === 'querytokenparam'
  );
}

function isSafeApiKeyMetadataKey(normalized: string): boolean {
  return normalized === 'apikeyenv' || normalized === 'apikeyheader';
}

function normalizeSecretKey(value: string): string {
  return value.replace(/[^a-zA-Z0-9]/g, '').toLowerCase();
}

function authenticateManagerRequest(
  request: FastifyRequest,
  config: GatewayConfig
): { ok: true } | { ok: false; statusCode: number; message: string } {
  const managerApiKey = process.env.MANAGER_API_KEY?.trim();
  if (!managerApiKey) {
    if (isLocalIp(request.ip) && isLoopbackHost(config.host) && !hasForwardedClientIpHeaders(request)) {
      return { ok: true };
    }

    return {
      ok: false,
      statusCode: 403,
      message:
        'MANAGER_API_KEY is not configured. Manager API only accepts direct localhost requests when the gateway listener is bound to loopback.'
    };
  }

  const explicitKey = readHeader(request.headers['x-manager-key']);
  const authHeader = readHeader(request.headers.authorization);
  const bearer = readBearerToken(authHeader);
  const suppliedKey = (explicitKey || bearer || '').trim();

  if (!suppliedKey) {
    return {
      ok: false,
      statusCode: 401,
      message: 'Missing manager api key. Use x-manager-key or Authorization: Bearer <key>.'
    };
  }

  if (suppliedKey !== managerApiKey) {
    return {
      ok: false,
      statusCode: 403,
      message: 'Invalid manager api key.'
    };
  }

  return { ok: true };
}

function sendProviderManagementDisabled(reply: FastifyReply) {
  return reply.code(405).send({
    error: {
      message: PROVIDER_MANAGEMENT_DISABLED_MESSAGE
    }
  });
}

function formatPluginExtensionSummary(extension: { key: string; transport?: string }) {
  return {
    key: extension.key,
    transport: extension.transport
  };
}

function buildProviderHealthSnapshots(providers: ProviderConfig[]) {
  return providers.map((providerConfig) => ({
    name: providerConfig.name,
    provider: providerFromProviderType(providerConfig.type),
    type: providerConfig.type,
    models: providerConfig.models,
    health: providerConfig.health || {
      status: 'unknown'
    }
  }));
}

function readHealthCheckProviderSelector(body: Record<string, unknown>): string | undefined {
  return (
    readStringValue(body.providerName) ||
    readStringValue(body.provider_name) ||
    readStringValue(body.provider) ||
    readStringValue(body.name)
  );
}

function selectProvidersForHealthCheck(
  providers: ProviderConfig[],
  selector: string | undefined
): ProviderConfig[] {
  if (!selector) {
    return providers;
  }

  const normalized = selector.trim().toLowerCase();
  const providerType = parseProvider(normalized);
  return providers.filter((providerConfig) => {
    if (providerConfig.name.trim().toLowerCase() === normalized) {
      return true;
    }

    return providerType !== undefined && providerFromProviderType(providerConfig.type) === providerType;
  });
}

function containsProviderPayload(body: Record<string, unknown>): boolean {
  return Object.prototype.hasOwnProperty.call(body, 'providers') || Object.prototype.hasOwnProperty.call(body, 'Providers');
}

function isLocalIp(ip: string): boolean {
  const normalized = ip.trim().toLowerCase();
  return normalized === '127.0.0.1' || normalized === '::1' || normalized === '::ffff:127.0.0.1';
}

function isLoopbackHost(host: string): boolean {
  const normalized = host.trim().toLowerCase();
  return normalized === 'localhost' || normalized === '127.0.0.1' || normalized === '::1' || normalized === '[::1]';
}

function hasForwardedClientIpHeaders(request: FastifyRequest): boolean {
  return (
    request.headers.forwarded !== undefined ||
    request.headers['x-forwarded-for'] !== undefined ||
    request.headers['x-real-ip'] !== undefined ||
    request.headers['x-client-ip'] !== undefined
  );
}

function readGatewayConfigFile(path: string): Record<string, unknown> {
  if (!existsSync(path)) {
    return {};
  }

  const raw = readFileSync(path, 'utf8').trim();
  if (!raw) {
    return {};
  }

  const parsed = JSON.parse(raw);
  if (!isObject(parsed)) {
    throw new Error('gateway.config.json must be a JSON object.');
  }

  return parsed as Record<string, unknown>;
}

function writeJsonFile(path: string, payload: Record<string, unknown>): void {
  const parentDir = dirname(path);
  mkdirSync(parentDir, { recursive: true });

  const tempPath = `${path}.tmp`;
  const content = `${JSON.stringify(payload, null, 2)}\n`;
  writeFileSync(tempPath, content, 'utf8');
  renameSync(tempPath, path);
}

function collectReloadWarnings(
  previousConfig: Record<string, unknown>,
  nextConfig: GatewayConfig,
  rawNextConfig?: Record<string, unknown>
): string[] {
  const warnings: string[] = [];

  const previousHost = readStringValue(previousConfig.host);
  const previousPort = readNumberValue(previousConfig.port);
  if (previousHost !== nextConfig.host || previousPort !== nextConfig.port) {
    warnings.push('host/port updated in file, but listener address will take effect after process restart.');
  }

  const previousAgent = isObject(previousConfig.agent)
    ? (previousConfig.agent as Record<string, unknown>)
    : undefined;
  const previousStorage = previousAgent && isObject(previousAgent.storage)
    ? (previousAgent.storage as Record<string, unknown>)
    : undefined;
  if (
    previousStorage &&
    readStringValue(previousStorage.dir) &&
    nextConfig.agent.storage.type === 'filesystem' &&
    readStringValue(previousStorage.dir) !== nextConfig.agent.storage.dir
  ) {
    warnings.push('agent.storage.dir changed, but running agent persistence is already initialized; restart recommended.');
  }

  const previousMcpServers = previousAgent?.mcpServers;
  if (JSON.stringify(previousMcpServers || []) !== JSON.stringify(nextConfig.agent.mcpServers || [])) {
    warnings.push('agent.mcpServers changed, but MCP client connections are not hot-reloaded; restart required.');
  }

  const previousMcpGateway = isObject(previousConfig.mcpGateway)
    ? (previousConfig.mcpGateway as Record<string, unknown>)
    : undefined;
  const previousMcpEndpoint = readStringValue(previousMcpGateway?.endpoint);
  const previousWs = previousMcpGateway && isObject(previousMcpGateway.websocket)
    ? (previousMcpGateway.websocket as Record<string, unknown>)
    : undefined;
  const previousWsEndpoint = readStringValue(previousWs?.endpoint);
  if (
    (previousMcpEndpoint && previousMcpEndpoint !== nextConfig.mcpGateway.endpoint) ||
    (previousWsEndpoint && previousWsEndpoint !== nextConfig.mcpGateway.websocket.endpoint)
  ) {
    warnings.push('mcpGateway endpoint path changed, but HTTP/WS route bindings require process restart.');
  }

  warnings.push(...collectPluginConfigWarnings(rawNextConfig, nextConfig));

  return warnings;
}

function collectPluginConfigWarnings(
  rawConfig: Record<string, unknown> | undefined,
  nextConfig: GatewayConfig
): string[] {
  const warnings: string[] = [];
  const rawPlugins = rawConfig?.plugins;
  if (rawPlugins !== undefined && !Array.isArray(rawPlugins)) {
    warnings.push('plugins must be an array; configured gateway plugins were ignored.');
    return warnings;
  }

  if (Array.isArray(rawPlugins)) {
    const parsedPluginKeys = new Set((nextConfig.plugins || []).map((plugin) => plugin.key));
    for (const [index, entry] of rawPlugins.entries()) {
      if (!isObject(entry)) {
        warnings.push(`plugins[${index}] is not an object and was ignored.`);
        continue;
      }

      const rawPlugin = entry as Record<string, unknown>;
      const key = readStringValue(rawPlugin.key);
      if (!key) {
        warnings.push(`plugins[${index}] is missing key and was ignored.`);
        continue;
      }

      if (rawPlugin.enabled === false) {
        continue;
      }

      const hasModulePath = Boolean(readStringValue(rawPlugin.modulePath) || readStringValue(rawPlugin.path));
      const hasHooks = rawPlugin.providerHooks !== undefined || rawPlugin.providerHook !== undefined || rawPlugin.hooks !== undefined;
      if (!hasModulePath && !hasHooks) {
        warnings.push(`plugins[${index}] "${key}" has no modulePath or providerHooks and was ignored.`);
      }

      if (isObject(rawPlugin.manifest) && !readStringValue((rawPlugin.manifest as Record<string, unknown>).name)) {
        warnings.push(`plugins[${index}] "${key}" manifest.name is missing; manifest metadata was ignored.`);
      }

      if (rawPlugin.watchFiles !== undefined && !Array.isArray(rawPlugin.watchFiles) && typeof rawPlugin.watchFiles !== 'string') {
        warnings.push(`plugins[${index}] "${key}" watchFiles must be an array or comma-delimited string.`);
      }

      if (!parsedPluginKeys.has(key) && hasModulePath) {
        warnings.push(`plugins[${index}] "${key}" was not loaded by config parser; check enabled/key/modulePath fields.`);
      }
    }
  }

  const rawProviderPlugins = rawConfig?.providerPlugins;
  if (rawProviderPlugins !== undefined && !Array.isArray(rawProviderPlugins)) {
    warnings.push('providerPlugins must be an array; configured legacy provider plugins were ignored.');
  }

  return warnings;
}

function cloneJsonObject(value: Record<string, unknown>): Record<string, unknown> {
  return JSON.parse(JSON.stringify(value)) as Record<string, unknown>;
}

function cloneJsonValue<T>(value: T): T {
  if (value === undefined) {
    return value;
  }

  return JSON.parse(JSON.stringify(value)) as T;
}

function readStringValue(value: unknown): string | undefined {
  if (typeof value !== 'string') {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed || undefined;
}

function readNumberValue(value: unknown): number | undefined {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return undefined;
  }
  return value;
}
