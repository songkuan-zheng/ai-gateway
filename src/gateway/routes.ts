import type {
  FastifyInstance,
  FastifyReply,
  FastifyRequest,
  preHandlerHookHandler
} from 'fastify';
import type {
  GatewayConfig,
  ProviderConfig,
  SourceAdapter,
  SourceAdapterRoute,
  SourceAdapterRouteMethod,
  VirtualModelProfileConfig
} from '../types';
import { providerFromProviderType, readHeader } from '../utils';
import { addGatewayAuthModelCandidate, createGatewayAuthPreHandler } from './auth';
import { handleOpenAIEmbeddingsRequest } from './embeddings';
import {
  handleOpenAIImageEditsRequest,
  handleOpenAIImageGenerationsRequest,
  handleOpenAIModerationsRequest,
  handleOpenAIVideoContentRequest,
  handleOpenAIVideoGenerationRequest,
  handleOpenAIVideoStatusRequest,
  handleXAIVideoGenerationRequest,
  registerOpenAIMediaBodyParsers
} from './openai-json';
import { handleGatewayRequest, parseGeminiTail } from './handler';
import { createGatewayIdempotencyPreHandler } from './idempotency';
import { listGatewayVirtualModelProfiles, type GatewayRuntime } from './runtime';
import { decodeGatewayVideoId } from './video-compat';
import { executeGatewayPluginRequestHookStage } from '../plugins/hooks';

type ModelListFormat = 'openai' | 'anthropic';

interface ModelListQuery {
  format?: string;
  protocol?: string;
}

interface ModelPathParams {
  model: string;
}

interface ModelWildcardParams {
  '*': string;
}

interface GatewayModelListEntry {
  id: string;
  displayName: string;
  ownedBy: string;
  created: number;
  createdAt: string;
}

interface BaseModelListEntry extends GatewayModelListEntry {
  providerName: string;
  modelName: string;
}

interface PluginSourceAdapterRouteMatch {
  adapter: SourceAdapter;
  route: SourceAdapterRoute;
  method: SourceAdapterRouteMethod;
}

const unknownModelCreated = 0;
const unknownModelCreatedAt = '1970-01-01T00:00:00Z';

export function registerGatewayRoutes(
  fastify: FastifyInstance,
  config: GatewayConfig,
  runtime: GatewayRuntime
) {
  const gatewayAuthPreHandler = createGatewayAuthPreHandler(config.auth);
  const gatewayPluginPreAuthHandler = createGatewayPluginPreAuthHandler(config, runtime);
  const gatewayVideoModelPreHandler = createGatewayVideoModelPreHandler(config);
  const gatewayIdempotencyPreHandler = createGatewayIdempotencyPreHandler(config);
  const gatewayWritePreHandlers = [
    gatewayPluginPreAuthHandler,
    gatewayAuthPreHandler,
    gatewayIdempotencyPreHandler
  ];

  fastify.get<{ Querystring: ModelListQuery }>(
    '/v1/models',
    { preHandler: gatewayAuthPreHandler },
    async (request) => {
      const entries = buildGatewayModelListEntries(config, runtime);
      const format = resolveModelListFormat(request);

      if (format === 'anthropic') {
        return formatAnthropicModelList(entries);
      }

      return formatOpenAIModelList(entries);
    }
  );

  fastify.get<{ Params: ModelPathParams }>(
    '/v1/models/:model',
    { preHandler: gatewayAuthPreHandler },
    async (request, reply) => {
      return handleGetGatewayModel(request.params.model, reply, config, runtime);
    }
  );

  fastify.get<{ Params: ModelWildcardParams }>(
    '/v1/models/*',
    { preHandler: gatewayAuthPreHandler },
    async (request, reply) => {
      return handleGetGatewayModel(request.params['*'], reply, config, runtime);
    }
  );

  fastify.post('/v1/chat/completions', { preHandler: gatewayWritePreHandlers }, async (request, reply) => {
    return handleGatewayRequest(
      request,
      reply,
      {
        adapterKey: 'openai_chat'
      },
      config,
      runtime
    );
  });

  fastify.post('/v1/responses', { preHandler: gatewayWritePreHandlers }, async (request, reply) => {
    return handleGatewayRequest(
      request,
      reply,
      {
        adapterKey: 'openai_responses'
      },
      config,
      runtime
    );
  });

  fastify.post('/v1/embeddings', { preHandler: gatewayWritePreHandlers }, async (request, reply) => {
    return handleOpenAIEmbeddingsRequest(request, reply, config, runtime);
  });

  fastify.post('/v1/moderations', { preHandler: gatewayWritePreHandlers }, async (request, reply) => {
    return handleOpenAIModerationsRequest(request, reply, config, runtime);
  });

  fastify.post('/v1/images/generations', { preHandler: gatewayWritePreHandlers }, async (request, reply) => {
    return handleOpenAIImageGenerationsRequest(request, reply, config, runtime);
  });

  fastify.register(async (mediaFastify) => {
    registerOpenAIMediaBodyParsers(mediaFastify, config.bodyLimitBytes);
    mediaFastify.post('/v1/images/edits', { preHandler: gatewayWritePreHandlers }, async (request, reply) => {
      return handleOpenAIImageEditsRequest(request, reply, config, runtime);
    });
    mediaFastify.post('/v1/videos', { preHandler: gatewayWritePreHandlers }, async (request, reply) => {
      return handleOpenAIVideoGenerationRequest(request, reply, config, runtime);
    });
  });

  fastify.post('/v1/videos/generations', { preHandler: gatewayWritePreHandlers }, async (request, reply) => {
    return handleXAIVideoGenerationRequest(request, reply, config, runtime);
  });

  fastify.get<{ Params: { '*': string } }>(
    '/v1/videos/*',
    { preHandler: [gatewayVideoModelPreHandler, gatewayAuthPreHandler] },
    async (request, reply) => {
      const tail = request.params['*'];
      if (tail.endsWith('/content')) {
        const id = tail.slice(0, -'/content'.length);
        if (id && !id.includes('/')) {
          return handleOpenAIVideoContentRequest(request, reply, config, runtime, id);
        }
      } else if (tail && !tail.includes('/')) {
        return handleOpenAIVideoStatusRequest(request, reply, config, runtime, tail);
      }
      return reply.code(404).send({ error: { message: 'Video route not found.' } });
    }
  );

  fastify.post('/v1/messages', { preHandler: gatewayWritePreHandlers }, async (request, reply) => {
    return handleGatewayRequest(
      request,
      reply,
      {
        adapterKey: 'anthropic_messages'
      },
      config,
      runtime
    );
  });

  fastify.post<{ Params: { '*': string } }>(
    '/v1beta/models/*',
    { preHandler: gatewayWritePreHandlers },
    async (request, reply) => {
      return handleGeminiRequest(request, reply, 'v1beta', config, runtime);
    }
  );

  fastify.post('/v1beta/interactions', { preHandler: gatewayWritePreHandlers }, async (request, reply) => {
    return handleGatewayRequest(
      request,
      reply,
      {
        adapterKey: 'gemini_interactions',
        metadata: {
          apiVersion: 'v1beta'
        }
      },
      config,
      runtime
    );
  });

  fastify.post<{ Params: { '*': string } }>(
    '/v1/models/*',
    { preHandler: gatewayWritePreHandlers },
    async (request, reply) => {
      return handleGeminiRequest(request, reply, 'v1', config, runtime);
    }
  );

  fastify.post('/v1/interactions', { preHandler: gatewayWritePreHandlers }, async (request, reply) => {
    return handleGatewayRequest(
      request,
      reply,
      {
        adapterKey: 'gemini_interactions',
        metadata: {
          apiVersion: 'v1'
        }
      },
      config,
      runtime
    );
  });

  fastify.all<{ Params: { '*': string } }>('/*', async (request, reply) => {
    const match = resolvePluginSourceAdapterRoute(runtime, request.method, request.url);
    if (!match) {
      return reply.code(404).send({
        error: {
          message: 'Route not found.'
        }
      });
    }

    const canContinue = await runGatewayRoutePreHandlers(gatewayWritePreHandlers, request, reply);
    if (!canContinue) {
      return reply;
    }

    return handleGatewayRequest(
      request,
      reply,
      {
        adapterKey: match.adapter.key,
        metadata: {
          ...(match.route.metadata || {}),
          sourceRoute: match.route.path,
          sourceRouteMethod: match.method
        }
      },
      config,
      runtime
    );
  });
}

function resolvePluginSourceAdapterRoute(
  runtime: GatewayRuntime,
  method: string,
  rawUrl: string
): PluginSourceAdapterRouteMatch | undefined {
  const requestMethod = normalizeSourceAdapterRouteMethod(method);
  if (!requestMethod) {
    return undefined;
  }

  const requestPath = normalizeRequestPath(rawUrl);
  for (const adapter of runtime.sourceAdapters.list()) {
    for (const route of adapter.routes || []) {
      const routePath = normalizeSourceAdapterRoutePath(route.path);
      if (!routePath) {
        continue;
      }

      const routeMethod = normalizeSourceAdapterRouteMethod(route.method || 'POST');
      if (routeMethod !== requestMethod) {
        continue;
      }

      if (doesSourceAdapterRouteMatchPath(routePath, requestPath)) {
        return {
          adapter,
          route,
          method: routeMethod
        };
      }
    }
  }

  return undefined;
}

async function runGatewayRoutePreHandlers(
  handlers: preHandlerHookHandler[],
  request: FastifyRequest,
  reply: FastifyReply
): Promise<boolean> {
  for (const handler of handlers) {
    await runGatewayRoutePreHandler(handler, request, reply);
    if (reply.sent) {
      return false;
    }
  }

  return true;
}

function runGatewayRoutePreHandler(
  handler: preHandlerHookHandler,
  request: FastifyRequest,
  reply: FastifyReply
): Promise<void> {
  return new Promise((resolve, reject) => {
    let doneCalled = false;
    const done = (error?: Error) => {
      if (doneCalled) {
        return;
      }

      doneCalled = true;
      if (error) {
        reject(error);
        return;
      }
      resolve();
    };

    try {
      const callable = handler as unknown as (
        request: FastifyRequest,
        reply: FastifyReply,
        done: (error?: Error) => void
      ) => unknown;
      const result = callable(request, reply, done);
      if (result && typeof (result as Promise<unknown>).then === 'function') {
        void Promise.resolve(result).then(() => done(), reject);
        return;
      }

      if (handler.length < 3) {
        done();
      }
    } catch (error) {
      reject(error);
    }
  });
}

function normalizeRequestPath(rawUrl: string): string {
  const path = rawUrl.split('?')[0] || '/';
  return path.startsWith('/') ? path : `/${path}`;
}

function normalizeSourceAdapterRoutePath(path: string | undefined): string | undefined {
  const normalized = path?.trim();
  if (!normalized || !normalized.startsWith('/')) {
    return undefined;
  }

  return normalized;
}

function normalizeSourceAdapterRouteMethod(method: string | undefined): SourceAdapterRouteMethod | undefined {
  const normalized = method?.trim().toUpperCase();
  if (
    normalized === 'GET' ||
    normalized === 'POST' ||
    normalized === 'PUT' ||
    normalized === 'PATCH' ||
    normalized === 'DELETE'
  ) {
    return normalized;
  }

  return undefined;
}

function doesSourceAdapterRouteMatchPath(routePath: string, requestPath: string): boolean {
  if (routePath.endsWith('/*')) {
    const prefix = routePath.slice(0, -1);
    return requestPath.startsWith(prefix);
  }

  return routePath === requestPath;
}

function createGatewayPluginPreAuthHandler(
  config: GatewayConfig,
  runtime: GatewayRuntime
): preHandlerHookHandler {
  return async function gatewayPluginPreAuthHandler(request, reply): Promise<void> {
    const result = await executeGatewayPluginRequestHookStage(
      runtime.requestHooks.list(),
      'beforeAuth',
      {
        request,
        config,
        route: {
          method: request.method,
          url: request.url,
          route: request.routeOptions?.url
        }
      }
    );
    if (result.ok) {
      return;
    }

    reply.code(result.status || 403).send({
      error: {
        message: `Gateway plugin "${result.pluginKey}" beforeAuth failed: ${result.error}`,
        details: result.details
      }
    });
  };
}

function createGatewayVideoModelPreHandler(config: GatewayConfig): preHandlerHookHandler {
  return async function gatewayVideoModelPreHandler(request): Promise<void> {
    const tail = (request.params as { '*'?: unknown } | undefined)?.['*'];
    if (typeof tail !== 'string') {
      return;
    }
    const requestId = tail.endsWith('/content')
      ? tail.slice(0, -'/content'.length)
      : tail;
    if (!requestId || requestId.includes('/')) {
      return;
    }
    const reference = decodeGatewayVideoId(requestId, {
      signingSecret: config.media?.videoIdSigningSecret,
      ttlMs: config.media?.videoIdTtlMs
    });
    addGatewayAuthModelCandidate(request, reference?.model);
  };
}

function handleGetGatewayModel(
  rawModelId: string,
  reply: FastifyReply,
  config: GatewayConfig,
  runtime: GatewayRuntime
) {
  const modelId = decodeModelPathParam(rawModelId);
  const entry = buildGatewayModelListEntries(config, runtime).find((item) => item.id === modelId);
  if (!entry) {
    return reply.code(404).send({
      error: {
        message: `Model not found: ${modelId}`,
        type: 'invalid_request_error',
        code: 'model_not_found'
      }
    });
  }

  return formatOpenAIModelEntry(entry);
}

function resolveModelListFormat(
  request: FastifyRequest<{ Querystring: ModelListQuery }>
): ModelListFormat {
  const explicitFormat =
    parseModelListFormat(request.query?.format) ||
    parseModelListFormat(request.query?.protocol) ||
    parseModelListFormat(readHeader(request.headers['x-gateway-model-list-format']));

  if (explicitFormat) {
    return explicitFormat;
  }

  if (
    readHeader(request.headers['anthropic-version']) ||
    readHeader(request.headers['anthropic-beta'])
  ) {
    return 'anthropic';
  }

  return 'openai';
}

function parseModelListFormat(value: string | undefined): ModelListFormat | undefined {
  const normalized = value?.trim().toLowerCase();
  if (!normalized) {
    return undefined;
  }

  if (normalized === 'anthropic' || normalized === 'claude') {
    return 'anthropic';
  }

  if (normalized === 'openai') {
    return 'openai';
  }

  return undefined;
}

function formatOpenAIModelList(entries: GatewayModelListEntry[]) {
  return {
    object: 'list',
    data: entries.map(formatOpenAIModelEntry)
  };
}

function formatOpenAIModelEntry(entry: GatewayModelListEntry) {
  return {
    id: entry.id,
    object: 'model',
    created: entry.created,
    owned_by: entry.ownedBy
  };
}

function formatAnthropicModelList(entries: GatewayModelListEntry[]) {
  return {
    data: entries.map((entry) => ({
      created_at: entry.createdAt,
      display_name: entry.displayName,
      id: entry.id,
      type: 'model'
    })),
    first_id: entries[0]?.id ?? null,
    has_more: false,
    last_id: entries.at(-1)?.id ?? null
  };
}

function buildGatewayModelListEntries(
  config: GatewayConfig,
  runtime?: Pick<GatewayRuntime, 'virtualModelProfiles'>
): GatewayModelListEntry[] {
  const seen = new Set<string>();
  const entries: GatewayModelListEntry[] = [];
  const baseEntries: BaseModelListEntry[] = [];
  const bareModelIds = config.modelList?.bareModelIds === true;

  const pushEntry = (entry: GatewayModelListEntry): void => {
    if (!entry.id || seen.has(entry.id)) {
      return;
    }

    seen.add(entry.id);
    entries.push(entry);
  };

  for (const providerConfig of config.providers) {
    const ownedBy = resolveProviderOwner(providerConfig);
    for (const rawModelName of providerConfig.models) {
      const modelName = rawModelName.trim();
      if (!modelName) {
        continue;
      }

      const id = buildBaseModelListId(providerConfig.name, modelName, bareModelIds);
      const entry: BaseModelListEntry = {
        id,
        displayName: modelName,
        ownedBy,
        created: unknownModelCreated,
        createdAt: unknownModelCreatedAt,
        providerName: providerConfig.name,
        modelName
      };
      baseEntries.push(entry);
      pushEntry(entry);
    }
  }

  for (const entry of materializeVirtualModelListEntries(config, baseEntries, runtime)) {
    pushEntry(entry);
  }

  return entries;
}

function materializeVirtualModelListEntries(
  config: GatewayConfig,
  baseEntries: BaseModelListEntry[],
  runtime?: Pick<GatewayRuntime, 'virtualModelProfiles'>
): GatewayModelListEntry[] {
  const entries: GatewayModelListEntry[] = [];
  const configuredProviderNames = new Set(baseEntries.map((entry) => entry.providerName));
  const bareModelIds = config.modelList?.bareModelIds === true;

  for (const profile of listGatewayVirtualModelProfiles(config, runtime)) {
    if (!shouldMaterializeVirtualModel(profile)) {
      continue;
    }

    for (const baseEntry of baseEntries) {
      for (const prefix of profile.match.prefixes) {
        const id = buildPrefixedVirtualModelListId(baseEntry, prefix, bareModelIds);
        entries.push(createVirtualModelListEntry(profile, id, baseEntry.id, baseEntry.ownedBy));
      }

      for (const suffix of profile.match.suffixes) {
        const id = buildSuffixedVirtualModelListId(baseEntry, suffix, bareModelIds);
        entries.push(createVirtualModelListEntry(profile, id, baseEntry.id, baseEntry.ownedBy));
      }
    }

    if (!profile.baseModel?.fixedModel) {
      continue;
    }

    for (const alias of profile.match.exactAliases) {
      const id = resolveExactVirtualModelAlias(alias, profile.baseModel.fixedModel, bareModelIds);
      if (!id) {
        continue;
      }

      const owner = extractProviderName(id) || extractProviderName(profile.baseModel.fixedModel);
      if (!owner || !configuredProviderNames.has(owner)) {
        continue;
      }

      entries.push(createVirtualModelListEntry(profile, id, profile.baseModel.fixedModel, owner));
    }
  }

  return entries;
}

function buildBaseModelListId(providerName: string, modelName: string, bareModelIds: boolean): string {
  return bareModelIds ? modelName : `${providerName}/${modelName}`;
}

function buildPrefixedVirtualModelListId(
  baseEntry: BaseModelListEntry,
  prefix: string,
  bareModelIds: boolean
): string {
  return bareModelIds
    ? `${prefix}${baseEntry.modelName}`
    : `${baseEntry.providerName}/${prefix}${baseEntry.modelName}`;
}

function buildSuffixedVirtualModelListId(
  baseEntry: BaseModelListEntry,
  suffix: string,
  bareModelIds: boolean
): string {
  return bareModelIds
    ? `${baseEntry.modelName}${suffix}`
    : `${baseEntry.providerName}/${baseEntry.modelName}${suffix}`;
}

function shouldMaterializeVirtualModel(profile: VirtualModelProfileConfig): boolean {
  return (
    profile.enabled !== false &&
    profile.materialization.enabled !== false &&
    profile.materialization.includeInGatewayModels !== false
  );
}

function createVirtualModelListEntry(
  profile: VirtualModelProfileConfig,
  id: string,
  baseModelId: string,
  ownedBy: string | undefined
): GatewayModelListEntry {
  return {
    id,
    displayName: renderVirtualModelDisplayName(profile, id, baseModelId),
    ownedBy: ownedBy || extractProviderName(id) || 'gateway',
    created: unknownModelCreated,
    createdAt: unknownModelCreatedAt
  };
}

function renderVirtualModelDisplayName(
  profile: VirtualModelProfileConfig,
  aliasModelId: string,
  baseModelId: string
): string {
  const template = profile.materialization.displayNameTemplate;
  if (template) {
    return template
      .replaceAll('{alias}', aliasModelId)
      .replaceAll('{baseModel}', baseModelId)
      .replaceAll('{profileKey}', profile.key)
      .replaceAll('{profileDisplayName}', profile.displayName);
  }

  return extractModelName(aliasModelId) || aliasModelId;
}

function resolveProviderOwner(providerConfig: ProviderConfig): string {
  return providerConfig.name || providerFromProviderType(providerConfig.type);
}

function resolveExactVirtualModelAlias(
  alias: string,
  fixedModelId: string,
  bareModelIds: boolean
): string | undefined {
  const normalizedAlias = alias.trim();
  if (!normalizedAlias) {
    return undefined;
  }

  if (bareModelIds || normalizedAlias.includes('/')) {
    return normalizedAlias;
  }

  const providerName = extractProviderName(fixedModelId);
  return providerName ? `${providerName}/${normalizedAlias}` : undefined;
}

function extractProviderName(modelId: string | undefined): string | undefined {
  const normalized = modelId?.trim();
  if (!normalized) {
    return undefined;
  }

  const slashIndex = normalized.indexOf('/');
  if (slashIndex <= 0 || slashIndex >= normalized.length - 1) {
    return undefined;
  }

  return normalized.slice(0, slashIndex);
}

function extractModelName(modelId: string | undefined): string | undefined {
  const normalized = modelId?.trim();
  if (!normalized) {
    return undefined;
  }

  const slashIndex = normalized.indexOf('/');
  if (slashIndex <= 0 || slashIndex >= normalized.length - 1) {
    return normalized;
  }

  return normalized.slice(slashIndex + 1);
}

function decodeModelPathParam(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

async function handleGeminiRequest(
  request: FastifyRequest<{ Params: { '*': string } }>,
  reply: FastifyReply,
  apiVersion: string,
  config: GatewayConfig,
  runtime: GatewayRuntime
) {
  const tail = String(request.params['*'] || '');
  const parsed = parseGeminiTail(tail);

  if (!parsed) {
    return reply.code(400).send({
      error: {
        message: 'Invalid Gemini route. Expected /models/{model}:generateContent'
      }
    });
  }

  return handleGatewayRequest(
    request,
    reply,
    {
      adapterKey: parsed.action === 'streamGenerateContent' ? 'gemini_stream' : 'gemini_generate',
      metadata: {
        model: parsed.model,
        action: parsed.action,
        apiVersion
      }
    },
    config,
    runtime
  );
}
