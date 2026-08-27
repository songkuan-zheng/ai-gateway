import type { IncomingHttpHeaders, IncomingMessage } from 'node:http';
import { randomUUID } from 'node:crypto';
import type { Socket } from 'node:net';
import { URL } from 'node:url';
import type { FastifyInstance, FastifyRequest } from 'fastify';
import { WebSocket, WebSocketServer, type RawData } from 'ws';
import { calculateUsageBilling, publishBillingEvent } from '../billing';
import {
  buildOpenAIHeaders,
  normalizeOpenAIResponsesCompletedEventPayload
} from '../adapters/builtins/common';
import type {
  BillingRate,
  GatewayConfig,
  GatewayPluginRouteResolution,
  GatewayPluginTargetRoute,
  GatewaySourceContext,
  HeaderBag,
  Provider,
  ProviderConfig,
  ProviderPlugin,
  StandardUsage,
  UpstreamRequest
} from '../types';
import { err, ok, type Result } from '../types';
import { parseProvider } from '../utils';
import { authenticateGatewayRequest, evaluateApiKeyModelRestriction } from './auth';
import {
  parseGatewayCodexWsSourceAdapterKey,
  transformClientMessageToCodexRequest,
  type GatewayCodexWsSourceAdapterKey
} from './codex-websocket-conversion';
import { evaluateGatewayPrecheck } from './precheck';
import { recordGatewayPluginHookExecution } from './metrics';
import type { GatewayRuntime } from './runtime';
import { resolveGatewayClientIp } from './client-ip';
import { shouldRunProviderPlugin } from '../provider/plugins';
import {
  applyGatewayPluginRequestTransforms,
  executeGatewayPluginRequestHookStage,
  resolveGatewayPluginRoute
} from '../plugins/hooks';
import { runGatewayPluginProtectedOperation } from '../plugins/execution';
import {
  shouldBlockLiveStreamingForStrictBilling,
  strictBillingLiveStreamingUnsupportedMessage
} from './strict-billing';
import { applyHealthAwareRouting } from './health-routing';
import { recordProviderHealthFailure, recordProviderHealthResponse } from './provider-health';
import { applyGatewayScheduling, recordGatewaySchedulingResponse } from './scheduler';
import {
  checkProviderCircuitBreaker,
  recordProviderCircuitBreakerFailure,
  recordProviderCircuitBreakerResponse
} from './upstream-circuit-breaker';
import { acquireProviderConcurrencySlot } from './upstream-concurrency';

interface GatewaySocketContext {
  headers: IncomingHttpHeaders;
  requestUrl: string;
  request: FastifyRequest;
  sourceAdapterHint?: GatewayCodexWsSourceAdapterKey;
  targetProviderConfig?: ProviderConfig;
  billingModel?: string;
}

interface ResponsesWebSocketTargetRoute {
  provider: Provider;
  providerConfig?: ProviderConfig;
}

interface ResponsesWebSocketUpstreamTarget {
  provider: Provider;
  baseUrl: string;
  apiKey?: string;
  providerConfig?: ProviderConfig;
}

const blockedForwardHeaderSet = new Set([
  'host',
  'connection',
  'upgrade',
  'keep-alive',
  'proxy-connection',
  'transfer-encoding',
  'content-length',
  'content-type',
  'authorization',
  'proxy-authorization',
  'sec-websocket-key',
  'sec-websocket-version',
  'sec-websocket-extensions',
  'sec-websocket-protocol',
  'sec-websocket-accept'
]);

const internalGatewayQueryParamSet = new Set(['source_adapter', 'source']);
const codexDefaultInstructions = 'You are a helpful assistant.';
const pendingDownstreamMessageLimit = 16;
type WebSocketPayload = RawData | string;

export function registerGatewayResponsesWebSocketRoute(
  fastify: FastifyInstance,
  config: GatewayConfig,
  runtime?: Pick<GatewayRuntime, 'providerPlugins' | 'requestHooks' | 'requestTransforms' | 'routeResolvers'>
): void {
  const maxPayload = resolveGatewayWebSocketMaxPayloadBytes(config);
  const websocketServer = new WebSocketServer({ noServer: true, maxPayload });
  const socketContext = new WeakMap<WebSocket, GatewaySocketContext>();

  const onUpgrade = (request: IncomingMessage, socket: Socket, head: Buffer): void => {
    const requestUrl = safeParseRequestUrl(request);
    if (!requestUrl || !isResponsesWebSocketPath(requestUrl.pathname)) {
      return;
    }

    void authorizeAndUpgrade(request, socket, head, requestUrl);
  };

  async function authorizeAndUpgrade(
    request: IncomingMessage,
    socket: Socket,
    head: Buffer,
    requestUrl: URL
  ): Promise<void> {
    const pluginCompatibleRequest = createWebSocketPluginCompatibleRequest(request, requestUrl, fastify);
    const beforeAuthResult = await executeGatewayPluginRequestHookStage(
      runtime?.requestHooks?.list() || [],
      'beforeAuth',
      {
        request: pluginCompatibleRequest,
        config,
        route: {
          method: request.method || 'GET',
          url: request.url || requestUrl.pathname,
          route: 'WS /v1/responses'
        }
      }
    );
    if (!beforeAuthResult.ok) {
      rejectUpgrade(
        socket,
        beforeAuthResult.status || 403,
        `Gateway plugin "${beforeAuthResult.pluginKey}" beforeAuth failed: ${beforeAuthResult.error}`
      );
      return;
    }

    let authResult;
    try {
      authResult = await authenticateGatewayRequest(
        pluginCompatibleRequest,
        config.auth
      );
    } catch (error) {
      fastify.log.warn(
        {
          details: error instanceof Error ? error.message : String(error)
        },
        'Gateway websocket auth check failed unexpectedly.'
      );
      rejectUpgrade(socket, 500, 'Gateway websocket auth check failed.');
      return;
    }

    if (!authResult.ok) {
      rejectUpgrade(socket, authResult.statusCode || 401, authResult.error || 'Unauthorized');
      return;
    }

    if (shouldBlockLiveStreamingForStrictBilling(config)) {
      rejectUpgrade(socket, 400, strictBillingLiveStreamingUnsupportedMessage);
      return;
    }

    websocketServer.handleUpgrade(request, socket, head, (ws) => {
      pluginCompatibleRequest.gatewayIdentity = authResult.ok ? authResult.identity : undefined;
      pluginCompatibleRequest.gatewayApiKeyRestrictions = authResult.ok
        ? authResult.apiKeyRestrictions
        : undefined;
      socketContext.set(ws, {
        headers: request.headers,
        requestUrl: requestUrl.toString(),
        request: pluginCompatibleRequest,
        sourceAdapterHint: readSourceAdapterHintFromRequestUrl(requestUrl)
      });
      websocketServer.emit('connection', ws, request);
    });
  }

  websocketServer.on('connection', (downstreamSocket) => {
    void initializeWebSocketRelay(downstreamSocket);
  });

  async function initializeWebSocketRelay(downstreamSocket: WebSocket): Promise<void> {
    const context = socketContext.get(downstreamSocket);
    if (!context) {
      downstreamSocket.close(1008, 'Unauthorized');
      return;
    }

    let upstreamSocket: WebSocket | undefined;
    let releaseConcurrency: (() => void) | undefined;
    let concurrencyReleased = false;
    const initializationAbortController = new AbortController();
    const releaseInitializationConcurrency = (): void => {
      if (concurrencyReleased || !releaseConcurrency) {
        return;
      }
      concurrencyReleased = true;
      releaseConcurrency();
    };
    const initializationCancelled = (): boolean => (
      initializationAbortController.signal.aborted ||
      downstreamSocket.readyState === WebSocket.CLOSING ||
      downstreamSocket.readyState === WebSocket.CLOSED
    );
    const onDownstreamClosedDuringInitialization = (): void => {
      initializationAbortController.abort(
        new Error('Downstream websocket closed while the upstream relay was initializing.')
      );
      releaseInitializationConcurrency();
      if (
        upstreamSocket &&
        (upstreamSocket.readyState === WebSocket.CONNECTING || upstreamSocket.readyState === WebSocket.OPEN)
      ) {
        upstreamSocket.terminate();
      }
    };
    downstreamSocket.once('close', onDownstreamClosedDuringInitialization);

    try {
      let sourceAdapterKey = context.sourceAdapterHint || 'openai_responses';
      const beforeRoutingTransformResult = await applyGatewayPluginRequestTransforms(
        runtime?.requestTransforms?.list() || [],
        {
          stage: 'beforeRouting',
          request: context.request,
          config,
          route: {
            method: context.request.method,
            url: context.request.url,
            route: 'WS /v1/responses',
            sourceAdapterKey,
            sourceRoute: 'websocket'
          },
          source: {
            adapterKey: sourceAdapterKey,
            metadata: {
              sourceRoute: 'websocket'
            }
          },
          sourceProvider: 'openai',
          sourceAdapterKey,
          targetProvider: 'openai'
        }
      );
      if (initializationCancelled()) {
        return;
      }
      if (!beforeRoutingTransformResult.ok) {
        downstreamSocket.close(
          1008,
          `Gateway plugin "${beforeRoutingTransformResult.pluginKey}" beforeRouting request transform failed: ${beforeRoutingTransformResult.error}`
        );
        return;
      }
      if (beforeRoutingTransformResult.value.source?.adapterKey) {
        const transformedSourceAdapterKey = parseGatewayCodexWsSourceAdapterKey(
          beforeRoutingTransformResult.value.source.adapterKey
        );
        if (!transformedSourceAdapterKey) {
          downstreamSocket.close(
            1008,
            'Gateway plugin request transform selected an unsupported websocket source adapter.'
          );
          return;
        }
        sourceAdapterKey = transformedSourceAdapterKey;
        context.sourceAdapterHint = transformedSourceAdapterKey;
      }
      const routeResolutionResult = await resolveGatewayPluginRoute(
        runtime?.routeResolvers?.list() || [],
        {
          request: context.request,
          config,
          route: {
            method: context.request.method,
            url: context.request.url,
            route: 'WS /v1/responses',
            sourceAdapterKey,
            sourceRoute: 'websocket'
          },
          source: {
            adapterKey: sourceAdapterKey,
            metadata: {
              sourceRoute: 'websocket'
            }
          },
          sourceProvider: 'openai',
          sourceAdapterKey,
          targetProvider: 'openai'
        }
      );
      if (initializationCancelled()) {
        return;
      }
      if (!routeResolutionResult.ok) {
        downstreamSocket.close(
          1008,
          `Gateway plugin "${routeResolutionResult.pluginKey}" route resolver failed: ${routeResolutionResult.error}`
        );
        return;
      }
      const targetRoutesResult = resolveResponsesWebSocketTargetRoutes(
        config,
        context,
        routeResolutionResult.value
      );
      if (!targetRoutesResult.ok) {
        downstreamSocket.close(1008, targetRoutesResult.error);
        return;
      }
      const scheduledTargetRoutes = await applyGatewayScheduling(targetRoutesResult.value, {
        config,
        request: context.request
      });
      if (initializationCancelled()) {
        return;
      }
      const targetRoutes = await applyHealthAwareRouting(scheduledTargetRoutes, config);
      if (initializationCancelled()) {
        return;
      }
      const selectedTargetRoute = targetRoutes[0];
      if (!selectedTargetRoute) {
        downstreamSocket.close(1013, 'No compatible /v1/responses websocket upstream provider is available.');
        return;
      }
      const upstreamTargetResult = resolveResponsesWebSocketTarget(config, selectedTargetRoute);
      if (!upstreamTargetResult.ok) {
        downstreamSocket.close(1008, upstreamTargetResult.error);
        return;
      }
      const upstreamTarget = upstreamTargetResult.value;
      context.targetProviderConfig = upstreamTarget.providerConfig;
      const beforeRoutingResult = await executeGatewayPluginRequestHookStage(
        runtime?.requestHooks?.list() || [],
        'beforeRouting',
        {
          request: context.request,
          config,
          route: {
            method: context.request.method,
            url: context.request.url,
            route: 'WS /v1/responses',
            sourceAdapterKey,
            sourceRoute: 'websocket'
          },
          source: {
            adapterKey: sourceAdapterKey,
            metadata: {
              sourceRoute: 'websocket'
            }
          },
          sourceProvider: 'openai',
          sourceAdapterKey,
          targetProvider: 'openai',
          targetProviderConfig: upstreamTarget.providerConfig
        }
      );
      if (initializationCancelled()) {
        return;
      }
      if (!beforeRoutingResult.ok) {
        downstreamSocket.close(
          1008,
          `Gateway plugin "${beforeRoutingResult.pluginKey}" beforeRouting failed: ${beforeRoutingResult.error}`
        );
        return;
      }

      const upstreamUrl = buildResponsesUpstreamUrl(upstreamTarget.baseUrl, context.requestUrl);
      const upstreamHeaders = buildUpstreamHeaders(context.headers, {
        openaiApiKey: upstreamTarget.apiKey,
        auth: config.auth,
        allowEnvApiKeyFallback: !looksLikeCodexBaseUrl(upstreamTarget.baseUrl)
      });
      const pluginContext: WebSocketProviderPluginContext = {
        request: context.request,
        config,
        source: {
          adapterKey: sourceAdapterKey
        },
        sourceProvider: 'openai',
        sourceAdapterKey,
        targetProvider: 'openai',
        targetProviderConfig: upstreamTarget.providerConfig,
        model: undefined,
        passthrough: true,
        streaming: true,
        plugins:
          runtime?.providerPlugins.resolve('openai', upstreamTarget.providerConfig?.name) || []
      };
      const upstreamRequestResult = await applyWebSocketProviderRequestPlugins(
        pluginContext,
        {
          url: upstreamUrl,
          headers: upstreamHeaders,
          body: {}
        }
      );
      if (initializationCancelled()) {
        return;
      }
      if (!upstreamRequestResult.ok) {
        fastify.log.warn(
          {
            details: upstreamRequestResult.error,
            providerName: upstreamTarget.providerConfig?.name
          },
          'Gateway responses websocket provider plugin auth failed.'
        );
        downstreamSocket.close(1011, `Failed to init upstream websocket: ${upstreamRequestResult.error}`);
        return;
      }

      const circuit = await checkProviderCircuitBreaker(
        config,
        upstreamTarget.provider,
        upstreamTarget.providerConfig
      );
      if (initializationCancelled()) {
        return;
      }
      if (!circuit.ok) {
        recordGatewaySchedulingResponse({
          config,
          request: context.request,
          providerConfig: upstreamTarget.providerConfig,
          error: true
        });
        downstreamSocket.close(1013, circuit.message);
        return;
      }

      const slot = await acquireProviderConcurrencySlot(
        config,
        upstreamTarget.provider,
        upstreamTarget.providerConfig,
        initializationAbortController.signal
      );
      if (initializationCancelled()) {
        if (slot.ok) {
          releaseConcurrency = slot.release;
          releaseInitializationConcurrency();
        }
        return;
      }
      if (!slot.ok) {
        recordGatewaySchedulingResponse({
          config,
          request: context.request,
          providerConfig: upstreamTarget.providerConfig,
          error: true
        });
        downstreamSocket.close(1013, slot.message);
        return;
      }
      releaseConcurrency = slot.release;

      const normalizedWebSocketUrl = normalizeUrlForWebSocket(upstreamRequestResult.value.url);
      upstreamSocket = new WebSocket(normalizedWebSocketUrl, {
        headers: upstreamRequestResult.value.headers,
        maxPayload: resolveGatewayWebSocketMaxPayloadBytes(config),
        handshakeTimeout: resolveGatewayWebSocketHandshakeTimeoutMs(config)
      });
      if (initializationCancelled()) {
        upstreamSocket.terminate();
        releaseInitializationConcurrency();
        return;
      }
      recordResponsesWebSocketUpstreamConnection(upstreamSocket, upstreamTarget, context, config);
      downstreamSocket.off('close', onDownstreamClosedDuringInitialization);
      bindSocketRelay(
        downstreamSocket,
        upstreamSocket,
        fastify,
        context,
        config,
        runtime,
        releaseInitializationConcurrency
      );
    } catch (error) {
      releaseInitializationConcurrency();
      if (
        upstreamSocket &&
        (upstreamSocket.readyState === WebSocket.CONNECTING || upstreamSocket.readyState === WebSocket.OPEN)
      ) {
        upstreamSocket.terminate();
      }
      if (!initializationCancelled()) {
        downstreamSocket.close(1011, `Failed to init upstream websocket: ${toErrorMessage(error)}`);
      }
      return;
    }
  }

  fastify.server.on('upgrade', onUpgrade);
  fastify.addHook('onClose', async () => {
    fastify.server.off('upgrade', onUpgrade);
    for (const client of websocketServer.clients) {
      client.terminate();
    }
    await new Promise<void>((resolve) => {
      websocketServer.close(() => resolve());
    });
  });
}

function bindSocketRelay(
  downstreamSocket: WebSocket,
  upstreamSocket: WebSocket,
  fastify: FastifyInstance,
  context: GatewaySocketContext,
  config: GatewayConfig,
  runtime?: Pick<GatewayRuntime, 'requestHooks'>,
  releaseResources: () => void = noop
): void {
  const pendingDownstreamMessages: Array<{
    payload: WebSocketPayload;
    binary: boolean;
    byteLength: number;
  }> = [];
  const pendingDownstreamByteLimit = resolveGatewayWebSocketMaxPayloadBytes(config);
  let pendingDownstreamBytes = 0;
  let pendingUpstreamToDownstreamSends = 0;
  let upstreamCloseForceTimer: NodeJS.Timeout | undefined;
  let downstreamMessageQueue = Promise.resolve();
  let upstreamMessageQueue = Promise.resolve();
  let upstreamClosePending:
    | {
        code: number;
        reason: Buffer | string;
      }
    | undefined;
  let resourcesReleased = false;

  const releaseRelayResources = (): void => {
    if (resourcesReleased) {
      return;
    }
    resourcesReleased = true;
    releaseResources();
  };

  const closePeer = (peer: WebSocket, code: number, reason: Buffer | string): void => {
    const normalizedCode = normalizeCloseCode(code);
    const reasonText = typeof reason === 'string' ? reason : reason.toString('utf8');
    if (peer.readyState === WebSocket.OPEN || peer.readyState === WebSocket.CONNECTING) {
      try {
        peer.close(normalizedCode, reasonText);
      } catch {
        peer.terminate();
      }
    }
  };

  const flushUpstreamCloseToDownstream = (): void => {
    if (!upstreamClosePending) {
      return;
    }

    if (pendingUpstreamToDownstreamSends > 0) {
      return;
    }

    const { code, reason } = upstreamClosePending;
    upstreamClosePending = undefined;
    if (upstreamCloseForceTimer) {
      clearTimeout(upstreamCloseForceTimer);
      upstreamCloseForceTimer = undefined;
    }
    closePeer(downstreamSocket, code, reason);
  };

  const sendMessageToUpstream = (payload: WebSocketPayload, binary: boolean): void => {
    upstreamSocket.send(payload, { binary }, (error) => {
      if (error) {
        closePeer(downstreamSocket, 1011, 'Upstream send failed.');
      }
    });
  };

  const flushPendingDownstreamMessages = (): void => {
    if (upstreamSocket.readyState !== WebSocket.OPEN) {
      return;
    }

    while (pendingDownstreamMessages.length > 0) {
      const next = pendingDownstreamMessages.shift();
      if (!next) {
        return;
      }

      pendingDownstreamBytes = Math.max(0, pendingDownstreamBytes - next.byteLength);
      sendMessageToUpstream(next.payload, next.binary);
    }
  };

  downstreamSocket.on('message', (raw, isBinary) => {
    downstreamMessageQueue = downstreamMessageQueue
      .then(() => handleDownstreamMessage(raw, isBinary))
      .catch((error) => {
        fastify.log.warn(
          {
            details: toErrorMessage(error)
          },
          'Gateway responses websocket downstream message handling failed.'
        );
        closePeer(downstreamSocket, 1011, 'Gateway websocket message handling failed.');
      });
  });

  async function handleDownstreamMessage(raw: RawData, isBinary: boolean): Promise<void> {
    const messageForUpstream = await buildMessageForUpstream(
      raw,
      isBinary,
      context,
      downstreamSocket,
      fastify,
      config,
      runtime
    );
    if (!messageForUpstream) {
      return;
    }

    if (upstreamSocket.readyState === WebSocket.CONNECTING) {
      const byteLength = webSocketPayloadByteLength(messageForUpstream.payload);
      if (
        pendingDownstreamMessages.length >= pendingDownstreamMessageLimit ||
        pendingDownstreamBytes + byteLength > pendingDownstreamByteLimit
      ) {
        closePeer(downstreamSocket, 1013, 'Gateway websocket upstream connection is not ready.');
        closePeer(upstreamSocket, 1013, 'Gateway websocket upstream connection is not ready.');
        return;
      }

      pendingDownstreamMessages.push({
        ...messageForUpstream,
        byteLength
      });
      pendingDownstreamBytes += byteLength;
      return;
    }

    if (upstreamSocket.readyState !== WebSocket.OPEN) {
      return;
    }

    sendMessageToUpstream(messageForUpstream.payload, messageForUpstream.binary);
  }

  upstreamSocket.on('message', (raw, isBinary) => {
    if (downstreamSocket.readyState !== WebSocket.OPEN) {
      return;
    }

    const messageForDownstream = buildMessageForDownstream(raw, isBinary);
    pendingUpstreamToDownstreamSends += 1;
    const relayMessage = () => relayUpstreamMessageToDownstream(messageForDownstream);
    const processing = shouldBlockLiveStreamingForStrictBilling(config)
      ? (upstreamMessageQueue = upstreamMessageQueue.then(relayMessage, relayMessage))
      : relayMessage();
    void processing
      .catch((error) => {
        fastify.log.warn(
          {
            requestId: context.request.id,
            details: toErrorMessage(error)
          },
          'Gateway responses websocket upstream message relay failed.'
        );
        closePeer(upstreamSocket, 1011, 'Gateway websocket message relay failed.');
        closePeer(downstreamSocket, 1011, 'Gateway websocket message relay failed.');
      })
      .finally(() => {
        pendingUpstreamToDownstreamSends = Math.max(0, pendingUpstreamToDownstreamSends - 1);
        flushUpstreamCloseToDownstream();
      });
  });

  async function relayUpstreamMessageToDownstream(message: {
    payload: WebSocketPayload;
    binary: boolean;
  }): Promise<void> {
    const strictBilling = shouldBlockLiveStreamingForStrictBilling(config);
    const processBilling = () => publishWebSocketBillingEventFromDownstreamPayload(
      message.payload,
      message.binary,
      context,
      config,
      fastify
    );

    if (strictBilling) {
      try {
        await processBilling();
      } catch (billingError) {
        logWebSocketBillingError(billingError);
        closePeer(upstreamSocket, 1011, 'Gateway websocket billing enforcement failed.');
        closePeer(downstreamSocket, 1011, 'Gateway websocket billing enforcement failed.');
        return;
      }
    }

    if (downstreamSocket.readyState !== WebSocket.OPEN) {
      return;
    }

    const sent = await new Promise<boolean>((resolve) => {
      downstreamSocket.send(message.payload, { binary: message.binary }, (error) => {
        if (error) {
          closePeer(upstreamSocket, 1011, 'Downstream send failed.');
          resolve(false);
          return;
        }
        resolve(true);
      });
    });
    if (!sent || strictBilling) {
      return;
    }

    void processBilling().catch(logWebSocketBillingError);
  }

  function logWebSocketBillingError(billingError: unknown): void {
    fastify.log.warn(
      {
        requestId: context.request.id,
        details: billingError instanceof Error ? billingError.message : String(billingError)
      },
      'Failed to process websocket billing event.'
    );
  }

  downstreamSocket.on('close', (code, reason) => {
    if (upstreamCloseForceTimer) {
      clearTimeout(upstreamCloseForceTimer);
      upstreamCloseForceTimer = undefined;
    }
    releaseRelayResources();
    closePeer(upstreamSocket, code, reason);
  });

  upstreamSocket.on('close', (code, reason) => {
    releaseRelayResources();
    upstreamClosePending = { code, reason };
    if (
      pendingUpstreamToDownstreamSends > 0 &&
      !shouldBlockLiveStreamingForStrictBilling(config) &&
      !upstreamCloseForceTimer
    ) {
      upstreamCloseForceTimer = setTimeout(() => {
        upstreamCloseForceTimer = undefined;
        if (!upstreamClosePending) {
          return;
        }

        const pendingClose = upstreamClosePending;
        upstreamClosePending = undefined;
        closePeer(downstreamSocket, pendingClose.code, pendingClose.reason);
      }, 500);
      upstreamCloseForceTimer.unref?.();
    }
    flushUpstreamCloseToDownstream();
  });

  upstreamSocket.on('open', () => {
    flushPendingDownstreamMessages();
  });

  downstreamSocket.on('error', (error) => {
    releaseRelayResources();
    fastify.log.warn(
      {
        details: toErrorMessage(error)
      },
      'Gateway responses websocket downstream error.'
    );
    upstreamSocket.terminate();
  });

  upstreamSocket.on('error', (error) => {
    releaseRelayResources();
    fastify.log.warn(
      {
        details: toErrorMessage(error)
      },
      'Gateway responses websocket upstream error.'
    );
    if (downstreamSocket.readyState === WebSocket.OPEN) {
      downstreamSocket.close(1011, 'Gateway upstream websocket error.');
    } else if (downstreamSocket.readyState === WebSocket.CONNECTING) {
      downstreamSocket.terminate();
    }
  });
}

function buildMessageForDownstream(
  raw: RawData,
  isBinary: boolean
): { payload: WebSocketPayload; binary: boolean } {
  if (isBinary) {
    return {
      payload: raw,
      binary: true
    };
  }

  return {
    payload: normalizeResponseCompletedTextPayload(rawDataToUtf8String(raw)),
    binary: false
  };
}

async function publishWebSocketBillingEventFromDownstreamPayload(
  payload: WebSocketPayload,
  isBinary: boolean,
  context: GatewaySocketContext,
  config: GatewayConfig,
  fastify: FastifyInstance
): Promise<void> {
  if (!config.billing?.enabled || isBinary || typeof payload !== 'string') {
    return;
  }

  const billingSnapshot = extractWebSocketCompletedBillingSnapshot(payload);
  if (!billingSnapshot) {
    if (config.billing.requireUsage && isWebSocketCompletedResponsePayload(payload)) {
      throw new Error('Billing usage is required but could not be parsed for websocket response.');
    }
    return;
  }

  const targetProvider = 'openai';
  const model = billingSnapshot.model || context.billingModel;
  const billing = calculateUsageBilling(
    targetProvider,
    billingSnapshot.usage,
    config.billing,
    resolveWebSocketBillingRate(config, targetProvider, model, context.targetProviderConfig)
  );
  if (config.billing.requireRates && hasBillableWebSocketUsage(billingSnapshot.usage) && billing.cost.total <= 0) {
    throw new Error(
      `Billing rates are required but produced zero cost for websocket provider ${targetProvider}${model ? ` model ${model}` : ''}.`
    );
  }
  const event = {
    eventId: randomUUID(),
    emittedAt: new Date().toISOString(),
    requestId: context.request.id,
    clientIp: resolveGatewayClientIp(context.request, config),
    route: {
      method: 'WS',
      url: sanitizeRequestUrlForEvent(context.request.url || context.requestUrl)
    },
    source: {
      provider: 'openai' as Provider,
      adapterKey: context.sourceAdapterHint || 'openai_responses'
    },
    target: {
      provider: targetProvider as Provider,
      providerName: context.targetProviderConfig?.name,
      model
    },
    fallback: {
      used: false,
      attempts: 0
    },
    identity: context.request.gatewayIdentity,
    outcome: {
      status: 'success' as const,
      statusCode: 200
    },
    billing
  };

  const delivered = await publishBillingEvent(event).catch((error) => {
    fastify.log.warn(
      {
        requestId: context.request.id,
        provider: targetProvider,
        details: error instanceof Error ? error.message : String(error)
      },
      'Failed to deliver websocket billing event.'
    );
    throw error;
  });
  if (!delivered && (config.billing.delivery?.requirePublisher || config.billing.delivery?.requireOutbox)) {
    throw new Error('WebSocket billing event was not delivered to any configured billing publisher or outbox.');
  }
}

function extractWebSocketCompletedBillingSnapshot(
  payload: string
): { usage: StandardUsage; model?: string } | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(payload);
  } catch {
    return undefined;
  }
  if (!isRecord(parsed) || parsed.type !== 'response.completed' || !isRecord(parsed.response)) {
    return undefined;
  }

  const usage = parseWebSocketUsage(parsed.response.usage);
  if (!usage) {
    return undefined;
  }

  return {
    usage,
    model: readStringField(parsed.response.model)
  };
}

function isWebSocketCompletedResponsePayload(payload: string): boolean {
  try {
    const parsed = JSON.parse(payload) as unknown;
    return isRecord(parsed) && parsed.type === 'response.completed';
  } catch {
    return false;
  }
}

function parseWebSocketUsage(value: unknown): StandardUsage | undefined {
  if (!isRecord(value)) {
    return undefined;
  }

  const inputTokens = readNumberField(value.input_tokens) ?? readNumberField(value.prompt_tokens);
  const outputTokens = readNumberField(value.output_tokens) ?? readNumberField(value.completion_tokens);
  const totalTokens = readNumberField(value.total_tokens);
  if (inputTokens === undefined && outputTokens === undefined && totalTokens === undefined) {
    return undefined;
  }

  const inputDetails = isRecord(value.input_tokens_details) ? value.input_tokens_details : undefined;
  return {
    input_tokens: inputTokens ?? Math.max((totalTokens ?? 0) - (outputTokens ?? 0), 0),
    output_tokens: outputTokens ?? Math.max((totalTokens ?? 0) - (inputTokens ?? 0), 0),
    total_tokens: totalTokens ?? (inputTokens ?? 0) + (outputTokens ?? 0),
    cache_read_tokens: readNumberField(inputDetails?.cached_tokens) ?? 0,
    cache_write_tokens: 0,
    cache_duration_seconds: 0
  };
}

function hasBillableWebSocketUsage(usage: StandardUsage): boolean {
  return [
    usage.input_tokens,
    usage.output_tokens,
    usage.cache_read_tokens,
    usage.cache_write_tokens,
    usage.total_tokens
  ].some((value) => typeof value === 'number' && Number.isFinite(value) && value > 0);
}

function resolveWebSocketBillingRate(
  config: GatewayConfig,
  provider: Provider,
  model: string | undefined,
  providerConfig: ProviderConfig | undefined
): BillingRate {
  return (
    (model ? providerConfig?.billing.byModel[model] : undefined) ||
    providerConfig?.billing.default ||
    config.billing.rates[provider]
  );
}

function normalizeResponseCompletedTextPayload(payload: string): string {
  let parsed: unknown;
  try {
    parsed = JSON.parse(payload);
  } catch {
    return payload;
  }

  if (!isRecord(parsed)) {
    return payload;
  }

  const normalized = normalizeOpenAIResponsesCompletedEventPayload(parsed);
  return normalized === parsed ? payload : JSON.stringify(normalized);
}

async function buildMessageForUpstream(
  raw: RawData,
  isBinary: boolean,
  context: GatewaySocketContext,
  downstreamSocket: WebSocket,
  fastify: FastifyInstance,
  config: GatewayConfig,
  runtime?: Pick<GatewayRuntime, 'requestHooks'>
): Promise<{ payload: WebSocketPayload; binary: boolean } | undefined> {
  if (isBinary) {
    return {
      payload: raw,
      binary: true
    };
  }

  const textPayload = rawDataToUtf8String(raw);
  const transformed = transformClientMessageToCodexRequest(textPayload, {
    sourceAdapterHint: context.sourceAdapterHint
  });
  if (transformed.kind === 'error') {
    sendInvalidRequestEvent(downstreamSocket, transformed.message);
    return undefined;
  }

  if (transformed.kind === 'converted') {
    fastify.log.debug(
      {
        sourceAdapterKey: transformed.sourceAdapterKey
      },
      'Gateway websocket request converted to Codex response.create.'
    );
  }

  const normalizedPayload = maybeNormalizeCodexResponseCreatePayload(
    transformed.payload,
    config.openaiBaseUrl
  );
  const guardResult = await evaluateWebSocketResponseCreateGuards(
    context,
    normalizedPayload,
    config,
    runtime
  );
  if (!guardResult.ok) {
    sendWebSocketErrorEvent(
      downstreamSocket,
      guardResult.statusCode,
      guardResult.message
    );
    return undefined;
  }

  return {
    payload: normalizedPayload,
    binary: false
  };
}

async function evaluateWebSocketResponseCreateGuards(
  context: GatewaySocketContext,
  payload: string,
  config: GatewayConfig,
  runtime?: Pick<GatewayRuntime, 'requestHooks'>
): Promise<{ ok: true } | { ok: false; statusCode: number; message: string }> {
  const responseCreatePayload = parseResponseCreatePayload(payload);
  if (!responseCreatePayload) {
    return { ok: true };
  }

  const model = readStringField(responseCreatePayload.model);
  context.billingModel = model;
  const sourceAdapterKey = context.sourceAdapterHint || 'openai_responses';
  const hookInput = {
    request: context.request,
    config,
    route: {
      method: context.request.method,
      url: context.request.url,
      route: 'WS /v1/responses',
      sourceAdapterKey,
      sourceRoute: 'websocket'
    },
    source: {
      adapterKey: sourceAdapterKey,
      metadata: {
        sourceRoute: 'websocket'
      }
    },
    sourceProvider: 'openai' as Provider,
    sourceAdapterKey,
    targetProvider: 'openai' as Provider,
    targetProviderConfig: context.targetProviderConfig,
    model,
    requestBody: responseCreatePayload
  };

  const modelRestriction = evaluateApiKeyModelRestriction(context.request, model, {
    provider: 'openai',
    providerConfig: context.targetProviderConfig
  });
  if (!modelRestriction.ok) {
    return {
      ok: false,
      statusCode: modelRestriction.statusCode,
      message: modelRestriction.error
    };
  }

  const beforePrecheckResult = await executeGatewayPluginRequestHookStage<{
    allow: false;
    statusCode?: number;
    message: string;
    details?: Record<string, unknown>;
  }>(
    runtime?.requestHooks?.list() || [],
    'beforePrecheck',
    hookInput
  );
  if (!beforePrecheckResult.ok) {
    return {
      ok: false,
      statusCode: beforePrecheckResult.status || 403,
      message: `Gateway plugin "${beforePrecheckResult.pluginKey}" beforePrecheck failed: ${beforePrecheckResult.error}`
    };
  }
  const precheckDecision = beforePrecheckResult.value;
  if (
    precheckDecision &&
    typeof precheckDecision === 'object' &&
    'allow' in precheckDecision &&
    precheckDecision.allow === false
  ) {
    return {
      ok: false,
      statusCode: precheckDecision.statusCode || 403,
      message: precheckDecision.message
    };
  }

  const precheck = await evaluateGatewayPrecheck({
    request: context.request,
    config,
    targetProvider: 'openai',
    targetProviderConfig: context.targetProviderConfig,
    model,
    requestBody: responseCreatePayload
  });
  const afterPrecheckResult = await executeGatewayPluginRequestHookStage(
    runtime?.requestHooks?.list() || [],
    'afterPrecheck',
    ({
      ...hookInput,
      result: precheck
    } as typeof hookInput & { result: typeof precheck })
  );
  if (!afterPrecheckResult.ok) {
    return {
      ok: false,
      statusCode: afterPrecheckResult.status || 403,
      message: `Gateway plugin "${afterPrecheckResult.pluginKey}" afterPrecheck failed: ${afterPrecheckResult.error}`
    };
  }

  if (!precheck.ok) {
    return {
      ok: false,
      statusCode: precheck.statusCode,
      message: precheck.message
    };
  }

  return { ok: true };
}

function parseResponseCreatePayload(payload: string): Record<string, unknown> | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(payload);
  } catch {
    return undefined;
  }

  return isRecord(parsed) && parsed.type === 'response.create'
    ? parsed
    : undefined;
}

function readStringField(value: unknown): string | undefined {
  if (typeof value !== 'string') {
    return undefined;
  }

  const normalized = value.trim();
  return normalized || undefined;
}

function readNumberField(value: unknown): number | undefined {
  const numberValue = typeof value === 'number'
    ? value
    : typeof value === 'string' && value.trim()
      ? Number(value)
      : undefined;
  return numberValue !== undefined && Number.isFinite(numberValue) && numberValue >= 0
    ? numberValue
    : undefined;
}

function sanitizeRequestUrlForEvent(url: string): string {
  try {
    const parsed = new URL(url, 'http://gateway.local');
    for (const key of ['key', 'api_key', 'apikey', 'token', 'access_token']) {
      if (parsed.searchParams.has(key)) {
        parsed.searchParams.set(key, '***');
      }
    }

    const query = parsed.searchParams.toString();
    return query ? `${parsed.pathname}?${query}` : parsed.pathname;
  } catch {
    return url;
  }
}

function buildUpstreamHeaders(
  incomingHeaders: IncomingHttpHeaders,
  config: Pick<GatewayConfig, 'openaiApiKey' | 'auth'> & {
    allowEnvApiKeyFallback?: boolean;
  }
): Record<string, string> {
  const authHeaders = buildOpenAIHeaders(
    withCodexAuthorizationOverride(incomingHeaders) as HeaderBag,
    config
  );
  if (!authHeaders.ok) {
    throw new Error(authHeaders.error);
  }

  const headers: Record<string, string> = {};
  for (const [key, value] of Object.entries(incomingHeaders)) {
    const normalizedKey = key.toLowerCase();
    if (blockedForwardHeaderSet.has(normalizedKey)) {
      continue;
    }

    const headerValue = normalizeHeaderValue(value);
    if (!headerValue) {
      continue;
    }

    headers[normalizedKey] = headerValue;
  }

  headers.authorization = authHeaders.value.authorization;
  const organization = authHeaders.value['openai-organization'];
  if (organization) {
    headers['openai-organization'] = organization;
  }
  const project = authHeaders.value['openai-project'];
  if (project) {
    headers['openai-project'] = project;
  }
  const codexAccountId =
    normalizeHeaderValue(incomingHeaders['chatgpt-account-id']) ||
    normalizeHeaderValue(incomingHeaders['x-codex-account-id']);
  if (codexAccountId) {
    headers['chatgpt-account-id'] = codexAccountId;
  }

  return headers;
}

function buildResponsesUpstreamUrl(openAIBaseUrl: string, requestUrl: string): string {
  const parsedBase = new URL(openAIBaseUrl);
  const normalizedPath = parsedBase.pathname.replace(/\/+$/, '');
  const upstreamPath = normalizedPath.endsWith('/responses')
    ? normalizedPath
    : `${normalizedPath}/responses`;
  parsedBase.pathname = upstreamPath;

  const incoming = new URL(requestUrl, 'http://gateway.local');
  const mergedParams = new URLSearchParams(parsedBase.search);
  for (const [key, value] of incoming.searchParams.entries()) {
    if (internalGatewayQueryParamSet.has(key.toLowerCase())) {
      continue;
    }

    mergedParams.set(key, value);
  }
  parsedBase.search = mergedParams.toString();

  switch (parsedBase.protocol) {
    case 'http:':
      parsedBase.protocol = 'ws:';
      break;
    case 'https:':
      parsedBase.protocol = 'wss:';
      break;
    default:
      break;
  }

  return parsedBase.toString();
}

function normalizeUrlForWebSocket(url: string): string {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return url;
  }

  if (parsed.protocol === 'http:') {
    parsed.protocol = 'ws:';
    return parsed.toString();
  }

  if (parsed.protocol === 'https:') {
    parsed.protocol = 'wss:';
    return parsed.toString();
  }

  return parsed.toString();
}

function withCodexAuthorizationOverride(headers: IncomingHttpHeaders): IncomingHttpHeaders {
  const cloned: IncomingHttpHeaders = { ...headers };
  const codexAccessToken = normalizeHeaderValue(cloned['x-codex-access-token']);
  if (!codexAccessToken) {
    return cloned;
  }

  cloned.authorization = `Bearer ${codexAccessToken}`;
  return cloned;
}

function maybeNormalizeCodexResponseCreatePayload(payload: string, openAIBaseUrl: string): string {
  if (!looksLikeCodexBaseUrl(openAIBaseUrl)) {
    return payload;
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(payload);
  } catch {
    return payload;
  }

  if (!isRecord(parsed) || parsed.type !== 'response.create') {
    return payload;
  }

  const normalized: Record<string, unknown> = {
    ...parsed
  };
  if (normalized.stream !== true) {
    normalized.stream = true;
  }
  if (normalized.store !== false) {
    normalized.store = false;
  }
  const instructions = typeof normalized.instructions === 'string' ? normalized.instructions.trim() : '';
  if (!instructions) {
    normalized.instructions = codexDefaultInstructions;
  }

  return JSON.stringify(normalized);
}

function looksLikeCodexBaseUrl(baseUrl: string): boolean {
  try {
    const parsed = new URL(baseUrl);
    return parsed.hostname === 'chatgpt.com' || parsed.pathname.includes('/backend-api/codex');
  } catch {
    return false;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function recordResponsesWebSocketUpstreamConnection(
  upstreamSocket: WebSocket,
  upstreamTarget: ResponsesWebSocketUpstreamTarget,
  context: GatewaySocketContext,
  config: GatewayConfig
): void {
  const startedAt = Date.now();
  let opened = false;

  upstreamSocket.once('open', () => {
    opened = true;
    const latencyMs = Date.now() - startedAt;
    recordProviderHealthResponse(
      upstreamTarget.providerConfig,
      101,
      latencyMs,
      new Date(),
      config.providerHealthCheck?.storage
    );
    void recordProviderCircuitBreakerResponse(
      config,
      upstreamTarget.provider,
      upstreamTarget.providerConfig,
      200
    );
    recordGatewaySchedulingResponse({
      config,
      request: context.request,
      providerConfig: upstreamTarget.providerConfig,
      statusCode: 200
    });
  });

  upstreamSocket.once('error', () => {
    if (opened) {
      return;
    }
    const latencyMs = Date.now() - startedAt;
    recordProviderHealthFailure(
      upstreamTarget.providerConfig,
      latencyMs,
      new Date(),
      config.providerHealthCheck?.storage
    );
    void recordProviderCircuitBreakerFailure(
      config,
      upstreamTarget.provider,
      upstreamTarget.providerConfig
    );
    recordGatewaySchedulingResponse({
      config,
      request: context.request,
      providerConfig: upstreamTarget.providerConfig,
      error: true
    });
  });
}

function resolveResponsesWebSocketTargetRoutes(
  config: GatewayConfig,
  context: GatewaySocketContext,
  routeResolution?: GatewayPluginRouteResolution
): { ok: true; value: ResponsesWebSocketTargetRoute[] } | { ok: false; error: string } {
  const providers = Array.isArray(config.providers) ? config.providers : [];
  const pluginTargetsResult = resolveResponsesWebSocketPluginTargetRoutes(
    providers,
    routeResolution
  );
  if (!pluginTargetsResult.ok) {
    return pluginTargetsResult;
  }
  if (pluginTargetsResult.value && pluginTargetsResult.value.length > 0) {
    return {
      ok: true,
      value: dedupeResponsesWebSocketTargetRoutes(pluginTargetsResult.value)
    };
  }

  const hints = readTargetProviderHints(context);
  if (hints) {
    const routes: ResponsesWebSocketTargetRoute[] = [];
    for (const hint of hints) {
      const hintedRouteResult = resolveResponsesWebSocketTargetProviderHint(providers, hint);
      if (!hintedRouteResult.ok) {
        return hintedRouteResult;
      }
      routes.push(...hintedRouteResult.value);
    }
    const deduped = dedupeResponsesWebSocketTargetRoutes(routes);
    if (deduped.length === 0) {
      return {
        ok: false,
        error: 'x-target-provider must select an OpenAI Responses-compatible provider.'
      };
    }
    return {
      ok: true,
      value: deduped
    };
  }

  return resolveDefaultResponsesWebSocketTargetRoutes(config, providers);
}

function resolveResponsesWebSocketTarget(
  config: Pick<GatewayConfig, 'openaiBaseUrl' | 'openaiApiKey'>,
  route: ResponsesWebSocketTargetRoute
):
  | {
      ok: true;
      value: ResponsesWebSocketUpstreamTarget;
    }
  | { ok: false; error: string } {
  if (route.provider !== 'openai') {
    return {
      ok: false,
      error: `Target provider "${route.provider}" is not compatible with /v1/responses websocket.`
    };
  }
  if (route.providerConfig && !isOpenAIResponsesWebSocketCompatibleProvider(route.providerConfig)) {
    return {
      ok: false,
      error: `Target provider "${route.providerConfig.name}" is not compatible with /v1/responses websocket.`
    };
  }

  return {
    ok: true,
    value: {
      provider: 'openai',
      baseUrl: route.providerConfig?.baseurl || config.openaiBaseUrl,
      apiKey: route.providerConfig?.apikey || config.openaiApiKey,
      providerConfig: route.providerConfig
    }
  };
}

function resolveResponsesWebSocketPluginTargetRoutes(
  providers: ProviderConfig[],
  routeResolution: GatewayPluginRouteResolution | undefined
): { ok: true; value?: ResponsesWebSocketTargetRoute[] } | { ok: false; error: string } {
  if (!routeResolution) {
    return { ok: true };
  }

  const rawRoutes = routeResolution.targetProviders;
  if (rawRoutes && rawRoutes.length > 0) {
    const routes: ResponsesWebSocketTargetRoute[] = [];
    for (const rawRoute of rawRoutes) {
      const routeResult = resolveResponsesWebSocketPluginTargetProviderRoutes(providers, rawRoute);
      if (!routeResult.ok) {
        return routeResult;
      }
      routes.push(...routeResult.value);
    }
    return {
      ok: true,
      value: dedupeResponsesWebSocketTargetRoutes(routes)
    };
  }

  if (routeResolution.targetProvider || routeResolution.targetProviderName || routeResolution.targetProviderConfig) {
    const routeResult = resolveResponsesWebSocketPluginTargetProviderRoutes(
      providers,
      {
        provider: routeResolution.targetProvider,
        providerName: routeResolution.targetProviderName,
        providerConfig: routeResolution.targetProviderConfig
      }
    );
    if (!routeResult.ok) {
      return routeResult;
    }
    return {
      ok: true,
      value: routeResult.value
    };
  }

  return { ok: true };
}

function resolveResponsesWebSocketPluginTargetProviderRoutes(
  providers: ProviderConfig[],
  route: GatewayPluginTargetRoute
): { ok: true; value: ResponsesWebSocketTargetRoute[] } | { ok: false; error: string } {
  if (route.providerConfig) {
    if (route.provider && route.provider !== 'openai') {
      return {
        ok: false,
        error: `Gateway plugin route resolver selected provider "${route.provider}", which is not compatible with /v1/responses websocket.`
      };
    }
    if (!isOpenAIResponsesWebSocketCompatibleProvider(route.providerConfig)) {
      return {
        ok: false,
        error: `Gateway plugin route resolver selected provider "${route.providerConfig.name}", which is not compatible with /v1/responses websocket.`
      };
    }
    return {
      ok: true,
      value: [
        {
          provider: 'openai',
          providerConfig: route.providerConfig
        }
      ]
    };
  }

  if (route.providerName) {
    const providerConfig = findProviderConfigByName(providers, route.providerName);
    if (!providerConfig) {
      return {
        ok: false,
        error: `Gateway plugin route resolver selected unknown provider "${route.providerName}".`
      };
    }
    if (!isOpenAIResponsesWebSocketCompatibleProvider(providerConfig)) {
      return {
        ok: false,
        error: `Gateway plugin route resolver selected provider "${providerConfig.name}", which is not compatible with /v1/responses websocket.`
      };
    }
    return {
      ok: true,
      value: [
        {
          provider: 'openai',
          providerConfig
        }
      ]
    };
  }

  if (route.provider) {
    if (route.provider !== 'openai') {
      return {
        ok: false,
        error: `Gateway plugin route resolver selected provider "${route.provider}", which is not compatible with /v1/responses websocket.`
      };
    }
    return {
      ok: true,
      value: resolveOpenAIResponsesProviderRoutes(providers)
    };
  }

  return {
    ok: false,
    error: 'Gateway plugin route resolver returned an invalid target provider.'
  };
}

function resolveResponsesWebSocketTargetProviderHint(
  providers: ProviderConfig[],
  hintRaw: string
): { ok: true; value: ResponsesWebSocketTargetRoute[] } | { ok: false; error: string } {
  const hint = hintRaw.trim();
  if (!hint) {
    return {
      ok: false,
      error: 'x-target-provider must not be empty.'
    };
  }

  const byName = findProviderConfigByName(providers, hint);
  if (byName) {
    if (!isOpenAIResponsesWebSocketCompatibleProvider(byName)) {
      return {
        ok: false,
        error: `Target provider "${byName.name}" is not compatible with /v1/responses websocket.`
      };
    }
    return {
      ok: true,
      value: [
        {
          provider: 'openai',
          providerConfig: byName
        }
      ]
    };
  }

  const parsedProvider = parseProvider(hint);
  if (!parsedProvider || parsedProvider !== 'openai') {
    return {
      ok: false,
      error: `Target provider "${hint}" is not compatible with /v1/responses websocket.`
    };
  }

  return {
    ok: true,
    value: resolveOpenAIResponsesProviderRoutes(providers)
  };
}

function resolveDefaultResponsesWebSocketTargetRoutes(
  config: GatewayConfig,
  providers: ProviderConfig[]
): { ok: true; value: ResponsesWebSocketTargetRoute[] } | { ok: false; error: string } {
  const defaultTargets = Array.isArray(config.defaultTargetProviders) && config.defaultTargetProviders.length > 0
    ? config.defaultTargetProviders
    : config.defaultTargetProvider
      ? [config.defaultTargetProvider]
      : undefined;

  if (!defaultTargets) {
    return {
      ok: true,
      value: resolveOpenAIResponsesProviderRoutes(providers)
    };
  }

  const routes: ResponsesWebSocketTargetRoute[] = [];
  for (const provider of defaultTargets) {
    if (provider === 'openai') {
      routes.push(...resolveOpenAIResponsesProviderRoutes(providers));
    }
  }

  const deduped = dedupeResponsesWebSocketTargetRoutes(routes);
  if (deduped.length === 0) {
    return {
      ok: false,
      error: 'Configured default target provider does not support /v1/responses websocket.'
    };
  }
  return {
    ok: true,
    value: deduped
  };
}

function resolveOpenAIResponsesProviderRoutes(providers: ProviderConfig[]): ResponsesWebSocketTargetRoute[] {
  const responsesProviders = providers.filter((item) => isOpenAIResponsesWebSocketCompatibleProvider(item));
  if (responsesProviders.length === 0) {
    return [{ provider: 'openai' }];
  }

  return responsesProviders.map((providerConfig) => ({
    provider: 'openai',
    providerConfig
  }));
}

function isOpenAIResponsesWebSocketCompatibleProvider(providerConfig: ProviderConfig): boolean {
  return providerConfig.type === 'openai_responses';
}

function findProviderConfigByName(
  providers: ProviderConfig[],
  name: string
): ProviderConfig | undefined {
  const normalized = name.trim().toLowerCase();
  if (!normalized) {
    return undefined;
  }

  return providers.find((item) => item.name.trim().toLowerCase() === normalized);
}

function dedupeResponsesWebSocketTargetRoutes(
  routes: ResponsesWebSocketTargetRoute[]
): ResponsesWebSocketTargetRoute[] {
  const deduped: ResponsesWebSocketTargetRoute[] = [];
  const usedKeys = new Set<string>();

  for (const route of routes) {
    const key = route.providerConfig ? `name:${route.providerConfig.name}` : `type:${route.provider}`;
    if (usedKeys.has(key)) {
      continue;
    }

    usedKeys.add(key);
    deduped.push(route);
  }

  return deduped;
}

interface WebSocketProviderPluginContext {
  request: FastifyRequest;
  config: GatewayConfig;
  source: GatewaySourceContext;
  sourceProvider: Provider;
  sourceAdapterKey: string;
  targetProvider: Provider;
  targetProviderConfig?: ProviderConfig;
  model?: string;
  passthrough: boolean;
  streaming: boolean;
  forceCodexOauthRefreshOnce?: boolean;
  plugins: ProviderPlugin[];
}

async function applyWebSocketProviderRequestPlugins(
  context: WebSocketProviderPluginContext,
  baseUpstreamRequest: UpstreamRequest
): Promise<Result<UpstreamRequest>> {
  let upstreamRequest = baseUpstreamRequest;

  for (const plugin of context.plugins) {
    const pluginInput = {
      request: context.request,
      config: context.config,
      source: context.source,
      sourceProvider: context.sourceProvider,
      sourceAdapterKey: context.sourceAdapterKey,
      targetProvider: context.targetProvider,
      targetProviderConfig: context.targetProviderConfig,
      targetProviderName: context.targetProviderConfig?.name,
      model: context.model,
      passthrough: context.passthrough,
      streaming: context.streaming,
      forceCodexOauthRefreshOnce: context.forceCodexOauthRefreshOnce,
      upstreamRequest,
      standardRequest: undefined
    };
    if (!shouldRunProviderPlugin(plugin, pluginInput)) {
      if (plugin.authenticate) {
        recordGatewayPluginHookExecution({
          pluginKey: plugin.key,
          kind: 'provider',
          hook: 'authenticate',
          outcome: 'skipped'
        });
      }
      if (plugin.transformRequest) {
        recordGatewayPluginHookExecution({
          pluginKey: plugin.key,
          kind: 'provider',
          hook: 'transformRequest',
          outcome: 'skipped'
        });
      }
      continue;
    }

    if (plugin.authenticate) {
      const startedAt = process.hrtime.bigint();
      const executionResult = await runGatewayPluginProtectedOperation({
        pluginKey: plugin.key,
        kind: 'provider',
        hook: 'authenticate',
        execution: plugin.execution,
        operation: () => plugin.authenticate?.(pluginInput)
      });
      if (!executionResult.ok) {
        recordGatewayPluginHookExecution({
          pluginKey: plugin.key,
          kind: 'provider',
          hook: 'authenticate',
          outcome: executionResult.reason,
          durationMs: elapsedMs(startedAt)
        });
        return err(`Provider plugin "${plugin.key}" auth failed: ${executionResult.error}`);
      }
      if ('skipped' in executionResult) {
        recordGatewayPluginHookExecution({
          pluginKey: plugin.key,
          kind: 'provider',
          hook: 'authenticate',
          outcome: executionResult.reason,
          durationMs: elapsedMs(startedAt)
        });
      } else if (!executionResult.value?.ok) {
        recordGatewayPluginHookExecution({
          pluginKey: plugin.key,
          kind: 'provider',
          hook: 'authenticate',
          outcome: 'error',
          durationMs: elapsedMs(startedAt)
        });
        return err(`Provider plugin "${plugin.key}" auth failed: ${executionResult.value?.error || 'unknown error'}`);
      } else {
        upstreamRequest = executionResult.value.value;
        recordGatewayPluginHookExecution({
          pluginKey: plugin.key,
          kind: 'provider',
          hook: 'authenticate',
          outcome: 'success',
          durationMs: elapsedMs(startedAt)
        });
      }
    }

    if (plugin.transformRequest) {
      const startedAt = process.hrtime.bigint();
      const executionResult = await runGatewayPluginProtectedOperation({
        pluginKey: plugin.key,
        kind: 'provider',
        hook: 'transformRequest',
        execution: plugin.execution,
        operation: () => plugin.transformRequest?.({
          ...pluginInput,
          upstreamRequest
        })
      });
      if (!executionResult.ok) {
        recordGatewayPluginHookExecution({
          pluginKey: plugin.key,
          kind: 'provider',
          hook: 'transformRequest',
          outcome: executionResult.reason,
          durationMs: elapsedMs(startedAt)
        });
        return err(`Provider plugin "${plugin.key}" request transform failed: ${executionResult.error}`);
      }
      if ('skipped' in executionResult) {
        recordGatewayPluginHookExecution({
          pluginKey: plugin.key,
          kind: 'provider',
          hook: 'transformRequest',
          outcome: executionResult.reason,
          durationMs: elapsedMs(startedAt)
        });
      } else if (!executionResult.value?.ok) {
        recordGatewayPluginHookExecution({
          pluginKey: plugin.key,
          kind: 'provider',
          hook: 'transformRequest',
          outcome: 'error',
          durationMs: elapsedMs(startedAt)
        });
        return err(`Provider plugin "${plugin.key}" request transform failed: ${executionResult.value?.error || 'unknown error'}`);
      } else {
        upstreamRequest = executionResult.value.value;
        recordGatewayPluginHookExecution({
          pluginKey: plugin.key,
          kind: 'provider',
          hook: 'transformRequest',
          outcome: 'success',
          durationMs: elapsedMs(startedAt)
        });
      }
    }
  }

  return ok(upstreamRequest);
}

function readTargetProviderHints(context: GatewaySocketContext): string[] | undefined {
  const fromHeaderList = normalizeHeaderValue(context.headers['x-target-providers']);
  if (fromHeaderList) {
    return splitTargetProviderHints(fromHeaderList);
  }

  const fromHeader = normalizeHeaderValue(context.headers['x-target-provider']);
  if (fromHeader) {
    return [fromHeader];
  }

  try {
    const url = new URL(context.requestUrl);
    const fromQueryList =
      url.searchParams.get('target_providers') ||
      url.searchParams.get('target-providers');
    if (fromQueryList?.trim()) {
      return splitTargetProviderHints(fromQueryList);
    }
    const fromQuery =
      url.searchParams.get('target_provider') ||
      url.searchParams.get('target-provider');
    const normalizedQuery = fromQuery?.trim();
    if (normalizedQuery) {
      return [normalizedQuery];
    }
  } catch {
    return undefined;
  }

  return undefined;
}

function splitTargetProviderHints(value: string): string[] {
  return value
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

function readSourceAdapterHintFromRequestUrl(url: URL): GatewayCodexWsSourceAdapterKey | undefined {
  const fromSourceAdapter = parseGatewayCodexWsSourceAdapterKey(url.searchParams.get('source_adapter') || undefined);
  if (fromSourceAdapter) {
    return fromSourceAdapter;
  }

  return parseGatewayCodexWsSourceAdapterKey(url.searchParams.get('source') || undefined);
}

function resolveGatewayWebSocketMaxPayloadBytes(config: GatewayConfig): number {
  if (Number.isFinite(config.bodyLimitBytes) && config.bodyLimitBytes > 0) {
    return Math.max(1024, Math.floor(config.bodyLimitBytes));
  }

  return 1024 * 1024;
}

function elapsedMs(startedAt: bigint): number {
  return Number(process.hrtime.bigint() - startedAt) / 1_000_000;
}

function webSocketPayloadByteLength(payload: WebSocketPayload): number {
  if (typeof payload === 'string') {
    return Buffer.byteLength(payload, 'utf8');
  }

  if (Buffer.isBuffer(payload)) {
    return payload.byteLength;
  }

  if (Array.isArray(payload)) {
    return payload.reduce((total, item) => total + webSocketPayloadByteLength(item), 0);
  }

  if (payload instanceof ArrayBuffer) {
    return payload.byteLength;
  }

  return 0;
}

function rawDataToUtf8String(rawData: RawData): string {
  if (typeof rawData === 'string') {
    return rawData;
  }

  if (Buffer.isBuffer(rawData)) {
    return rawData.toString('utf8');
  }

  if (Array.isArray(rawData)) {
    return Buffer.concat(rawData).toString('utf8');
  }

  return Buffer.from(rawData).toString('utf8');
}

function sendInvalidRequestEvent(socket: WebSocket, message: string): void {
  sendWebSocketErrorEvent(socket, 400, message);
}

function sendWebSocketErrorEvent(socket: WebSocket, status: number, message: string): void {
  if (socket.readyState !== WebSocket.OPEN) {
    return;
  }

  socket.send(
    JSON.stringify({
      type: 'error',
      status,
      error: {
        type: status === 429 ? 'rate_limit_error' : 'invalid_request_error',
        message
      }
    })
  );
}

function normalizeHeaderValue(value: string | string[] | undefined): string | undefined {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed || undefined;
  }

  if (Array.isArray(value) && value.length > 0) {
    const first = value[0]?.trim();
    return first || undefined;
  }

  return undefined;
}

function noop(): void {}

function normalizeCloseCode(code: number): number {
  if (code >= 1000 && code <= 4999) {
    return code;
  }

  return 1011;
}

function isResponsesWebSocketPath(pathname: string): boolean {
  const normalized = pathname.replace(/\/+$/, '');
  return normalized === '/v1/responses';
}

function safeParseRequestUrl(request: IncomingMessage): URL | undefined {
  const host = request.headers.host || 'localhost';
  const path = request.url || '/';
  try {
    return new URL(path, `http://${host}`);
  } catch {
    return undefined;
  }
}

function createWebSocketPluginCompatibleRequest(
  request: IncomingMessage,
  requestUrl: URL,
  fastify: FastifyInstance
): FastifyRequest {
  const query: Record<string, string | string[]> = {};
  for (const [key, value] of requestUrl.searchParams.entries()) {
    const current = query[key];
    if (current === undefined) {
      query[key] = value;
      continue;
    }

    if (Array.isArray(current)) {
      current.push(value);
      continue;
    }

    query[key] = [current, value];
  }

  return {
    id: `ws-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
    headers: request.headers,
    method: request.method || 'GET',
    url: request.url || requestUrl.pathname,
    ip: request.socket.remoteAddress || '',
    query,
    body: undefined,
    log: fastify.log
  } as FastifyRequest;
}

function rejectUpgrade(socket: Socket, statusCode: number, message: string): void {
  const body = JSON.stringify({
    error: message
  });
  const response =
    `HTTP/1.1 ${statusCode} ${resolveStatusMessage(statusCode)}\r\n` +
    'Connection: close\r\n' +
    'Content-Type: application/json\r\n' +
    `Content-Length: ${Buffer.byteLength(body, 'utf8')}\r\n` +
    '\r\n' +
    body;

  socket.write(response);
  socket.destroy();
}

function resolveGatewayWebSocketHandshakeTimeoutMs(config: GatewayConfig): number {
  const configured = Number(config.upstreamTimeoutMs);
  if (!Number.isFinite(configured) || configured <= 0) {
    return 60000;
  }
  return Math.max(1, Math.min(Math.trunc(configured), 300000));
}

function resolveStatusMessage(statusCode: number): string {
  if (statusCode === 401) {
    return 'Unauthorized';
  }

  if (statusCode === 403) {
    return 'Forbidden';
  }

  if (statusCode === 404) {
    return 'Not Found';
  }

  if (statusCode === 500) {
    return 'Internal Server Error';
  }

  return 'Bad Request';
}

function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
