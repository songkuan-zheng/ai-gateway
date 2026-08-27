import { createServer, type IncomingHttpHeaders, type Server } from 'node:http';
import {
  createServer as createNetServer,
  type AddressInfo,
  type Server as NetServer,
  type Socket as NetSocket
} from 'node:net';
import Fastify from 'fastify';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { WebSocket, WebSocketServer } from 'ws';
import { closeBillingPublisher, initializeBillingPublisher, type BillingQueueEvent } from '../billing';
import { waitForLoopbackListener } from '../__tests__/listener-readiness';
import { ProviderPluginRegistry } from '../adapters/registry';
import type { GatewayConfig } from '../types';
import type { GatewayPluginOutbox } from '../plugins/events';
import { registerGatewayResponsesWebSocketRoute } from './websocket';
import { createGatewayRuntime } from './runtime';
import { resetProviderCircuitBreakerForTests } from './upstream-circuit-breaker';
import { resetProviderConcurrencyForTests } from './upstream-concurrency';

describe('gateway responses websocket relay', () => {
  const cleanupTasks: Array<() => Promise<void>> = [];

  afterEach(async () => {
    resetProviderCircuitBreakerForTests();
    resetProviderConcurrencyForTests();
    while (cleanupTasks.length > 0) {
      const task = cleanupTasks.pop();
      if (!task) {
        continue;
      }
      await task();
    }
    await closeBillingPublisher();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it('forwards response.completed before closing when upstream closes immediately', async () => {
    const upstream = await startUpstreamResponsesWebSocketServer({
      expectedPath: '/v1/responses'
    });
    cleanupTasks.push(async () => {
      await upstream.close();
    });

    const gateway = Fastify({ logger: false });
    const gatewayConfig = createWsGatewayTestConfig(`http://127.0.0.1:${upstream.port}/v1`);
    registerGatewayResponsesWebSocketRoute(gateway, gatewayConfig);
    await gateway.listen({ host: '127.0.0.1', port: 0 });
    cleanupTasks.push(async () => {
      await gateway.close();
    });

    const gatewayPort = (gateway.server.address() as AddressInfo).port;
    await waitForLoopbackListener(gatewayPort);
    const receivedMessages: string[] = [];

    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const socket = new WebSocket(`ws://127.0.0.1:${gatewayPort}/v1/responses`);
      const timeout = setTimeout(() => {
        settled = true;
        socket.terminate();
        reject(new Error('Timed out waiting for response.completed event.'));
      }, 8000);

      socket.on('open', () => {
        socket.send(
          JSON.stringify({
            type: 'response.create',
            model: 'gpt-5.4-mini',
            input: 'hello',
            stream: true
          })
        );
      });

      socket.on('message', (raw) => {
        const message = raw.toString();
        receivedMessages.push(message);
        if (!message.includes('"type":"response.completed"')) {
          return;
        }

        if (settled) {
          return;
        }

        settled = true;
        clearTimeout(timeout);
        socket.close(1000, 'test-done');
        resolve();
      });

      socket.on('close', () => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timeout);
        reject(new Error('WebSocket closed before response.completed event was received.'));
      });

      socket.on('error', (error) => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timeout);
        reject(error);
      });
    });

    expect(receivedMessages.some((message) => message.includes('"type":"response.completed"'))).toBe(true);
    const completedMessage = receivedMessages.find((message) => message.includes('"type":"response.completed"'));
    const completedPayload = JSON.parse(completedMessage || '{}') as {
      response?: { usage?: Record<string, unknown> };
    };
    expect(completedPayload.response?.usage).toMatchObject({
      input_tokens: 0,
      input_tokens_details: {
        cached_tokens: 0
      },
      output_tokens: 0,
      output_tokens_details: {
        reasoning_tokens: 0
      },
      total_tokens: 0
    });
  }, 12000);

  it('maps codex headers and normalizes response.create payload for codex backend', async () => {
    const upstream = await startUpstreamResponsesWebSocketServer({
      expectedPath: '/backend-api/codex/responses'
    });
    cleanupTasks.push(async () => {
      await upstream.close();
    });

    const gateway = Fastify({ logger: false });
    const gatewayConfig = createWsGatewayTestConfig(`http://127.0.0.1:${upstream.port}/backend-api/codex`);
    registerGatewayResponsesWebSocketRoute(gateway, gatewayConfig);
    await gateway.listen({ host: '127.0.0.1', port: 0 });
    cleanupTasks.push(async () => {
      await gateway.close();
    });

    const gatewayPort = (gateway.server.address() as AddressInfo).port;
    await waitForLoopbackListener(gatewayPort);

    await waitForResponseCompleted(`ws://127.0.0.1:${gatewayPort}/v1/responses`, {
      'x-codex-access-token': 'atk-test-codex',
      'x-codex-account-id': 'acct-test-codex'
    }, {
      type: 'response.create',
      model: 'gpt-5.4-mini',
      input: 'hello codex ws'
    });

    expect(upstream.state.upgradeHeaders?.authorization).toBe('Bearer atk-test-codex');
    expect(upstream.state.upgradeHeaders?.['chatgpt-account-id']).toBe('acct-test-codex');

    const upstreamPayload = JSON.parse(upstream.state.receivedMessages[0] || '{}') as Record<string, unknown>;
    expect(upstreamPayload.type).toBe('response.create');
    expect(upstreamPayload.stream).toBe(true);
    expect(upstreamPayload.store).toBe(false);
    expect(upstreamPayload.instructions).toBe('You are a helpful assistant.');
  });

  it('prefers openai_responses provider base url for websocket upstream target', async () => {
    const upstream = await startUpstreamResponsesWebSocketServer({
      expectedPath: '/backend-api/codex/responses'
    });
    cleanupTasks.push(async () => {
      await upstream.close();
    });

    const gateway = Fastify({ logger: false });
    const gatewayConfig = createWsGatewayTestConfig(`http://127.0.0.1:${upstream.port}/v1`);
    gatewayConfig.providers = [
      {
        name: 'bigmodel',
        type: 'openai_chat_completions',
        apikey: 'bigmodel-key',
        baseurl: 'https://open.bigmodel.cn/api/coding/paas/v4',
        models: ['glm-5'],
        extraHeaders: {
          default: {},
          byModel: {}
        },
        extraBody: {
          default: {},
          byModel: {}
        },
        billing: {
          byModel: {}
        }
      },
      {
        name: 'codex',
        type: 'openai_responses',
        baseurl: `http://127.0.0.1:${upstream.port}/backend-api/codex`,
        models: ['gpt-5.4'],
        extraHeaders: {
          default: {},
          byModel: {}
        },
        extraBody: {
          default: {},
          byModel: {}
        },
        billing: {
          byModel: {}
        }
      }
    ] as any;
    registerGatewayResponsesWebSocketRoute(gateway, gatewayConfig);
    await gateway.listen({ host: '127.0.0.1', port: 0 });
    cleanupTasks.push(async () => {
      await gateway.close();
    });

    const gatewayPort = (gateway.server.address() as AddressInfo).port;
    await waitForLoopbackListener(gatewayPort);
    await waitForResponseCompleted(`ws://127.0.0.1:${gatewayPort}/v1/responses`, {
      authorization: 'Bearer codex-access-token'
    }, {
      type: 'response.create',
      model: 'gpt-5.4-mini',
      input: 'hello target'
    });

    expect(upstream.state.upgradePath).toBe('/backend-api/codex/responses');
  });

  it('does not treat chat completions providers as responses websocket targets', async () => {
    const upstream = await startUpstreamResponsesWebSocketServer({
      expectedPath: '/v1/responses'
    });
    cleanupTasks.push(async () => {
      await upstream.close();
    });

    const gateway = Fastify({ logger: false });
    const gatewayConfig = createWsGatewayTestConfig(`http://127.0.0.1:${upstream.port}/v1`);
    gatewayConfig.providers = [
      {
        name: 'chat-only',
        type: 'openai_chat_completions',
        apikey: 'chat-provider-key',
        baseurl: `http://127.0.0.1:${upstream.port}/chat`,
        models: ['gpt-chat'],
        extraHeaders: {
          default: {},
          byModel: {}
        },
        extraBody: {
          default: {},
          byModel: {}
        },
        billing: {
          byModel: {}
        }
      }
    ] as any;
    registerGatewayResponsesWebSocketRoute(gateway, gatewayConfig);
    await gateway.listen({ host: '127.0.0.1', port: 0 });
    cleanupTasks.push(async () => {
      await gateway.close();
    });

    const gatewayPort = (gateway.server.address() as AddressInfo).port;
    await waitForLoopbackListener(gatewayPort);
    await waitForResponseCompleted(`ws://127.0.0.1:${gatewayPort}/v1/responses`, undefined, {
      type: 'response.create',
      model: 'gpt-5.4-mini',
      input: 'hello target'
    });

    expect(upstream.state.upgradePath).toBe('/v1/responses');
    expect(upstream.state.upgradeHeaders?.authorization).toBe('Bearer openai-test-key');
  });

  it('rejects explicit chat provider hints for responses websocket', async () => {
    const gateway = Fastify({ logger: false });
    const gatewayConfig = createWsGatewayTestConfig('http://127.0.0.1:9/v1');
    gatewayConfig.providers = [
      {
        name: 'chat-only',
        type: 'openai_chat_completions',
        apikey: 'chat-provider-key',
        baseurl: 'http://127.0.0.1:9/chat',
        models: ['gpt-chat'],
        extraHeaders: {
          default: {},
          byModel: {}
        },
        extraBody: {
          default: {},
          byModel: {}
        },
        billing: {
          byModel: {}
        }
      }
    ] as any;
    registerGatewayResponsesWebSocketRoute(gateway, gatewayConfig);
    await gateway.listen({ host: '127.0.0.1', port: 0 });
    cleanupTasks.push(async () => {
      await gateway.close();
    });

    const gatewayPort = (gateway.server.address() as AddressInfo).port;
    await waitForLoopbackListener(gatewayPort);
    const closeResult = await waitForWebSocketClose(
      `ws://127.0.0.1:${gatewayPort}/v1/responses`,
      {
        'x-target-provider': 'chat-only'
      },
      {
        type: 'response.create',
        model: 'gpt-chat',
        input: 'blocked target'
      }
    );

    expect(closeResult.code).toBe(1008);
    expect(closeResult.reason).toContain('not compatible with /v1/responses websocket');
  });

  it('enforces upstream concurrency limits for responses websocket connections', async () => {
    const upstream = await startUpstreamResponsesWebSocketServer({
      expectedPath: '/v1/responses'
    });
    cleanupTasks.push(async () => {
      await upstream.close();
    });

    const gateway = Fastify({ logger: false });
    const gatewayConfig = createWsGatewayTestConfig(`http://127.0.0.1:${upstream.port}/v1`);
    gatewayConfig.providers = [
      {
        name: 'codex',
        type: 'openai_responses',
        baseurl: `http://127.0.0.1:${upstream.port}/v1`,
        models: ['gpt-5.4'],
        extraHeaders: {
          default: {},
          byModel: {}
        },
        extraBody: {
          default: {},
          byModel: {}
        },
        billing: {
          byModel: {}
        }
      }
    ] as any;
    gatewayConfig.upstreamConcurrency = {
      enabled: true,
      maxInFlightPerProvider: 1,
      queueTimeoutMs: 0,
      storage: {
        type: 'memory'
      }
    };
    registerGatewayResponsesWebSocketRoute(gateway, gatewayConfig);
    await gateway.listen({ host: '127.0.0.1', port: 0 });
    cleanupTasks.push(async () => {
      await gateway.close();
    });

    const gatewayPort = (gateway.server.address() as AddressInfo).port;
    await waitForLoopbackListener(gatewayPort);
    const firstSocket = await openWebSocket(`ws://127.0.0.1:${gatewayPort}/v1/responses`);
    cleanupTasks.push(async () => {
      firstSocket.terminate();
    });
    await waitForCondition(() => upstream.state.activeConnections === 1);

    const closeResult = await waitForWebSocketClose(
      `ws://127.0.0.1:${gatewayPort}/v1/responses`,
      undefined,
      {
        type: 'response.create',
        model: 'gpt-5.4-mini',
        input: 'queued'
      }
    );

    expect(closeResult.code).toBe(1013);
    expect(closeResult.reason).toContain('concurrency');
    expect(upstream.state.activeConnections).toBe(1);
  });

  it('does not open an upstream websocket after the downstream closes during initialization', async () => {
    const upstream = await startUpstreamResponsesWebSocketServer({
      expectedPath: '/v1/responses'
    });
    cleanupTasks.push(async () => {
      await upstream.close();
    });

    const gateway = Fastify({ logger: false });
    const gatewayConfig = createWsGatewayTestConfig(`http://127.0.0.1:${upstream.port}/v1`);
    gatewayConfig.upstreamConcurrency = {
      enabled: true,
      maxInFlightPerProvider: 1,
      queueTimeoutMs: 100,
      storage: {
        type: 'memory'
      }
    };
    const runtime = createGatewayRuntime(gatewayConfig);
    let transformCalls = 0;
    let firstTransformStarted = false;
    let releaseFirstTransform: (() => void) | undefined;
    const firstTransformBlocker = new Promise<void>((resolve) => {
      releaseFirstTransform = resolve;
    });
    runtime.requestTransforms.register({
      key: 'delayed-websocket-initialization',
      stage: 'beforeRouting',
      async transform() {
        transformCalls += 1;
        if (transformCalls === 1) {
          firstTransformStarted = true;
          await firstTransformBlocker;
        }
      }
    });
    registerGatewayResponsesWebSocketRoute(gateway, gatewayConfig, runtime);
    await gateway.listen({ host: '127.0.0.1', port: 0 });
    cleanupTasks.push(async () => {
      await gateway.close();
    });

    const gatewayPort = (gateway.server.address() as AddressInfo).port;
    await waitForLoopbackListener(gatewayPort);
    const websocketUrl = `ws://127.0.0.1:${gatewayPort}/v1/responses`;
    const abandonedSocket = await openWebSocket(websocketUrl);
    await waitForCondition(() => firstTransformStarted);
    abandonedSocket.terminate();
    await waitForCondition(() => abandonedSocket.readyState === WebSocket.CLOSED);
    releaseFirstTransform?.();
    await waitForCondition(() => transformCalls === 1);
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(upstream.state.activeConnections).toBe(0);
    await waitForResponseCompleted(websocketUrl, undefined, {
      type: 'response.create',
      model: 'gpt-5.4-mini',
      input: 'still available'
    });
    expect(transformCalls).toBe(2);
  });

  it('times out an upstream websocket handshake and releases the concurrency slot', async () => {
    const upstream = await startHangingTcpServer();
    cleanupTasks.push(async () => {
      await upstream.close();
    });

    const gateway = Fastify({ logger: false });
    const gatewayConfig = createWsGatewayTestConfig(`http://127.0.0.1:${upstream.port}/v1`);
    gatewayConfig.upstreamTimeoutMs = 50;
    gatewayConfig.upstreamConcurrency = {
      enabled: true,
      maxInFlightPerProvider: 1,
      queueTimeoutMs: 0,
      storage: {
        type: 'memory'
      }
    };
    registerGatewayResponsesWebSocketRoute(gateway, gatewayConfig);
    await gateway.listen({ host: '127.0.0.1', port: 0 });
    cleanupTasks.push(async () => {
      await gateway.close();
    });

    const gatewayPort = (gateway.server.address() as AddressInfo).port;
    await waitForLoopbackListener(gatewayPort);
    const websocketUrl = `ws://127.0.0.1:${gatewayPort}/v1/responses`;
    const payload = {
      type: 'response.create',
      model: 'gpt-5.4-mini',
      input: 'handshake timeout'
    };

    const firstClose = await waitForWebSocketClose(websocketUrl, undefined, payload);
    expect(firstClose.code).toBe(1011);
    expect(firstClose.reason).toContain('upstream websocket error');

    const secondClose = await waitForWebSocketClose(websocketUrl, undefined, payload);
    expect(secondClose.code).toBe(1011);
    expect(secondClose.reason).toContain('upstream websocket error');
    expect(upstream.acceptedConnections).toBe(2);
  });

  it('keeps single /responses suffix when base url already ends with /responses', async () => {
    const upstream = await startUpstreamResponsesWebSocketServer({
      expectedPath: '/backend-api/codex/responses'
    });
    cleanupTasks.push(async () => {
      await upstream.close();
    });

    const gateway = Fastify({ logger: false });
    const gatewayConfig = createWsGatewayTestConfig(
      `http://127.0.0.1:${upstream.port}/backend-api/codex/responses`
    );
    registerGatewayResponsesWebSocketRoute(gateway, gatewayConfig);
    await gateway.listen({ host: '127.0.0.1', port: 0 });
    cleanupTasks.push(async () => {
      await gateway.close();
    });

    const gatewayPort = (gateway.server.address() as AddressInfo).port;
    await waitForLoopbackListener(gatewayPort);
    await waitForResponseCompleted(`ws://127.0.0.1:${gatewayPort}/v1/responses`, undefined, {
      type: 'response.create',
      model: 'gpt-5.4-mini',
      input: 'hello dedupe'
    });

    expect(upstream.state.upgradePath).toBe('/backend-api/codex/responses');
  });

  it('applies provider plugin auth headers for websocket upstream connection', async () => {
    const upstream = await startUpstreamResponsesWebSocketServer({
      expectedPath: '/backend-api/codex/responses'
    });
    cleanupTasks.push(async () => {
      await upstream.close();
    });

    const gateway = Fastify({ logger: false });
    const gatewayConfig = createWsGatewayTestConfig(`http://127.0.0.1:${upstream.port}/v1`);
    gatewayConfig.providers = [
      {
        name: 'codex',
        type: 'openai_responses',
        baseurl: `http://127.0.0.1:${upstream.port}/backend-api/codex`,
        models: ['gpt-5.4'],
        extraHeaders: {
          default: {},
          byModel: {}
        },
        extraBody: {
          default: {},
          byModel: {}
        },
        billing: {
          byModel: {}
        }
      }
    ] as any;

    const providerPlugins = new ProviderPluginRegistry();
    providerPlugins.register({
      key: 'ws-plugin-auth',
      provider: 'openai',
      providerName: 'codex',
      authenticate: (input) => {
        return {
          ok: true,
          value: {
            ...input.upstreamRequest,
            headers: {
              ...input.upstreamRequest.headers,
              authorization: 'Bearer plugin-token',
              'chatgpt-account-id': 'acct-from-plugin'
            }
          }
        };
      }
    });

    registerGatewayResponsesWebSocketRoute(gateway, gatewayConfig, {
      providerPlugins
    } as any);
    await gateway.listen({ host: '127.0.0.1', port: 0 });
    cleanupTasks.push(async () => {
      await gateway.close();
    });

    const gatewayPort = (gateway.server.address() as AddressInfo).port;
    await waitForLoopbackListener(gatewayPort);
    await waitForResponseCompleted(`ws://127.0.0.1:${gatewayPort}/v1/responses`, {
      authorization: 'Bearer gateway-token'
    }, {
      type: 'response.create',
      model: 'gpt-5.4-mini',
      input: 'hello plugin'
    });

    expect(upstream.state.upgradeHeaders?.authorization).toBe('Bearer plugin-token');
    expect(upstream.state.upgradeHeaders?.['chatgpt-account-id']).toBe('acct-from-plugin');
  });

  it('enforces introspected model restrictions before forwarding websocket response.create messages', async () => {
    const upstream = await startUpstreamResponsesWebSocketServer({
      expectedPath: '/v1/responses'
    });
    cleanupTasks.push(async () => {
      await upstream.close();
    });

    const fetchMock = vi.fn(async () =>
      new Response(
        JSON.stringify({
          active: true,
          userId: 'user-1',
          tenantId: 'tenant-a',
          restrictions: {
            allowedModels: ['openai/gpt-5']
          }
        }),
        {
          status: 200,
          headers: {
            'content-type': 'application/json'
          }
        }
      )
    );
    vi.stubGlobal('fetch', fetchMock);

    const gateway = Fastify({ logger: false });
    const gatewayConfig = createWsGatewayTestConfig(`http://127.0.0.1:${upstream.port}/v1`);
    gatewayConfig.auth = {
      ...gatewayConfig.auth,
      enabled: true,
      mode: 'http_introspection',
      required: true,
      introspection: {
        ...gatewayConfig.auth.introspection,
        endpoint: 'http://auth.local/introspect'
      }
    };
    registerGatewayResponsesWebSocketRoute(gateway, gatewayConfig);
    await gateway.listen({ host: '127.0.0.1', port: 0 });
    cleanupTasks.push(async () => {
      await gateway.close();
    });

    const gatewayPort = (gateway.server.address() as AddressInfo).port;
    await waitForLoopbackListener(gatewayPort);
    const errorPayload = await waitForWebSocketError(
      `ws://127.0.0.1:${gatewayPort}/v1/responses`,
      {
        authorization: 'Bearer restricted-token'
      },
      {
        type: 'response.create',
        model: 'o3',
        input: 'blocked'
      }
    );

    expect(errorPayload.status).toBe(403);
    expect(errorPayload.error?.message).toContain('o3');
    expect(upstream.state.receivedMessages).toHaveLength(0);
  });

  it('runs request hooks for websocket response.create precheck decisions', async () => {
    const upstream = await startUpstreamResponsesWebSocketServer({
      expectedPath: '/v1/responses'
    });
    cleanupTasks.push(async () => {
      await upstream.close();
    });

    const gateway = Fastify({ logger: false });
    const gatewayConfig = createWsGatewayTestConfig(`http://127.0.0.1:${upstream.port}/v1`);
    const beforePrecheck = vi.fn((input: any) => {
      expect(input.model).toBe('gpt-5.4-mini');
      return {
        ok: true,
        value: {
          allow: false,
          statusCode: 429,
          message: 'blocked by websocket plugin'
        }
      };
    });

    registerGatewayResponsesWebSocketRoute(gateway, gatewayConfig, {
      providerPlugins: new ProviderPluginRegistry(),
      requestHooks: {
        list: () => [
          {
            key: 'ws-before-precheck',
            beforePrecheck
          }
        ]
      }
    } as any);
    await gateway.listen({ host: '127.0.0.1', port: 0 });
    cleanupTasks.push(async () => {
      await gateway.close();
    });

    const gatewayPort = (gateway.server.address() as AddressInfo).port;
    await waitForLoopbackListener(gatewayPort);
    const errorPayload = await waitForWebSocketError(
      `ws://127.0.0.1:${gatewayPort}/v1/responses`,
      undefined,
      {
        type: 'response.create',
        model: 'gpt-5.4-mini',
        input: 'blocked by hook'
      }
    );

    expect(errorPayload.status).toBe(429);
    expect(errorPayload.error?.message).toBe('blocked by websocket plugin');
    expect(beforePrecheck).toHaveBeenCalledTimes(1);
    expect(upstream.state.receivedMessages).toHaveLength(0);
  });

  it('publishes billing events from websocket response.completed usage', async () => {
    const upstream = await startUpstreamResponsesWebSocketServer({
      expectedPath: '/v1/responses',
      completedResponse: {
        model: 'gpt-5.4-mini',
        usage: {
          input_tokens: 12,
          output_tokens: 8,
          total_tokens: 20,
          input_tokens_details: {
            cached_tokens: 2
          }
        }
      }
    });
    cleanupTasks.push(async () => {
      await upstream.close();
    });

    const billingEvents: BillingQueueEvent[] = [];
    const outbox: GatewayPluginOutbox<BillingQueueEvent> = {
      key: 'ws-billing-outbox',
      transport: 'memory-test',
      append: async (event) => {
        billingEvents.push(event);
        return true;
      }
    };
    const gatewayConfig = createWsGatewayTestConfig(`http://127.0.0.1:${upstream.port}/v1`);
    gatewayConfig.billing = {
      enabled: true,
      currency: 'USD',
      delivery: {
        mode: 'async',
        requirePublisher: false,
        requireOutbox: false,
        shutdownDrainTimeoutMs: 100
      },
      requireUsage: false,
      requireRates: false,
      rates: {
        openai: {
          inputPerMillionUsd: 1,
          outputPerMillionUsd: 2,
          cacheReadPerMillionUsd: 0.5,
          cacheWritePerMillionUsd: 0
        },
        anthropic: {
          inputPerMillionUsd: 0,
          outputPerMillionUsd: 0
        },
        gemini: {
          inputPerMillionUsd: 0,
          outputPerMillionUsd: 0
        }
      }
    };
    await initializeBillingPublisher(
      {
        enabled: false,
        queueName: 'gateway-billing',
        jobName: 'billing.usage',
        removeOnComplete: 1000,
        removeOnFail: 5000
      },
      {
        enabled: false,
        transport: 'http',
        endpoint: undefined,
        command: undefined,
        args: [],
        cwd: undefined,
        env: {},
        timeoutMs: 5000,
        maxAttempts: 1,
        baseDelayMs: 0,
        maxDelayMs: 0,
        requireAck: false,
        headers: {}
      },
      undefined,
      {
        outboxes: [outbox]
      },
      gatewayConfig.billing.delivery
    );

    const gateway = Fastify({ logger: false });
    registerGatewayResponsesWebSocketRoute(gateway, gatewayConfig);
    await gateway.listen({ host: '127.0.0.1', port: 0 });
    cleanupTasks.push(async () => {
      await gateway.close();
    });

    const gatewayPort = (gateway.server.address() as AddressInfo).port;
    await waitForLoopbackListener(gatewayPort);
    await waitForResponseCompleted(`ws://127.0.0.1:${gatewayPort}/v1/responses?api_key=secret`, undefined, {
      type: 'response.create',
      model: 'gpt-5.4-mini',
      input: 'bill websocket'
    });
    await waitForCondition(() => billingEvents.length === 1);

    expect(billingEvents[0]).toMatchObject({
      route: {
        method: 'WS',
        url: '/v1/responses?api_key=***'
      },
      target: {
        provider: 'openai',
        model: 'gpt-5.4-mini'
      },
      billing: {
        usage: {
          input_tokens: 12,
          output_tokens: 8,
          cache_read_tokens: 2,
          total_tokens: 20
        }
      }
    });
    expect(billingEvents[0]?.billing.cost.total).toBeGreaterThan(0);
  });

  it('rejects the websocket upgrade when strict billing is enabled', async () => {
    const upstream = await startUpstreamResponsesWebSocketServer({
      expectedPath: '/v1/responses',
      completedResponse: {
        model: 'gpt-5.4-mini',
        usage: {
          input_tokens: 12,
          output_tokens: 8,
          total_tokens: 20
        }
      }
    });
    cleanupTasks.push(async () => {
      await upstream.close();
    });

    const gateway = Fastify({ logger: false });
    const gatewayConfig = createWsGatewayTestConfig(`http://127.0.0.1:${upstream.port}/v1`);
    gatewayConfig.billing = {
      enabled: true,
      currency: 'USD',
      delivery: {
        mode: 'async',
        requirePublisher: false,
        requireOutbox: false,
        shutdownDrainTimeoutMs: 100
      },
      requireUsage: false,
      requireRates: true,
      rates: {
        openai: {
          inputPerMillionUsd: 0,
          outputPerMillionUsd: 0
        },
        anthropic: {
          inputPerMillionUsd: 0,
          outputPerMillionUsd: 0
        },
        gemini: {
          inputPerMillionUsd: 0,
          outputPerMillionUsd: 0
        }
      }
    };
    registerGatewayResponsesWebSocketRoute(gateway, gatewayConfig);
    await gateway.listen({ host: '127.0.0.1', port: 0 });
    cleanupTasks.push(async () => {
      await gateway.close();
    });

    const gatewayPort = (gateway.server.address() as AddressInfo).port;
    await waitForLoopbackListener(gatewayPort);
    const rejection = await waitForWebSocketUpgradeRejection(
      `ws://127.0.0.1:${gatewayPort}/v1/responses`
    );

    expect(rejection.statusCode).toBe(400);
    expect(rejection.body).toContain(
      'Live streaming responses are not supported when strict billing enforcement is enabled.'
    );
    expect(upstream.state.activeConnections).toBe(0);
  });
});

async function startHangingTcpServer(): Promise<{
  port: number;
  acceptedConnections: number;
  close: () => Promise<void>;
}> {
  const sockets = new Set<NetSocket>();
  let acceptedConnections = 0;
  const server = createNetServer((socket) => {
    acceptedConnections += 1;
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
  });
  await listenNetServer(server);

  return {
    port: (server.address() as AddressInfo).port,
    get acceptedConnections() {
      return acceptedConnections;
    },
    close: async () => {
      for (const socket of sockets) {
        socket.destroy();
      }
      await closeNetServer(server);
    }
  };
}

async function startUpstreamResponsesWebSocketServer(options: {
  expectedPath?: string;
  completedResponse?: Record<string, unknown>;
} = {}): Promise<{
  port: number;
  state: {
    upgradePath?: string;
    upgradeHeaders?: IncomingHttpHeaders;
    activeConnections: number;
    receivedMessages: string[];
  };
  close: () => Promise<void>;
}> {
  const expectedPath = options.expectedPath || '/v1/responses';
  const server = createServer();
  const websocketServer = new WebSocketServer({ noServer: true });
  const state: {
    upgradePath?: string;
    upgradeHeaders?: IncomingHttpHeaders;
    activeConnections: number;
    receivedMessages: string[];
  } = {
    activeConnections: 0,
    receivedMessages: []
  };

  server.on('upgrade', (request, socket, head) => {
    const url = new URL(request.url || '/', 'http://localhost');
    state.upgradePath = url.pathname;
    state.upgradeHeaders = request.headers;
    if (url.pathname !== expectedPath) {
      socket.destroy();
      return;
    }

    websocketServer.handleUpgrade(request, socket, head, (ws) => {
      websocketServer.emit('connection', ws, request);
    });
  });

  websocketServer.on('connection', (socket) => {
    state.activeConnections += 1;
    socket.on('close', () => {
      state.activeConnections = Math.max(0, state.activeConnections - 1);
    });
    socket.on('message', (raw) => {
      state.receivedMessages.push(raw.toString());
      const completionPayload = JSON.stringify({
        type: 'response.completed',
        response: {
          id: 'resp_test_1',
          object: 'response',
          status: 'completed',
          ...options.completedResponse,
          output: [
            {
              id: 'msg_test_1',
              type: 'message',
              role: 'assistant',
              status: 'completed',
              content: [
                {
                  type: 'output_text',
                  text: 'x'.repeat(64 * 1024)
                }
              ]
            }
          ]
        }
      });

      socket.send(completionPayload);
      socket.close(1000, 'done');
    });
  });

  await listen(server);
  const port = (server.address() as AddressInfo).port;

  return {
    port,
    state,
    close: async () => {
      for (const client of websocketServer.clients) {
        client.terminate();
      }
      await closeWebSocketServer(websocketServer);
      await closeServer(server);
    }
  };
}

async function openWebSocket(
  websocketUrl: string,
  headers?: Record<string, string>
): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(websocketUrl, {
      headers
    });
    const timeout = setTimeout(() => {
      socket.terminate();
      reject(new Error('Timed out waiting for websocket open.'));
    }, 8000);

    socket.on('open', () => {
      clearTimeout(timeout);
      resolve(socket);
    });

    socket.on('close', () => {
      clearTimeout(timeout);
      reject(new Error('WebSocket closed before opening.'));
    });

    socket.on('error', (error) => {
      clearTimeout(timeout);
      reject(error);
    });
  });
}

async function waitForResponseCompleted(
  websocketUrl: string,
  headers: Record<string, string> | undefined,
  payload: Record<string, unknown>
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    let settled = false;
    const socket = new WebSocket(websocketUrl, {
      headers
    });
    const timeout = setTimeout(() => {
      settled = true;
      socket.terminate();
      reject(new Error('Timed out waiting for response.completed event.'));
    }, 8000);

    socket.on('open', () => {
      socket.send(JSON.stringify(payload));
    });

    socket.on('message', (raw) => {
      const message = raw.toString();
      if (!message.includes('"type":"response.completed"')) {
        return;
      }

      if (settled) {
        return;
      }

      settled = true;
      clearTimeout(timeout);
      socket.close(1000, 'test-done');
      resolve();
    });

    socket.on('close', () => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      reject(new Error('WebSocket closed before response.completed event was received.'));
    });

    socket.on('error', (error) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      reject(error);
    });
  });
}

async function waitForWebSocketError(
  websocketUrl: string,
  headers: Record<string, string> | undefined,
  payload: Record<string, unknown>
): Promise<{ status?: number; error?: { message?: string } }> {
  return new Promise((resolve, reject) => {
    let settled = false;
    const socket = new WebSocket(websocketUrl, {
      headers
    });
    const timeout = setTimeout(() => {
      settled = true;
      socket.terminate();
      reject(new Error('Timed out waiting for websocket error event.'));
    }, 8000);

    socket.on('open', () => {
      socket.send(JSON.stringify(payload));
    });

    socket.on('message', (raw) => {
      let parsed: { type?: string; status?: number; error?: { message?: string } };
      try {
        parsed = JSON.parse(raw.toString());
      } catch {
        return;
      }

      if (parsed.type !== 'error') {
        return;
      }

      if (settled) {
        return;
      }

      settled = true;
      clearTimeout(timeout);
      socket.close(1000, 'test-done');
      resolve(parsed);
    });

    socket.on('close', () => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      reject(new Error('WebSocket closed before error event was received.'));
    });

    socket.on('error', (error) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      reject(error);
    });
  });
}

async function waitForWebSocketClose(
  websocketUrl: string,
  headers: Record<string, string> | undefined,
  payload: Record<string, unknown>
): Promise<{ code: number; reason: string; messages: string[] }> {
  return new Promise((resolve, reject) => {
    const messages: string[] = [];
    const socket = new WebSocket(websocketUrl, {
      headers
    });
    const timeout = setTimeout(() => {
      socket.terminate();
      reject(new Error('Timed out waiting for websocket close.'));
    }, 8000);

    socket.on('open', () => {
      socket.send(JSON.stringify(payload));
    });

    socket.on('message', (raw) => {
      messages.push(raw.toString());
    });

    socket.on('close', (code, reason) => {
      clearTimeout(timeout);
      resolve({
        code,
        reason: reason.toString('utf8'),
        messages
      });
    });

    socket.on('error', (error) => {
      clearTimeout(timeout);
      reject(error);
    });
  });
}

async function waitForWebSocketUpgradeRejection(
  websocketUrl: string
): Promise<{ statusCode?: number; body: string }> {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(websocketUrl);
    const timeout = setTimeout(() => {
      socket.terminate();
      reject(new Error('Timed out waiting for websocket upgrade rejection.'));
    }, 8000);

    socket.on('unexpected-response', (_request, response) => {
      let body = '';
      response.setEncoding('utf8');
      response.on('data', (chunk) => {
        body += String(chunk);
      });
      response.on('end', () => {
        clearTimeout(timeout);
        socket.terminate();
        resolve({
          statusCode: response.statusCode,
          body
        });
      });
    });

    socket.on('open', () => {
      clearTimeout(timeout);
      socket.terminate();
      reject(new Error('WebSocket upgrade unexpectedly succeeded.'));
    });

    socket.on('error', (error) => {
      clearTimeout(timeout);
      reject(error);
    });
  });
}

async function waitForCondition(condition: () => boolean, timeoutMs = 1000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (condition()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error('Timed out waiting for condition.');
}

function createWsGatewayTestConfig(openaiBaseUrl: string): GatewayConfig {
  return {
    openaiBaseUrl,
    openaiApiKey: 'openai-test-key',
    auth: {
      enabled: false,
      mode: 'trusted_header',
      required: false,
      trustedCidrs: [],
      identityHeaders: {
        userId: 'x-auth-user-id',
        tenantId: 'x-auth-tenant-id',
        subject: 'x-auth-sub',
        organizationId: 'x-auth-organization-id',
        plan: 'x-auth-plan'
      },
      signature: {
        enabled: false,
        header: 'x-auth-signature',
        timestampHeader: 'x-auth-ts',
        secretEnv: 'AUTH_HEADER_SIGNING_SECRET',
        maxSkewSec: 120
      },
      introspection: {
        endpoint: undefined,
        timeoutMs: 3000,
        tokenHeader: 'authorization',
        tokenBearerOnly: true,
        requestTokenField: 'token',
        credentialHeader: 'x-gateway-auth',
        credentialEnv: 'AUTH_INTROSPECTION_SHARED_SECRET',
        responseMap: {
          active: 'active',
          userId: 'userId',
          tenantId: 'tenantId',
          subject: 'sub',
          organizationId: 'organizationId',
          plan: 'plan'
        }
      }
    }
  } as unknown as GatewayConfig;
}

async function listen(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.listen(0, '127.0.0.1', (error?: Error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
  const address = server.address() as AddressInfo;
  await waitForLoopbackListener(address.port);
}

async function listenNetServer(server: NetServer): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.listen(0, '127.0.0.1', (error?: Error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

async function closeWebSocketServer(server: WebSocketServer): Promise<void> {
  await new Promise<void>((resolve) => {
    server.close(() => resolve());
  });
}

async function closeServer(server: Server): Promise<void> {
  await new Promise<void>((resolve) => {
    server.close(() => resolve());
  });
}

async function closeNetServer(server: NetServer): Promise<void> {
  await new Promise<void>((resolve) => {
    server.close(() => resolve());
  });
}
