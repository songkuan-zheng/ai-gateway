import { createServer, type Server } from 'node:http';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { WebSocketServer } from 'ws';
import { parseGatewayConfigFromRaw } from '../config';
import { renderGatewayMetrics, resetGatewayMetricsForTests } from '../gateway/metrics';
import { resetGatewayPluginExecutionStateForTests } from '../plugins/execution';
import type { GatewayPluginEventPublisher, GatewayPluginOutbox } from '../plugins/events';
import type { BillingQueueConfig, BillingWebhookConfig } from '../types';
import {
  closeBillingPublisher,
  initializeBillingPublisher,
  publishBillingEvent,
  validateBillingPublisherRequirements,
  type BillingQueueEvent
} from './publisher';

describe('billing publisher', () => {
  const servers: Server[] = [];
  const webSocketServers: WebSocketServer[] = [];
  const tempDirs: string[] = [];

  afterEach(async () => {
    await closeBillingPublisher();
    resetGatewayPluginExecutionStateForTests();
    resetGatewayMetricsForTests();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
    await Promise.all(webSocketServers.splice(0).map((server) => closeWebSocketServer(server)));
    await Promise.all(servers.splice(0).map((server) => closeHttpServer(server)));
    for (const dir of tempDirs.splice(0)) {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('publishes billing events through HTTP webhook', async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 204 }));
    vi.stubGlobal('fetch', fetchMock);
    await initializeBillingPublisher(buildQueueConfig(false), buildWebhookConfig('http', 'https://billing.example/events'));

    const delivered = await publishBillingEvent(buildEvent());

    expect(delivered).toBe(true);
    expect(fetchMock).toHaveBeenCalledWith(
      'https://billing.example/events',
      expect.objectContaining({
        method: 'POST',
        body: expect.stringContaining('"eventId":"billing-event-1"')
      })
    );
    expect(renderGatewayMetricsForTest()).toContain(
      'gateway_billing_events_total{outcome="delivered",transport="http"} 1'
    );
  });

  it('publishes billing events through WebSocket webhook transport', async () => {
    const { server, webSocketServer, url, nextMessage } = await startWebSocketSink();
    servers.push(server);
    webSocketServers.push(webSocketServer);
    await initializeBillingPublisher(buildQueueConfig(false), buildWebhookConfig('websocket', url));

    const delivered = await publishBillingEvent(buildEvent());

    expect(delivered).toBe(true);
    const received = await nextMessage;
    expect(received.headers['x-billing-key']).toBe('billing-secret');
    expect(received.payload).toMatchObject({
      eventId: 'billing-event-1',
      requestId: 'request-1'
    });
  });

  it('publishes billing events through stdio webhook transport', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'gateway-billing-stdio-publisher-'));
    tempDirs.push(dir);
    const outputPath = join(dir, 'billing-event.jsonl');
    await initializeBillingPublisher(buildQueueConfig(false), {
      ...buildWebhookConfig('stdio', ''),
      endpoint: undefined,
      command: process.execPath,
      args: [
        '-e',
        'const fs=require("fs");let input="";process.stdin.on("data",c=>input+=c);process.stdin.on("end",()=>fs.writeFileSync(process.env.OUT,input));'
      ],
      env: {
        OUT: outputPath
      }
    });

    const delivered = await publishBillingEvent(buildEvent());

    expect(delivered).toBe(true);
    expect(JSON.parse(readFileSync(outputPath, 'utf8'))).toMatchObject({
      eventId: 'billing-event-1',
      requestId: 'request-1'
    });
  });

  it('records failed billing delivery metrics when webhook delivery fails', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ error: 'down' }), {
        status: 503
      })
    );
    vi.stubGlobal('fetch', fetchMock);
    await initializeBillingPublisher(buildQueueConfig(false), {
      ...buildWebhookConfig('http', 'https://billing.example/events'),
      maxAttempts: 1
    });

    await expect(publishBillingEvent(buildEvent())).rejects.toThrow(
      'HTTP event sink request failed with status 503'
    );

    expect(renderGatewayMetricsForTest()).toContain(
      'gateway_billing_events_total{outcome="failed",transport="http"} 1'
    );
  });

  it('records not configured billing delivery metrics when no publisher is active', async () => {
    await initializeBillingPublisher(buildQueueConfig(false), {
      ...buildWebhookConfig('http', ''),
      enabled: false
    });

    const delivered = await publishBillingEvent(buildEvent());

    expect(delivered).toBe(false);
    expect(renderGatewayMetricsForTest()).toContain(
      'gateway_billing_events_total{outcome="not_configured",transport="none"} 1'
    );
  });

  it('publishes billing events through plugin outboxes and publishers', async () => {
    const outboxEvents: BillingQueueEvent[] = [];
    const publisherEvents: BillingQueueEvent[] = [];
    const outboxClose = vi.fn();
    const publisherClose = vi.fn();
    const outbox: GatewayPluginOutbox<BillingQueueEvent> = {
      key: 'billing-kafka-outbox',
      transport: 'kafka',
      append: vi.fn(async (event) => {
        outboxEvents.push(event);
        return true;
      }),
      close: outboxClose
    };
    const publisher: GatewayPluginEventPublisher<BillingQueueEvent> = {
      key: 'billing-kafka-publisher',
      transport: 'kafka-live',
      publish: vi.fn(async (event) => {
        publisherEvents.push(event);
        return true;
      }),
      close: publisherClose
    };
    await initializeBillingPublisher(
      buildQueueConfig(false),
      {
        ...buildWebhookConfig('http', ''),
        enabled: false
      },
      undefined,
      {
        publishers: [publisher],
        outboxes: [outbox]
      }
    );

    const delivered = await publishBillingEvent(buildEvent());

    expect(delivered).toBe(true);
    expect(outbox.append).toHaveBeenCalledTimes(1);
    expect(publisher.publish).toHaveBeenCalledTimes(1);
    expect(outboxEvents[0]?.eventId).toBe('billing-event-1');
    expect(publisherEvents[0]?.eventId).toBe('billing-event-1');
    const metrics = renderGatewayMetricsForTest();
    expect(metrics).toContain(
      'gateway_billing_events_total{outcome="delivered",transport="plugin:outbox:kafka"} 1'
    );
    expect(metrics).toContain(
      'gateway_billing_events_total{outcome="delivered",transport="plugin:publisher:kafka-live"} 1'
    );

    await closeBillingPublisher();
    expect(outboxClose).toHaveBeenCalledTimes(1);
    expect(publisherClose).toHaveBeenCalledTimes(1);
  });

  it('retries plugin billing outbox delivery when configured', async () => {
    const append = vi
      .fn()
      .mockRejectedValueOnce(new Error('temporary kafka failure'))
      .mockResolvedValueOnce(true);
    const outbox: GatewayPluginOutbox<BillingQueueEvent> = {
      key: 'billing-retry-outbox',
      transport: 'kafka',
      delivery: {
        maxAttempts: 2,
        baseDelayMs: 0,
        maxDelayMs: 0
      },
      append
    };
    await initializeBillingPublisher(
      buildQueueConfig(false),
      {
        ...buildWebhookConfig('http', ''),
        enabled: false
      },
      undefined,
      {
        outboxes: [outbox]
      }
    );

    const delivered = await publishBillingEvent(buildEvent());

    expect(delivered).toBe(true);
    expect(append).toHaveBeenCalledTimes(2);
  });

  it('fails open when a billing event hook times out', async () => {
    const append = vi.fn(async () => true);
    const transform = vi.fn(() => new Promise<never>(() => undefined));
    await initializeBillingPublisher(
      buildQueueConfig(false),
      {
        ...buildWebhookConfig('http', ''),
        enabled: false
      },
      undefined,
      {
        eventHooks: [
          {
            key: 'slow-billing-event-hook',
            execution: {
              timeoutMs: 1,
              failureMode: 'fail_open'
            },
            transform
          }
        ],
        outboxes: [
          {
            key: 'billing-after-timeout',
            append
          }
        ]
      }
    );

    await expect(publishBillingEvent(buildEvent())).resolves.toBe(true);

    expect(transform).toHaveBeenCalledTimes(1);
    expect(append).toHaveBeenCalledTimes(1);
    expect(renderGatewayMetricsForTest()).toContain(
      'gateway_plugin_hook_executions_total{hook="transform",kind="billing_event",outcome="timeout",plugin_key="slow-billing-event-hook"} 1'
    );
  });

  it('validates required billing publishers and outboxes', async () => {
    await initializeBillingPublisher(buildQueueConfig(false), {
      ...buildWebhookConfig('http', ''),
      enabled: false
    });

    const strictConfig = parseGatewayConfigFromRaw({
      billing: {
        delivery: {
          requirePublisher: true,
          requireOutbox: true
        }
      }
    });
    expect(() => validateBillingPublisherRequirements(strictConfig.billing)).toThrow(
      'requires a plugin billing outbox'
    );

    await initializeBillingPublisher(
      buildQueueConfig(false),
      {
        ...buildWebhookConfig('http', ''),
        enabled: false
      },
      undefined,
      {
        outboxes: [
          {
            key: 'required-outbox',
            append: async () => true
          }
        ]
      }
    );

    expect(() => validateBillingPublisherRequirements(strictConfig.billing)).not.toThrow();
    expect(() =>
      validateBillingPublisherRequirements(
        strictConfig.billing,
        { publishers: [], outboxes: [] },
        { ...buildWebhookConfig('http', ''), enabled: false }
      )
    ).toThrow('requires a plugin billing outbox');

    await initializeBillingPublisher(buildQueueConfig(false), {
      ...buildWebhookConfig('http', ''),
      enabled: false
    });
    expect(() =>
      validateBillingPublisherRequirements(
        strictConfig.billing,
        {
          outboxes: [
            {
              key: 'candidate-outbox',
              append: async () => true
            }
          ]
        },
        { ...buildWebhookConfig('http', ''), enabled: false }
      )
    ).not.toThrow();
  });

  it('does not let a successful webhook mask a required outbox failure', async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 204 }));
    vi.stubGlobal('fetch', fetchMock);
    const strictConfig = parseGatewayConfigFromRaw({
      billing: {
        delivery: {
          mode: 'await',
          requireOutbox: true
        }
      }
    });
    const append = vi.fn().mockRejectedValue(new Error('outbox unavailable'));

    await initializeBillingPublisher(
      buildQueueConfig(false),
      buildWebhookConfig('http', 'https://billing.example/events'),
      undefined,
      {
        outboxes: [
          {
            key: 'required-outbox',
            append
          }
        ]
      },
      strictConfig.billing.delivery
    );

    await expect(publishBillingEvent(buildEvent())).rejects.toThrow(
      'Required billing outbox delivery failed: outbox unavailable'
    );
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(append).toHaveBeenCalledTimes(1);
  });

  it('returns not delivered when every required outbox declines the event', async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 204 }));
    vi.stubGlobal('fetch', fetchMock);
    const strictConfig = parseGatewayConfigFromRaw({
      billing: {
        delivery: {
          requireOutbox: true
        }
      }
    });

    await initializeBillingPublisher(
      buildQueueConfig(false),
      buildWebhookConfig('http', 'https://billing.example/events'),
      undefined,
      {
        outboxes: [
          {
            key: 'declining-outbox',
            append: async () => false
          }
        ]
      },
      strictConfig.billing.delivery
    );

    await expect(publishBillingEvent(buildEvent())).resolves.toBe(false);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('drains pending billing deliveries before closing plugin publishers', async () => {
    let resolveAppend!: () => void;
    const appendStarted = new Promise<void>((resolve) => {
      resolveAppend = resolve;
    });
    const close = vi.fn();
    const outbox: GatewayPluginOutbox<BillingQueueEvent> = {
      key: 'slow-outbox',
      append: async () => {
        await appendStarted;
        return true;
      },
      close
    };
    await initializeBillingPublisher(
      buildQueueConfig(false),
      {
        ...buildWebhookConfig('http', ''),
        enabled: false
      },
      undefined,
      {
        outboxes: [outbox]
      },
      {
        mode: 'async',
        requirePublisher: false,
        requireOutbox: false,
        shutdownDrainTimeoutMs: 1000
      }
    );

    const publishPromise = publishBillingEvent(buildEvent());
    await Promise.resolve();
    resolveAppend();
    await closeBillingPublisher();

    await expect(publishPromise).resolves.toBe(true);
    expect(close).toHaveBeenCalledTimes(1);
  });
});

function buildQueueConfig(enabled: boolean): BillingQueueConfig {
  return {
    enabled,
    queueName: 'gateway-billing',
    jobName: 'billing.usage',
    removeOnComplete: 1000,
    removeOnFail: 5000
  };
}

function buildWebhookConfig(
  transport: BillingWebhookConfig['transport'],
  endpoint: string
): BillingWebhookConfig {
  return {
    enabled: true,
    transport,
    endpoint,
    command: undefined,
    args: [],
    cwd: undefined,
    env: {},
    timeoutMs: 5000,
    maxAttempts: 3,
    baseDelayMs: 200,
    maxDelayMs: 2000,
    requireAck: false,
    headers: {
      'x-billing-key': 'billing-secret'
    }
  };
}

function buildEvent(): BillingQueueEvent {
  return {
    eventId: 'billing-event-1',
    emittedAt: '2026-06-08T00:00:00.000Z',
    requestId: 'request-1',
    route: {
      method: 'POST',
      url: '/v1/responses'
    },
    source: {
      provider: 'openai',
      adapterKey: 'openai_responses'
    },
    target: {
      provider: 'openai',
      model: 'gpt-4.1-mini',
      providerName: 'openai-main'
    },
    fallback: {
      used: false,
      attempts: 0
    },
    billing: {
      provider: 'openai',
      currency: 'USD',
      usage: {
        input_tokens: 10,
        output_tokens: 20,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
        total_tokens: 30,
        cache_duration_seconds: 0
      },
      rates: {
        input_per_million_usd: 1,
        output_per_million_usd: 2,
        cache_read_per_million_usd: 0,
        cache_write_per_million_usd: 0
      },
      cost: {
        input: 0.00001,
        output: 0.00004,
        cache_read: 0,
        cache_write: 0,
        tiered: 0,
        total: 0.00005
      },
      breakdown: {
        input: [],
        output: [],
        cache_read: [],
        cache_write: []
      }
    }
  };
}

function renderGatewayMetricsForTest(): string {
  return renderGatewayMetrics(
    parseGatewayConfigFromRaw({
      metrics: {
        enabled: true,
        includeProviderHealth: false
      }
    })
  );
}

async function startWebSocketSink(): Promise<{
  server: Server;
  webSocketServer: WebSocketServer;
  url: string;
  nextMessage: Promise<{ headers: Record<string, string | string[] | undefined>; payload: unknown }>;
}> {
  const server = createServer();
  const webSocketServer = new WebSocketServer({ server });
  let resolveMessage!: (value: {
    headers: Record<string, string | string[] | undefined>;
    payload: unknown;
  }) => void;
  const nextMessage = new Promise<{ headers: Record<string, string | string[] | undefined>; payload: unknown }>(
    (resolve) => {
      resolveMessage = resolve;
    }
  );
  webSocketServer.on('connection', (socket, request) => {
    socket.on('message', (data) => {
      resolveMessage({
        payload: JSON.parse(data.toString()),
        headers: request.headers
      });
    });
  });
  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', resolve);
  });

  const address = server.address() as AddressInfo;
  return {
    server,
    webSocketServer,
    url: `ws://127.0.0.1:${address.port}/billing`,
    nextMessage
  };
}

function closeWebSocketServer(server: WebSocketServer): Promise<void> {
  for (const client of server.clients) {
    client.close();
  }
  return new Promise((resolve) => server.close(() => resolve()));
}

function closeHttpServer(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}
