import type {
  GatewayBillingTrace,
  BillingQueueConfig,
  BillingWebhookConfig,
  GatewayRequestClientContext,
  GatewayRequestIdentity,
  Provider
} from '../types';
import { publishJsonEventToExternalSink } from '../external-event-sink';
import { recordGatewayBillingDelivery, recordGatewayPluginHookExecution } from '../gateway/metrics';
import {
  closeGatewayPluginExtensions,
  executeGatewayPluginEventDelivery,
  initializeGatewayPluginExtensions,
  type GatewayPluginEventPublisher,
  type GatewayPluginOutbox
} from '../plugins/events';
import type { GatewayPluginEventHook } from '../types';
import type { BillingResult } from './calculate';

export interface BillingPublisherLogger {
  info(payload: unknown, message?: string): void;
  warn(payload: unknown, message?: string): void;
}

export interface BillingQueueEvent {
  eventId: string;
  emittedAt: string;
  requestId: string;
  clientIp?: string;
  attempt?: {
    kind?: 'upstream_attempt';
    sequence?: number;
  };
  route: {
    method: string;
    url: string;
  };
  source: {
    provider: Provider;
    adapterKey: string;
  };
  target: {
    provider: Provider;
    model?: string;
    providerName?: string;
  };
  fallback: {
    used: boolean;
    attempts: number;
  };
  performance?: {
    latency_ms?: number;
  };
  identity?: GatewayRequestIdentity;
  clientContext?: GatewayRequestClientContext;
  trace?: GatewayBillingTrace;
  outcome?: {
    status: 'success' | 'error' | 'timeout' | 'rate-limited';
    statusCode?: number;
    errorMessage?: string;
  };
  attempts?: Array<{
    provider: Provider;
    providerName?: string;
    stage: string;
    message: string;
    status?: number;
    details?: unknown;
  }>;
  billing: BillingResult;
}

let queueConfig: BillingQueueConfig | undefined;
let webhookConfig: BillingWebhookConfig | undefined;
let logger: BillingPublisherLogger | undefined;
let pluginPublishers: GatewayPluginEventPublisher<BillingQueueEvent>[] = [];
let pluginOutboxes: GatewayPluginOutbox<BillingQueueEvent>[] = [];
let pluginEventHooks: GatewayPluginEventHook<BillingQueueEvent>[] = [];

export interface BillingPublisherPluginExtensions {
  publishers?: GatewayPluginEventPublisher<BillingQueueEvent>[];
  outboxes?: GatewayPluginOutbox<BillingQueueEvent>[];
  eventHooks?: GatewayPluginEventHook<BillingQueueEvent>[];
}

export async function initializeBillingPublisher(
  queuePublisherConfig: BillingQueueConfig,
  webhookPublisherConfig: BillingWebhookConfig,
  log?: BillingPublisherLogger,
  pluginExtensions?: BillingPublisherPluginExtensions
): Promise<void> {
  logger = log;
  await closeGatewayPluginExtensions([...pluginPublishers, ...pluginOutboxes]);
  pluginPublishers = pluginExtensions?.publishers || [];
  pluginOutboxes = pluginExtensions?.outboxes || [];
  pluginEventHooks = pluginExtensions?.eventHooks || [];
  try {
    await initializeGatewayPluginExtensions([...pluginPublishers, ...pluginOutboxes], { logger });
  } catch (error) {
    pluginPublishers = [];
    pluginOutboxes = [];
    throw error;
  }
  initializeWebhookPublisher(webhookPublisherConfig);
  initializeQueuePublisher(queuePublisherConfig);
  initializePluginPublishers(pluginPublishers, pluginOutboxes);
}

function initializeWebhookPublisher(config: BillingWebhookConfig): void {
  if (!config.enabled) {
    webhookConfig = config;
    logger?.info(
      {
        enabled: false
      },
      'Billing webhook publisher is disabled.'
    );
    return;
  }

  const target = normalizeWebhookTarget(config);
  if (!target) {
    webhookConfig = {
      ...config,
      enabled: false
    };
    logger?.warn(
      {
        configuredEnabled: true
      },
      'Billing webhook publisher is enabled but target is missing. Webhook delivery is disabled.'
    );
    return;
  }

  webhookConfig = {
    ...config,
    endpoint: config.endpoint?.trim(),
    command: config.command?.trim()
  };
  logger?.info(
    {
      target,
      transport: config.transport,
      timeoutMs: config.timeoutMs,
      maxAttempts: config.maxAttempts,
      headerKeys: Object.keys(config.headers)
    },
    'Billing webhook publisher initialized.'
  );
}

function initializeQueuePublisher(config: BillingQueueConfig): void {
  queueConfig = {
    ...config,
    enabled: false
  };
  if (!config.enabled) {
    logger?.info(
      {
        enabled: false
      },
      'Billing queue publisher is disabled.'
    );
    return;
  }

  logger?.warn(
    {
      queueName: config.queueName,
      jobName: config.jobName
    },
    'Billing queue publisher is disabled because the gateway does not open database or Redis connections. Use billingWebhook or an external protocol adapter for delivery.'
  );
}

export async function publishBillingEvent(event: BillingQueueEvent): Promise<boolean> {
  const preparedEvent = await applyBillingEventHooks(event);
  if (!preparedEvent) {
    recordGatewayBillingDelivery({
      outcome: 'not_delivered',
      transport: 'plugin:event_hook'
    });
    return false;
  }

  const deliveries: Array<{
    transport: string;
    promise: Promise<boolean>;
  }> = [];

  if (webhookConfig?.enabled && normalizeWebhookTarget(webhookConfig)) {
    deliveries.push({
      transport: webhookConfig.transport,
      promise: publishJsonEventToExternalSink(preparedEvent, webhookConfig)
    });
  }

  for (const outbox of pluginOutboxes) {
    deliveries.push({
      transport: formatPluginTransport('outbox', outbox),
      promise: appendBillingEventToPluginOutbox(outbox, preparedEvent)
    });
  }

  for (const publisher of pluginPublishers) {
    deliveries.push({
      transport: formatPluginTransport('publisher', publisher),
      promise: publishBillingEventToPluginPublisher(publisher, preparedEvent)
    });
  }

  if (deliveries.length === 0) {
    recordGatewayBillingDelivery({
      outcome: 'not_configured',
      transport: 'none'
    });
    return false;
  }

  const settled = await Promise.allSettled(deliveries.map((delivery) => delivery.promise));
  let delivered = false;
  const failures: string[] = [];

  for (const [index, result] of settled.entries()) {
    const transport = deliveries[index]?.transport || 'unknown';
    if (result.status === 'fulfilled') {
      if (result.value) {
        recordGatewayBillingDelivery({
          outcome: 'delivered',
          transport
        });
      } else {
        recordGatewayBillingDelivery({
          outcome: 'not_delivered',
          transport
        });
      }
      delivered = delivered || result.value;
      continue;
    }

    recordGatewayBillingDelivery({
      outcome: 'failed',
      transport
    });
    failures.push(toErrorMessage(result.reason));
  }

  if (failures.length > 0) {
    logger?.warn(
      {
        details: failures
      },
      'One or more billing publishers failed to deliver event.'
    );
  }

  if (delivered) {
    return true;
  }

  if (failures.length > 0) {
    throw new Error(failures.join(' | '));
  }

  return false;
}

export function hasBillingEventPublisher(): boolean {
  return Boolean(
    (webhookConfig?.enabled && normalizeWebhookTarget(webhookConfig)) ||
      pluginOutboxes.length > 0 ||
      pluginPublishers.length > 0
  );
}

export async function closeBillingPublisher(): Promise<void> {
  await closeGatewayPluginExtensions([...pluginPublishers, ...pluginOutboxes]);
  pluginPublishers = [];
  pluginOutboxes = [];
  pluginEventHooks = [];
  webhookConfig = undefined;
  queueConfig = undefined;
}

function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function normalizeWebhookTarget(config: BillingWebhookConfig): string | undefined {
  if (config.transport === 'stdio') {
    return config.command?.trim() || undefined;
  }

  return config.endpoint?.trim() || undefined;
}

function initializePluginPublishers(
  publishers: GatewayPluginEventPublisher<BillingQueueEvent>[],
  outboxes: GatewayPluginOutbox<BillingQueueEvent>[]
): void {
  if (publishers.length === 0 && outboxes.length === 0) {
    return;
  }

  logger?.info(
    {
      billingPublishers: publishers.map((publisher) => publisher.key),
      billingOutboxes: outboxes.map((outbox) => outbox.key)
    },
    'Billing plugin publishers initialized.'
  );
}

async function appendBillingEventToPluginOutbox(
  outbox: GatewayPluginOutbox<BillingQueueEvent>,
  event: BillingQueueEvent
): Promise<boolean> {
  return executeGatewayPluginEventDelivery(outbox, event, (context) => outbox.append(event, context));
}

async function publishBillingEventToPluginPublisher(
  publisher: GatewayPluginEventPublisher<BillingQueueEvent>,
  event: BillingQueueEvent
): Promise<boolean> {
  return executeGatewayPluginEventDelivery(publisher, event, (context) => publisher.publish(event, context));
}

async function applyBillingEventHooks(event: BillingQueueEvent): Promise<BillingQueueEvent | undefined> {
  let nextEvent = event;
  for (const hook of pluginEventHooks) {
    const startedAt = process.hrtime.bigint();
    try {
      const result = await hook.transform?.({
        event: nextEvent
      });
      if (result === false) {
        recordGatewayPluginHookExecution({
          pluginKey: hook.key,
          kind: 'billing_event',
          hook: 'transform',
          outcome: 'dropped',
          durationMs: Number(process.hrtime.bigint() - startedAt) / 1_000_000
        });
        return undefined;
      }
      if (result && typeof result === 'object' && 'ok' in result) {
        if (!result.ok) {
          throw new Error(result.error);
        }
        if (result.value === false) {
          recordGatewayPluginHookExecution({
            pluginKey: hook.key,
            kind: 'billing_event',
            hook: 'transform',
            outcome: 'dropped',
            durationMs: Number(process.hrtime.bigint() - startedAt) / 1_000_000
          });
          return undefined;
        }
        if (result.value && typeof result.value === 'object') {
          nextEvent = result.value as BillingQueueEvent;
        }
        recordGatewayPluginHookExecution({
          pluginKey: hook.key,
          kind: 'billing_event',
          hook: 'transform',
          outcome: 'success',
          durationMs: Number(process.hrtime.bigint() - startedAt) / 1_000_000
        });
        continue;
      }
      if (result && typeof result === 'object') {
        nextEvent = result as BillingQueueEvent;
      }
      recordGatewayPluginHookExecution({
        pluginKey: hook.key,
        kind: 'billing_event',
        hook: 'transform',
        outcome: 'success',
        durationMs: Number(process.hrtime.bigint() - startedAt) / 1_000_000
      });
    } catch (error) {
      recordGatewayPluginHookExecution({
        pluginKey: hook.key,
        kind: 'billing_event',
        hook: 'transform',
        outcome: 'error',
        durationMs: Number(process.hrtime.bigint() - startedAt) / 1_000_000
      });
      logger?.warn(
        {
          hook: hook.key,
          details: error instanceof Error ? error.message : String(error)
        },
        'Billing plugin event hook failed.'
      );
      throw error;
    }
  }

  return nextEvent;
}

function formatPluginTransport(
  kind: 'publisher' | 'outbox',
  extension: { key: string; transport?: string }
): string {
  const transport = extension.transport?.trim();
  if (transport) {
    return `plugin:${kind}:${transport}`;
  }

  return `plugin:${kind}:${extension.key}`;
}
