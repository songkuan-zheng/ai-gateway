import type {
  BillingConfig,
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
import { runGatewayPluginProtectedOperation } from '../plugins/execution';
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
let shutdownDrainTimeoutMs = 5000;
let deliveryRequirements: Pick<BillingConfig['delivery'], 'requirePublisher' | 'requireOutbox'> = {
  requirePublisher: false,
  requireOutbox: false
};
const pendingBillingDeliveries = new Set<Promise<boolean>>();

export interface BillingPublisherPluginExtensions {
  publishers?: GatewayPluginEventPublisher<BillingQueueEvent>[];
  outboxes?: GatewayPluginOutbox<BillingQueueEvent>[];
  eventHooks?: GatewayPluginEventHook<BillingQueueEvent>[];
}

export async function initializeBillingPublisher(
  queuePublisherConfig: BillingQueueConfig,
  webhookPublisherConfig: BillingWebhookConfig,
  log?: BillingPublisherLogger,
  pluginExtensions?: BillingPublisherPluginExtensions,
  deliveryConfig?: BillingConfig['delivery']
): Promise<void> {
  logger = log;
  await drainPendingBillingDeliveries(shutdownDrainTimeoutMs);
  shutdownDrainTimeoutMs = deliveryConfig?.shutdownDrainTimeoutMs ?? 5000;
  deliveryRequirements = {
    requirePublisher: deliveryConfig?.requirePublisher ?? false,
    requireOutbox: deliveryConfig?.requireOutbox ?? false
  };
  const nextPublishers = pluginExtensions?.publishers || [];
  const nextOutboxes = pluginExtensions?.outboxes || [];
  const retainedExtensions = new Set([...nextPublishers, ...nextOutboxes]);
  await closeGatewayPluginExtensions(
    [...pluginPublishers, ...pluginOutboxes].filter((extension) => !retainedExtensions.has(extension))
  );
  pluginPublishers = nextPublishers;
  pluginOutboxes = nextOutboxes;
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
  const requirements = { ...deliveryRequirements };
  const promise = publishBillingEventInternal(event, requirements);
  pendingBillingDeliveries.add(promise);
  promise.then(
    () => pendingBillingDeliveries.delete(promise),
    () => pendingBillingDeliveries.delete(promise)
  );
  return promise;
}

async function publishBillingEventInternal(
  event: BillingQueueEvent,
  requirements: Pick<BillingConfig['delivery'], 'requirePublisher' | 'requireOutbox'>
): Promise<boolean> {
  const preparedEvent = await applyBillingEventHooks(event);
  if (!preparedEvent) {
    recordGatewayBillingDelivery({
      outcome: 'not_delivered',
      transport: 'plugin:event_hook'
    });
    return false;
  }

  const deliveries: Array<{
    kind: 'webhook' | 'outbox' | 'publisher';
    transport: string;
    promise: Promise<boolean>;
  }> = [];

  if (webhookConfig?.enabled && normalizeWebhookTarget(webhookConfig)) {
    deliveries.push({
      kind: 'webhook',
      transport: webhookConfig.transport,
      promise: publishJsonEventToExternalSink(preparedEvent, webhookConfig)
    });
  }

  for (const outbox of pluginOutboxes) {
    deliveries.push({
      kind: 'outbox',
      transport: formatPluginTransport('outbox', outbox),
      promise: appendBillingEventToPluginOutbox(outbox, preparedEvent)
    });
  }

  for (const publisher of pluginPublishers) {
    deliveries.push({
      kind: 'publisher',
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
  let outboxDelivered = false;
  const failures: string[] = [];
  const outboxFailures: string[] = [];

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
      outboxDelivered = outboxDelivered || (deliveries[index]?.kind === 'outbox' && result.value);
      continue;
    }

    recordGatewayBillingDelivery({
      outcome: 'failed',
      transport
    });
    const failure = toErrorMessage(result.reason);
    failures.push(failure);
    if (deliveries[index]?.kind === 'outbox') {
      outboxFailures.push(failure);
    }
  }

  if (failures.length > 0) {
    logger?.warn(
      {
        details: failures
      },
      'One or more billing publishers failed to deliver event.'
    );
  }

  if (requirements.requireOutbox && !outboxDelivered) {
    if (outboxFailures.length > 0) {
      throw new Error(`Required billing outbox delivery failed: ${outboxFailures.join(' | ')}`);
    }
    return false;
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

export function hasBillingEventOutbox(): boolean {
  return pluginOutboxes.length > 0;
}

export function validateBillingPublisherRequirements(
  config: BillingConfig,
  extensions?: BillingPublisherPluginExtensions,
  webhook: BillingWebhookConfig | undefined = webhookConfig
): void {
  if (!config.enabled) {
    return;
  }

  const outboxes = extensions?.outboxes ?? pluginOutboxes;
  const publishers = extensions?.publishers ?? pluginPublishers;
  const hasOutbox = outboxes.length > 0;
  const hasPublisher = Boolean(
    (webhook?.enabled && normalizeWebhookTarget(webhook)) ||
      outboxes.length > 0 ||
      publishers.length > 0
  );

  if (config.delivery.requireOutbox && !hasOutbox) {
    throw new Error(
      'Billing delivery requires a plugin billing outbox, but no billingOutboxes are registered.'
    );
  }

  if (config.delivery.requirePublisher && !hasPublisher) {
    throw new Error(
      'Billing delivery requires at least one billing publisher or outbox, but none are configured.'
    );
  }
}

export async function closeBillingPublisher(): Promise<void> {
  await drainPendingBillingDeliveries(shutdownDrainTimeoutMs);
  await closeGatewayPluginExtensions([...pluginPublishers, ...pluginOutboxes]);
  pluginPublishers = [];
  pluginOutboxes = [];
  pluginEventHooks = [];
  webhookConfig = undefined;
  queueConfig = undefined;
  deliveryRequirements = {
    requirePublisher: false,
    requireOutbox: false
  };
}

export async function drainBillingPublisher(timeoutMs = shutdownDrainTimeoutMs): Promise<void> {
  await drainPendingBillingDeliveries(timeoutMs);
}

async function drainPendingBillingDeliveries(timeoutMs: number): Promise<void> {
  if (pendingBillingDeliveries.size === 0) {
    return;
  }

  const pending = Promise.allSettled(Array.from(pendingBillingDeliveries));
  if (timeoutMs <= 0) {
    await pending;
    return;
  }

  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      pending,
      new Promise<void>((resolve) => {
        timer = setTimeout(resolve, timeoutMs);
      })
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
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
    const executionResult = await runGatewayPluginProtectedOperation({
      pluginKey: hook.key,
      kind: 'billing_event',
      hook: 'transform',
      execution: hook.execution,
      operation: () => hook.transform?.({
        event: nextEvent
      })
    });
    if (!executionResult.ok) {
      recordGatewayPluginHookExecution({
        pluginKey: hook.key,
        kind: 'billing_event',
        hook: 'transform',
        outcome: executionResult.reason,
        durationMs: Number(process.hrtime.bigint() - startedAt) / 1_000_000
      });
      logger?.warn(
        {
          hook: hook.key,
          details: executionResult.error
        },
        'Billing plugin event hook failed.'
      );
      throw new Error(executionResult.error);
    }
    if ('skipped' in executionResult) {
      recordGatewayPluginHookExecution({
        pluginKey: hook.key,
        kind: 'billing_event',
        hook: 'transform',
        outcome: executionResult.reason,
        durationMs: Number(process.hrtime.bigint() - startedAt) / 1_000_000
      });
      continue;
    }

    const result = executionResult.value;
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
            details: result.error
          },
          'Billing plugin event hook failed.'
        );
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
