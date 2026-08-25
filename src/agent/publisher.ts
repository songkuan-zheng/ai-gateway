import { publishJsonEventToExternalSink } from '../external-event-sink';
import { recordGatewayPluginHookExecution } from '../gateway/metrics';
import {
  closeGatewayPluginExtensions,
  executeGatewayPluginEventDelivery,
  initializeGatewayPluginExtensions,
  type GatewayPluginEventPublisher,
  type GatewayPluginOutbox
} from '../plugins/events';
import type { AgentEventQueueConfig, AgentEventWebhookConfig, GatewayPluginEventHook } from '../types';
import type { AgentEvent, AgentEventType } from './types';

export interface AgentEventPublisherLogger {
  info(payload: unknown, message?: string): void;
  warn(payload: unknown, message?: string): void;
}

export interface AgentQueueEvent {
  eventId: string;
  emittedAt: string;
  eventType: AgentEventType;
  sessionId: string;
  correlationId: string;
  causationId?: string;
  eventTimestamp: string;
  payload: unknown;
}

let queueConfig: AgentEventQueueConfig | undefined;
let webhookConfig: AgentEventWebhookConfig | undefined;
let logger: AgentEventPublisherLogger | undefined;
let pluginPublishers: GatewayPluginEventPublisher<AgentQueueEvent>[] = [];
let pluginOutboxes: GatewayPluginOutbox<AgentQueueEvent>[] = [];
let pluginEventHooks: GatewayPluginEventHook<AgentQueueEvent>[] = [];

export interface AgentEventPublisherPluginExtensions {
  publishers?: GatewayPluginEventPublisher<AgentQueueEvent>[];
  outboxes?: GatewayPluginOutbox<AgentQueueEvent>[];
  eventHooks?: GatewayPluginEventHook<AgentQueueEvent>[];
}

export async function initializeAgentEventPublisher(
  config: AgentEventQueueConfig | undefined,
  webhookPublisherConfig?: AgentEventWebhookConfig,
  log?: AgentEventPublisherLogger,
  pluginExtensions?: AgentEventPublisherPluginExtensions
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
  const normalizedConfig = normalizeConfig(config);
  initializeQueuePublisher(normalizedConfig);
  initializePluginPublishers(pluginPublishers, pluginOutboxes);
}

export async function publishAgentEventToExternalSink(event: AgentEvent): Promise<boolean> {
  const preparedEvent = await applyAgentEventHooks(toWebhookEvent(event));
  if (!preparedEvent) {
    return false;
  }
  const deliveries: Array<Promise<boolean>> = [];

  if (webhookConfig?.enabled && normalizeWebhookTarget(webhookConfig)) {
    deliveries.push(publishJsonEventToExternalSink(preparedEvent, webhookConfig));
  }

  for (const outbox of pluginOutboxes) {
    deliveries.push(appendAgentEventToPluginOutbox(outbox, preparedEvent));
  }

  for (const publisher of pluginPublishers) {
    deliveries.push(publishAgentEventToPluginPublisher(publisher, preparedEvent));
  }

  if (deliveries.length === 0) {
    return false;
  }

  const settled = await Promise.allSettled(deliveries);
  let delivered = false;
  const failures: string[] = [];
  for (const result of settled) {
    if (result.status === 'fulfilled') {
      delivered = delivered || result.value;
      continue;
    }

    failures.push(result.reason instanceof Error ? result.reason.message : String(result.reason));
  }

  if (delivered) {
    if (failures.length > 0) {
      logger?.warn(
        {
          details: failures
        },
        'One or more agent event publishers failed after another publisher accepted the event.'
      );
    }
    return true;
  }

  if (failures.length > 0) {
    throw new Error(failures.join(' | '));
  }

  return false;
}

export async function publishAgentEventToQueue(event: AgentEvent): Promise<boolean> {
  return publishAgentEventToExternalSink(event);
}

export async function closeAgentEventPublisher(): Promise<void> {
  await closeGatewayPluginExtensions([...pluginPublishers, ...pluginOutboxes]);
  pluginPublishers = [];
  pluginOutboxes = [];
  pluginEventHooks = [];
  queueConfig = undefined;
  webhookConfig = undefined;
}

function initializeWebhookPublisher(config: AgentEventWebhookConfig | undefined): void {
  if (!config?.enabled) {
    webhookConfig = config;
    logger?.info(
      {
        enabled: false
      },
      'Agent event webhook publisher is disabled.'
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
      'Agent event webhook publisher is enabled but target is missing. Webhook delivery is disabled.'
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
    'Agent event webhook publisher initialized.'
  );
}

function initializeQueuePublisher(config: AgentEventQueueConfig): void {
  queueConfig = {
    ...config,
    enabled: false
  };
  if (!config.enabled) {
    logger?.info(
      {
        enabled: false
      },
      'Agent event queue publisher is disabled.'
    );
    return;
  }

  logger?.warn(
    {
      queueName: config.queueName,
      jobName: config.jobName
    },
    'Agent event queue publisher is disabled because the gateway does not open database or Redis connections. Use an external protocol adapter for delivery.'
  );
}

function normalizeConfig(config: AgentEventQueueConfig | undefined): AgentEventQueueConfig {
  return (
    config || {
      enabled: false,
      queueName: 'gateway-agent-events',
      jobName: 'agent.event',
      removeOnComplete: 1000,
      removeOnFail: 5000
    }
  );
}

function normalizeWebhookTarget(config: AgentEventWebhookConfig): string | undefined {
  if (config.transport === 'stdio') {
    return config.command?.trim() || undefined;
  }

  return config.endpoint?.trim() || undefined;
}

function toWebhookEvent(event: AgentEvent): AgentQueueEvent {
  return {
    eventId: event.id,
    emittedAt: new Date().toISOString(),
    eventType: event.type,
    sessionId: event.sessionId,
    correlationId: event.correlationId,
    causationId: event.causationId,
    eventTimestamp: event.timestamp,
    payload: event.payload
  };
}

function initializePluginPublishers(
  publishers: GatewayPluginEventPublisher<AgentQueueEvent>[],
  outboxes: GatewayPluginOutbox<AgentQueueEvent>[]
): void {
  if (publishers.length === 0 && outboxes.length === 0) {
    return;
  }

  logger?.info(
    {
      eventPublishers: publishers.map((publisher) => publisher.key),
      eventOutboxes: outboxes.map((outbox) => outbox.key)
    },
    'Agent event plugin publishers initialized.'
  );
}

async function appendAgentEventToPluginOutbox(
  outbox: GatewayPluginOutbox<AgentQueueEvent>,
  event: AgentQueueEvent
): Promise<boolean> {
  return executeGatewayPluginEventDelivery(outbox, event, (context) => outbox.append(event, context));
}

async function publishAgentEventToPluginPublisher(
  publisher: GatewayPluginEventPublisher<AgentQueueEvent>,
  event: AgentQueueEvent
): Promise<boolean> {
  return executeGatewayPluginEventDelivery(publisher, event, (context) => publisher.publish(event, context));
}

async function applyAgentEventHooks(event: AgentQueueEvent): Promise<AgentQueueEvent | undefined> {
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
          kind: 'agent_event',
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
            kind: 'agent_event',
            hook: 'transform',
            outcome: 'dropped',
            durationMs: Number(process.hrtime.bigint() - startedAt) / 1_000_000
          });
          return undefined;
        }
        if (result.value && typeof result.value === 'object') {
          nextEvent = result.value as AgentQueueEvent;
        }
        recordGatewayPluginHookExecution({
          pluginKey: hook.key,
          kind: 'agent_event',
          hook: 'transform',
          outcome: 'success',
          durationMs: Number(process.hrtime.bigint() - startedAt) / 1_000_000
        });
        continue;
      }
      if (result && typeof result === 'object') {
        nextEvent = result as AgentQueueEvent;
      }
      recordGatewayPluginHookExecution({
        pluginKey: hook.key,
        kind: 'agent_event',
        hook: 'transform',
        outcome: 'success',
        durationMs: Number(process.hrtime.bigint() - startedAt) / 1_000_000
      });
    } catch (error) {
      recordGatewayPluginHookExecution({
        pluginKey: hook.key,
        kind: 'agent_event',
        hook: 'transform',
        outcome: 'error',
        durationMs: Number(process.hrtime.bigint() - startedAt) / 1_000_000
      });
      logger?.warn(
        {
          hook: hook.key,
          details: error instanceof Error ? error.message : String(error)
        },
        'Agent event plugin hook failed.'
      );
      throw error;
    }
  }

  return nextEvent;
}
