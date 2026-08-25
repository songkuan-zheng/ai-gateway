import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { GatewayPluginEventPublisher, GatewayPluginOutbox } from '../plugins/events';
import type { AgentEventQueueConfig, AgentEventWebhookConfig } from '../types';
import type { AgentEvent } from './types';
import {
  closeAgentEventPublisher,
  initializeAgentEventPublisher,
  publishAgentEventToQueue,
  type AgentQueueEvent
} from './publisher';

describe('agent event queue publisher', () => {
  const tempDirs: string[] = [];

  afterEach(async () => {
    await closeAgentEventPublisher();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
    for (const dir of tempDirs.splice(0)) {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('does not publish when queue is disabled', async () => {
    await initializeAgentEventPublisher(buildQueueConfig(false));
    const delivered = await publishAgentEventToQueue(buildEvent());

    expect(delivered).toBe(false);
  });

  it('does not open a queue connection when queue config is enabled', async () => {
    await initializeAgentEventPublisher(buildQueueConfig(true));

    const delivered = await publishAgentEventToQueue(buildEvent());

    expect(delivered).toBe(false);
  });

  it('publishes agent events to webhook when enabled', async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 204 }));
    vi.stubGlobal('fetch', fetchMock);
    await initializeAgentEventPublisher(buildQueueConfig(false), buildWebhookConfig(true));

    const delivered = await publishAgentEventToQueue(buildEvent());

    expect(delivered).toBe(true);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledWith(
      'https://agent.example.com/events',
      expect.objectContaining({
        method: 'POST'
      })
    );
    const requestInit = fetchMock.mock.calls[0]?.[1] as RequestInit;
    expect(requestInit.headers).toMatchObject({
      'content-type': 'application/json',
      authorization: 'Bearer agent-secret'
    });
    expect(JSON.parse(String(requestInit.body))).toEqual({
      eventId: 'event-1',
      emittedAt: expect.any(String),
      eventType: 'USER_INPUT',
      sessionId: 'session-1',
      correlationId: 'corr-1',
      eventTimestamp: expect.any(String),
      payload: {
        text: 'hello'
      }
    });
  });

  it('publishes agent events through stdio webhook transport', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'gateway-agent-stdio-publisher-'));
    tempDirs.push(dir);
    const outputPath = join(dir, 'agent-event.jsonl');
    await initializeAgentEventPublisher(buildQueueConfig(false), {
      ...buildWebhookConfig(true),
      transport: 'stdio',
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

    const delivered = await publishAgentEventToQueue(buildEvent());

    expect(delivered).toBe(true);
    expect(JSON.parse(readFileSync(outputPath, 'utf8'))).toMatchObject({
      eventId: 'event-1',
      eventType: 'USER_INPUT',
      sessionId: 'session-1'
    });
  });

  it('throws when webhook delivery fails', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('bad gateway', { status: 502 })));
    await initializeAgentEventPublisher(buildQueueConfig(false), buildWebhookConfig(true));

    await expect(publishAgentEventToQueue(buildEvent())).rejects.toThrow(
      'HTTP event sink request failed with status 502: bad gateway'
    );
  });

  it('publishes agent events through plugin outboxes and publishers', async () => {
    const outboxEvents: AgentQueueEvent[] = [];
    const publisherEvents: AgentQueueEvent[] = [];
    const outboxClose = vi.fn();
    const publisherClose = vi.fn();
    const outbox: GatewayPluginOutbox<AgentQueueEvent> = {
      key: 'agent-event-outbox',
      transport: 'kafka',
      append: vi.fn(async (event) => {
        outboxEvents.push(event);
        return true;
      }),
      close: outboxClose
    };
    const publisher: GatewayPluginEventPublisher<AgentQueueEvent> = {
      key: 'agent-event-publisher',
      transport: 'kafka-live',
      publish: vi.fn(async (event) => {
        publisherEvents.push(event);
        return true;
      }),
      close: publisherClose
    };
    await initializeAgentEventPublisher(
      buildQueueConfig(false),
      buildWebhookConfig(false),
      undefined,
      {
        publishers: [publisher],
        outboxes: [outbox]
      }
    );

    const delivered = await publishAgentEventToQueue(buildEvent());

    expect(delivered).toBe(true);
    expect(outbox.append).toHaveBeenCalledTimes(1);
    expect(publisher.publish).toHaveBeenCalledTimes(1);
    expect(outboxEvents[0]).toMatchObject({
      eventId: 'event-1',
      eventType: 'USER_INPUT',
      sessionId: 'session-1'
    });
    expect(publisherEvents[0]).toMatchObject({
      eventId: 'event-1',
      eventType: 'USER_INPUT',
      sessionId: 'session-1'
    });

    await closeAgentEventPublisher();
    expect(outboxClose).toHaveBeenCalledTimes(1);
    expect(publisherClose).toHaveBeenCalledTimes(1);
  });
});

function buildQueueConfig(enabled: boolean): AgentEventQueueConfig {
  return {
    enabled,
    queueName: 'gateway-agent-events',
    jobName: 'agent.event',
    removeOnComplete: 1000,
    removeOnFail: 5000
  };
}

function buildWebhookConfig(enabled: boolean): AgentEventWebhookConfig {
  return {
    enabled,
    transport: 'http',
    endpoint: 'https://agent.example.com/events',
    command: undefined,
    args: [],
    cwd: undefined,
    env: {},
    timeoutMs: 5000,
    maxAttempts: 1,
    baseDelayMs: 200,
    maxDelayMs: 2000,
    requireAck: false,
    headers: {
      authorization: 'Bearer agent-secret'
    }
  };
}

function buildEvent(): AgentEvent {
  return {
    id: 'event-1',
    type: 'USER_INPUT',
    sessionId: 'session-1',
    timestamp: new Date().toISOString(),
    correlationId: 'corr-1',
    payload: {
      text: 'hello'
    }
  };
}
