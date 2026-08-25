import { describe, expect, it, vi } from 'vitest';
import {
  clearGatewayPluginDeadLetters,
  closeGatewayPluginExtensions,
  executeGatewayPluginDelivery,
  executeGatewayPluginEventDelivery,
  GatewayPluginExtensionRegistry,
  initializeGatewayPluginExtensions,
  listGatewayPluginDeadLetters
} from './events';

describe('GatewayPluginExtensionRegistry', () => {
  it('closes each plugin extension object at most once until it is registered again', async () => {
    const extension = {
      key: 'billing-kafka',
      close: vi.fn()
    };

    await closeGatewayPluginExtensions([extension, extension]);
    await closeGatewayPluginExtensions([extension]);
    expect(extension.close).toHaveBeenCalledTimes(1);

    const registry = new GatewayPluginExtensionRegistry<typeof extension>();
    registry.register(extension);
    await closeGatewayPluginExtensions([extension]);
    expect(extension.close).toHaveBeenCalledTimes(2);
  });

  it('runs init and ready before close lifecycle methods', async () => {
    const calls: string[] = [];
    const extension = {
      key: 'billing-kafka',
      init: vi.fn(() => {
        calls.push('init');
      }),
      ready: vi.fn(() => {
        calls.push('ready');
        return true;
      }),
      drain: vi.fn(() => {
        calls.push('drain');
      }),
      flush: vi.fn(() => {
        calls.push('flush');
      }),
      close: vi.fn(() => {
        calls.push('close');
      })
    };

    await initializeGatewayPluginExtensions([extension], {
      plugin: {
        key: 'billing-kafka',
        enabled: true,
        config: {
          topic: 'billing-events'
        },
        providerHooks: []
      }
    });
    await closeGatewayPluginExtensions([extension]);

    expect(calls).toEqual(['init', 'ready', 'drain', 'flush', 'close']);
  });

  it('retries plugin deliveries according to extension delivery options', async () => {
    const extension = {
      key: 'billing-kafka',
      delivery: {
        maxAttempts: 2,
        baseDelayMs: 0,
        maxDelayMs: 0
      }
    };
    const deliver = vi
      .fn()
      .mockRejectedValueOnce(new Error('temporary kafka failure'))
      .mockResolvedValueOnce(true);

    const delivered = await executeGatewayPluginDelivery(extension, deliver);

    expect(delivered).toBe(true);
    expect(deliver).toHaveBeenCalledTimes(2);
  });

  it('aborts the delivery signal when a plugin delivery times out', async () => {
    const extension = {
      key: 'billing-kafka',
      delivery: {
        timeoutMs: 5,
        maxAttempts: 1
      }
    };
    let observedSignal: AbortSignal | undefined;

    await expect(
      executeGatewayPluginDelivery(extension, ({ signal }) => {
        observedSignal = signal;
        return new Promise(() => undefined);
      })
    ).rejects.toThrow(/timed out/);

    expect(observedSignal?.aborted).toBe(true);
  });

  it('deduplicates delivered event deliveries when enabled', async () => {
    const extension = {
      key: 'billing-kafka-dedupe',
      delivery: {
        dedupe: true,
        dedupeTtlMs: 60000
      }
    };
    const firstDeliver = vi.fn().mockResolvedValue(true);
    const secondDeliver = vi.fn().mockResolvedValue(true);
    const event = {
      eventId: 'evt_gateway_plugin_dedupe'
    };

    await expect(executeGatewayPluginEventDelivery(extension, event, firstDeliver)).resolves.toBe(true);
    await expect(executeGatewayPluginEventDelivery(extension, event, secondDeliver)).resolves.toBe(false);

    expect(firstDeliver).toHaveBeenCalledTimes(1);
    expect(secondDeliver).not.toHaveBeenCalled();
  });

  it('records a dead letter when event delivery fails after retry exhaustion', async () => {
    clearGatewayPluginDeadLetters('billing-kafka-dead-letter');
    const deadLetter = vi.fn();
    const extension = {
      key: 'billing-kafka-dead-letter',
      transport: 'kafka',
      delivery: {
        maxAttempts: 2,
        baseDelayMs: 0,
        maxDelayMs: 0,
        deadLetter: {
          enabled: true,
          maxEntries: 10
        }
      },
      deadLetter
    };
    const deliver = vi.fn().mockRejectedValue(new Error('kafka unavailable'));
    const event = {
      eventId: 'evt_gateway_plugin_dead_letter',
      payload: {
        ok: false
      }
    };

    await expect(executeGatewayPluginEventDelivery(extension, event, deliver)).rejects.toThrow(
      /kafka unavailable/
    );

    const entries = listGatewayPluginDeadLetters('billing-kafka-dead-letter');
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({
      extensionKey: 'billing-kafka-dead-letter',
      transport: 'kafka',
      eventId: 'evt_gateway_plugin_dead_letter',
      attempts: 2,
      error: 'kafka unavailable',
      event
    });
    expect(deadLetter).toHaveBeenCalledWith(entries[0]);
    clearGatewayPluginDeadLetters('billing-kafka-dead-letter');
  });
});
