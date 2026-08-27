import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  clearGatewayPluginDeadLetters,
  closeGatewayPluginExtensions,
  configureGatewayPluginDeliveryStateStores,
  executeGatewayPluginDelivery,
  executeGatewayPluginEventDelivery,
  GatewayPluginExtensionRegistry,
  initializeGatewayPluginExtensions,
  listGatewayPluginDeadLetters
} from './events';

describe('GatewayPluginExtensionRegistry', () => {
  afterEach(async () => {
    configureGatewayPluginDeliveryStateStores([]);
    await clearGatewayPluginDeadLetters();
  });

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

  it('keeps timed-out physical deliveries inside the concurrency limit and does not overlap retries', async () => {
    const extension = {
      key: 'billing-kafka-timeout-concurrency',
      delivery: {
        timeoutMs: 5,
        maxAttempts: 2,
        baseDelayMs: 0,
        maxDelayMs: 0,
        concurrency: 1,
        maxQueueSize: 0
      }
    };
    let resolvePhysicalDelivery: ((value: boolean) => void) | undefined;
    const physicalDelivery = new Promise<boolean>((resolve) => {
      resolvePhysicalDelivery = resolve;
    });
    const deliver = vi.fn(() => physicalDelivery);

    await expect(executeGatewayPluginDelivery(extension, deliver)).rejects.toThrow(/timed out/);
    expect(deliver).toHaveBeenCalledTimes(1);
    await expect(
      executeGatewayPluginDelivery(extension, vi.fn().mockResolvedValue(true))
    ).rejects.toThrow(/queue is full/);
    expect(deliver).toHaveBeenCalledTimes(1);

    resolvePhysicalDelivery?.(true);
    await physicalDelivery;
    await new Promise<void>((resolve) => setImmediate(resolve));
    await expect(
      executeGatewayPluginDelivery(extension, vi.fn().mockResolvedValue(true))
    ).resolves.toBe(true);
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

  it('atomically deduplicates concurrent event deliveries', async () => {
    const extension = {
      key: 'billing-kafka-atomic-dedupe',
      delivery: {
        dedupe: true,
        dedupeTtlMs: 60000
      }
    };
    const deliver = vi.fn().mockResolvedValue(true);
    const event = {
      eventId: 'evt_gateway_plugin_atomic_dedupe'
    };

    const results = await Promise.all([
      executeGatewayPluginEventDelivery(extension, event, deliver),
      executeGatewayPluginEventDelivery(extension, event, deliver)
    ]);

    expect(results.sort()).toEqual([false, true]);
    expect(deliver).toHaveBeenCalledTimes(1);
  });

  it('records a dead letter when event delivery fails after retry exhaustion', async () => {
    await clearGatewayPluginDeadLetters('billing-kafka-dead-letter');
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

    const entries = await listGatewayPluginDeadLetters('billing-kafka-dead-letter');
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
    await clearGatewayPluginDeadLetters('billing-kafka-dead-letter');
  });

  it('uses a configured delivery state store for dedupe and dead letters', async () => {
    const delivered = new Set<string>();
    const deadLetters: unknown[] = [];
    configureGatewayPluginDeliveryStateStores([
      {
        key: 'persistent-state',
        claimDelivery: vi.fn((key: string) => {
          if (delivered.has(key)) {
            return false;
          }
          delivered.add(key);
          return true;
        }),
        releaseDeliveryClaim: vi.fn((key: string) => {
          delivered.delete(key);
        }),
        writeDeadLetter: vi.fn((entry) => {
          deadLetters.push(entry);
        }),
        listDeadLetters: vi.fn(() => deadLetters as any),
        clearDeadLetters: vi.fn(() => {
          const count = deadLetters.length;
          deadLetters.splice(0, deadLetters.length);
          delivered.clear();
          return count;
        })
      }
    ]);

    const extension = {
      key: 'billing-external-state',
      delivery: {
        dedupe: true,
        deadLetter: true,
        maxAttempts: 1
      }
    };
    const event = {
      eventId: 'evt_external_state'
    };

    await expect(executeGatewayPluginEventDelivery(extension, event, vi.fn().mockResolvedValue(true))).resolves.toBe(true);
    const secondDeliver = vi.fn().mockResolvedValue(true);
    await expect(executeGatewayPluginEventDelivery(extension, event, secondDeliver)).resolves.toBe(false);
    expect(secondDeliver).not.toHaveBeenCalled();

    await expect(
      executeGatewayPluginEventDelivery(
        {
          ...extension,
          key: 'billing-external-state-dead-letter'
        },
        { eventId: 'evt_external_state_dead_letter' },
        vi.fn().mockRejectedValue(new Error('external outage'))
      )
    ).rejects.toThrow(/external outage/);

    const entries = await listGatewayPluginDeadLetters();
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({
      extensionKey: 'billing-external-state-dead-letter',
      eventId: 'evt_external_state_dead_letter',
      error: 'external outage'
    });
  });

  it('does not split dedupe state across incomplete delivery state stores', async () => {
    const incompleteClaimDelivery = vi.fn(() => false);
    configureGatewayPluginDeliveryStateStores([
      {
        key: 'incomplete-dedupe-state',
        claimDelivery: incompleteClaimDelivery
      }
    ]);

    const extension = {
      key: 'billing-memory-dedupe-fallback',
      delivery: {
        dedupe: true,
        dedupeTtlMs: 60000
      }
    };
    const event = {
      eventId: 'evt_incomplete_external_state'
    };
    const firstDeliver = vi.fn().mockResolvedValue(true);
    const secondDeliver = vi.fn().mockResolvedValue(true);

    await expect(executeGatewayPluginEventDelivery(extension, event, firstDeliver)).resolves.toBe(true);
    await expect(executeGatewayPluginEventDelivery(extension, event, secondDeliver)).resolves.toBe(false);

    expect(firstDeliver).toHaveBeenCalledTimes(1);
    expect(secondDeliver).not.toHaveBeenCalled();
    expect(incompleteClaimDelivery).not.toHaveBeenCalled();
  });
});
