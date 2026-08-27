import { afterEach, describe, expect, it, vi } from 'vitest';
import { resetGatewayPluginExecutionStateForTests } from './execution';
import { executeGatewayPluginRequestHookStage } from './hooks';
import type { GatewayPluginRequestHookInput } from '../types';

describe('gateway plugin hook execution protection', () => {
  afterEach(() => {
    vi.useRealTimers();
    resetGatewayPluginExecutionStateForTests();
  });

  it('fails open when a request hook times out and failureMode is fail_open', async () => {
    const nextHook = vi.fn(() => ({
      ok: true as const
    }));
    const result = await executeGatewayPluginRequestHookStage(
      [
        {
          key: 'slow-hook',
          execution: {
            timeoutMs: 1,
            failureMode: 'fail_open'
          },
          beforeRouting: () => new Promise(() => undefined)
        },
        {
          key: 'next-hook',
          beforeRouting: nextHook
        }
      ],
      'beforeRouting',
      createHookInput()
    );

    expect(result).toMatchObject({
      ok: true
    });
    expect(nextHook).toHaveBeenCalledTimes(1);
  });

  it('fails closed when a request hook times out by default', async () => {
    const result = await executeGatewayPluginRequestHookStage(
      [
        {
          key: 'slow-hook',
          execution: {
            timeoutMs: 1
          },
          beforeRouting: () => new Promise(() => undefined)
        }
      ],
      'beforeRouting',
      createHookInput()
    );

    expect(result).toMatchObject({
      ok: false,
      pluginKey: 'slow-hook'
    });
    expect(result.ok ? undefined : result.error).toMatch(/timed out/);
  });

  it('opens a per-hook circuit after protected failures', async () => {
    const failingHook = vi.fn(() => {
      throw new Error('temporary plugin outage');
    });
    const hook = {
      key: 'flaky-hook',
      execution: {
        failureThreshold: 1,
        cooldownMs: 60000,
        failureMode: 'fail_open' as const
      },
      beforeRouting: failingHook
    };

    await expect(
      executeGatewayPluginRequestHookStage([hook], 'beforeRouting', createHookInput())
    ).resolves.toMatchObject({ ok: true });
    await expect(
      executeGatewayPluginRequestHookStage([hook], 'beforeRouting', createHookInput())
    ).resolves.toMatchObject({ ok: true });

    expect(failingHook).toHaveBeenCalledTimes(1);
  });

  it('keeps concurrency occupied after timeout until the underlying hook settles', async () => {
    const slowHook = vi.fn((): Promise<never> => new Promise(() => undefined));
    const hook = {
      key: 'slow-concurrent-hook',
      execution: {
        timeoutMs: 1,
        concurrency: 1,
        maxQueueSize: 0,
        failureMode: 'fail_open' as const
      },
      beforeRouting: slowHook
    };

    await expect(
      executeGatewayPluginRequestHookStage([hook], 'beforeRouting', createHookInput())
    ).resolves.toMatchObject({ ok: true });
    await expect(
      executeGatewayPluginRequestHookStage([hook], 'beforeRouting', createHookInput())
    ).resolves.toMatchObject({ ok: true });

    expect(slowHook).toHaveBeenCalledTimes(1);
  });

  it('rejects queued and subsequent executions when a timed-out hook never settles', async () => {
    vi.useFakeTimers();
    const slowHook = vi.fn((): Promise<never> => new Promise(() => undefined));
    const hook = {
      key: 'stuck-queued-hook',
      execution: {
        timeoutMs: 10,
        concurrency: 1,
        maxQueueSize: 10
      },
      beforeRouting: slowHook
    };

    const first = executeGatewayPluginRequestHookStage([hook], 'beforeRouting', createHookInput());
    const queued = executeGatewayPluginRequestHookStage([hook], 'beforeRouting', createHookInput());
    await vi.advanceTimersByTimeAsync(10);

    await expect(first).resolves.toMatchObject({ ok: false, pluginKey: 'stuck-queued-hook' });
    await expect(queued).resolves.toMatchObject({ ok: false, pluginKey: 'stuck-queued-hook' });
    await expect(
      executeGatewayPluginRequestHookStage([hook], 'beforeRouting', createHookInput())
    ).resolves.toMatchObject({ ok: false, pluginKey: 'stuck-queued-hook' });
    expect(slowHook).toHaveBeenCalledTimes(1);
  });
});

function createHookInput(): GatewayPluginRequestHookInput {
  return {
    request: {
      headers: {},
      method: 'POST',
      url: '/v1/responses'
    } as any,
    config: {} as any,
    route: {
      method: 'POST',
      url: '/v1/responses',
      route: '/v1/responses'
    },
    sourceProvider: 'openai',
    sourceAdapterKey: 'openai_responses',
    model: 'gpt-test'
  };
}
