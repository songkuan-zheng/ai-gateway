import type { FastifyRequest } from 'fastify';
import { afterEach, describe, expect, it } from 'vitest';
import { parseGatewayConfigFromRaw } from '../config';
import {
  applyGatewayScheduling,
  closeGatewaySchedulingStore,
  resetGatewaySchedulingStateForTests,
  setGatewaySchedulingRedisCommandExecutorForTests
} from './scheduler';

describe('gateway scheduler redis storage', () => {
  afterEach(async () => {
    resetGatewaySchedulingStateForTests();
    await closeGatewaySchedulingStore();
  });

  it('uses shared credential cooldown state when selecting candidates', async () => {
    const commands: string[][] = [];
    setGatewaySchedulingRedisCommandExecutorForTests(async (_storage, args) => {
      commands.push(args);
      if (args[0] === 'MGET') {
        return args.slice(1).map((_, index) => index === 0
          ? JSON.stringify({
              cooldownUntil: Date.now() + 60000,
              consecutiveFailures: 1,
              currentWeight: 0,
              counters: {}
            })
          : null);
      }
      if (args[0] === 'SET') {
        return 'OK';
      }
      throw new Error(`Unexpected Redis scheduling command: ${args.join(' ')}`);
    });
    const config = parseGatewayConfigFromRaw({
      scheduling: {
        enabled: true,
        cacheAffinity: {
          enabled: false
        },
        fallback: {
          maxAttempts: 2
        },
        storage: {
          type: 'redis',
          url: 'redis://redis.example:6379/0',
          keyPrefix: 'test:scheduling',
          connectTimeoutMs: 100,
          commandTimeoutMs: 100,
          stateTtlMs: 300000
        }
      },
      providers: [
        {
          name: 'openai-main',
          type: 'openai_responses',
          models: ['gpt-test'],
          credentials: [
            {
              id: 'primary',
              apikey: 'primary-key'
            },
            {
              id: 'secondary',
              apikey: 'secondary-key'
            }
          ]
        }
      ]
    });
    const request = {
      body: {
        input: 'test'
      },
      headers: {}
    } as FastifyRequest;

    const routes = await applyGatewayScheduling(
      [{ provider: 'openai' as const, providerConfig: config.providers[0] }],
      { config, request, requestModel: 'gpt-test' }
    );

    expect(commands[0]?.[0]).toBe('MGET');
    expect(routes.map((route) => route.providerConfig?.credentialId)).toEqual(['secondary']);
    expect(commands.some((command) => command[0] === 'SET')).toBe(true);
  });
});
