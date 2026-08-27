import Fastify from 'fastify';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { parseGatewayConfigFromRaw } from '../config';
import type { GatewayConfig } from '../types';
import { registerProviderWebhookRoutes } from './webhook';

describe('provider webhook config transactions', () => {
  afterEach(() => {
    delete process.env.PROVIDER_WEBHOOK_API_KEY;
    vi.restoreAllMocks();
  });

  it('rolls the runtime and active config back when a patch reload fails', async () => {
    process.env.PROVIDER_WEBHOOK_API_KEY = 'webhook-secret';
    const config = parseGatewayConfigFromRaw({
      providerExternal: {
        enabled: true,
        endpoint: 'https://providers.example/config'
      },
      billingWebhook: {
        enabled: false
      }
    });
    const reloads: boolean[] = [];
    const onConfigReload = vi.fn(async (candidate: GatewayConfig) => {
      reloads.push(candidate.billingWebhook.enabled);
      if (candidate.billingWebhook.enabled) {
        throw new Error('candidate runtime rejected');
      }
    });
    const app = Fastify({ logger: false });
    registerProviderWebhookRoutes(app, { config, onConfigReload });
    await app.ready();

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/internal/provider/webhook',
        headers: {
          'x-provider-webhook-key': 'webhook-secret'
        },
        payload: {
          type: 'config.patch',
          operations: [
            {
              op: 'set',
              path: 'billingWebhook',
              value: {
                enabled: true,
                endpoint: 'https://billing.example/events'
              }
            }
          ]
        }
      });

      expect(response.statusCode).toBe(500);
      expect(config.billingWebhook.enabled).toBe(false);
      expect(reloads).toEqual([true, false]);
    } finally {
      await app.close();
    }
  });
});
