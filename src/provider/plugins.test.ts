import { describe, expect, it } from 'vitest';
import { ProviderPluginRegistry } from '../adapters/registry';
import { parseGatewayConfigFromRaw } from '../config';
import { syncProviderPluginsFromConfig } from './plugins';

describe('configured provider plugins', () => {
  it('carries execution governance from declarative provider hooks into runtime plugins', () => {
    const registry = new ProviderPluginRegistry();
    const config = parseGatewayConfigFromRaw({
      plugins: [
        {
          key: 'billing-guard',
          providerHooks: {
            key: 'request-mutation',
            execution: {
              timeoutMs: 123,
              concurrency: 2,
              maxQueueSize: 5,
              failureThreshold: 3,
              cooldownMs: 456,
              failureMode: 'fail_open'
            },
            request: {
              headers: {
                'x-test-hook': 'enabled'
              }
            }
          }
        }
      ]
    });

    syncProviderPluginsFromConfig(registry, config);

    expect(registry.get('config:billing-guard:request-mutation')?.execution).toEqual({
      timeoutMs: 123,
      concurrency: 2,
      maxQueueSize: 5,
      failureThreshold: 3,
      cooldownMs: 456,
      failureMode: 'fail_open'
    });
  });
});
