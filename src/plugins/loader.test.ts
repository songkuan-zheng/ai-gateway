import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { parseGatewayConfigFromRaw } from '../config';
import { createGatewayRuntime } from '../gateway/runtime';
import { syncGatewayPluginModulesFromConfig } from './loader';

describe('syncGatewayPluginModulesFromConfig', () => {
  it('keeps previously loaded module plugins when a new module fails to load', async () => {
    const pluginDir = await mkdtemp(join(tmpdir(), 'gateway-plugin-loader-'));
    const modulePath = join(pluginDir, 'acme.mjs');
    await writePluginModule(modulePath, {
      key: 'acme_messages',
      provider: 'acme',
      providerTypes: ['acme_messages'],
      marker: 'old-module'
    });

    const runtime = createGatewayRuntime();
    const initialConfig = parseGatewayConfigFromRaw({
      plugins: [
        {
          key: 'acme',
          modulePath
        }
      ]
    });

    try {
      await syncGatewayPluginModulesFromConfig(runtime, initialConfig);
      expect(readAdapterMarker(runtime.targetAdapters.getByKey('acme_messages'))).toBe('old-module');

      const badConfig = parseGatewayConfigFromRaw({
        plugins: [
          {
            key: 'missing',
            modulePath: join(pluginDir, 'missing.mjs')
          }
        ]
      });
      await expect(syncGatewayPluginModulesFromConfig(runtime, badConfig)).rejects.toThrow(
        /modulePath does not exist/
      );

      expect(readAdapterMarker(runtime.targetAdapters.getByKey('acme_messages'))).toBe('old-module');
    } finally {
      await rm(pluginDir, { recursive: true, force: true });
    }
  });

  it('restores overwritten target adapters when module plugins are removed', async () => {
    const pluginDir = await mkdtemp(join(tmpdir(), 'gateway-plugin-loader-'));
    const modulePath = join(pluginDir, 'openai-override.mjs');
    await writePluginModule(modulePath, {
      key: 'openai_responses',
      provider: 'openai',
      providerTypes: ['openai_responses'],
      marker: 'module-override'
    });

    const runtime = createGatewayRuntime();
    const builtinAdapter = runtime.targetAdapters.getByKey('openai_responses');
    expect(builtinAdapter).toBeDefined();

    const enabledConfig = parseGatewayConfigFromRaw({
      plugins: [
        {
          key: 'openai-override',
          modulePath
        }
      ]
    });
    const disabledConfig = parseGatewayConfigFromRaw({
      plugins: []
    });

    try {
      await syncGatewayPluginModulesFromConfig(runtime, enabledConfig);
      expect(readAdapterMarker(runtime.targetAdapters.getByKey('openai_responses'))).toBe(
        'module-override'
      );

      await syncGatewayPluginModulesFromConfig(runtime, disabledConfig);
      expect(runtime.targetAdapters.getByKey('openai_responses')).toBe(builtinAdapter);
    } finally {
      await rm(pluginDir, { recursive: true, force: true });
    }
  });

  it('registers and removes billing and event plugin extensions', async () => {
    const pluginDir = await mkdtemp(join(tmpdir(), 'gateway-plugin-loader-'));
    const modulePath = join(pluginDir, 'event-extensions.mjs');
    await writePluginExtensionModule(modulePath);
    const globalState = globalThis as typeof globalThis & {
      __gatewayPluginExtensionClosed?: string[];
      __gatewayPluginExtensionInitialized?: string[];
    };
    globalState.__gatewayPluginExtensionClosed = [];
    globalState.__gatewayPluginExtensionInitialized = [];

    const runtime = createGatewayRuntime();
    const enabledConfig = parseGatewayConfigFromRaw({
      plugins: [
        {
          key: 'event-extensions',
          modulePath,
          config: {
            topic: 'gateway-events'
          }
        }
      ]
    });
    const disabledConfig = parseGatewayConfigFromRaw({
      plugins: []
    });

    try {
      await syncGatewayPluginModulesFromConfig(runtime, enabledConfig);
      expect(runtime.billingPublishers.get('billing-kafka')?.transport).toBe('kafka');
      expect(runtime.billingOutboxes.get('billing-outbox')?.transport).toBe('postgres');
      expect(runtime.agentEventPublishers.get('agent-event-kafka')?.transport).toBe('kafka');
      expect(runtime.agentEventOutboxes.get('agent-event-outbox')?.transport).toBe('postgres');
      expect(runtime.deliveryStateStores.get('delivery-state')).toBeDefined();
      expect(globalState.__gatewayPluginExtensionInitialized).toEqual([
        'billing-outbox:gateway-events'
      ]);

      await syncGatewayPluginModulesFromConfig(runtime, disabledConfig);
      expect(runtime.billingPublishers.get('billing-kafka')).toBeUndefined();
      expect(runtime.billingOutboxes.get('billing-outbox')).toBeUndefined();
      expect(runtime.agentEventPublishers.get('agent-event-kafka')).toBeUndefined();
      expect(runtime.agentEventOutboxes.get('agent-event-outbox')).toBeUndefined();
      expect(runtime.deliveryStateStores.get('delivery-state')).toBeUndefined();
      expect(globalState.__gatewayPluginExtensionClosed?.sort()).toEqual([
        'agent-event-kafka',
        'agent-event-outbox',
        'billing-kafka',
        'billing-outbox',
        'delivery-state'
      ]);
    } finally {
      delete globalState.__gatewayPluginExtensionClosed;
      delete globalState.__gatewayPluginExtensionInitialized;
      await rm(pluginDir, { recursive: true, force: true });
    }
  });

  it('keeps an unchanged object-exported delivery state store open across reloads', async () => {
    const pluginDir = await mkdtemp(join(tmpdir(), 'gateway-plugin-loader-'));
    const modulePath = join(pluginDir, 'object-state-store.mjs');
    await writeFile(
      modulePath,
      `
const state = globalThis.__gatewayObjectStateStoreLifecycle ||= { initialized: 0, closed: 0 };
let closed = false;
const store = {
  key: 'object-state-store',
  init() {
    closed = false;
    state.initialized += 1;
  },
  claimDelivery() {
    if (closed) throw new Error('state store is closed');
    return true;
  },
  releaseDeliveryClaim() {
    if (closed) throw new Error('state store is closed');
  },
  close() {
    closed = true;
    state.closed += 1;
  }
};
export default { deliveryStateStores: [store] };
`,
      'utf8'
    );
    const lifecycle = globalThis as typeof globalThis & {
      __gatewayObjectStateStoreLifecycle?: { initialized: number; closed: number };
    };
    const readLifecycle = () =>
      (globalThis as typeof lifecycle).__gatewayObjectStateStoreLifecycle;
    delete lifecycle.__gatewayObjectStateStoreLifecycle;
    const runtime = createGatewayRuntime();
    const config = parseGatewayConfigFromRaw({
      plugins: [{ key: 'object-state-store', modulePath }]
    });

    try {
      await syncGatewayPluginModulesFromConfig(runtime, config);
      await syncGatewayPluginModulesFromConfig(runtime, config);

      const store = runtime.deliveryStateStores.get('object-state-store');
      await expect(Promise.resolve(store?.claimDelivery?.('event-1', 1000))).resolves.toBe(true);
      expect(readLifecycle()).toEqual({
        initialized: 1,
        closed: 0
      });

      await expect(
        syncGatewayPluginModulesFromConfig(runtime, config, undefined, {
          beforeSwap: () => {
            throw new Error('candidate rejected');
          }
        })
      ).rejects.toThrow('candidate rejected');
      await expect(Promise.resolve(store?.claimDelivery?.('event-2', 1000))).resolves.toBe(true);
      expect(readLifecycle()).toEqual({
        initialized: 1,
        closed: 0
      });

      await syncGatewayPluginModulesFromConfig(
        runtime,
        parseGatewayConfigFromRaw({ plugins: [] })
      );
      expect(readLifecycle()?.closed).toBe(1);
    } finally {
      delete lifecycle.__gatewayObjectStateStoreLifecycle;
      await rm(pluginDir, { recursive: true, force: true });
    }
  });

  it('registers and removes provider packages and runtime hooks returned by module plugins', async () => {
    const pluginDir = await mkdtemp(join(tmpdir(), 'gateway-plugin-loader-'));
    const modulePath = join(pluginDir, 'provider-package.mjs');
    await writePluginProviderPackageModule(modulePath);

    const runtime = createGatewayRuntime();
    const config = parseGatewayConfigFromRaw({
      plugins: [
        {
          key: 'provider-package',
          modulePath
        }
      ]
    });

    try {
      await syncGatewayPluginModulesFromConfig(runtime, config);
      expect(config.providers.find((provider) => provider.name === 'acme-main')?.type).toBe(
        'acme_messages'
      );
      expect(runtime.requestHooks.get('tenant-guard')).toBeDefined();
      expect(runtime.requestTransforms.get('tenant-transform')).toBeDefined();
      expect(runtime.routeResolvers.get('tenant-router')).toBeDefined();
      expect(runtime.responseHooks.get('tenant-response')).toBeDefined();
      expect(runtime.streamHooks.get('stream-header')).toBeDefined();
      expect(runtime.httpRoutes.get('tenant-status')).toBeDefined();
      expect(runtime.billingEventHooks.get('billing-enrich')).toBeDefined();
      expect(runtime.agentEventHooks.get('agent-enrich')).toBeDefined();

      expect(config.plugins).toBeDefined();
      config.plugins![0]!.enabled = false;
      await syncGatewayPluginModulesFromConfig(runtime, config);
      expect(config.providers.find((provider) => provider.name === 'acme-main')).toBeUndefined();
      expect(runtime.requestHooks.get('tenant-guard')).toBeUndefined();
      expect(runtime.requestTransforms.get('tenant-transform')).toBeUndefined();
      expect(runtime.routeResolvers.get('tenant-router')).toBeUndefined();
      expect(runtime.responseHooks.get('tenant-response')).toBeUndefined();
      expect(runtime.streamHooks.get('stream-header')).toBeUndefined();
      expect(runtime.httpRoutes.get('tenant-status')).toBeUndefined();
      expect(runtime.billingEventHooks.get('billing-enrich')).toBeUndefined();
      expect(runtime.agentEventHooks.get('agent-enrich')).toBeUndefined();
    } finally {
      await rm(pluginDir, { recursive: true, force: true });
    }
  });

  it('rejects invalid module result shape without unloading the previous module', async () => {
    const pluginDir = await mkdtemp(join(tmpdir(), 'gateway-plugin-loader-'));
    const goodModulePath = join(pluginDir, 'good.mjs');
    const badModulePath = join(pluginDir, 'bad.mjs');
    await writePluginModule(goodModulePath, {
      key: 'acme_messages',
      provider: 'acme',
      providerTypes: ['acme_messages'],
      marker: 'valid-module'
    });
    await writeFile(
      badModulePath,
      `
export function createGatewayPlugin() {
  return {
    billingPublishers: [
      {
        transport: 'kafka',
        publish() {
          return true;
        }
      }
    ]
  };
}
`,
      'utf8'
    );

    const runtime = createGatewayRuntime();
    const goodConfig = parseGatewayConfigFromRaw({
      plugins: [
        {
          key: 'good',
          modulePath: goodModulePath
        }
      ]
    });
    const badConfig = parseGatewayConfigFromRaw({
      plugins: [
        {
          key: 'bad',
          modulePath: badModulePath
        }
      ]
    });

    try {
      await syncGatewayPluginModulesFromConfig(runtime, goodConfig);
      await expect(syncGatewayPluginModulesFromConfig(runtime, badConfig)).rejects.toThrow(
        /billingPublishers\[\] must include key/
      );
      expect(readAdapterMarker(runtime.targetAdapters.getByKey('acme_messages'))).toBe('valid-module');
    } finally {
      await rm(pluginDir, { recursive: true, force: true });
    }
  });

  it('rejects incomplete delivery state store method groups', async () => {
    const pluginDir = await mkdtemp(join(tmpdir(), 'gateway-plugin-loader-'));
    const modulePath = join(pluginDir, 'partial-delivery-state.mjs');
    await writeFile(
      modulePath,
      `
export function createGatewayPlugin() {
  return {
    deliveryStateStores: [
      {
        key: 'partial-state',
        claimDelivery() {
          return true;
        }
      }
    ]
  };
}
`,
      'utf8'
    );

    const runtime = createGatewayRuntime();
    const config = parseGatewayConfigFromRaw({
      plugins: [
        {
          key: 'partial-delivery-state',
          modulePath
        }
      ]
    });

    try {
      await expect(syncGatewayPluginModulesFromConfig(runtime, config)).rejects.toThrow(
        /must implement both claimDelivery\(\) and releaseDeliveryClaim\(\)/
      );
    } finally {
      await rm(pluginDir, { recursive: true, force: true });
    }
  });

  it('registers virtual model profiles returned by module plugins', async () => {
    const pluginDir = await mkdtemp(join(tmpdir(), 'gateway-plugin-loader-'));
    const modulePath = join(pluginDir, 'virtual-profile.mjs');
    await writeFile(
      modulePath,
      `
export function createGatewayPlugin() {
  return {
    virtualModelProfiles: [
      {
        id: 'plugin-virtual',
        key: 'plugin-virtual',
        displayName: 'Plugin Virtual',
        enabled: true,
        match: {
          exactAliases: ['plugin-model'],
          prefixes: [],
          suffixes: []
        },
        tools: [],
        execution: {
          mode: 'decorate_only',
          maxTurns: 1,
          maxToolCalls: 1,
          clientToolsPolicy: 'allow',
          streamMode: 'buffered'
        },
        materialization: {
          enabled: true,
          includeInGatewayModels: true
        }
      }
    ]
  };
}
`,
      'utf8'
    );

    const runtime = createGatewayRuntime();
    const config = parseGatewayConfigFromRaw({
      plugins: [
        {
          key: 'virtual-profile',
          modulePath
        }
      ]
    });

    try {
      await syncGatewayPluginModulesFromConfig(runtime, config);
      expect(runtime.virtualModelProfiles.get('plugin-virtual')?.displayName).toBe('Plugin Virtual');
    } finally {
      await rm(pluginDir, { recursive: true, force: true });
    }
  });

  it('reloads module factories when configured watchFiles change', async () => {
    const pluginDir = await mkdtemp(join(tmpdir(), 'gateway-plugin-loader-'));
    const modulePath = join(pluginDir, 'watched.mjs');
    const markerPath = join(pluginDir, 'marker.txt');
    await writeFile(markerPath, 'first', 'utf8');
    await writeFile(
      modulePath,
      `
import { readFileSync } from 'node:fs';
export function createGatewayPlugin() {
  const marker = readFileSync(${JSON.stringify(markerPath)}, 'utf8').trim();
  return {
    targetAdapters: [
      {
        key: 'watched_messages',
        provider: 'watched',
        providerTypes: ['watched_messages'],
        buildRequestFromStandard() {
          return {
            ok: true,
            value: {
              url: 'https://plugin.example/messages',
              headers: {},
              body: { marker }
            }
          };
        },
        toStandardResponse(payload) {
          return { ok: true, value: payload };
        }
      }
    ]
  };
}
`,
      'utf8'
    );

    const runtime = createGatewayRuntime();
    const config = parseGatewayConfigFromRaw({
      plugins: [
        {
          key: 'watched',
          modulePath,
          watchFiles: [markerPath]
        }
      ]
    });

    try {
      await syncGatewayPluginModulesFromConfig(runtime, config);
      expect(readAdapterMarker(runtime.targetAdapters.getByKey('watched_messages'))).toBe('first');
      await writeFile(markerPath, 'second', 'utf8');
      await syncGatewayPluginModulesFromConfig(runtime, config);
      expect(readAdapterMarker(runtime.targetAdapters.getByKey('watched_messages'))).toBe('second');
    } finally {
      await rm(pluginDir, { recursive: true, force: true });
    }
  });
});

async function writePluginModule(
  modulePath: string,
  adapter: {
    key: string;
    provider: string;
    providerTypes: string[];
    marker: string;
  }
): Promise<void> {
  await writeFile(
    modulePath,
    `
export function createGatewayPlugin() {
  return {
    targetAdapters: [
      {
        key: ${JSON.stringify(adapter.key)},
        provider: ${JSON.stringify(adapter.provider)},
        providerTypes: ${JSON.stringify(adapter.providerTypes)},
        buildRequestFromStandard() {
          return {
            ok: true,
            value: {
              url: 'https://plugin.example/messages',
              headers: {},
              body: {
                marker: ${JSON.stringify(adapter.marker)}
              }
            }
          };
        },
        toStandardResponse(payload) {
          return {
            ok: true,
            value: payload
          };
        }
      }
    ]
  };
}
`,
    'utf8'
  );
}

function readAdapterMarker(adapter: unknown): string | undefined {
  if (!adapter || typeof adapter !== 'object' || !('buildRequestFromStandard' in adapter)) {
    return undefined;
  }

  const result = (adapter as {
    buildRequestFromStandard: (input: unknown) => { ok: true; value: { body?: { marker?: string } } };
  }).buildRequestFromStandard({});
  return result.ok ? result.value.body?.marker : undefined;
}

async function writePluginExtensionModule(modulePath: string): Promise<void> {
  await writeFile(
    modulePath,
    `
function recordClose(key) {
  globalThis.__gatewayPluginExtensionClosed ||= [];
  globalThis.__gatewayPluginExtensionClosed.push(key);
}

function recordInit(key) {
  globalThis.__gatewayPluginExtensionInitialized ||= [];
  globalThis.__gatewayPluginExtensionInitialized.push(key);
}

export function createGatewayPlugin({ plugin }) {
  const topic = plugin.config?.topic || 'missing';
  return {
    billingPublishers: [
      {
        key: 'billing-kafka',
        transport: 'kafka',
        publish() {
          return true;
        },
        close() {
          recordClose('billing-kafka');
        }
      }
    ],
    billingOutboxes: [
      {
        key: 'billing-outbox',
        transport: 'postgres',
        init() {
          recordInit('billing-outbox:' + topic);
        },
        ready() {
          return true;
        },
        append() {
          return true;
        },
        close() {
          recordClose('billing-outbox');
        }
      }
    ],
    eventPublishers: [
      {
        key: 'agent-event-kafka',
        transport: 'kafka',
        publish() {
          return true;
        },
        close() {
          recordClose('agent-event-kafka');
        }
      }
    ],
    eventOutboxes: [
      {
        key: 'agent-event-outbox',
        transport: 'postgres',
        append() {
          return true;
        },
        close() {
          recordClose('agent-event-outbox');
        }
      }
    ],
    deliveryStateStores: [
      {
        key: 'delivery-state',
        claimDelivery() {
          return true;
        },
        releaseDeliveryClaim() {
          return undefined;
        },
        close() {
          recordClose('delivery-state');
        }
      }
    ]
  };
}
`,
    'utf8'
  );
}

async function writePluginProviderPackageModule(modulePath: string): Promise<void> {
  await writeFile(
    modulePath,
    `
export function createGatewayPlugin() {
  return {
    providers: [
      {
        name: 'acme-main',
        type: 'acme_messages',
        apikey: 'test-key',
        baseurl: 'https://api.acme.example',
        models: ['acme-large'],
        extraHeaders: {
          default: {},
          byModel: {}
        },
        extraBody: {
          default: {},
          byModel: {}
        },
        billing: {
          default: {
            inputPerMillionUsd: 0,
            outputPerMillionUsd: 0
          },
          byModel: {}
        }
      }
    ],
    requestHooks: [
      {
        key: 'tenant-guard',
        beforeRouting() {
          return {
            ok: true
          };
        }
      }
    ],
    requestTransforms: [
      {
        key: 'tenant-transform',
        transform() {
          return;
        }
      }
    ],
    routeResolvers: [
      {
        key: 'tenant-router',
        resolve() {
          return;
        }
      }
    ],
    responseHooks: [
      {
        key: 'tenant-response',
        transformResponse() {
          return;
        }
      }
    ],
    streamHooks: [
      {
        key: 'stream-header',
        transformResponse({ upstreamResponse }) {
          return upstreamResponse;
        }
      }
    ],
    httpRoutes: [
      {
        key: 'tenant-status',
        method: 'GET',
        path: '/tenant/status',
        auth: 'none',
        handler() {
          return {
            ok: true
          };
        }
      }
    ],
    billingEventHooks: [
      {
        key: 'billing-enrich',
        transform({ event }) {
          return event;
        }
      }
    ],
    eventHooks: [
      {
        key: 'agent-enrich',
        transform({ event }) {
          return event;
        }
      }
    ]
  };
}
`,
    'utf8'
  );
}
