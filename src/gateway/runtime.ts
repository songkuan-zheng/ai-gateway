import { createBuiltinSourceAdapters } from '../adapters/builtins/source';
import { createBuiltinTargetAdapters } from '../adapters/builtins/target';
import { ProviderPluginRegistry, SourceAdapterRegistry, TargetAdapterRegistry } from '../adapters/registry';
import type { AgentToolProvider } from '../agent/tools';
import {
  collectGatewayPluginExtensionHealth,
  GatewayPluginExtensionRegistry,
  type GatewayPluginHealth,
  type GatewayPluginEventPublisher,
  type GatewayPluginOutbox,
  type GatewayPluginDeliveryStateStore
} from '../plugins/events';
import { syncProviderPluginsFromConfig } from '../provider/plugins';
import type {
  GatewayConfig,
  GatewayPluginEventHook,
  GatewayPluginHttpRoute,
  GatewayPluginRequestHook,
  GatewayPluginRequestTransform,
  GatewayPluginResponseHook,
  GatewayPluginRouteResolver,
  GatewayPluginStreamHook,
  VirtualModelProfileConfig
} from '../types';

interface RegisterOptions {
  overwrite?: boolean;
}

export class VirtualModelProfileRegistry {
  private readonly profiles = new Map<string, VirtualModelProfileConfig>();

  register(profile: VirtualModelProfileConfig, options?: RegisterOptions): void {
    const exists = this.profiles.has(profile.key);
    if (exists && !options?.overwrite) {
      throw new Error(`Virtual model profile already registered: ${profile.key}`);
    }

    this.profiles.set(profile.key, profile);
  }

  get(key: string): VirtualModelProfileConfig | undefined {
    return this.profiles.get(key);
  }

  unregister(key: string): boolean {
    return this.profiles.delete(key);
  }

  list(): VirtualModelProfileConfig[] {
    return Array.from(this.profiles.values());
  }
}

export class GatewayPluginComponentRegistry<T extends { key: string }> {
  private readonly components = new Map<string, T>();

  register(component: T, options?: RegisterOptions): void {
    const exists = this.components.has(component.key);
    if (exists && !options?.overwrite) {
      throw new Error(`Gateway plugin component already registered: ${component.key}`);
    }

    this.components.set(component.key, component);
  }

  get(key: string): T | undefined {
    return this.components.get(key);
  }

  unregister(key: string): boolean {
    return this.components.delete(key);
  }

  list(): T[] {
    return Array.from(this.components.values());
  }
}

export interface GatewayRuntime {
  sourceAdapters: SourceAdapterRegistry;
  targetAdapters: TargetAdapterRegistry;
  providerPlugins: ProviderPluginRegistry;
  requestHooks: GatewayPluginComponentRegistry<GatewayPluginRequestHook>;
  requestTransforms: GatewayPluginComponentRegistry<GatewayPluginRequestTransform>;
  routeResolvers: GatewayPluginComponentRegistry<GatewayPluginRouteResolver>;
  responseHooks: GatewayPluginComponentRegistry<GatewayPluginResponseHook>;
  streamHooks: GatewayPluginComponentRegistry<GatewayPluginStreamHook>;
  httpRoutes: GatewayPluginComponentRegistry<GatewayPluginHttpRoute>;
  billingEventHooks: GatewayPluginComponentRegistry<GatewayPluginEventHook>;
  agentEventHooks: GatewayPluginComponentRegistry<GatewayPluginEventHook>;
  billingPublishers: GatewayPluginExtensionRegistry<GatewayPluginEventPublisher>;
  billingOutboxes: GatewayPluginExtensionRegistry<GatewayPluginOutbox>;
  agentEventPublishers: GatewayPluginExtensionRegistry<GatewayPluginEventPublisher>;
  agentEventOutboxes: GatewayPluginExtensionRegistry<GatewayPluginOutbox>;
  deliveryStateStores: GatewayPluginExtensionRegistry<GatewayPluginDeliveryStateStore>;
  virtualModelProfiles: VirtualModelProfileRegistry;
  toolProvider?: AgentToolProvider;
}

export interface GatewayRuntimePluginHealthGroup {
  kind: string;
  extensions: Array<GatewayPluginHealth & { key: string }>;
}

export interface GatewayRuntimePluginHealthSummary {
  status: 'ok' | 'degraded' | 'unhealthy';
  total: number;
  degraded: number;
  unhealthy: number;
}

export function createGatewayRuntime(
  config?: GatewayConfig,
  toolProvider?: AgentToolProvider
): GatewayRuntime {
  const sourceAdapters = new SourceAdapterRegistry();
  const targetAdapters = new TargetAdapterRegistry();
  const providerPlugins = new ProviderPluginRegistry();
  const requestHooks = new GatewayPluginComponentRegistry<GatewayPluginRequestHook>();
  const requestTransforms = new GatewayPluginComponentRegistry<GatewayPluginRequestTransform>();
  const routeResolvers = new GatewayPluginComponentRegistry<GatewayPluginRouteResolver>();
  const responseHooks = new GatewayPluginComponentRegistry<GatewayPluginResponseHook>();
  const streamHooks = new GatewayPluginComponentRegistry<GatewayPluginStreamHook>();
  const httpRoutes = new GatewayPluginComponentRegistry<GatewayPluginHttpRoute>();
  const billingEventHooks = new GatewayPluginComponentRegistry<GatewayPluginEventHook>();
  const agentEventHooks = new GatewayPluginComponentRegistry<GatewayPluginEventHook>();
  const billingPublishers = new GatewayPluginExtensionRegistry<GatewayPluginEventPublisher>();
  const billingOutboxes = new GatewayPluginExtensionRegistry<GatewayPluginOutbox>();
  const agentEventPublishers = new GatewayPluginExtensionRegistry<GatewayPluginEventPublisher>();
  const agentEventOutboxes = new GatewayPluginExtensionRegistry<GatewayPluginOutbox>();
  const deliveryStateStores = new GatewayPluginExtensionRegistry<GatewayPluginDeliveryStateStore>();
  const virtualModelProfiles = new VirtualModelProfileRegistry();

  for (const adapter of createBuiltinSourceAdapters()) {
    sourceAdapters.register(adapter);
  }

  for (const adapter of createBuiltinTargetAdapters()) {
    targetAdapters.register(adapter);
  }

  if (config) {
    syncProviderPluginsFromConfig(providerPlugins, config);
  }

  return {
    sourceAdapters,
    targetAdapters,
    providerPlugins,
    requestHooks,
    requestTransforms,
    routeResolvers,
    responseHooks,
    streamHooks,
    httpRoutes,
    billingEventHooks,
    agentEventHooks,
    billingPublishers,
    billingOutboxes,
    agentEventPublishers,
    agentEventOutboxes,
    deliveryStateStores,
    virtualModelProfiles,
    toolProvider
  };
}

export function listGatewayVirtualModelProfiles(
  config: GatewayConfig,
  runtime?: Pick<GatewayRuntime, 'virtualModelProfiles'>
): VirtualModelProfileConfig[] {
  return [
    ...(config.virtualModelProfiles || []),
    ...(runtime?.virtualModelProfiles.list() || [])
  ];
}

export async function collectGatewayRuntimePluginHealth(
  runtime: Pick<GatewayRuntime, 'billingPublishers' | 'billingOutboxes' | 'agentEventPublishers' | 'agentEventOutboxes' | 'deliveryStateStores'>
): Promise<GatewayRuntimePluginHealthGroup[]> {
  return [
    {
      kind: 'billing_publisher',
      extensions: await collectGatewayPluginExtensionHealth(runtime.billingPublishers.list())
    },
    {
      kind: 'billing_outbox',
      extensions: await collectGatewayPluginExtensionHealth(runtime.billingOutboxes.list())
    },
    {
      kind: 'agent_event_publisher',
      extensions: await collectGatewayPluginExtensionHealth(runtime.agentEventPublishers.list())
    },
    {
      kind: 'agent_event_outbox',
      extensions: await collectGatewayPluginExtensionHealth(runtime.agentEventOutboxes.list())
    },
    {
      kind: 'delivery_state_store',
      extensions: await collectGatewayPluginExtensionHealth(runtime.deliveryStateStores.list())
    }
  ];
}

export function summarizeGatewayRuntimePluginHealth(
  groups: GatewayRuntimePluginHealthGroup[]
): GatewayRuntimePluginHealthSummary {
  let total = 0;
  let degraded = 0;
  let unhealthy = 0;
  for (const group of groups) {
    for (const extension of group.extensions) {
      total += 1;
      if (extension.status === 'degraded') {
        degraded += 1;
      }
      if (extension.status === 'unhealthy') {
        unhealthy += 1;
      }
    }
  }

  return {
    status: unhealthy > 0 ? 'unhealthy' : degraded > 0 ? 'degraded' : 'ok',
    total,
    degraded,
    unhealthy
  };
}
