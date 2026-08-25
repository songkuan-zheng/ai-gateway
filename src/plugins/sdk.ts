import type {
  GatewayConfig,
  GatewayPluginEventHook,
  GatewayPluginConfig,
  GatewayPluginRequestHook,
  GatewayPluginStreamHook,
  GatewayPluginManifest,
  ProviderPlugin,
  ProviderConfig,
  SourceAdapter,
  TargetAdapter,
  VirtualModelProfileConfig
} from '../types';
import type {
  GatewayPluginDeliveryContext,
  GatewayPluginDeliveryOptions,
  GatewayPluginEventPublisher,
  GatewayPluginExtension,
  GatewayPluginHealth,
  GatewayPluginHealthStatus,
  GatewayPluginLifecycleContext,
  GatewayPluginDeadLetter,
  GatewayPluginOutbox
} from './events';

export interface GatewayPluginFactoryInput {
  config: GatewayConfig;
  plugin: GatewayPluginConfig;
}

export interface GatewayPluginModuleResult {
  providers?: ProviderConfig[];
  sourceAdapters?: SourceAdapter[];
  targetAdapters?: TargetAdapter[];
  providerHooks?: ProviderPlugin[];
  providerPlugins?: ProviderPlugin[];
  requestHooks?: GatewayPluginRequestHook[];
  streamHooks?: GatewayPluginStreamHook[];
  billingEventHooks?: GatewayPluginEventHook[];
  eventHooks?: GatewayPluginEventHook[];
  agentEventHooks?: GatewayPluginEventHook[];
  virtualModelProfiles?: VirtualModelProfileConfig[];
  billingPublishers?: GatewayPluginEventPublisher[];
  billingOutboxes?: GatewayPluginOutbox[];
  eventPublishers?: GatewayPluginEventPublisher[];
  eventOutboxes?: GatewayPluginOutbox[];
  agentEventPublishers?: GatewayPluginEventPublisher[];
  agentEventOutboxes?: GatewayPluginOutbox[];
}

export type GatewayPluginFactory = (
  input: GatewayPluginFactoryInput
) => GatewayPluginModuleResult | Promise<GatewayPluginModuleResult>;

export function defineGatewayPlugin(factory: GatewayPluginFactory): GatewayPluginFactory;
export function defineGatewayPlugin(plugin: GatewayPluginModuleResult): GatewayPluginModuleResult;
export function defineGatewayPlugin(
  plugin: GatewayPluginFactory | GatewayPluginModuleResult
): GatewayPluginFactory | GatewayPluginModuleResult {
  return plugin;
}

export function defineGatewayPluginManifest(
  manifest: GatewayPluginManifest
): GatewayPluginManifest {
  return manifest;
}

export type {
  GatewayConfig,
  GatewayPluginDeadLetter,
  GatewayPluginEventHook,
  GatewayPluginConfig,
  GatewayPluginDeliveryContext,
  GatewayPluginDeliveryOptions,
  GatewayPluginEventPublisher,
  GatewayPluginExtension,
  GatewayPluginHealth,
  GatewayPluginHealthStatus,
  GatewayPluginLifecycleContext,
  GatewayPluginManifest,
  GatewayPluginRequestHook,
  GatewayPluginStreamHook,
  GatewayPluginOutbox,
  ProviderConfig,
  ProviderPlugin,
  SourceAdapter,
  TargetAdapter,
  VirtualModelProfileConfig
};
