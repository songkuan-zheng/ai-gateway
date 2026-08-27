import type {
  GatewayConfig,
  GatewayPluginEventHook,
  GatewayPluginConfig,
  GatewayPluginHttpRoute,
  GatewayPluginHttpRouteInput,
  GatewayPluginRequestHook,
  GatewayPluginRequestTransformInput,
  GatewayPluginRequestTransform,
  GatewayPluginRequestTransformValue,
  GatewayPluginResponseHook,
  GatewayPluginResponseHookInput,
  GatewayPluginResponseTransformValue,
  GatewayPluginRouteResolution,
  GatewayPluginRouteResolver,
  GatewayPluginTargetRoute,
  GatewayPluginStreamHook,
  GatewayPluginStreamHookInput,
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
  GatewayPluginOutbox,
  GatewayPluginDeliveryStateStore
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
  requestTransforms?: GatewayPluginRequestTransform[];
  routeResolvers?: GatewayPluginRouteResolver[];
  responseHooks?: GatewayPluginResponseHook[];
  streamHooks?: GatewayPluginStreamHook[];
  httpRoutes?: GatewayPluginHttpRoute[];
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
  deliveryStateStores?: GatewayPluginDeliveryStateStore[];
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
  GatewayPluginDeliveryStateStore,
  GatewayPluginEventHook,
  GatewayPluginConfig,
  GatewayPluginDeliveryContext,
  GatewayPluginDeliveryOptions,
  GatewayPluginEventPublisher,
  GatewayPluginExtension,
  GatewayPluginHealth,
  GatewayPluginHealthStatus,
  GatewayPluginHttpRoute,
  GatewayPluginHttpRouteInput,
  GatewayPluginLifecycleContext,
  GatewayPluginManifest,
  GatewayPluginRequestHook,
  GatewayPluginRequestTransformInput,
  GatewayPluginRequestTransform,
  GatewayPluginRequestTransformValue,
  GatewayPluginResponseHook,
  GatewayPluginResponseHookInput,
  GatewayPluginResponseTransformValue,
  GatewayPluginRouteResolution,
  GatewayPluginRouteResolver,
  GatewayPluginStreamHookInput,
  GatewayPluginStreamHook,
  GatewayPluginTargetRoute,
  GatewayPluginOutbox,
  ProviderConfig,
  ProviderPlugin,
  SourceAdapter,
  TargetAdapter,
  VirtualModelProfileConfig
};
