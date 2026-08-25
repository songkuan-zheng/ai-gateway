import { existsSync, statSync } from 'node:fs';
import { dirname, isAbsolute, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import type {
  GatewayConfig,
  GatewayPluginConfig,
  GatewayPluginEventHook,
  GatewayPluginManifest,
  GatewayPluginRequestHook,
  GatewayPluginStreamHook,
  ProviderConfig,
  ProviderPlugin,
  SourceAdapter,
  TargetAdapter,
  VirtualModelProfileConfig
} from '../types';
import type { GatewayRuntime, VirtualModelProfileRegistry } from '../gateway/runtime';
import { resolveTargetAdapterKeys } from '../adapters/registry';
import {
  closeGatewayPluginExtensions,
  GatewayPluginExtensionRegistry,
  initializeGatewayPluginExtensions,
  type GatewayPluginEventPublisher,
  type GatewayPluginExtension,
  type GatewayPluginOutbox
} from './events';

export interface GatewayPluginLoaderLogger {
  info?: (...args: unknown[]) => void;
  warn?: (...args: unknown[]) => void;
}

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

type GatewayPluginFactory = (
  input: GatewayPluginFactoryInput
) => GatewayPluginModuleResult | Promise<GatewayPluginModuleResult>;

interface GatewayPluginModuleExports {
  createGatewayPlugin?: GatewayPluginFactory;
  default?: GatewayPluginFactory | GatewayPluginModuleResult;
  manifest?: GatewayPluginManifest;
}

interface RegistrySnapshot<T> {
  key: string;
  hadPrevious: boolean;
  previous?: T;
  previousIndex?: number;
}

interface PluginComponentRegistry<T extends { key: string }> {
  get(key: string): T | undefined;
  register(component: T, options?: { overwrite?: boolean }): void;
  unregister(key: string): boolean;
}

interface RegisteredModulePluginState {
  providers: RegistrySnapshot<ProviderConfig>[];
  sourceAdapters: RegistrySnapshot<SourceAdapter>[];
  targetAdapters: RegistrySnapshot<TargetAdapter>[];
  providerPlugins: RegistrySnapshot<ProviderPlugin>[];
  requestHooks: RegistrySnapshot<GatewayPluginRequestHook>[];
  streamHooks: RegistrySnapshot<GatewayPluginStreamHook>[];
  billingEventHooks: RegistrySnapshot<GatewayPluginEventHook>[];
  agentEventHooks: RegistrySnapshot<GatewayPluginEventHook>[];
  virtualModelProfiles: RegistrySnapshot<VirtualModelProfileConfig>[];
  billingPublishers: RegistrySnapshot<GatewayPluginEventPublisher>[];
  billingOutboxes: RegistrySnapshot<GatewayPluginOutbox>[];
  agentEventPublishers: RegistrySnapshot<GatewayPluginEventPublisher>[];
  agentEventOutboxes: RegistrySnapshot<GatewayPluginOutbox>[];
}

interface LoadedGatewayPluginModule {
  plugin: GatewayPluginConfig;
  result: GatewayPluginModuleResult;
}

const registeredModulePluginState = new WeakMap<GatewayRuntime, RegisteredModulePluginState>();

export async function syncGatewayPluginModulesFromConfig(
  runtime: GatewayRuntime,
  config: GatewayConfig,
  logger?: GatewayPluginLoaderLogger
): Promise<void> {
  const loadedModules: LoadedGatewayPluginModule[] = [];
  const initializedModuleExtensions: GatewayPluginExtension[] = [];
  try {
    for (const plugin of config.plugins || []) {
      if (!plugin.enabled || !plugin.modulePath) {
        continue;
      }

      const result = await loadGatewayPluginModule(plugin, config);
      const moduleExtensions = collectGatewayPluginModuleExtensions(result);
      await initializeGatewayPluginExtensions(moduleExtensions, { config, plugin, logger });
      initializedModuleExtensions.push(...moduleExtensions);
      loadedModules.push({
        plugin,
        result
      });
    }
  } catch (error) {
    await closeGatewayPluginExtensions(initializedModuleExtensions);
    throw error;
  }

  await unregisterPreviousModulePlugins(runtime, config);

  const registered: RegisteredModulePluginState = {
    providers: [],
    sourceAdapters: [],
    targetAdapters: [],
    providerPlugins: [],
    requestHooks: [],
    streamHooks: [],
    billingEventHooks: [],
    agentEventHooks: [],
    virtualModelProfiles: [],
    billingPublishers: [],
    billingOutboxes: [],
    agentEventPublishers: [],
    agentEventOutboxes: []
  };

  for (const { plugin, result: moduleResult } of loadedModules) {
    for (const provider of moduleResult.providers || []) {
      registered.providers.push(captureProviderConfigSnapshot(config, provider));
      registerPluginProviderConfig(config, provider);
    }
    for (const sourceAdapter of moduleResult.sourceAdapters || []) {
      registered.sourceAdapters.push(captureSourceAdapterSnapshot(runtime, sourceAdapter));
      runtime.sourceAdapters.register(sourceAdapter, { overwrite: true });
    }
    for (const targetAdapter of moduleResult.targetAdapters || []) {
      for (const key of resolveTargetAdapterKeys(targetAdapter)) {
        registered.targetAdapters.push(captureTargetAdapterSnapshot(runtime, key));
      }
      runtime.targetAdapters.register(targetAdapter, { overwrite: true });
    }
    for (const providerPlugin of [
      ...(moduleResult.providerHooks || []),
      ...(moduleResult.providerPlugins || [])
    ]) {
      registered.providerPlugins.push(captureProviderPluginSnapshot(runtime, providerPlugin));
      runtime.providerPlugins.register(providerPlugin, { overwrite: true });
    }
    for (const requestHook of moduleResult.requestHooks || []) {
      registered.requestHooks.push(captureComponentSnapshot(runtime.requestHooks, requestHook));
      runtime.requestHooks.register(requestHook, { overwrite: true });
    }
    for (const streamHook of moduleResult.streamHooks || []) {
      registered.streamHooks.push(captureComponentSnapshot(runtime.streamHooks, streamHook));
      runtime.streamHooks.register(streamHook, { overwrite: true });
    }
    for (const billingHook of moduleResult.billingEventHooks || []) {
      registered.billingEventHooks.push(captureComponentSnapshot(runtime.billingEventHooks, billingHook));
      runtime.billingEventHooks.register(billingHook, { overwrite: true });
    }
    for (const agentEventHook of [
      ...(moduleResult.eventHooks || []),
      ...(moduleResult.agentEventHooks || [])
    ]) {
      registered.agentEventHooks.push(captureComponentSnapshot(runtime.agentEventHooks, agentEventHook));
      runtime.agentEventHooks.register(agentEventHook, { overwrite: true });
    }
    for (const profile of moduleResult.virtualModelProfiles || []) {
      registered.virtualModelProfiles.push(
        captureVirtualModelProfileSnapshot(runtime.virtualModelProfiles, profile)
      );
      runtime.virtualModelProfiles.register(profile, { overwrite: true });
    }
    for (const billingPublisher of moduleResult.billingPublishers || []) {
      registered.billingPublishers.push(
        captureExtensionSnapshot(runtime.billingPublishers, billingPublisher)
      );
      runtime.billingPublishers.register(billingPublisher, { overwrite: true });
    }
    for (const billingOutbox of moduleResult.billingOutboxes || []) {
      registered.billingOutboxes.push(captureExtensionSnapshot(runtime.billingOutboxes, billingOutbox));
      runtime.billingOutboxes.register(billingOutbox, { overwrite: true });
    }
    for (const eventPublisher of [
      ...(moduleResult.eventPublishers || []),
      ...(moduleResult.agentEventPublishers || [])
    ]) {
      registered.agentEventPublishers.push(
        captureExtensionSnapshot(runtime.agentEventPublishers, eventPublisher)
      );
      runtime.agentEventPublishers.register(eventPublisher, { overwrite: true });
    }
    for (const eventOutbox of [
      ...(moduleResult.eventOutboxes || []),
      ...(moduleResult.agentEventOutboxes || [])
    ]) {
      registered.agentEventOutboxes.push(
        captureExtensionSnapshot(runtime.agentEventOutboxes, eventOutbox)
      );
      runtime.agentEventOutboxes.register(eventOutbox, { overwrite: true });
    }

    logger?.info?.(
      {
        key: plugin.key,
        modulePath: plugin.modulePath,
        sourceAdapters: moduleResult.sourceAdapters?.length || 0,
        targetAdapters: moduleResult.targetAdapters?.length || 0,
        providerHooks: (moduleResult.providerHooks?.length || 0) + (moduleResult.providerPlugins?.length || 0),
        providers: moduleResult.providers?.length || 0,
        requestHooks: moduleResult.requestHooks?.length || 0,
        streamHooks: moduleResult.streamHooks?.length || 0,
        billingEventHooks: moduleResult.billingEventHooks?.length || 0,
        agentEventHooks: (moduleResult.eventHooks?.length || 0) + (moduleResult.agentEventHooks?.length || 0),
        virtualModelProfiles: moduleResult.virtualModelProfiles?.length || 0,
        billingPublishers: moduleResult.billingPublishers?.length || 0,
        billingOutboxes: moduleResult.billingOutboxes?.length || 0,
        eventPublishers:
          (moduleResult.eventPublishers?.length || 0) +
          (moduleResult.agentEventPublishers?.length || 0),
        eventOutboxes:
          (moduleResult.eventOutboxes?.length || 0) +
          (moduleResult.agentEventOutboxes?.length || 0)
      },
      'Loaded gateway plugin module.'
    );
  }

  registeredModulePluginState.set(runtime, registered);
}

async function unregisterPreviousModulePlugins(runtime: GatewayRuntime, config: GatewayConfig): Promise<void> {
  const previous = registeredModulePluginState.get(runtime);
  if (!previous) {
    return;
  }

  await restoreExtensionSnapshots(runtime.agentEventOutboxes, previous.agentEventOutboxes);
  await restoreExtensionSnapshots(runtime.agentEventPublishers, previous.agentEventPublishers);
  await restoreExtensionSnapshots(runtime.billingOutboxes, previous.billingOutboxes);
  await restoreExtensionSnapshots(runtime.billingPublishers, previous.billingPublishers);
  restoreComponentSnapshots(runtime.agentEventHooks, previous.agentEventHooks);
  restoreComponentSnapshots(runtime.billingEventHooks, previous.billingEventHooks);
  restoreComponentSnapshots(runtime.streamHooks, previous.streamHooks);
  restoreComponentSnapshots(runtime.requestHooks, previous.requestHooks);
  restoreProviderPluginSnapshots(runtime, previous.providerPlugins);
  restoreVirtualModelProfileSnapshots(runtime.virtualModelProfiles, previous.virtualModelProfiles);
  restoreTargetAdapterSnapshots(runtime, previous.targetAdapters);
  restoreSourceAdapterSnapshots(runtime, previous.sourceAdapters);
  restoreProviderConfigSnapshots(config, previous.providers);

  registeredModulePluginState.delete(runtime);
}

function captureSourceAdapterSnapshot(
  runtime: GatewayRuntime,
  adapter: SourceAdapter
): RegistrySnapshot<SourceAdapter> {
  const previous = runtime.sourceAdapters.get(adapter.key);
  return {
    key: adapter.key,
    hadPrevious: previous !== undefined,
    previous
  };
}

function captureTargetAdapterSnapshot(
  runtime: GatewayRuntime,
  key: string
): RegistrySnapshot<TargetAdapter> {
  const previous = runtime.targetAdapters.getByKey(key);
  return {
    key,
    hadPrevious: previous !== undefined,
    previous
  };
}

function captureProviderPluginSnapshot(
  runtime: GatewayRuntime,
  plugin: ProviderPlugin
): RegistrySnapshot<ProviderPlugin> {
  const previous = runtime.providerPlugins.get(plugin.key);
  return {
    key: plugin.key,
    hadPrevious: previous !== undefined,
    previous
  };
}

function captureProviderConfigSnapshot(
  config: GatewayConfig,
  provider: ProviderConfig
): RegistrySnapshot<ProviderConfig> {
  const index = config.providers.findIndex((item) => item.name === provider.name);
  const previous = index >= 0 ? config.providers[index] : undefined;
  return {
    key: provider.name,
    hadPrevious: previous !== undefined,
    previous,
    previousIndex: index >= 0 ? index : undefined
  };
}

function captureVirtualModelProfileSnapshot(
  registry: VirtualModelProfileRegistry,
  profile: VirtualModelProfileConfig
): RegistrySnapshot<VirtualModelProfileConfig> {
  const previous = registry.get(profile.key);
  return {
    key: profile.key,
    hadPrevious: previous !== undefined,
    previous
  };
}

function captureComponentSnapshot<T extends { key: string }>(
  registry: PluginComponentRegistry<T>,
  component: T
): RegistrySnapshot<T> {
  const previous = registry.get(component.key);
  return {
    key: component.key,
    hadPrevious: previous !== undefined,
    previous
  };
}

function captureExtensionSnapshot<T extends GatewayPluginExtension>(
  registry: GatewayPluginExtensionRegistry<T>,
  extension: T
): RegistrySnapshot<T> {
  const previous = registry.get(extension.key);
  return {
    key: extension.key,
    hadPrevious: previous !== undefined,
    previous
  };
}

function collectGatewayPluginModuleExtensions(result: GatewayPluginModuleResult): GatewayPluginExtension[] {
  return [
    ...(result.billingPublishers || []),
    ...(result.billingOutboxes || []),
    ...(result.eventPublishers || []),
    ...(result.eventOutboxes || []),
    ...(result.agentEventPublishers || []),
    ...(result.agentEventOutboxes || [])
  ];
}

function registerPluginProviderConfig(config: GatewayConfig, provider: ProviderConfig): void {
  const index = config.providers.findIndex((item) => item.name === provider.name);
  if (index >= 0) {
    config.providers[index] = provider;
    return;
  }

  config.providers.push(provider);
}

function restoreSourceAdapterSnapshots(
  runtime: GatewayRuntime,
  snapshots: RegistrySnapshot<SourceAdapter>[]
): void {
  for (const snapshot of [...snapshots].reverse()) {
    if (snapshot.hadPrevious && snapshot.previous) {
      runtime.sourceAdapters.register(snapshot.previous, { overwrite: true });
    } else {
      runtime.sourceAdapters.unregister(snapshot.key);
    }
  }
}

function restoreTargetAdapterSnapshots(
  runtime: GatewayRuntime,
  snapshots: RegistrySnapshot<TargetAdapter>[]
): void {
  for (const snapshot of [...snapshots].reverse()) {
    if (snapshot.hadPrevious && snapshot.previous) {
      runtime.targetAdapters.register(snapshot.previous, { overwrite: true });
    } else {
      runtime.targetAdapters.unregister(snapshot.key);
    }
  }
}

function restoreProviderPluginSnapshots(
  runtime: GatewayRuntime,
  snapshots: RegistrySnapshot<ProviderPlugin>[]
): void {
  for (const snapshot of [...snapshots].reverse()) {
    if (snapshot.hadPrevious && snapshot.previous) {
      runtime.providerPlugins.register(snapshot.previous, { overwrite: true });
    } else {
      runtime.providerPlugins.unregister(snapshot.key);
    }
  }
}

function restoreProviderConfigSnapshots(
  config: GatewayConfig,
  snapshots: RegistrySnapshot<ProviderConfig>[]
): void {
  for (const snapshot of [...snapshots].reverse()) {
    const currentIndex = config.providers.findIndex((provider) => provider.name === snapshot.key);
    if (snapshot.hadPrevious && snapshot.previous) {
      if (currentIndex >= 0) {
        config.providers[currentIndex] = snapshot.previous;
      } else if (snapshot.previousIndex !== undefined && snapshot.previousIndex >= 0) {
        config.providers.splice(snapshot.previousIndex, 0, snapshot.previous);
      } else {
        config.providers.push(snapshot.previous);
      }
    } else if (currentIndex >= 0) {
      config.providers.splice(currentIndex, 1);
    }
  }
}

function restoreComponentSnapshots<T extends { key: string }>(
  registry: PluginComponentRegistry<T>,
  snapshots: RegistrySnapshot<T>[]
): void {
  for (const snapshot of [...snapshots].reverse()) {
    if (snapshot.hadPrevious && snapshot.previous) {
      registry.register(snapshot.previous, { overwrite: true });
    } else {
      registry.unregister(snapshot.key);
    }
  }
}

function restoreVirtualModelProfileSnapshots(
  registry: VirtualModelProfileRegistry,
  snapshots: RegistrySnapshot<VirtualModelProfileConfig>[]
): void {
  for (const snapshot of [...snapshots].reverse()) {
    if (snapshot.hadPrevious && snapshot.previous) {
      registry.register(snapshot.previous, { overwrite: true });
    } else {
      registry.unregister(snapshot.key);
    }
  }
}

async function restoreExtensionSnapshots<T extends GatewayPluginExtension>(
  registry: GatewayPluginExtensionRegistry<T>,
  snapshots: RegistrySnapshot<T>[]
): Promise<void> {
  for (const snapshot of [...snapshots].reverse()) {
    const current = registry.get(snapshot.key);
    if (current && current !== snapshot.previous) {
      await closeGatewayPluginExtensions([current]);
    }

    if (snapshot.hadPrevious && snapshot.previous) {
      registry.register(snapshot.previous, { overwrite: true });
    } else {
      registry.unregister(snapshot.key);
    }
  }
}

async function loadGatewayPluginModule(
  plugin: GatewayPluginConfig,
  config: GatewayConfig
): Promise<GatewayPluginModuleResult> {
  const modulePath = resolveGatewayPluginModulePath(plugin.modulePath);
  if (!existsSync(modulePath)) {
    throw new Error(`Gateway plugin "${plugin.key}" modulePath does not exist: ${plugin.modulePath}`);
  }

  const moduleUrl = pathToFileURL(modulePath);
  moduleUrl.searchParams.set('gatewayPlugin', plugin.key);
  moduleUrl.searchParams.set('mtime', resolveGatewayPluginModuleCacheKey(plugin, modulePath));

  const exports = (await import(moduleUrl.href)) as GatewayPluginModuleExports;
  const manifest = normalizeGatewayPluginManifest(plugin, exports.manifest || plugin.manifest);
  const factory = exports.createGatewayPlugin || exports.default;
  if (!factory) {
    throw new Error(
      `Gateway plugin "${plugin.key}" must export createGatewayPlugin() or a default plugin factory/object.`
    );
  }

  const result =
    typeof factory === 'function'
      ? await factory({ config, plugin })
      : factory;
  const normalized = normalizeGatewayPluginModuleResult(plugin, result);
  validateGatewayPluginModuleResult(plugin, manifest, normalized);
  return normalized;
}

function resolveGatewayPluginModulePath(modulePath: string | undefined): string {
  const normalized = modulePath?.trim();
  if (!normalized) {
    throw new Error('Gateway plugin modulePath is required.');
  }

  return isAbsolute(normalized) ? normalized : resolve(process.cwd(), normalized);
}

function resolveGatewayPluginModuleCacheKey(plugin: GatewayPluginConfig, modulePath: string): string {
  const watchedFiles = new Set<string>([modulePath]);
  const baseDir = dirname(modulePath);
  for (const filePath of [...(plugin.watchFiles || []), ...(plugin.manifest?.files || [])]) {
    const normalized = filePath.trim();
    if (!normalized) {
      continue;
    }

    watchedFiles.add(isAbsolute(normalized) ? normalized : resolve(baseDir, normalized));
  }

  return Array.from(watchedFiles)
    .sort()
    .map((filePath) => {
      try {
        const stat = statSync(filePath);
        return `${filePath}:${stat.mtimeMs}:${stat.size}`;
      } catch {
        return `${filePath}:missing`;
      }
    })
    .join('|');
}

function normalizeGatewayPluginModuleResult(
  plugin: GatewayPluginConfig,
  result: GatewayPluginModuleResult | undefined
): GatewayPluginModuleResult {
  if (!result || typeof result !== 'object') {
    throw new Error(`Gateway plugin "${plugin.key}" factory must return a plugin object.`);
  }

  return {
    providers: normalizeArray<ProviderConfig>(result.providers),
    sourceAdapters: normalizeArray<SourceAdapter>(result.sourceAdapters),
    targetAdapters: normalizeArray<TargetAdapter>(result.targetAdapters),
    providerHooks: normalizeArray<ProviderPlugin>(result.providerHooks),
    providerPlugins: normalizeArray<ProviderPlugin>(result.providerPlugins),
    requestHooks: normalizeArray<GatewayPluginRequestHook>(result.requestHooks),
    streamHooks: normalizeArray<GatewayPluginStreamHook>(result.streamHooks),
    billingEventHooks: normalizeArray<GatewayPluginEventHook>(result.billingEventHooks),
    eventHooks: normalizeArray<GatewayPluginEventHook>(result.eventHooks),
    agentEventHooks: normalizeArray<GatewayPluginEventHook>(result.agentEventHooks),
    virtualModelProfiles: normalizeArray<VirtualModelProfileConfig>(result.virtualModelProfiles),
    billingPublishers: normalizeArray<GatewayPluginEventPublisher>(result.billingPublishers),
    billingOutboxes: normalizeArray<GatewayPluginOutbox>(result.billingOutboxes),
    eventPublishers: normalizeArray<GatewayPluginEventPublisher>(result.eventPublishers),
    eventOutboxes: normalizeArray<GatewayPluginOutbox>(result.eventOutboxes),
    agentEventPublishers: normalizeArray<GatewayPluginEventPublisher>(result.agentEventPublishers),
    agentEventOutboxes: normalizeArray<GatewayPluginOutbox>(result.agentEventOutboxes)
  };
}

function normalizeGatewayPluginManifest(
  plugin: GatewayPluginConfig,
  manifest: GatewayPluginManifest | undefined
): GatewayPluginManifest | undefined {
  if (!manifest) {
    return undefined;
  }

  if (typeof manifest.name !== 'string' || manifest.name.trim().length === 0) {
    throw new Error(`Gateway plugin "${plugin.key}" manifest.name must be a non-empty string.`);
  }

  if (
    manifest.capabilities !== undefined &&
    (!Array.isArray(manifest.capabilities) ||
      manifest.capabilities.some((capability) => typeof capability !== 'string' || capability.trim().length === 0))
  ) {
    throw new Error(`Gateway plugin "${plugin.key}" manifest.capabilities must be a string array.`);
  }

  if (
    manifest.files !== undefined &&
    (!Array.isArray(manifest.files) ||
      manifest.files.some((filePath) => typeof filePath !== 'string' || filePath.trim().length === 0))
  ) {
    throw new Error(`Gateway plugin "${plugin.key}" manifest.files must be a string array.`);
  }

  return manifest;
}

function validateGatewayPluginModuleResult(
  plugin: GatewayPluginConfig,
  manifest: GatewayPluginManifest | undefined,
  result: GatewayPluginModuleResult
): void {
  validateUniqueKeys(plugin, 'sourceAdapters', result.sourceAdapters);
  validateUniqueKeys(plugin, 'providerHooks', result.providerHooks);
  validateUniqueKeys(plugin, 'providerPlugins', result.providerPlugins);
  validateUniqueKeys(plugin, 'requestHooks', result.requestHooks);
  validateUniqueKeys(plugin, 'streamHooks', result.streamHooks);
  validateUniqueKeys(plugin, 'billingEventHooks', result.billingEventHooks);
  validateUniqueKeys(plugin, 'eventHooks', result.eventHooks);
  validateUniqueKeys(plugin, 'agentEventHooks', result.agentEventHooks);
  validateUniqueKeys(plugin, 'virtualModelProfiles', result.virtualModelProfiles);
  validateUniqueKeys(plugin, 'billingPublishers', result.billingPublishers);
  validateUniqueKeys(plugin, 'billingOutboxes', result.billingOutboxes);
  validateUniqueKeys(plugin, 'eventPublishers', result.eventPublishers);
  validateUniqueKeys(plugin, 'eventOutboxes', result.eventOutboxes);
  validateUniqueKeys(plugin, 'agentEventPublishers', result.agentEventPublishers);
  validateUniqueKeys(plugin, 'agentEventOutboxes', result.agentEventOutboxes);

  for (const provider of result.providers || []) {
    if (!isNonEmptyString(provider.name) || !isNonEmptyString(provider.type)) {
      throw new Error(`Gateway plugin "${plugin.key}" providers[] must include name and type.`);
    }
    if (!Array.isArray(provider.models)) {
      throw new Error(`Gateway plugin "${plugin.key}" provider "${provider.name}" must include models array.`);
    }
  }

  for (const adapter of result.sourceAdapters || []) {
    if (!isNonEmptyString(adapter.key)) {
      throw new Error(`Gateway plugin "${plugin.key}" sourceAdapters[] must include key.`);
    }
    if (!isNonEmptyString(adapter.provider)) {
      throw new Error(`Gateway plugin "${plugin.key}" source adapter "${adapter.key}" must include provider.`);
    }
    for (const methodName of ['toStandardRequest', 'fromStandardResponse', 'isStreamingRequest', 'buildPassthroughRequest']) {
      if (typeof (adapter as unknown as Record<string, unknown>)[methodName] !== 'function') {
        throw new Error(`Gateway plugin "${plugin.key}" source adapter "${adapter.key}" is missing ${methodName}().`);
      }
    }
  }

  for (const adapter of result.targetAdapters || []) {
    const keys = resolveTargetAdapterKeys(adapter);
    if (keys.length === 0) {
      throw new Error(`Gateway plugin "${plugin.key}" targetAdapters[] must expose at least one key.`);
    }
    if (!isNonEmptyString(adapter.provider)) {
      throw new Error(`Gateway plugin "${plugin.key}" target adapter "${keys[0]}" must include provider.`);
    }
    for (const methodName of ['buildRequestFromStandard', 'toStandardResponse']) {
      if (typeof (adapter as unknown as Record<string, unknown>)[methodName] !== 'function') {
        throw new Error(`Gateway plugin "${plugin.key}" target adapter "${keys[0]}" is missing ${methodName}().`);
      }
    }
  }

  for (const providerPlugin of [
    ...(result.providerHooks || []),
    ...(result.providerPlugins || [])
  ]) {
    if (!isNonEmptyString(providerPlugin.key)) {
      throw new Error(`Gateway plugin "${plugin.key}" provider hook must include key.`);
    }
    if (
      typeof providerPlugin.authenticate !== 'function' &&
      typeof providerPlugin.transformRequest !== 'function' &&
      typeof providerPlugin.transformResponse !== 'function'
    ) {
      throw new Error(`Gateway plugin "${plugin.key}" provider hook "${providerPlugin.key}" must include at least one hook method.`);
    }
  }

  for (const profile of result.virtualModelProfiles || []) {
    if (!isNonEmptyString(profile.key) || !isNonEmptyString(profile.id) || !isNonEmptyString(profile.displayName)) {
      throw new Error(`Gateway plugin "${plugin.key}" virtualModelProfiles[] must include id, key, and displayName.`);
    }
  }

  validateEventExtensions(plugin, 'billingPublishers', result.billingPublishers, 'publish');
  validateEventExtensions(plugin, 'billingOutboxes', result.billingOutboxes, 'append');
  validateEventExtensions(plugin, 'eventPublishers', result.eventPublishers, 'publish');
  validateEventExtensions(plugin, 'eventOutboxes', result.eventOutboxes, 'append');
  validateEventExtensions(plugin, 'agentEventPublishers', result.agentEventPublishers, 'publish');
  validateEventExtensions(plugin, 'agentEventOutboxes', result.agentEventOutboxes, 'append');
  validateRequestHooks(plugin, result.requestHooks);
  validateStreamHooks(plugin, result.streamHooks);
  validateEventHooks(plugin, 'billingEventHooks', result.billingEventHooks);
  validateEventHooks(plugin, 'eventHooks', result.eventHooks);
  validateEventHooks(plugin, 'agentEventHooks', result.agentEventHooks);

  if (manifest?.capabilities && manifest.capabilities.length > 0) {
    const actualCapabilities = collectGatewayPluginResultCapabilities(result);
    const missingCapabilities = manifest.capabilities.filter((capability) => !actualCapabilities.has(capability));
    if (missingCapabilities.length > 0) {
      throw new Error(
        `Gateway plugin "${plugin.key}" manifest declares capabilities not returned by module: ${missingCapabilities.join(', ')}.`
      );
    }
  }
}

function validateEventExtensions<T extends GatewayPluginExtension>(
  plugin: GatewayPluginConfig,
  section: string,
  extensions: T[] | undefined,
  methodName: 'publish' | 'append'
): void {
  for (const extension of extensions || []) {
    if (!isNonEmptyString(extension.key)) {
      throw new Error(`Gateway plugin "${plugin.key}" ${section}[] must include key.`);
    }
    if (typeof (extension as unknown as Record<string, unknown>)[methodName] !== 'function') {
      throw new Error(`Gateway plugin "${plugin.key}" ${section} "${extension.key}" is missing ${methodName}().`);
    }
  }
}

function validateRequestHooks(
  plugin: GatewayPluginConfig,
  hooks: GatewayPluginRequestHook[] | undefined
): void {
  for (const hook of hooks || []) {
    if (!isNonEmptyString(hook.key)) {
      throw new Error(`Gateway plugin "${plugin.key}" requestHooks[] must include key.`);
    }
    if (
      typeof hook.beforeAuth !== 'function' &&
      typeof hook.beforeRouting !== 'function' &&
      typeof hook.beforePrecheck !== 'function' &&
      typeof hook.afterPrecheck !== 'function'
    ) {
      throw new Error(`Gateway plugin "${plugin.key}" request hook "${hook.key}" must include at least one hook method.`);
    }
  }
}

function validateStreamHooks(
  plugin: GatewayPluginConfig,
  hooks: GatewayPluginStreamHook[] | undefined
): void {
  for (const hook of hooks || []) {
    if (!isNonEmptyString(hook.key)) {
      throw new Error(`Gateway plugin "${plugin.key}" streamHooks[] must include key.`);
    }
    if (typeof hook.transformResponse !== 'function') {
      throw new Error(`Gateway plugin "${plugin.key}" stream hook "${hook.key}" must include transformResponse().`);
    }
  }
}

function validateEventHooks(
  plugin: GatewayPluginConfig,
  section: string,
  hooks: GatewayPluginEventHook[] | undefined
): void {
  for (const hook of hooks || []) {
    if (!isNonEmptyString(hook.key)) {
      throw new Error(`Gateway plugin "${plugin.key}" ${section}[] must include key.`);
    }
    if (typeof hook.transform !== 'function') {
      throw new Error(`Gateway plugin "${plugin.key}" ${section} "${hook.key}" must include transform().`);
    }
  }
}

function validateUniqueKeys(
  plugin: GatewayPluginConfig,
  section: string,
  items: Array<{ key?: string }> | undefined
): void {
  const seen = new Set<string>();
  for (const item of items || []) {
    if (!item?.key) {
      continue;
    }
    if (seen.has(item.key)) {
      throw new Error(`Gateway plugin "${plugin.key}" ${section} contains duplicate key: ${item.key}`);
    }
    seen.add(item.key);
  }
}

function collectGatewayPluginResultCapabilities(result: GatewayPluginModuleResult): Set<string> {
  const capabilities = new Set<string>();
  if ((result.providerHooks?.length || 0) + (result.providerPlugins?.length || 0) > 0) {
    capabilities.add('providerHooks');
    capabilities.add('providerPlugins');
  }
  for (const [key, value] of Object.entries(result)) {
    if (Array.isArray(value) && value.length > 0) {
      capabilities.add(key);
    }
  }
  return capabilities;
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

function normalizeArray<T>(value: unknown): T[] {
  return Array.isArray(value) ? (value.filter(Boolean) as T[]) : [];
}
