# Gateway Plugins

Gateway plugins provide one unified configuration surface for extending the gateway.
They replace the need to think about separate "provider plugin" and "protocol adapter"
configuration formats.

The single top-level field is `plugins`.

```json
{
  "plugins": [
    {
      "key": "openai-main-patch",
      "enabled": true,
      "match": {
        "provider": "openai",
        "providerName": "openai-main"
      },
      "providerHooks": {
        "request": {
          "headers": {
            "x-custom-feature": "enabled"
          },
          "bodySet": {
            "metadata.gateway": "next-ai"
          }
        }
      }
    },
    {
      "key": "acme",
      "enabled": true,
      "modulePath": "./plugins/acme/index.mjs",
      "config": {
        "brokers": ["kafka-1:9092"],
        "billingTopic": "gateway-billing-events"
      }
    }
  ]
}
```

`providerPlugins` is still supported for backward compatibility. New configurations
should prefer `plugins`.

## Capabilities

A plugin can provide one or more capabilities.

| Capability | Purpose | Use when |
| --- | --- | --- |
| `providers` | Register complete provider configs from a module package. | A plugin ships both the upstream protocol/client defaults and one or more provider entries. |
| `providerHooks` | Patch an existing provider request or response flow. | The upstream is mostly OpenAI, Anthropic, or Gemini compatible, but needs different headers, auth, query params, or small body/response changes. |
| `requestHooks` | Run code before auth, routing, or precheck decisions. | A plugin needs tenant policy, custom auth gates, request enrichment, or quota vetoes before provider execution. |
| `streamHooks` | Transform an upstream streaming `Response` before it is relayed. | A plugin needs to wrap, inspect, or replace live streaming output without buffering the whole response. |
| `targetAdapters` | Define a complete upstream protocol. | The upstream request or response format is not compatible with the built-in protocols. |
| `sourceAdapters` | Define a client-facing request protocol. | Clients send a custom inbound request format to the gateway. |
| `virtualModelProfiles` | Register virtual model aliases and tool-loop profiles. | A plugin needs to ship model aliases or internal-tool behavior with its adapter/hook package. |
| `billingEventHooks` | Transform or drop billing events before plugin outboxes/publishers receive them. | Billing events need plugin-owned enrichment, masking, or routing metadata. |
| `eventHooks` / `agentEventHooks` | Transform or drop agent event envelopes before plugin outboxes/publishers receive them. | Agent events need plugin-owned enrichment, masking, or routing metadata. |
| `billingOutboxes` | Append billing events to a durable plugin-managed outbox. | Billing must be accepted into Postgres, Kafka, or another reliable buffer before downstream delivery. |
| `billingPublishers` | Publish billing events through a plugin-managed transport. | Billing can be sent directly to Kafka, Pulsar, NATS, or another external sink. |
| `eventOutboxes` / `agentEventOutboxes` | Append agent event envelopes to a durable plugin-managed outbox. | Agent events need a reliable external buffer. |
| `eventPublishers` / `agentEventPublishers` | Publish agent event envelopes through a plugin-managed transport. | Agent events can be sent directly to an external event stream. |

In other words:

- Hook capability modifies an already-built request or already-read response payload.
- Provider package capability registers provider entries together with plugin code.
- Request/stream/event hooks run at gateway lifecycle points outside the provider adapter.
- Adapter capability defines the protocol itself.
- Outbox capability accepts events into durable plugin-managed storage.
- Publisher capability sends events directly to an external transport.

## Provider Hooks

Provider hooks are inline, declarative rules under `plugins[].providerHooks`.

```json
{
  "plugins": [
    {
      "key": "vendor-header-patch",
      "enabled": true,
      "match": {
        "providerName": "vendor-main"
      },
      "providerHooks": {
        "auth": {
          "headers": {
            "authorization": "Bearer {{ env.VENDOR_TOKEN }}"
          }
        },
        "request": {
          "query": {
            "api-version": "2026-01-01"
          },
          "bodyRemove": ["unsupported_field"],
          "bodySet": {
            "stream_options.include_usage": true
          }
        },
        "response": {
          "bodySet": {
            "usage.total_tokens": "{{ upstreamPayload.token_count }}"
          }
        }
      }
    }
  ]
}
```

`providerHooks` accepts either one object or an array of objects. The plugin-level
`match` applies as the default match for each hook. A hook can override that default
with its own `provider`, `providerName`, `models`, `sourceAdapters`, or
`sourceRoutes`.

Supported declarative fields:

- `auth.headers`
- `auth.query`
- `auth.bodySet`
- `auth.bodyMerge`
- `auth.bodyRemove`
- `request.headers`
- `request.query`
- `request.bodySet`
- `request.bodyMerge`
- `request.bodyRemove`
- `response.bodySet`
- `response.bodyMerge`
- `response.bodyRemove`
- `codexOauth`
- `deepseekThinking`

Supported value references include:

- `{{ env.NAME }}`
- `{{ request.headers.x-header }}`
- `{{ request.body.user.id }}`
- `{{ upstreamRequest.body.model }}`
- `{{ upstreamPayload.data.id }}`
- `{{ target.provider }}`
- `{{ target.providerName }}`
- `{{ model }}`
- `{{ source.route }}`
- `{{ source.metadata.NAME }}`

A string that is exactly one reference preserves the referenced value type. A string
with embedded references interpolates them:

```json
{
  "request": {
    "headers": {
      "authorization": "Bearer {{ env.VENDOR_TOKEN }}",
      "x-user": "user={{ request.headers.x-auth-user-id }}"
    }
  }
}
```

Hooks can also declare a `when` condition:

```json
{
  "providerHooks": {
    "models": ["gpt-*"],
    "sourceAdapters": ["openai_responses"],
    "when": {
      "from": "request.headers.x-enable-plugin",
      "equals": "yes"
    },
    "request": {
      "headers": {
        "x-feature": "enabled"
      }
    }
  }
}
```

`when` supports `from`, `exists`, `equals`, `notEquals`, `includes`, `matches`,
`all`, `any`, and `not`.

Strict mode can be enabled on a hook:

```json
{
  "providerHooks": {
    "request": {
      "strict": true,
      "headers": {
        "x-user-id": "{{ request.headers.x-auth-user-id }}"
      }
    }
  }
}
```

With `strict: true`, a missing reference fails the provider attempt instead of being
silently skipped.

## Module Plugins

Use `modulePath` when a plugin needs code. Module plugins are local, trusted Node.js
modules loaded by the gateway process.

```json
{
  "plugins": [
    {
      "key": "acme",
      "enabled": true,
      "modulePath": "./plugins/acme/index.mjs",
      "watchFiles": ["./schema.json"],
      "manifest": {
        "name": "acme",
        "version": "1.0.0",
        "capabilities": ["targetAdapters"],
        "files": ["./schema.json"]
      }
    }
  ]
}
```

The module must export `createGatewayPlugin()` or a default factory/object. It can
also export `manifest`.

```js
import { defineGatewayPlugin, defineGatewayPluginManifest } from '@the-next-ai/ai-gateway/plugins/sdk';

export const manifest = defineGatewayPluginManifest({
  name: 'acme',
  version: '1.0.0',
  capabilities: ['providers', 'targetAdapters', 'requestHooks']
});

export const createGatewayPlugin = defineGatewayPlugin(({ config, plugin }) => {
  const pluginConfig = plugin.config || {};

  return {
    targetAdapters: [acmeMessagesTargetAdapter],
    sourceAdapters: [],
    providerHooks: [],
    requestHooks: [],
    streamHooks: []
  };
});
```

`plugins[].config` is passed through unchanged to `createGatewayPlugin()`.
Unknown top-level fields on a plugin entry are also copied into `plugin.config` for
backward-compatible private plugin settings.
`watchFiles` and `manifest.files` are included in the module cache key, so a reload
re-executes the plugin factory when those files change. This is intended for schemas,
templates, and other files read by the factory.

The returned object supports:

```ts
interface GatewayPluginModuleResult {
  providers?: ProviderConfig[];
  sourceAdapters?: SourceAdapter[];
  targetAdapters?: TargetAdapter[];
  providerHooks?: ProviderPlugin[];
  providerPlugins?: ProviderPlugin[];
  requestHooks?: GatewayPluginRequestHook[];
  streamHooks?: GatewayPluginStreamHook[];
  billingEventHooks?: GatewayPluginEventHook<BillingQueueEvent>[];
  eventHooks?: GatewayPluginEventHook<AgentQueueEvent>[];
  agentEventHooks?: GatewayPluginEventHook<AgentQueueEvent>[];
  virtualModelProfiles?: VirtualModelProfileConfig[];
  billingPublishers?: GatewayPluginEventPublisher<BillingQueueEvent>[];
  billingOutboxes?: GatewayPluginOutbox<BillingQueueEvent>[];
  eventPublishers?: GatewayPluginEventPublisher<AgentQueueEvent>[];
  eventOutboxes?: GatewayPluginOutbox<AgentQueueEvent>[];
  agentEventPublishers?: GatewayPluginEventPublisher<AgentQueueEvent>[];
  agentEventOutboxes?: GatewayPluginOutbox<AgentQueueEvent>[];
}
```

`providerPlugins` is accepted as an alias for `providerHooks` for code modules.
`eventPublishers` and `eventOutboxes` are aliases for agent event publishers and
outboxes.
Module return values are validated before the old module state is unregistered; a bad
adapter, hook, extension, or manifest keeps the previous runtime state active.

## Request, Stream, And Event Hooks

Module hooks run inside the gateway process and are registered by key. All hook
executions are recorded in metrics, including matched, skipped, success, blocked, and
error outcomes.

```ts
interface GatewayPluginRequestHook {
  key: string;
  provider?: string;
  providerName?: string;
  models?: string[];
  sourceAdapters?: string[];
  sourceRoutes?: string[];
  beforeAuth?(input: GatewayPluginRequestHookInput): GatewayPluginHookResult | void | Promise<GatewayPluginHookResult | void>;
  beforeRouting?(input: GatewayPluginRequestHookInput): GatewayPluginHookResult | void | Promise<GatewayPluginHookResult | void>;
  beforePrecheck?(input: GatewayPluginRequestHookInput): GatewayPluginHookResult<GatewayPluginPrecheckDecision | void> | void | Promise<GatewayPluginHookResult<GatewayPluginPrecheckDecision | void> | void>;
  afterPrecheck?(input: GatewayPluginRequestHookInput & { result: unknown }): GatewayPluginHookResult | void | Promise<GatewayPluginHookResult | void>;
}

interface GatewayPluginStreamHook {
  key: string;
  provider?: string;
  providerName?: string;
  models?: string[];
  sourceAdapters?: string[];
  sourceRoutes?: string[];
  transformResponse(input: GatewayPluginStreamHookInput): Response | GatewayPluginHookResult<Response> | Promise<Response | GatewayPluginHookResult<Response>>;
}

interface GatewayPluginEventHook<TEvent> {
  key: string;
  transform(input: GatewayPluginEventHookInput<TEvent>): TEvent | false | GatewayPluginHookResult<TEvent | false> | Promise<TEvent | false | GatewayPluginHookResult<TEvent | false>>;
}
```

Request hook stages:

- `beforeAuth`: runs before gateway auth on gateway write routes.
- `beforeRouting`: runs after the request body/model is parsed and before target provider resolution.
- `beforePrecheck`: can return `{ allow: false, statusCode, message, details }` to block before quota/precheck evaluation.
- `afterPrecheck`: observes the computed precheck result and can fail the request with `{ ok: false, status, error, details }`.

Stream hooks run on streaming upstream responses before relay. They receive the upstream
request, upstream response, selected provider, standard request, and route/source
metadata. Return a replacement `Response` to wrap or transform the stream.

Event hooks run before plugin event outboxes and publishers. Return an event object to
replace the event, return `false` to intentionally drop it, or return `{ ok: false,
error }` to fail delivery.

## Billing And Event Extensions

Billing and agent event extensions run inside the gateway process. They can use any
client library available to the plugin module, such as Kafka, Postgres, Pulsar, or
NATS clients.

```ts
interface GatewayPluginEventPublisher<TEvent> {
  key: string;
  transport?: string;
  delivery?: GatewayPluginDeliveryOptions;
  init?(context: GatewayPluginLifecycleContext): void | Promise<void>;
  ready?(): boolean | Promise<boolean>;
  health?(): GatewayPluginHealth | Promise<GatewayPluginHealth>;
  flush?(): void | Promise<void>;
  drain?(): void | Promise<void>;
  publish(event: TEvent, context?: GatewayPluginDeliveryContext): boolean | void | Promise<boolean | void>;
  close?(): void | Promise<void>;
}

interface GatewayPluginOutbox<TEvent> {
  key: string;
  transport?: string;
  delivery?: GatewayPluginDeliveryOptions;
  init?(context: GatewayPluginLifecycleContext): void | Promise<void>;
  ready?(): boolean | Promise<boolean>;
  health?(): GatewayPluginHealth | Promise<GatewayPluginHealth>;
  flush?(): void | Promise<void>;
  drain?(): void | Promise<void>;
  append(event: TEvent, context?: GatewayPluginDeliveryContext): boolean | void | Promise<boolean | void>;
  close?(): void | Promise<void>;
}

interface GatewayPluginDeliveryOptions {
  timeoutMs?: number;
  maxAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  backoffMultiplier?: number;
  concurrency?: number;
  maxQueueSize?: number;
  dedupe?: boolean;
  dedupeTtlMs?: number;
  deadLetter?: boolean | GatewayPluginDeadLetterOptions;
}

interface GatewayPluginDeliveryContext {
  signal: AbortSignal;
  attempt: number;
  maxAttempts: number;
  eventId?: string;
  idempotencyKey?: string;
}

interface GatewayPluginDeadLetterOptions {
  enabled?: boolean;
  maxEntries?: number;
}
```

Return `false` from `publish()` or `append()` when the event was intentionally not
accepted. Throw an error when delivery failed and should be counted as a failed
attempt. `close()` is called when publishers are reloaded or the gateway shuts down.
`drain()` and `flush()` are called before `close()`. Delivery options apply to plugin
outbox/publisher calls and provide timeout, retry, per-extension concurrency limiting,
and bounded queue backpressure.
When `timeoutMs` fires, `context.signal` is aborted. Plugins that call external
systems should pass that signal to their client library when possible.
When `dedupe` is enabled, successful deliveries are remembered by
`extension.key:eventId` for `dedupeTtlMs`. Repeated events are reported as
`not_delivered` and the plugin operation is not called.
When `deadLetter` is enabled, failed event deliveries are retained in the in-process
dead-letter store after retry exhaustion. Extensions can also implement
`deadLetter(entry)` to forward failed events to plugin-owned storage.
Billing event publishing keeps the existing asynchronous request path: an outbox
append is durable after the plugin accepts it, but the gateway response is not blocked
until that append completes.

Example Kafka-style module:

```js
export function createGatewayPlugin({ plugin }) {
  const kafka = createKafkaClient(plugin.config);

  return {
    billingOutboxes: [
      {
        key: 'billing-kafka-outbox',
        transport: 'kafka',
        delivery: {
          maxAttempts: 3,
          baseDelayMs: 100,
          maxDelayMs: 2000,
          timeoutMs: 5000,
          concurrency: 8,
          maxQueueSize: 1000
        },
        async init() {
          await kafka.connect();
        },
        async ready() {
          return kafka.isConnected();
        },
        async health() {
          return {
            status: kafka.isConnected() ? 'healthy' : 'unhealthy'
          };
        },
        async append(event) {
          await kafka.producer().send({
            topic: plugin.config.billingTopic,
            messages: [
              {
                key: event.eventId,
                value: JSON.stringify(event)
              }
            ]
          });
          return true;
        },
        async flush() {
          await kafka.producer().flush();
        },
        async close() {
          await kafka.disconnect();
        }
      }
    ],
    eventPublishers: [
      {
        key: 'agent-event-kafka',
        transport: 'kafka',
        async publish(event) {
          await kafka.producer().send({
            topic: 'gateway-agent-events',
            messages: [
              {
                key: event.eventId,
                value: JSON.stringify(event)
              }
            ]
          });
          return true;
        }
      }
    ]
  };
}
```

For billing, outbox and publisher attempts are recorded in
`gateway_billing_events_total` with transport labels such as
`plugin:outbox:kafka` or `plugin:publisher:kafka`.
All plugin extension deliveries are also recorded in
`gateway_plugin_deliveries_total`.

## Target Adapters

A target adapter defines an upstream protocol. It converts the gateway's
`StandardRequest` into an upstream request, and converts the upstream response payload
back into `StandardResponse`.

```js
export const acmeMessagesTargetAdapter = {
  key: 'acme_messages',
  provider: 'acme',
  providerTypes: ['acme_messages'],

  buildRequestFromStandard(input) {
    return {
      ok: true,
      value: {
        method: 'POST',
        url: `${input.targetProviderConfig.baseurl}/messages`,
        headers: {
          'content-type': 'application/json',
          authorization: `Bearer ${input.targetProviderConfig.apikey}`
        },
        body: {
          model: input.standardRequest.model,
          prompt: input.standardRequest.input
        }
      }
    };
  },

  toStandardResponse(payload) {
    return {
      ok: true,
      value: {
        id: payload.id || 'resp_acme',
        object: 'response',
        status: 'completed',
        model: payload.model || 'unknown',
        output_text: payload.text || '',
        output: [
          {
            id: 'msg_acme',
            type: 'message',
            role: 'assistant',
            status: 'completed',
            content: [
              {
                type: 'output_text',
                text: payload.text || '',
                annotations: []
              }
            ]
          }
        ],
        usage: payload.usage || {}
      }
    };
  }
};
```

Then configure a provider that uses the adapter's provider type:

```json
{
  "providers": [
    {
      "name": "acme-main",
      "type": "acme_messages",
      "baseurl": "https://api.acme.example",
      "apikey": "secret",
      "models": ["acme-large"]
    }
  ],
  "plugins": [
    {
      "key": "acme",
      "enabled": true,
      "modulePath": "./plugins/acme/index.mjs"
    }
  ],
  "defaultTargetProvider": "acme"
}
```

Provider type conventions:

- Built-in provider types remain supported, such as `openai_responses`,
  `anthropic_messages`, and `gemini_generate_content`.
- Custom provider types are accepted as safe lowercase tokens.
- The provider group is inferred from the part before the first underscore. For
  example, `acme_messages` maps to provider `acme`, and `my-provider_messages`
  maps to provider `my-provider`.
- For custom protocols, route by provider name (`x-target-provider: acme-main`) or
  provider group (`x-target-provider: acme`).

## Upstream Request Shape

Adapters return an `UpstreamRequest`.

```ts
interface UpstreamRequest {
  method?: string;
  url: string;
  headers: Record<string, string>;
  body: unknown;
  bodyEncoding?: 'json' | 'text' | 'form' | 'bytes' | 'none';
}
```

Defaults:

- `method` defaults to `POST`.
- `bodyEncoding` defaults to `json`.

`bodyEncoding` controls how the body is sent:

- `json`: `JSON.stringify(body)`
- `text`: send a string body, or JSON stringify non-strings
- `form`: send `URLSearchParams`
- `bytes`: send the body as a Fetch `BodyInit`
- `none`: send no request body

## Source Adapters

Most plugins only need target adapters because the gateway already accepts OpenAI,
Anthropic, and Gemini-style client requests.

Use a source adapter only when clients send a custom inbound protocol.

```ts
interface SourceAdapter {
  key: string;
  provider: Provider;
  routes?: SourceAdapterRoute[];
  toStandardRequest(input: SourceAdapterRequestInput): Result<StandardRequest>;
  fromStandardResponse(input: SourceAdapterResponseInput): unknown;
  isStreamingRequest(input: SourceAdapterRequestInput): boolean;
  buildPassthroughRequest(input: SourceAdapterRequestInput): Result<UpstreamRequest>;
}

interface SourceAdapterRoute {
  method?: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';
  path: string;
  metadata?: Record<string, string>;
}
```

Registering a source adapter through a module plugin makes it available in the runtime
registry. If the adapter declares `routes`, the gateway's fallback route dispatcher
will match those paths at request time and call the gateway handler with that source
adapter key. This also works after config reload because the dispatcher reads the
runtime registry dynamically.

## Load And Reload Behavior

Plugin modules are loaded during runtime config application:

- server startup
- manager config reload
- provider webhook config reload
- external config reload

When config reloads, previously loaded module adapters and module provider hooks are
unregistered and the currently configured modules are loaded again.

Inline `providerHooks` are converted to the same runtime provider hook interface as
legacy `providerPlugins`.

## Operations

Plugin runtime state and extension health are exposed through:

- `GET /health`: returns a plugin summary with total/degraded/unhealthy counts.
- `GET /manager/plugins`: returns configured plugin entries and runtime extension summaries.
- `GET /manager/plugins/catalog`: returns configured plugins plus runtime providers, adapters, hooks, publishers, and outboxes.
- `GET /manager/plugins/health`: returns grouped extension health.
- `POST /manager/plugins`: adds or updates a configured plugin entry, then reloads config.
- `PATCH /manager/plugins/:key`: updates mutable plugin state such as `enabled`.
- `POST /manager/plugins/:key/enable`: enables a configured plugin.
- `POST /manager/plugins/:key/disable`: disables a configured plugin.
- `POST /manager/plugins/reload`: reloads the current manager config file.
- `GET /manager/plugins/dead-letters`: lists retained plugin delivery dead letters.
- `DELETE /manager/plugins/:key/dead-letters`: clears retained dead letters for one plugin extension.
- `GET /metrics` with `metrics.includeProviderHealth=true`: includes
  `gateway_plugin_info` and `gateway_plugin_health_status`.

Generic extension delivery attempts are exported as
`gateway_plugin_deliveries_total{extension_key,transport,outcome}`. Outcomes include
`delivered`, `not_delivered`, `failed`, `timeout`, and `queue_full`.

Hook executions are exported as:

- `gateway_plugin_hook_executions_total{plugin_key,kind,hook,outcome}`
- `gateway_plugin_hook_duration_ms_sum{plugin_key,kind,hook,outcome}`

`kind` identifies provider, request, stream, billing event, or agent event hook
execution. `hook` identifies the lifecycle method, such as `beforeRouting`,
`transformResponse`, or `transform`.

## Security Model

`modulePath` loads code into the gateway process. Treat module plugins as trusted code.

Do not load plugin modules from untrusted users or writable shared directories. A module
plugin can execute arbitrary Node.js code with the gateway process permissions.

For untrusted extension points, use an external service over HTTP, WebSocket, gRPC, or
stdio and expose only data/configuration, not executable modules.

## Migration From providerPlugins

Old config:

```json
{
  "providerPlugins": [
    {
      "key": "openai-main-dynamic",
      "providerName": "openai-main",
      "request": {
        "bodySet": {
          "metadata.gateway": "next-ai"
        }
      }
    }
  ]
}
```

New config:

```json
{
  "plugins": [
    {
      "key": "openai-main-dynamic",
      "match": {
        "providerName": "openai-main"
      },
      "providerHooks": {
        "request": {
          "bodySet": {
            "metadata.gateway": "next-ai"
          }
        }
      }
    }
  ]
}
```

The old format remains valid, so migration can happen incrementally.

## Choosing A Capability

Use `providerHooks` when:

- the upstream is already compatible with a built-in target adapter
- only small request or response JSON changes are needed
- only auth headers, query params, or credentials need patching

Use `targetAdapters` when:

- the upstream request format is structurally different
- the response format is structurally different
- streaming events need custom parsing
- usage, tools, or reasoning have provider-specific semantics

Use `sourceAdapters` when:

- clients call the gateway with a new client-facing protocol
- the gateway must return that client-facing response shape
