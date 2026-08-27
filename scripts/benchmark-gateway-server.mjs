#!/usr/bin/env node

import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { availableParallelism, tmpdir } from 'node:os';
import { join } from 'node:path';
import { performance } from 'node:perf_hooks';

const options = parseOptions(process.argv.slice(2));
const root = process.cwd();

const gatewayProcesses = [];
let mockServer;
let tempDir;

try {
  if (!options.skipBuild) {
    await runCommand(process.execPath, ['build.js'], {
      cwd: root,
      env: process.env
    });
  }

  const mockPort = await getAvailablePort();
  mockServer = await startMockUpstream(mockPort);
  tempDir = await mkdtemp(join(tmpdir(), 'next-ai-gateway-bench-'));
  const baseUrls = [];
  for (let index = 0; index < options.instances; index += 1) {
    const gatewayPort = await getAvailablePort();
    const configPath = join(tempDir, `gateway-${index}.config.json`);
    await writeFile(configPath, JSON.stringify(createBenchmarkConfig(gatewayPort, mockPort), null, 2));

    const gatewayProcess = spawn(process.execPath, ['dist/index.js'], {
      cwd: root,
      env: {
        ...process.env,
        GATEWAY_CONFIG_PATH: configPath,
        PORT: String(gatewayPort),
        HOST: '127.0.0.1',
        BILLING_ENABLED: 'false',
        GATEWAY_METRICS_ENABLED: 'false',
        PROVIDER_HEALTH_CHECK_ENABLED: 'false'
      },
      stdio: ['ignore', 'pipe', 'pipe']
    });

    gatewayProcesses.push(gatewayProcess);
    const gatewayLogs = collectProcessLogs(gatewayProcess);
    await waitForGateway(`http://127.0.0.1:${gatewayPort}/health`, gatewayProcess, gatewayLogs);
    baseUrls.push(`http://127.0.0.1:${gatewayPort}`);
  }

  const results = [];
  for (const scenario of createScenarios()) {
    await runWarmup(baseUrls, scenario, options.warmup, options.concurrency);
    results.push(await runBenchmark(baseUrls, scenario, options.requests, options.concurrency));
  }

  printResults(results, options);
} finally {
  for (const gatewayProcess of gatewayProcesses) {
    await stopProcess(gatewayProcess);
  }
  if (mockServer) {
    await closeServer(mockServer);
  }
  if (tempDir) {
    await rm(tempDir, { recursive: true, force: true });
  }
}

function parseOptions(args) {
  const parsed = {
    requests: 200,
    concurrency: 20,
    warmup: 20,
    instances: 1,
    skipBuild: false
  };

  for (const arg of args) {
    const [name, value] = arg.split('=', 2);
    if (name === '--requests' && value) {
      parsed.requests = readPositiveInteger(value, parsed.requests);
    } else if (name === '--concurrency' && value) {
      parsed.concurrency = readPositiveInteger(value, parsed.concurrency);
    } else if (name === '--warmup' && value) {
      parsed.warmup = readNonNegativeInteger(value, parsed.warmup);
    } else if (name === '--instances' && value) {
      parsed.instances = value === 'auto'
        ? Math.max(1, availableParallelism())
        : readPositiveInteger(value, parsed.instances);
    } else if (name === '--skip-build') {
      parsed.skipBuild = true;
    }
  }

  return parsed;
}

function readPositiveInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function readNonNegativeInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

function createBenchmarkConfig(gatewayPort, mockPort) {
  const mockBaseUrl = `http://127.0.0.1:${mockPort}`;
  return {
    host: '127.0.0.1',
    port: gatewayPort,
    upstreamTimeoutMs: 250,
    providers: [
      {
        name: 'openai-responses',
        type: 'openai_responses',
        apikey: 'sk-bench',
        baseurl: `${mockBaseUrl}/v1`,
        models: ['gpt-bench']
      },
      {
        name: 'openai-chat',
        type: 'openai_chat_completions',
        apikey: 'sk-bench',
        baseurl: `${mockBaseUrl}/v1`,
        models: ['gpt-chat-bench'],
        openaiChatStreamUsage: 'disabled'
      },
      {
        name: 'anthropic-main',
        type: 'anthropic_messages',
        apikey: 'sk-bench',
        baseurl: mockBaseUrl,
        models: ['claude-bench']
      },
      {
        name: 'gemini-main',
        type: 'gemini_generate_content',
        apikey: 'sk-bench',
        baseurl: mockBaseUrl,
        models: ['gemini-bench']
      }
    ],
    auth: {
      enabled: false
    },
    billing: {
      enabled: false
    },
    metrics: {
      enabled: false
    },
    providerHealthCheck: {
      enabled: false
    },
    precheck: {
      enabled: false
    },
    mcpGateway: {
      enabled: false
    },
    agent: {
      storage: {
        type: 'memory'
      },
      mcpServers: []
    }
  };
}

function createScenarios() {
  return [
    {
      name: 'GET /health',
      method: 'GET',
      path: '/health'
    },
    {
      name: 'GET /v1/models',
      method: 'GET',
      path: '/v1/models'
    },
    {
      name: 'OpenAI Responses passthrough',
      method: 'POST',
      path: '/v1/responses',
      targetProvider: 'openai-responses',
      body: {
        model: 'openai-responses/gpt-bench',
        input: 'Return a short benchmark response.',
        max_output_tokens: 64
      }
    },
    {
      name: 'OpenAI Chat passthrough',
      method: 'POST',
      path: '/v1/chat/completions',
      targetProvider: 'openai-chat',
      body: {
        model: 'openai-chat/gpt-chat-bench',
        messages: [{ role: 'user', content: 'Return a short benchmark response.' }],
        max_tokens: 64
      }
    },
    {
      name: 'OpenAI Chat upstream timeout',
      method: 'POST',
      path: '/v1/chat/completions',
      targetProvider: 'openai-chat',
      expectStatus: 502,
      body: {
        model: 'openai-chat/gpt-chat-bench',
        messages: [{ role: 'user', content: 'Simulate a slow upstream response.' }],
        metadata: {
          benchmarkDelayMs: 500
        },
        max_tokens: 64
      }
    },
    {
      name: 'Anthropic Messages passthrough',
      method: 'POST',
      path: '/v1/messages',
      targetProvider: 'anthropic-main',
      body: {
        model: 'anthropic-main/claude-bench',
        messages: [{ role: 'user', content: 'Return a short benchmark response.' }],
        max_tokens: 64
      }
    },
    {
      name: 'Gemini generateContent passthrough',
      method: 'POST',
      path: '/v1beta/models/gemini-main%2Fgemini-bench:generateContent',
      targetProvider: 'gemini-main',
      body: {
        contents: [{ role: 'user', parts: [{ text: 'Return a short benchmark response.' }] }],
        generationConfig: {
          maxOutputTokens: 64
        }
      }
    },
    {
      name: 'OpenAI Chat -> Anthropic conversion',
      method: 'POST',
      path: '/v1/chat/completions',
      targetProvider: 'anthropic-main',
      body: {
        model: 'anthropic-main/claude-bench',
        messages: [{ role: 'user', content: 'Return a short benchmark response.' }],
        max_tokens: 64
      }
    },
    {
      name: 'Anthropic -> OpenAI Responses conversion',
      method: 'POST',
      path: '/v1/messages',
      targetProvider: 'openai-responses',
      body: {
        model: 'openai-responses/gpt-bench',
        messages: [{ role: 'user', content: 'Return a short benchmark response.' }],
        max_tokens: 64
      }
    },
    {
      name: 'Gemini -> OpenAI Responses conversion',
      method: 'POST',
      path: '/v1beta/models/openai-responses%2Fgpt-bench:generateContent',
      targetProvider: 'openai-responses',
      body: {
        contents: [{ role: 'user', parts: [{ text: 'Return a short benchmark response.' }] }],
        generationConfig: {
          maxOutputTokens: 64
        }
      }
    }
  ];
}

async function runWarmup(baseUrls, scenario, requests, concurrency) {
  if (requests <= 0) {
    return;
  }

  await runBenchmark(baseUrls, scenario, requests, Math.min(concurrency, requests), false);
}

async function runBenchmark(baseUrls, scenario, requests, concurrency, collect = true) {
  const latencies = [];
  let next = 0;
  const startedAt = performance.now();
  const workerCount = Math.min(concurrency, requests);

  await Promise.all(Array.from({ length: workerCount }, async () => {
    while (true) {
      const index = next;
      next += 1;
      if (index >= requests) {
        return;
      }

      const requestStartedAt = performance.now();
      await sendScenarioRequest(baseUrls[index % baseUrls.length], scenario);
      if (collect) {
        latencies.push(performance.now() - requestStartedAt);
      }
    }
  }));

  const durationMs = performance.now() - startedAt;
  if (!collect) {
    return undefined;
  }

  latencies.sort((a, b) => a - b);
  return {
    name: scenario.name,
    requests,
    concurrency: workerCount,
    durationMs,
    rps: requests / (durationMs / 1000),
    meanMs: average(latencies),
    p50Ms: percentile(latencies, 50),
    p95Ms: percentile(latencies, 95),
    p99Ms: percentile(latencies, 99),
    minMs: latencies[0],
    maxMs: latencies[latencies.length - 1]
  };
}

async function sendScenarioRequest(baseUrl, scenario) {
  const headers = {
    accept: 'application/json'
  };
  const init = {
    method: scenario.method,
    headers
  };

  if (scenario.targetProvider) {
    headers['x-target-provider'] = scenario.targetProvider;
  }

  if (scenario.body !== undefined) {
    headers['content-type'] = 'application/json';
    headers.authorization = 'Bearer sk-bench-client';
    init.body = JSON.stringify(scenario.body);
  }

  const response = await fetch(`${baseUrl}${scenario.path}`, init);
  const text = await response.text();
  const expectedStatus = scenario.expectStatus || 200;
  if (response.status !== expectedStatus) {
    throw new Error(`${scenario.name} expected ${expectedStatus} but received ${response.status}: ${text.slice(0, 500)}`);
  }

  if (text) {
    JSON.parse(text);
  }
}

function printResults(results, options) {
  console.log('');
  console.log(`Gateway server benchmark: requests=${options.requests}, concurrency=${options.concurrency}, warmup=${options.warmup}, instances=${options.instances}`);
  console.log('');
  console.log('| Scenario | req/s | mean ms | p50 ms | p95 ms | p99 ms | min ms | max ms |');
  console.log('|---|---:|---:|---:|---:|---:|---:|---:|');
  for (const result of results) {
    console.log(
      `| ${result.name} | ${formatNumber(result.rps)} | ${formatNumber(result.meanMs)} | ${formatNumber(result.p50Ms)} | ${formatNumber(result.p95Ms)} | ${formatNumber(result.p99Ms)} | ${formatNumber(result.minMs)} | ${formatNumber(result.maxMs)} |`
    );
  }
  console.log('');
}

function formatNumber(value) {
  return value.toFixed(2);
}

function average(values) {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function percentile(values, percentileValue) {
  if (values.length === 0) {
    return 0;
  }

  const index = Math.ceil((percentileValue / 100) * values.length) - 1;
  return values[Math.min(values.length - 1, Math.max(0, index))];
}

async function startMockUpstream(port) {
  const server = createServer(async (request, response) => {
    const body = await readJsonBody(request);
    const url = new URL(request.url || '/', `http://${request.headers.host || '127.0.0.1'}`);
    const delayMs = readBenchmarkDelayMs(body);
    if (delayMs > 0) {
      await delay(delayMs);
    }
    const payload = buildMockPayload(url, body);
    response.writeHead(200, {
      'content-type': 'application/json'
    });
    response.end(JSON.stringify(payload));
  });

  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', () => {
      server.off('error', reject);
      resolve();
    });
  });
  return server;
}

function readBenchmarkDelayMs(body) {
  const raw = body?.metadata?.benchmarkDelayMs ?? body?.benchmarkDelayMs;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 0;
  }

  return Math.min(5000, Math.trunc(parsed));
}

async function readJsonBody(request) {
  const chunks = [];
  for await (const chunk of request) {
    chunks.push(chunk);
  }

  if (chunks.length === 0) {
    return {};
  }

  const raw = Buffer.concat(chunks).toString('utf8');
  try {
    return JSON.parse(raw);
  } catch {
    return {};
  }
}

function buildMockPayload(url, body) {
  if (url.pathname.endsWith('/chat/completions')) {
    return createOpenAIChatPayload(body.model || 'gpt-chat-bench');
  }

  if (url.pathname.endsWith('/responses')) {
    return createOpenAIResponsesPayload(body.model || 'gpt-bench');
  }

  if (url.pathname.endsWith('/v1/messages')) {
    return createAnthropicPayload(body.model || 'claude-bench');
  }

  if (/\/models\/.+:(generateContent|streamGenerateContent)$/.test(url.pathname)) {
    const match = url.pathname.match(/\/models\/(.+):(generateContent|streamGenerateContent)$/);
    const model = match ? decodeURIComponent(match[1]) : 'gemini-bench';
    return createGeminiPayload(model);
  }

  return {
    ok: true
  };
}

function createOpenAIResponsesPayload(model) {
  return {
    id: 'resp_bench_server',
    object: 'response',
    status: 'completed',
    model,
    output_text: 'mock benchmark response',
    output: [
      {
        id: 'msg_bench_server',
        type: 'message',
        role: 'assistant',
        status: 'completed',
        content: [
          {
            type: 'output_text',
            text: 'mock benchmark response',
            annotations: []
          }
        ]
      }
    ],
    usage: {
      input_tokens: 12,
      output_tokens: 4,
      total_tokens: 16
    }
  };
}

function createOpenAIChatPayload(model) {
  return {
    id: 'chatcmpl_bench_server',
    object: 'chat.completion',
    model,
    choices: [
      {
        index: 0,
        message: {
          role: 'assistant',
          content: 'mock benchmark response'
        },
        finish_reason: 'stop'
      }
    ],
    usage: {
      prompt_tokens: 12,
      completion_tokens: 4,
      total_tokens: 16
    }
  };
}

function createAnthropicPayload(model) {
  return {
    id: 'msg_bench_server',
    type: 'message',
    role: 'assistant',
    model,
    content: [
      {
        type: 'text',
        text: 'mock benchmark response'
      }
    ],
    stop_reason: 'end_turn',
    usage: {
      input_tokens: 12,
      output_tokens: 4
    }
  };
}

function createGeminiPayload(model) {
  return {
    candidates: [
      {
        index: 0,
        content: {
          role: 'model',
          parts: [
            {
              text: 'mock benchmark response'
            }
          ]
        },
        finishReason: 'STOP'
      }
    ],
    usageMetadata: {
      promptTokenCount: 12,
      candidatesTokenCount: 4,
      totalTokenCount: 16
    },
    modelVersion: model
  };
}

async function waitForGateway(url, process, logs) {
  const deadline = Date.now() + 10000;
  while (Date.now() < deadline) {
    if (process.exitCode !== null) {
      throw new Error(`Gateway exited before becoming ready.\n${logs()}`);
    }

    try {
      const response = await fetch(url);
      if (response.ok) {
        return;
      }
    } catch {
      // Retry until timeout.
    }

    await delay(100);
  }

  throw new Error(`Timed out waiting for gateway readiness.\n${logs()}`);
}

function collectProcessLogs(process) {
  const lines = [];
  const append = (chunk) => {
    lines.push(chunk.toString('utf8'));
    if (lines.length > 40) {
      lines.splice(0, lines.length - 40);
    }
  };
  process.stdout.on('data', append);
  process.stderr.on('data', append);
  return () => lines.join('');
}

async function getAvailablePort() {
  const server = createServer();
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      server.off('error', reject);
      resolve();
    });
  });
  const address = server.address();
  await closeServer(server);
  if (!address || typeof address === 'string') {
    throw new Error('Failed to allocate a local port.');
  }
  return address.port;
}

function closeServer(server) {
  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function runCommand(command, args, options) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      ...options,
      stdio: 'inherit'
    });
    child.on('error', reject);
    child.on('exit', (code, signal) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`${command} ${args.join(' ')} failed with code=${code}, signal=${signal}`));
    });
  });
}

function stopProcess(process) {
  return new Promise((resolve) => {
    if (process.exitCode !== null) {
      resolve();
      return;
    }

    const timeout = setTimeout(() => {
      process.kill('SIGKILL');
    }, 3000);
    process.once('exit', () => {
      clearTimeout(timeout);
      resolve();
    });
    process.kill('SIGTERM');
  });
}
