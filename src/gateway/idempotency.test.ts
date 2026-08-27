import { Readable } from 'node:stream';
import Fastify from 'fastify';
import { afterEach, describe, expect, it } from 'vitest';
import { parseGatewayConfigFromRaw } from '../config';
import type { GatewayConfig, GatewayRequestIdentity } from '../types';
import {
  applyDeferredGatewayIdempotency,
  closeGatewayIdempotencyStore,
  createGatewayIdempotencyPreHandler,
  registerGatewayIdempotencyHooks,
  resetGatewayIdempotencyForTests,
  setGatewayIdempotencyRedisCommandExecutorForTests
} from './idempotency';

describe('gateway idempotency', () => {
  afterEach(async () => {
    resetGatewayIdempotencyForTests();
    await closeGatewayIdempotencyStore();
  });

  it('replays a cached successful JSON POST response without invoking the handler again', async () => {
    const config = createConfig();
    const app = Fastify({ logger: false });
    registerGatewayIdempotencyHooks(app, config);
    const preHandler = createGatewayIdempotencyPreHandler(config);
    let calls = 0;
    app.post('/v1/test', { preHandler }, async (_request, reply) => {
      calls += 1;
      return reply.header('x-upstream-call', String(calls)).send({ calls });
    });
    await app.ready();

    try {
      const first = await app.inject({
        method: 'POST',
        url: '/v1/test',
        headers: {
          'idempotency-key': 'retry-key'
        },
        payload: {
          prompt: 'hello'
        }
      });
      const second = await app.inject({
        method: 'POST',
        url: '/v1/test',
        headers: {
          'idempotency-key': 'retry-key'
        },
        payload: {
          prompt: 'hello'
        }
      });

      expect(first.statusCode).toBe(200);
      expect(second.statusCode).toBe(200);
      expect(JSON.parse(first.body)).toEqual({ calls: 1 });
      expect(JSON.parse(second.body)).toEqual({ calls: 1 });
      expect(first.headers['x-gateway-idempotency-status']).toBe('stored');
      expect(second.headers['x-gateway-idempotency-status']).toBe('replayed');
      expect(second.headers['x-upstream-call']).toBe('1');
      expect(calls).toBe(1);
    } finally {
      await app.close();
    }
  });

  it('hashes binary request bodies directly for replay and conflict detection', async () => {
    const config = createConfig();
    const app = Fastify({ logger: false });
    app.addContentTypeParser(
      'multipart/form-data',
      { parseAs: 'buffer' },
      (_request, body, done) => done(null, body)
    );
    registerGatewayIdempotencyHooks(app, config);
    const preHandler = createGatewayIdempotencyPreHandler(config);
    let calls = 0;
    app.post('/v1/test', { preHandler }, async () => {
      calls += 1;
      return { calls };
    });
    await app.ready();

    const headers = {
      'content-type': 'multipart/form-data; boundary=binary-test',
      'idempotency-key': 'binary-key'
    };
    try {
      const first = await app.inject({
        method: 'POST',
        url: '/v1/test',
        headers,
        payload: Buffer.from('--binary-test\r\nfirst\r\n--binary-test--\r\n')
      });
      const replay = await app.inject({
        method: 'POST',
        url: '/v1/test',
        headers,
        payload: Buffer.from('--binary-test\r\nfirst\r\n--binary-test--\r\n')
      });
      const conflict = await app.inject({
        method: 'POST',
        url: '/v1/test',
        headers,
        payload: Buffer.from('--binary-test\r\nsecond\r\n--binary-test--\r\n')
      });

      expect(first.statusCode).toBe(200);
      expect(replay.headers['x-gateway-idempotency-status']).toBe('replayed');
      expect(conflict.statusCode).toBe(409);
      expect(conflict.headers['x-gateway-idempotency-status']).toBe('conflict');
      expect(calls).toBe(1);
    } finally {
      await app.close();
    }
  });

  it('rejects reuse of the same key with a different request fingerprint', async () => {
    const config = createConfig();
    const app = Fastify({ logger: false });
    registerGatewayIdempotencyHooks(app, config);
    const preHandler = createGatewayIdempotencyPreHandler(config);
    let calls = 0;
    app.post('/v1/test', { preHandler }, async () => {
      calls += 1;
      return { calls };
    });
    await app.ready();

    try {
      await app.inject({
        method: 'POST',
        url: '/v1/test',
        headers: {
          'idempotency-key': 'conflict-key'
        },
        payload: {
          prompt: 'first'
        }
      });
      const conflict = await app.inject({
        method: 'POST',
        url: '/v1/test',
        headers: {
          'idempotency-key': 'conflict-key'
        },
        payload: {
          prompt: 'second'
        }
      });

      expect(conflict.statusCode).toBe(409);
      expect(conflict.headers['x-gateway-idempotency-status']).toBe('conflict');
      expect(JSON.parse(conflict.body).error.code).toBe('idempotency_key_conflict');
      expect(calls).toBe(1);
    } finally {
      await app.close();
    }
  });

  it('can defer fingerprinting until route-specific data has been applied', async () => {
    const config = createConfig();
    const app = Fastify({ logger: false });
    registerGatewayIdempotencyHooks(app, config);
    const preHandler = createGatewayIdempotencyPreHandler(config, { defer: true });
    let calls = 0;
    app.post('/v1/test', { preHandler }, async (request, reply) => {
      const pluginRoute = String(request.headers['x-plugin-route'] || 'default');
      const handled = await applyDeferredGatewayIdempotency(request, reply, config, {
        pluginRoute
      });
      if (handled) {
        return reply;
      }

      calls += 1;
      return { calls, pluginRoute };
    });
    await app.ready();

    try {
      const first = await app.inject({
        method: 'POST',
        url: '/v1/test',
        headers: {
          'idempotency-key': 'deferred-route-key',
          'x-plugin-route': 'primary'
        },
        payload: {
          prompt: 'same'
        }
      });
      const replay = await app.inject({
        method: 'POST',
        url: '/v1/test',
        headers: {
          'idempotency-key': 'deferred-route-key',
          'x-plugin-route': 'primary'
        },
        payload: {
          prompt: 'same'
        }
      });
      const conflict = await app.inject({
        method: 'POST',
        url: '/v1/test',
        headers: {
          'idempotency-key': 'deferred-route-key',
          'x-plugin-route': 'secondary'
        },
        payload: {
          prompt: 'same'
        }
      });

      expect(first.statusCode).toBe(200);
      expect(replay.statusCode).toBe(200);
      expect(conflict.statusCode).toBe(409);
      expect(JSON.parse(first.body)).toEqual({ calls: 1, pluginRoute: 'primary' });
      expect(JSON.parse(replay.body)).toEqual({ calls: 1, pluginRoute: 'primary' });
      expect(replay.headers['x-gateway-idempotency-status']).toBe('replayed');
      expect(conflict.headers['x-gateway-idempotency-status']).toBe('conflict');
      expect(calls).toBe(1);
    } finally {
      await app.close();
    }
  });

  it('scopes cached responses by authenticated gateway identity', async () => {
    const config = createConfig();
    const app = Fastify({ logger: false });
    registerGatewayIdempotencyHooks(app, config);
    const preHandler = createGatewayIdempotencyPreHandler(config);
    let calls = 0;
    app.post(
      '/v1/test',
      {
        preHandler: [
          async (request) => {
            const userId = String(request.headers['x-test-user'] || 'anonymous');
            request.gatewayIdentity = {
              source: 'trusted_header',
              billingSubjectKey: `user:${userId}`,
              userId
            } satisfies GatewayRequestIdentity;
          },
          preHandler
        ]
      },
      async () => {
        calls += 1;
        return { calls };
      }
    );
    await app.ready();

    try {
      const first = await app.inject({
        method: 'POST',
        url: '/v1/test',
        headers: {
          'idempotency-key': 'shared-key',
          'x-test-user': 'alice'
        },
        payload: {
          prompt: 'same'
        }
      });
      const second = await app.inject({
        method: 'POST',
        url: '/v1/test',
        headers: {
          'idempotency-key': 'shared-key',
          'x-test-user': 'bob'
        },
        payload: {
          prompt: 'same'
        }
      });
      const replay = await app.inject({
        method: 'POST',
        url: '/v1/test',
        headers: {
          'idempotency-key': 'shared-key',
          'x-test-user': 'alice'
        },
        payload: {
          prompt: 'same'
        }
      });

      expect(first.statusCode).toBe(200);
      expect(second.statusCode).toBe(200);
      expect(replay.statusCode).toBe(200);
      expect(JSON.parse(first.body)).toEqual({ calls: 1 });
      expect(JSON.parse(second.body)).toEqual({ calls: 2 });
      expect(JSON.parse(replay.body)).toEqual({ calls: 1 });
      expect(second.headers['x-gateway-idempotency-status']).toBe('stored');
      expect(replay.headers['x-gateway-idempotency-status']).toBe('replayed');
      expect(calls).toBe(2);
    } finally {
      await app.close();
    }
  });

  it('scopes cached responses by SDK-compatible auth headers when auth is disabled', async () => {
    const config = createConfig();
    const app = Fastify({ logger: false });
    registerGatewayIdempotencyHooks(app, config);
    const preHandler = createGatewayIdempotencyPreHandler(config);
    let calls = 0;
    app.post('/v1/test', { preHandler }, async () => {
      calls += 1;
      return { calls };
    });
    await app.ready();

    try {
      const first = await app.inject({
        method: 'POST',
        url: '/v1/test',
        headers: {
          'idempotency-key': 'sdk-key',
          'x-goog-api-key': 'token-a'
        },
        payload: {
          prompt: 'same'
        }
      });
      const second = await app.inject({
        method: 'POST',
        url: '/v1/test',
        headers: {
          'idempotency-key': 'sdk-key',
          'x-goog-api-key': 'token-b'
        },
        payload: {
          prompt: 'same'
        }
      });
      const replay = await app.inject({
        method: 'POST',
        url: '/v1/test',
        headers: {
          'idempotency-key': 'sdk-key',
          'x-goog-api-key': 'token-a'
        },
        payload: {
          prompt: 'same'
        }
      });

      expect(first.statusCode).toBe(200);
      expect(second.statusCode).toBe(200);
      expect(replay.statusCode).toBe(200);
      expect(JSON.parse(first.body)).toEqual({ calls: 1 });
      expect(JSON.parse(second.body)).toEqual({ calls: 2 });
      expect(JSON.parse(replay.body)).toEqual({ calls: 1 });
      expect(calls).toBe(2);
    } finally {
      await app.close();
    }
  });

  it('waits for an in-flight matching request and replays the completed response', async () => {
    const config = createConfig();
    const app = Fastify({ logger: false });
    registerGatewayIdempotencyHooks(app, config);
    const preHandler = createGatewayIdempotencyPreHandler(config);
    let calls = 0;
    app.post('/v1/test', { preHandler }, async () => {
      calls += 1;
      await delay(20);
      return { calls };
    });
    await app.ready();

    try {
      const request = {
        method: 'POST' as const,
        url: '/v1/test',
        headers: {
          'idempotency-key': 'pending-key'
        },
        payload: {
          prompt: 'same'
        }
      };
      const [first, second] = await Promise.all([app.inject(request), app.inject(request)]);

      expect(first.statusCode).toBe(200);
      expect(second.statusCode).toBe(200);
      expect(JSON.parse(first.body)).toEqual({ calls: 1 });
      expect(JSON.parse(second.body)).toEqual({ calls: 1 });
      expect(calls).toBe(1);
    } finally {
      await app.close();
    }
  });

  it('stops waiting for an in-flight memory entry after pendingWaitTimeoutMs', async () => {
    const config = createConfig();
    config.idempotency.pendingWaitTimeoutMs = 10;
    const app = Fastify({ logger: false });
    registerGatewayIdempotencyHooks(app, config);
    const preHandler = createGatewayIdempotencyPreHandler(config);
    let calls = 0;
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    app.post('/v1/test', { preHandler }, async () => {
      calls += 1;
      await gate;
      return { calls };
    });
    await app.ready();

    const request = {
      method: 'POST' as const,
      url: '/v1/test',
      headers: {
        'idempotency-key': 'pending-timeout-key'
      },
      payload: {
        prompt: 'same'
      }
    };
    try {
      const firstRequest = app.inject(request);
      await delay(5);
      const second = await app.inject(request);

      expect(second.statusCode).toBe(409);
      expect(second.headers['x-gateway-idempotency-status']).toBe('in-progress');
      expect(JSON.parse(second.body).error.code).toBe('idempotency_request_in_progress');
      expect(calls).toBe(1);

      release();
      const first = await firstRequest;
      expect(first.statusCode).toBe(200);
    } finally {
      release();
      await app.close();
    }
  });

  it('replays cached responses from Redis idempotency storage', async () => {
    const config = createRedisConfig();
    installFakeRedisIdempotencyStore();
    const app = Fastify({ logger: false });
    registerGatewayIdempotencyHooks(app, config);
    const preHandler = createGatewayIdempotencyPreHandler(config);
    let calls = 0;
    app.post('/v1/test', { preHandler }, async (_request, reply) => {
      calls += 1;
      return reply.header('x-upstream-call', String(calls)).send({ calls });
    });
    await app.ready();

    try {
      const request = {
        method: 'POST' as const,
        url: '/v1/test',
        headers: {
          'idempotency-key': 'redis-retry-key'
        },
        payload: {
          prompt: 'same'
        }
      };
      const first = await app.inject(request);
      const replay = await app.inject(request);

      expect(first.statusCode).toBe(200);
      expect(replay.statusCode).toBe(200);
      expect(JSON.parse(first.body)).toEqual({ calls: 1 });
      expect(JSON.parse(replay.body)).toEqual({ calls: 1 });
      expect(first.headers['x-gateway-idempotency-status']).toBe('stored');
      expect(replay.headers['x-gateway-idempotency-status']).toBe('replayed');
      expect(replay.headers['x-upstream-call']).toBe('1');
      expect(calls).toBe(1);
    } finally {
      await app.close();
    }
  });

  it('waits on Redis pending entries before replaying a completed response', async () => {
    const config = createRedisConfig();
    installFakeRedisIdempotencyStore();
    const app = Fastify({ logger: false });
    registerGatewayIdempotencyHooks(app, config);
    const preHandler = createGatewayIdempotencyPreHandler(config);
    let calls = 0;
    app.post('/v1/test', { preHandler }, async () => {
      calls += 1;
      await delay(20);
      return { calls };
    });
    await app.ready();

    try {
      const request = {
        method: 'POST' as const,
        url: '/v1/test',
        headers: {
          'idempotency-key': 'redis-pending-key'
        },
        payload: {
          prompt: 'same'
        }
      };
      const [first, replay] = await Promise.all([app.inject(request), app.inject(request)]);

      expect(first.statusCode).toBe(200);
      expect(replay.statusCode).toBe(200);
      expect(JSON.parse(first.body)).toEqual({ calls: 1 });
      expect(JSON.parse(replay.body)).toEqual({ calls: 1 });
      expect(calls).toBe(1);
    } finally {
      await app.close();
    }
  });

  it('does not allow stale Redis owners to overwrite a later pending entry', async () => {
    const config = createRedisConfig();
    config.idempotency.ttlMs = 200;
    installFakeRedisIdempotencyStore();
    const app = Fastify({ logger: false });
    registerGatewayIdempotencyHooks(app, config);
    const preHandler = createGatewayIdempotencyPreHandler(config);
    let calls = 0;
    app.post('/v1/test', { preHandler }, async () => {
      calls += 1;
      const currentCall = calls;
      if (currentCall === 1) {
        await delay(320);
      }
      return { calls: currentCall };
    });
    await app.ready();

    try {
      const request = {
        method: 'POST' as const,
        url: '/v1/test',
        headers: {
          'idempotency-key': 'redis-owner-key'
        },
        payload: {
          prompt: 'same'
        }
      };
      const firstRequest = app.inject(request);
      await delay(240);
      const second = await app.inject(request);
      const first = await firstRequest;
      const replay = await app.inject(request);

      expect(first.statusCode).toBe(200);
      expect(second.statusCode).toBe(200);
      expect(replay.statusCode).toBe(200);
      expect(JSON.parse(first.body)).toEqual({ calls: 1 });
      expect(JSON.parse(second.body)).toEqual({ calls: 2 });
      expect(JSON.parse(replay.body)).toEqual({ calls: 2 });
      expect(replay.headers['x-gateway-idempotency-status']).toBe('replayed');
      expect(calls).toBe(2);
    } finally {
      await app.close();
    }
  });

  it('caches non-event-stream Readable responses after the stream completes', async () => {
    const config = createConfig();
    const app = Fastify({ logger: false });
    registerGatewayIdempotencyHooks(app, config);
    const preHandler = createGatewayIdempotencyPreHandler(config);
    let calls = 0;
    app.post('/v1/streamed-json', { preHandler }, async (_request, reply) => {
      calls += 1;
      return reply
        .header('content-type', 'application/json')
        .header('x-upstream-call', String(calls))
        .send(Readable.from([JSON.stringify({ calls })]));
    });
    await app.ready();

    try {
      const request = {
        method: 'POST' as const,
        url: '/v1/streamed-json',
        headers: {
          'idempotency-key': 'streamed-json-key'
        },
        payload: {
          prompt: 'same'
        }
      };
      const first = await app.inject(request);
      const second = await app.inject(request);

      expect(first.statusCode).toBe(200);
      expect(second.statusCode).toBe(200);
      expect(JSON.parse(first.body)).toEqual({ calls: 1 });
      expect(JSON.parse(second.body)).toEqual({ calls: 1 });
      expect(first.headers['x-gateway-idempotency-status']).toBe('stored');
      expect(second.headers['x-gateway-idempotency-status']).toBe('replayed');
      expect(second.headers['x-upstream-call']).toBe('1');
      expect(calls).toBe(1);
    } finally {
      await app.close();
    }
  });

  it('caches non-event-stream Readable responses in Redis storage', async () => {
    const config = createRedisConfig();
    installFakeRedisIdempotencyStore();
    const app = Fastify({ logger: false });
    registerGatewayIdempotencyHooks(app, config);
    const preHandler = createGatewayIdempotencyPreHandler(config);
    let calls = 0;
    app.post('/v1/redis-streamed-json', { preHandler }, async (_request, reply) => {
      calls += 1;
      return reply
        .header('content-type', 'application/json')
        .header('x-upstream-call', String(calls))
        .send(Readable.from([JSON.stringify({ calls })]));
    });
    await app.ready();

    try {
      const request = {
        method: 'POST' as const,
        url: '/v1/redis-streamed-json',
        headers: {
          'idempotency-key': 'redis-streamed-json-key'
        },
        payload: {
          prompt: 'same'
        }
      };
      const first = await app.inject(request);
      const second = await app.inject(request);

      expect(first.statusCode).toBe(200);
      expect(second.statusCode).toBe(200);
      expect(JSON.parse(first.body)).toEqual({ calls: 1 });
      expect(JSON.parse(second.body)).toEqual({ calls: 1 });
      expect(first.headers['x-gateway-idempotency-status']).toBe('stored');
      expect(second.headers['x-gateway-idempotency-status']).toBe('replayed');
      expect(second.headers['x-upstream-call']).toBe('1');
      expect(calls).toBe(1);
    } finally {
      await app.close();
    }
  });

  it('does not cache responses larger than maxResponseBytes', async () => {
    const config = createConfig();
    config.idempotency.maxResponseBytes = 16;
    const app = Fastify({ logger: false });
    registerGatewayIdempotencyHooks(app, config);
    const preHandler = createGatewayIdempotencyPreHandler(config);
    let calls = 0;
    app.post('/v1/large', { preHandler }, async () => {
      calls += 1;
      return { calls, output: 'x'.repeat(64) };
    });
    await app.ready();

    const request = {
      method: 'POST' as const,
      url: '/v1/large',
      headers: {
        'idempotency-key': 'large-response-key'
      },
      payload: {
        prompt: 'same'
      }
    };
    try {
      const first = await app.inject(request);
      const second = await app.inject(request);

      expect(first.statusCode).toBe(200);
      expect(second.statusCode).toBe(200);
      expect(JSON.parse(first.body).calls).toBe(1);
      expect(JSON.parse(second.body).calls).toBe(2);
      expect(second.headers['x-gateway-idempotency-status']).not.toBe('replayed');
      expect(calls).toBe(2);
    } finally {
      await app.close();
    }
  });

  it('evicts old memory entries when maxTotalBytes is exceeded', async () => {
    const config = createConfig();
    config.idempotency.maxResponseBytes = 100;
    config.idempotency.maxTotalBytes = 100;
    const app = Fastify({ logger: false });
    registerGatewayIdempotencyHooks(app, config);
    const preHandler = createGatewayIdempotencyPreHandler(config);
    let calls = 0;
    app.post('/v1/bounded-cache', { preHandler }, async () => {
      calls += 1;
      return { calls, output: 'x'.repeat(40) };
    });
    await app.ready();

    const inject = (key: string) => app.inject({
      method: 'POST',
      url: '/v1/bounded-cache',
      headers: {
        'idempotency-key': key
      },
      payload: {
        prompt: 'same'
      }
    });
    try {
      await inject('bounded-key-a');
      await inject('bounded-key-b');
      const replayOfEvicted = await inject('bounded-key-a');

      expect(replayOfEvicted.statusCode).toBe(200);
      expect(JSON.parse(replayOfEvicted.body).calls).toBe(3);
      expect(replayOfEvicted.headers['x-gateway-idempotency-status']).not.toBe('replayed');
      expect(calls).toBe(3);
    } finally {
      await app.close();
    }
  });

  it('drops buffered stream chunks once maxResponseBytes is exceeded', async () => {
    const config = createConfig();
    config.idempotency.maxResponseBytes = 16;
    const app = Fastify({ logger: false });
    registerGatewayIdempotencyHooks(app, config);
    const preHandler = createGatewayIdempotencyPreHandler(config);
    let calls = 0;
    app.post('/v1/large-stream', { preHandler }, async (_request, reply) => {
      calls += 1;
      return reply
        .header('content-type', 'application/json')
        .send(Readable.from(['{"output":"', 'x'.repeat(64), `","calls":${calls}}`]));
    });
    await app.ready();

    const request = {
      method: 'POST' as const,
      url: '/v1/large-stream',
      headers: {
        'idempotency-key': 'large-stream-key'
      },
      payload: {
        prompt: 'same'
      }
    };
    try {
      const first = await app.inject(request);
      const second = await app.inject(request);

      expect(first.statusCode).toBe(200);
      expect(second.statusCode).toBe(200);
      expect(JSON.parse(first.body).calls).toBe(1);
      expect(JSON.parse(second.body).calls).toBe(2);
      expect(calls).toBe(2);
    } finally {
      await app.close();
    }
  });

  it('does not cache event-stream responses', async () => {
    const config = createConfig();
    const app = Fastify({ logger: false });
    registerGatewayIdempotencyHooks(app, config);
    const preHandler = createGatewayIdempotencyPreHandler(config);
    let calls = 0;
    app.post('/v1/stream', { preHandler }, async (_request, reply) => {
      calls += 1;
      return reply.header('content-type', 'text/event-stream').send(`data: ${calls}\n\n`);
    });
    await app.ready();

    try {
      const request = {
        method: 'POST' as const,
        url: '/v1/stream',
        headers: {
          'idempotency-key': 'stream-key'
        },
        payload: {
          stream: true
        }
      };
      const first = await app.inject(request);
      const second = await app.inject(request);

      expect(first.body).toBe('data: 1\n\n');
      expect(second.body).toBe('data: 2\n\n');
      expect(calls).toBe(2);
    } finally {
      await app.close();
    }
  });
});

function createConfig(): GatewayConfig {
  return parseGatewayConfigFromRaw({
    idempotency: {
      enabled: true,
      ttlMs: 60000,
      maxEntries: 100
    }
  });
}

function createRedisConfig(): GatewayConfig {
  const config = createConfig();
  config.idempotency.pendingWaitTimeoutMs = 500;
  config.idempotency.pollIntervalMs = 5;
  config.idempotency.storage = {
    type: 'redis',
    url: 'redis://127.0.0.1:6379/0',
    keyPrefix: 'test:gateway:idempotency',
    connectTimeoutMs: 100,
    commandTimeoutMs: 100
  };
  return config;
}

function installFakeRedisIdempotencyStore(): void {
  const values = new Map<string, { value: string; expiresAt?: number }>();
  const readValue = (key: string): string | undefined => {
    const stored = values.get(key);
    if (!stored) {
      return undefined;
    }
    if (stored.expiresAt !== undefined && stored.expiresAt <= Date.now()) {
      values.delete(key);
      return undefined;
    }
    return stored.value;
  };
  const writeValue = (key: string, value: string, ttlMs: string | undefined): void => {
    const ttl = Number(ttlMs);
    values.set(key, {
      value,
      expiresAt: Number.isFinite(ttl) && ttl > 0 ? Date.now() + ttl : undefined
    });
  };

  setGatewayIdempotencyRedisCommandExecutorForTests(async (_storage, args) => {
    const command = args[0];
    if (command === 'GET') {
      return readValue(args[1] || '') ?? null;
    }
    if (command === 'EVAL' && args[1]?.includes("return {0, existing}")) {
      const key = args[3] || '';
      const existing = readValue(key);
      if (existing) {
        return [0, existing];
      }
      const entry = args[4] || '';
      writeValue(key, entry, args[5]);
      return [1, entry];
    }
    if (command === 'EVAL' && args[1]?.includes("redis.call('SET', KEYS[1], ARGV[3]")) {
      const key = args[3] || '';
      const requestHash = args[4] || '';
      const ownerToken = args[5] || '';
      const existing = readValue(key);
      if (!existing) {
        return 0;
      }
      const parsed = JSON.parse(existing) as {
        state?: string;
        requestHash?: string;
        ownerToken?: string;
      };
      if (
        parsed.state !== 'pending' ||
        parsed.requestHash !== requestHash ||
        parsed.ownerToken !== ownerToken
      ) {
        return -1;
      }
      writeValue(key, args[6] || '', args[7]);
      return 1;
    }
    if (command === 'EVAL' && args[1]?.includes("return redis.call('DEL', KEYS[1])")) {
      const key = args[3] || '';
      const requestHash = args[4] || '';
      const ownerToken = args[5] || '';
      const existing = readValue(key);
      if (!existing) {
        return 0;
      }
      const parsed = JSON.parse(existing) as {
        state?: string;
        requestHash?: string;
        ownerToken?: string;
      };
      if (
        parsed.state !== 'pending' ||
        parsed.requestHash !== requestHash ||
        parsed.ownerToken !== ownerToken
      ) {
        return -1;
      }
      return values.delete(key) ? 1 : 0;
    }
    throw new Error(`Unexpected Redis command in test: ${args.join(' ')}`);
  });
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
