import { describe, expect, it, vi } from 'vitest';
import { SimpleRedisClient, type RedisReply } from './redis-client';

interface RedisClientInternals {
  socket?: object;
  buffer: Buffer;
  pending: Array<{
    resolve: (value: RedisReply) => void;
    reject: (error: Error) => void;
    timer: NodeJS.Timeout;
  }>;
  handleData(socket: object, chunk: Buffer): void;
  handleSocketClose(socket: object): void;
}

describe('SimpleRedisClient connection lifecycle', () => {
  it('ignores data and close events from a superseded socket', () => {
    const client = createClient() as unknown as RedisClientInternals;
    const staleSocket = {};
    const currentSocket = {};
    const resolve = vi.fn();
    const reject = vi.fn();
    client.socket = currentSocket;
    client.pending.push({
      resolve,
      reject,
      timer: setTimeout(() => undefined, 1000)
    });

    client.handleData(staleSocket, Buffer.from('+STALE\r\n'));
    client.handleSocketClose(staleSocket);

    expect(client.socket).toBe(currentSocket);
    expect(client.pending).toHaveLength(1);
    expect(resolve).not.toHaveBeenCalled();
    expect(reject).not.toHaveBeenCalled();

    client.handleData(currentSocket, Buffer.from('+CURRENT\r\n'));
    expect(resolve).toHaveBeenCalledWith('CURRENT');
  });

  it('clears partial response data and rejects pending commands when the current socket closes', () => {
    const client = createClient() as unknown as RedisClientInternals;
    const currentSocket = {};
    const reject = vi.fn();
    client.socket = currentSocket;
    client.buffer = Buffer.from('$5\r\nhe');
    client.pending.push({
      resolve: vi.fn(),
      reject,
      timer: setTimeout(() => undefined, 1000)
    });

    client.handleSocketClose(currentSocket);

    expect(client.socket).toBeUndefined();
    expect(client.buffer).toHaveLength(0);
    expect(client.pending).toHaveLength(0);
    expect(reject).toHaveBeenCalledWith(expect.objectContaining({ message: 'test redis socket closed.' }));
  });
});

function createClient(): SimpleRedisClient {
  return new SimpleRedisClient({
    url: 'redis://127.0.0.1:6379',
    connectTimeoutMs: 100,
    commandTimeoutMs: 100,
    errorPrefix: 'test redis'
  });
}
