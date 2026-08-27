import net from 'node:net';
import tls from 'node:tls';

export type RedisReply = string | number | null | RedisReply[];

export interface SimpleRedisClientOptions {
  url: string;
  connectTimeoutMs: number;
  commandTimeoutMs: number;
  errorPrefix?: string;
}

interface ParsedRedisUrl {
  tls: boolean;
  host: string;
  port: number;
  username?: string;
  password?: string;
  db?: number;
}

export class SimpleRedisClient {
  private socket?: net.Socket | tls.TLSSocket;
  private connecting?: Promise<void>;
  private buffer = Buffer.alloc(0);
  private pending: Array<{
    resolve: (value: RedisReply) => void;
    reject: (error: Error) => void;
    timer: NodeJS.Timeout;
  }> = [];

  constructor(private readonly options: SimpleRedisClientOptions) {}

  async command(args: string[]): Promise<RedisReply> {
    await this.ensureConnected();
    return this.rawCommand(args);
  }

  async close(): Promise<void> {
    const socket = this.socket;
    this.socket = undefined;
    this.connecting = undefined;
    this.buffer = Buffer.alloc(0);
    this.rejectAll(new Error(`${formatErrorPrefix(this.options.errorPrefix)} client closed.`));
    if (!socket) {
      return;
    }

    socket.destroy();
  }

  private async ensureConnected(): Promise<void> {
    if (this.socket && !this.socket.destroyed && !this.connecting) {
      return;
    }

    if (!this.connecting) {
      this.connecting = this.connect();
    }

    const connecting = this.connecting;
    try {
      await connecting;
    } finally {
      if (this.connecting === connecting) {
        this.connecting = undefined;
      }
    }
  }

  private async connect(): Promise<void> {
    const parsed = parseRedisUrl(this.options.url, this.options.errorPrefix);
    const socket = parsed.tls
      ? tls.connect({
          host: parsed.host,
          port: parsed.port,
          servername: parsed.host
        })
      : net.createConnection({
          host: parsed.host,
          port: parsed.port
        });
    if (this.socket) {
      this.rejectAll(new Error(`${formatErrorPrefix(this.options.errorPrefix)} connection was replaced.`));
    }
    this.socket = socket;
    this.buffer = Buffer.alloc(0);
    socket.setNoDelay(true);
    socket.on('data', (chunk) => this.handleData(socket, Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
    socket.on('error', (error) => this.handleSocketError(socket, error));
    socket.on('close', () => this.handleSocketClose(socket));

    await waitForSocketConnect(socket, this.options.connectTimeoutMs, parsed.tls, this.options.errorPrefix);
    if (this.socket !== socket) {
      throw new Error(`${formatErrorPrefix(this.options.errorPrefix)} connection was superseded.`);
    }

    if (parsed.password) {
      await this.rawCommand(
        parsed.username
          ? ['AUTH', parsed.username, parsed.password]
          : ['AUTH', parsed.password],
        socket
      );
    }

    if (parsed.db !== undefined && parsed.db > 0) {
      await this.rawCommand(['SELECT', String(parsed.db)], socket);
    }
  }

  private rawCommand(args: string[], expectedSocket?: net.Socket | tls.TLSSocket): Promise<RedisReply> {
    const socket = this.socket;
    if (!socket || socket.destroyed || (expectedSocket && socket !== expectedSocket)) {
      return Promise.reject(new Error(`${formatErrorPrefix(this.options.errorPrefix)} socket is not connected.`));
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.removePending(resolve);
        reject(new Error(`${formatErrorPrefix(this.options.errorPrefix)} command timed out.`));
        socket.destroy();
      }, this.options.commandTimeoutMs);
      this.pending.push({ resolve, reject, timer });
      socket.write(serializeRedisCommand(args), (error) => {
        if (error) {
          clearTimeout(timer);
          this.removePending(resolve);
          reject(error);
        }
      });
    });
  }

  private removePending(resolve: (value: RedisReply) => void): void {
    const index = this.pending.findIndex((item) => item.resolve === resolve);
    if (index >= 0) {
      this.pending.splice(index, 1);
    }
  }

  private handleData(socket: net.Socket | tls.TLSSocket, chunk: Buffer): void {
    if (this.socket !== socket) {
      return;
    }

    this.buffer = Buffer.concat([this.buffer, chunk]);
    while (this.pending.length > 0) {
      const parsed = parseRedisReply(this.buffer, 0);
      if (!parsed) {
        return;
      }

      this.buffer = this.buffer.subarray(parsed.offset);
      const pending = this.pending.shift();
      if (!pending) {
        return;
      }

      clearTimeout(pending.timer);
      if (parsed.value instanceof Error) {
        pending.reject(parsed.value);
      } else {
        pending.resolve(parsed.value);
      }
    }
  }

  private handleSocketError(socket: net.Socket | tls.TLSSocket, error: Error): void {
    if (this.socket !== socket) {
      return;
    }
    this.rejectAll(error);
  }

  private handleSocketClose(socket: net.Socket | tls.TLSSocket): void {
    if (this.socket !== socket) {
      return;
    }
    this.socket = undefined;
    this.connecting = undefined;
    this.buffer = Buffer.alloc(0);
    this.rejectAll(new Error(`${formatErrorPrefix(this.options.errorPrefix)} socket closed.`));
  }

  private rejectAll(error: Error): void {
    const pending = this.pending.splice(0);
    for (const item of pending) {
      clearTimeout(item.timer);
      item.reject(error);
    }
  }
}

export function formatRedisNumber(value: number): string {
  return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(12)));
}

function parseRedisUrl(value: string, errorPrefix?: string): ParsedRedisUrl {
  const parsed = new URL(value);
  if (parsed.protocol !== 'redis:' && parsed.protocol !== 'rediss:') {
    throw new Error(`${formatErrorPrefix(errorPrefix)} url must use redis:// or rediss://.`);
  }

  const dbRaw = parsed.pathname.replace(/^\//, '').trim();
  const db = dbRaw ? Number(dbRaw) : undefined;
  if (db !== undefined && (!Number.isInteger(db) || db < 0)) {
    throw new Error(`${formatErrorPrefix(errorPrefix)} url has an invalid database index.`);
  }

  return {
    tls: parsed.protocol === 'rediss:',
    host: parsed.hostname || '127.0.0.1',
    port: parsed.port ? Number(parsed.port) : 6379,
    username: parsed.username ? decodeURIComponent(parsed.username) : undefined,
    password: parsed.password ? decodeURIComponent(parsed.password) : undefined,
    db
  };
}

function waitForSocketConnect(
  socket: net.Socket | tls.TLSSocket,
  timeoutMs: number,
  secure: boolean,
  errorPrefix?: string
): Promise<void> {
  return new Promise((resolve, reject) => {
    const connectEvent = secure ? 'secureConnect' : 'connect';
    const timer = setTimeout(() => {
      cleanup();
      socket.destroy();
      reject(new Error(`${formatErrorPrefix(errorPrefix)} connection timed out.`));
    }, timeoutMs);
    const onConnect = () => {
      cleanup();
      resolve();
    };
    const onError = (error: Error) => {
      cleanup();
      reject(error);
    };
    const cleanup = () => {
      clearTimeout(timer);
      socket.off(connectEvent, onConnect);
      socket.off('error', onError);
    };

    socket.once(connectEvent, onConnect);
    socket.once('error', onError);
  });
}

function serializeRedisCommand(args: string[]): Buffer {
  const chunks: string[] = [`*${args.length}\r\n`];
  for (const arg of args) {
    const value = Buffer.from(arg);
    chunks.push(`$${value.length}\r\n`, arg, '\r\n');
  }

  return Buffer.from(chunks.join(''));
}

function parseRedisReply(
  buffer: Buffer,
  offset: number
): { value: RedisReply | Error; offset: number } | undefined {
  if (offset >= buffer.length) {
    return undefined;
  }

  const type = String.fromCharCode(buffer[offset]);
  if (type === '+' || type === '-' || type === ':') {
    const lineEnd = buffer.indexOf('\r\n', offset + 1);
    if (lineEnd < 0) {
      return undefined;
    }

    const line = buffer.toString('utf8', offset + 1, lineEnd);
    if (type === '+') {
      return { value: line, offset: lineEnd + 2 };
    }
    if (type === '-') {
      return { value: new Error(line), offset: lineEnd + 2 };
    }

    return { value: Number(line), offset: lineEnd + 2 };
  }

  if (type === '$') {
    const lineEnd = buffer.indexOf('\r\n', offset + 1);
    if (lineEnd < 0) {
      return undefined;
    }

    const length = Number(buffer.toString('utf8', offset + 1, lineEnd));
    if (length < 0) {
      return { value: null, offset: lineEnd + 2 };
    }

    const start = lineEnd + 2;
    const end = start + length;
    if (buffer.length < end + 2) {
      return undefined;
    }

    return {
      value: buffer.toString('utf8', start, end),
      offset: end + 2
    };
  }

  if (type === '*') {
    const lineEnd = buffer.indexOf('\r\n', offset + 1);
    if (lineEnd < 0) {
      return undefined;
    }

    const count = Number(buffer.toString('utf8', offset + 1, lineEnd));
    if (count < 0) {
      return { value: null, offset: lineEnd + 2 };
    }

    const values: RedisReply[] = [];
    let cursor = lineEnd + 2;
    for (let index = 0; index < count; index += 1) {
      const item = parseRedisReply(buffer, cursor);
      if (!item) {
        return undefined;
      }

      if (item.value instanceof Error) {
        return item;
      }

      values.push(item.value);
      cursor = item.offset;
    }

    return { value: values, offset: cursor };
  }

  return { value: new Error(`Unsupported Redis reply type: ${type}`), offset: buffer.length };
}

function formatErrorPrefix(value: string | undefined): string {
  return value?.trim() || 'Redis';
}
