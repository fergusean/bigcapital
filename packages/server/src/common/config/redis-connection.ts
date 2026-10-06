import { RedisOptions } from 'ioredis';

/** Connection settings shared by cache, throttling, and BullMQ clients. */
export function redisConnectionOptions(scope: 'REDIS' | 'QUEUE'): RedisOptions {
  const value = (name: string) =>
    process.env[`${scope}_${name}`] || process.env[`REDIS_${name}`];
  const username = value('USERNAME') || undefined;
  const password = value('PASSWORD') || undefined;
  const options: RedisOptions = {
    host: value('HOST') || 'localhost',
    port: parseInt(value('PORT'), 10) || 6379,
    username,
    password,
    db: parseInt(value('DB'), 10) || 0,
  };

  const sentinelJson = value('SENTINELS');
  if (sentinelJson) {
    const sentinels = JSON.parse(sentinelJson);
    if (
      !Array.isArray(sentinels) ||
      sentinels.length === 0 ||
      sentinels.some(
        (sentinel) =>
          !sentinel ||
          typeof sentinel.host !== 'string' ||
          sentinel.host.length === 0 ||
          !Number.isInteger(sentinel.port) ||
          sentinel.port < 1 ||
          sentinel.port > 65535,
      )
    ) {
      throw new Error(
        `${scope}_SENTINELS must be a nonempty host/port JSON array`,
      );
    }
    options.sentinels = sentinels;
    options.name = value('SENTINEL_NAME') || 'redis';
    options.sentinelUsername = value('SENTINEL_USERNAME') || username;
    options.sentinelPassword = value('SENTINEL_PASSWORD') || password;
  }

  return options;
}
