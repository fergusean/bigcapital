import { registerAs } from '@nestjs/config';
import { redisConnectionOptions } from './redis-connection';

export default registerAs('queue', () => ({
  connection: {
    ...redisConnectionOptions('QUEUE'),
    maxRetriesPerRequest: null,
  },
  // BullMQ requires its own prefix rather than ioredis's keyPrefix.
  prefix: process.env.QUEUE_PREFIX || 'bull',
}));
