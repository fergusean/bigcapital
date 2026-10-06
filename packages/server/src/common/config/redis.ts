import { registerAs } from '@nestjs/config';
import { redisConnectionOptions } from './redis-connection';

export default registerAs('redis', () => ({
  ...redisConnectionOptions('REDIS'),
  keyPrefix: process.env.REDIS_KEY_PREFIX || '',
}));
