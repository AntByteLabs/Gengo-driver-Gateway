import pino from 'pino';

export const logger = pino({
  name: 'driver-gateway',
  level: process.env['NODE_ENV'] === 'production' ? 'info' : 'debug',
  ...(process.env['NODE_ENV'] !== 'production' && {
    transport: {
      target: 'pino-pretty',
      options: {
        colorize: true,
        translateTime: 'SYS:standard',
        ignore: 'pid,hostname',
      },
    },
  }),
});
