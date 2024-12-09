import { Logger, createLogger, transports, format } from 'winston';

import { ILogger } from '../../types/logger';

class LoggerService implements ILogger {
  private logger: Logger;

  constructor(
    private appId: string = 'appId',
    private instanceId: string = 'instanceId',
    private name: string = 'name',
    private logLevel: string = 'info',
    customLogger?: Logger,
  ) {
    this.logger = customLogger || this.createDefaultLogger();
  }

  private createDefaultLogger(): Logger {
    // Custom format to ensure error objects include message and stack
    const errorFormat = format((info) => {
      if (info.error instanceof Error) {
        return {
          ...info,
          error: {
            message: info.error.message,
            stack: info.error.stack,
          },
        };
      }
      return info;
    });

    return createLogger({
      level: this.logLevel,
      format: format.combine(
        format.timestamp(),
        format.errors({ stack: true }),
        errorFormat(),
        format((info) => {
          info.ts = info.timestamp;
          delete info.timestamp;
          return info;
        })(),
        format.json(),
      ),
      transports: [new transports.Console()],
      defaultMeta: {
        app: this.name || this.appId,
        id: this.instanceId,
      },
    });
  }

  info(message: string, ...meta: any[]): void {
    this.logger.info(message, ...meta);
  }

  error(message: string, ...meta: any[]): void {
    this.logger.error(message, ...meta);
  }

  warn(message: string, ...meta: any[]): void {
    this.logger.warn(message, ...meta);
  }

  debug(message: string, ...meta: any[]): void {
    this.logger.debug(message, ...meta);
  }
}

export { LoggerService, ILogger };
