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
    return createLogger({
      level: this.logLevel,
      format: format.combine(
        format.colorize(),
        format.timestamp(),
        format.printf((info) => {
          const { timestamp, level, message } = info;
          // Extract the object from the `info` object's `Symbol(splat)` field
          const symbols = Object.getOwnPropertySymbols(info);
          const splatSymbol = symbols.find(
            (symbol) => symbol.toString() === 'Symbol(splat)',
          );
          let splatData = {};
          if (splatSymbol) {
            splatData = info[splatSymbol][0] || {};
          }
          // Pass it to the `tagify` method
          const tags = this.tagify(splatData);

          return `${timestamp} [${level}] [${this.name || this.appId}:${this.instanceId}] ${message} ${tags}`;
        }),
      ),
      transports: [new transports.Console()],
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

  tagify(obj: Record<string, unknown>): string {
    if (!obj) {
      return '';
    }
    const tags: string[] = [];
    try {
      Object.entries(obj).forEach(([key, val]) => {
        let value: any = val;
        if (typeof val === 'function') {
          val = val();
        }
        if (val instanceof Date) {
          value = val.toISOString();
        } else if (typeof val === 'object' && val !== null) {
          value = JSON.stringify(val);
        } else {
          value = value ? value.toString() : value;
        }
        tags.push(`${key}:${value}`);
      });
    } catch (err) {
      this.error('tagify-error', err);
    }
    return tags.join(' ');
  }
}

export { LoggerService, ILogger };
