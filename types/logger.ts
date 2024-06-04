export interface ILogger {
  info(message: string, ...meta: any[]): void;
  error(message: string, ...meta: any[]): void;
  warn(message: string, ...meta: any[]): void;
  debug(message: string, ...meta: any[]): void;
}

export type LogLevel = 'silly' | 'debug' | 'info' | 'warn' | 'error' | 'silent';
