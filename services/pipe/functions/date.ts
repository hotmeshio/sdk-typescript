import { formatISODate } from '../../../modules/utils';

type DateInput = Date | string | number;

class DateHandler {
  /**
   * It is so common in mapping operations to use a string (ISO) date as input. This helper
   * method allows for a more-concise mapping ruleset by avoiding date initialization boilerplate
   * code and instead handles the ISO, Milliseconds, and ECMAScript Date input types.
   * @param input
   * @returns
   */
  static getDateInstance(input: DateInput): Date {
    const ISO_REGEX = /^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z)?$/;

    if (typeof input === 'string') {
      if (ISO_REGEX.test(input)) {
        return new Date(input);
      }

      const milliseconds = parseInt(input, 10);
      if (!isNaN(milliseconds)) {
        return new Date(milliseconds);
      }
    } else if (input instanceof Date) {
      return input;
    } else if (typeof input === 'number') {
      return new Date(input);
    }

    throw new Error('Invalid date format');
  }

  fromISOString(isoString: string): Date {
    return new Date(isoString);
  }

  now(): number {
    return Date.now();
  }

  parse(dateString: string): number {
    return Date.parse(dateString);
  }

  getDate(date: DateInput): number {
    return DateHandler.getDateInstance(date).getDate();
  }

  getDay(date: DateInput): number {
    return DateHandler.getDateInstance(date).getDay();
  }

  getFullYear(date: DateInput): number {
    return DateHandler.getDateInstance(date).getFullYear();
  }

  getHours(date: DateInput): number {
    return DateHandler.getDateInstance(date).getHours();
  }

  getMilliseconds(date: DateInput): number {
    return DateHandler.getDateInstance(date).getMilliseconds();
  }

  getMinutes(date: DateInput): number {
    return DateHandler.getDateInstance(date).getMinutes();
  }

  getMonth(date: DateInput): number {
    return DateHandler.getDateInstance(date).getMonth();
  }

  getSeconds(date: DateInput): number {
    return DateHandler.getDateInstance(date).getSeconds();
  }

  getTime(date: DateInput): number {
    return DateHandler.getDateInstance(date).getTime();
  }

  getTimezoneOffset(date: DateInput): number {
    return DateHandler.getDateInstance(date).getTimezoneOffset();
  }

  getUTCDate(date: DateInput): number {
    return DateHandler.getDateInstance(date).getUTCDate();
  }

  getUTCDay(date: DateInput): number {
    return DateHandler.getDateInstance(date).getUTCDay();
  }

  getUTCFullYear(date: DateInput): number {
    return DateHandler.getDateInstance(date).getUTCFullYear();
  }

  getUTCHours(date: DateInput): number {
    return DateHandler.getDateInstance(date).getUTCHours();
  }

  getUTCMilliseconds(date: DateInput): number {
    return DateHandler.getDateInstance(date).getUTCMilliseconds();
  }

  getUTCMinutes(date: DateInput): number {
    return DateHandler.getDateInstance(date).getUTCMinutes();
  }

  getUTCMonth(date: DateInput): number {
    return DateHandler.getDateInstance(date).getUTCMonth();
  }

  getUTCSeconds(date: DateInput): number {
    return DateHandler.getDateInstance(date).getUTCSeconds();
  }

  setMilliseconds(date: DateInput, ms: number): number {
    return DateHandler.getDateInstance(date).setMilliseconds(ms);
  }

  setMinutes(
    date: DateInput,
    minutes: number,
    seconds?: number,
    ms?: number,
  ): number {
    return DateHandler.getDateInstance(date).setMinutes(minutes, seconds, ms);
  }

  setMonth(date: DateInput, month: number, day?: number): number {
    return DateHandler.getDateInstance(date).setMonth(month, day);
  }

  setSeconds(date: DateInput, seconds: number, ms?: number): number {
    return DateHandler.getDateInstance(date).setSeconds(seconds, ms);
  }

  setTime(date: DateInput, time: number): number {
    return DateHandler.getDateInstance(date).setTime(time);
  }

  setUTCDate(date: DateInput, day: number): number {
    return DateHandler.getDateInstance(date).setUTCDate(day);
  }

  setUTCFullYear(
    date: DateInput,
    year: number,
    month?: number,
    day?: number,
  ): number {
    return DateHandler.getDateInstance(date).setUTCFullYear(year, month, day);
  }

  setUTCHours(
    date: DateInput,
    hours: number,
    minutes?: number,
    seconds?: number,
    ms?: number,
  ): number {
    return DateHandler.getDateInstance(date).setUTCHours(
      hours,
      minutes,
      seconds,
      ms,
    );
  }

  setUTCMilliseconds(date: DateInput, ms: number): number {
    return DateHandler.getDateInstance(date).setUTCMilliseconds(ms);
  }

  setUTCMinutes(
    date: DateInput,
    minutes: number,
    seconds?: number,
    ms?: number,
  ): number {
    return DateHandler.getDateInstance(date).setUTCMinutes(
      minutes,
      seconds,
      ms,
    );
  }

  setUTCMonth(date: DateInput, month: number, day?: number): number {
    return DateHandler.getDateInstance(date).setUTCMonth(month, day);
  }

  setUTCSeconds(date: DateInput, seconds: number, ms?: number): number {
    return DateHandler.getDateInstance(date).setUTCSeconds(seconds, ms);
  }

  setDate(date: DateInput, day: number): number {
    return DateHandler.getDateInstance(date).setDate(day);
  }

  setFullYear(
    date: DateInput,
    year: number,
    month?: number,
    day?: number,
  ): number {
    return DateHandler.getDateInstance(date).setFullYear(year, month, day);
  }

  setHours(
    date: DateInput,
    hours: number,
    minutes?: number,
    seconds?: number,
    ms?: number,
  ): number {
    return DateHandler.getDateInstance(date).setHours(
      hours,
      minutes,
      seconds,
      ms,
    );
  }
  toDateString(date: DateInput): string {
    return DateHandler.getDateInstance(date).toDateString();
  }

  toISOString(date: DateInput): string {
    return DateHandler.getDateInstance(date).toISOString();
  }

  toISOXString(date?: DateInput): string {
    return formatISODate(date ? DateHandler.getDateInstance(date) : new Date());
  }

  toJSON(date: DateInput): string {
    return DateHandler.getDateInstance(date).toJSON();
  }

  toLocaleDateString(
    date: DateInput,
    locales?: string | string[],
    options?: Intl.DateTimeFormatOptions,
  ): string {
    return DateHandler.getDateInstance(date).toLocaleDateString(
      locales,
      options,
    );
  }

  toLocaleString(
    date: DateInput,
    locales?: string | string[],
    options?: Intl.DateTimeFormatOptions,
  ): string {
    return DateHandler.getDateInstance(date).toLocaleString(locales, options);
  }

  toLocaleTimeString(
    date: DateInput,
    locales?: string | string[],
    options?: Intl.DateTimeFormatOptions,
  ): string {
    return DateHandler.getDateInstance(date).toLocaleTimeString(
      locales,
      options,
    );
  }

  toString(date: DateInput): string {
    return DateHandler.getDateInstance(date).toString();
  }

  UTC(
    year: number,
    month: number,
    date?: number,
    hours?: number,
    minutes?: number,
    seconds?: number,
    ms?: number,
  ): number {
    return Date.UTC(year, month, date, hours, minutes, seconds, ms);
  }

  valueOf(date: DateInput): number {
    return DateHandler.getDateInstance(date).valueOf();
  }
}

export { DateHandler };
