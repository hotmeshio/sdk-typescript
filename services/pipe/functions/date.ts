import { formatISODate } from '../../../modules/utils';

type DateInput = Date | string | number;

/**
 * Provides date manipulation and formatting functions for use in HotMesh
 * mapping rules. Although inspired by JavaScript's Date API, these methods
 * follow a functional approach where each transformation expects one or more
 * input parameters from the prior row in the @pipe structure.
 *
 * Many methods accept various input formats (`Date`, `string`, and `number`),
 * implicitly casting to dates as necessary. The ISO 8601 Extended Format is
 * supported, including date strings like `YYYY-MM-DD`,
 * `YYYY-MM-DDTHH:mm:ss`, and `YYYY-MM-DDTHH:mm:ss.sssZ`. Strings or numbers
 * representing milliseconds since the Unix epoch are also accepted.
 *
 * @remarks Methods are invoked with the syntax {@link date.\<method\>}, e.g., `{@date.now}` or `{@date.getFullYear}`.
 */
class DateHandler {
  /**
   * Converts a date input (ISO string, milliseconds, or Date instance) into a
   * native Date object. This static helper centralises the parsing logic used
   * by every other DateHandler method, allowing concise mapping rules that
   * avoid date-initialisation boilerplate.
   *
   * @param {DateInput} input - A date value as an ISO 8601 string, numeric milliseconds since epoch, or an existing Date instance.
   * @returns {Date} A native Date object corresponding to the input.
   * @example
   * ```typescript
   * const d = DateHandler.getDateInstance('2023-04-23T12:00:00.000Z');
   * ```
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

  /**
   * Creates a new Date object from a string representation of a date in
   * ISO 8601 format.
   *
   * @param {string} isoString - An ISO 8601 date string (e.g., `"2023-04-23T12:00:00.000Z"`).
   * @returns {Date} A Date object corresponding to the provided ISO string.
   * @example
   * ```yaml
   * date_obj:
   *   "@pipe":
   *     - ["{a.output.data.isoDate}"]
   *     - ["{@date.fromISOString}"]
   * ```
   */
  fromISOString(isoString: string): Date {
    return new Date(isoString);
  }

  /**
   * Returns the current time in milliseconds since the Unix epoch
   * (January 1, 1970 00:00:00 UTC). Takes no parameters.
   *
   * @returns {number} The current timestamp in milliseconds.
   * @example
   * ```yaml
   * current_time:
   *   "@pipe":
   *     - ["{@date.now}"]
   * ```
   */
  now(): number {
    return Date.now();
  }

  /**
   * Returns today's date formatted as a `YYYY-MM-DD` string using UTC.
   * Takes no parameters, similar to `date.now`.
   *
   * @returns {string} Today's date in `YYYY-MM-DD` format.
   * @example
   * ```yaml
   * today:
   *   "@pipe":
   *     - ["{@date.yyyymmdd}"]
   * ```
   */
  yyyymmdd(): string {
    const d = new Date();
    const year = d.getUTCFullYear();
    const month = String(d.getUTCMonth() + 1).padStart(2, '0');
    const day = String(d.getUTCDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }

  /**
   * Parses a string representation of a date and returns the number of
   * milliseconds since the Unix epoch (January 1, 1970 00:00:00 UTC).
   *
   * @param {string} dateString - A date string to parse (e.g., `"April 23, 2023 12:00:00"`).
   * @returns {number} Milliseconds since the Unix epoch for the given date string.
   * @example
   * ```yaml
   * date_milliseconds:
   *   "@pipe":
   *     - ["{a.output.data.dateString}"]
   *     - ["{@date.parse}"]
   * ```
   */
  parse(dateString: string): number {
    return Date.parse(dateString);
  }

  /**
   * Returns the day of the month (1--31) for the specified date according to
   * local time.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {number} The day of the month (1--31).
   * @example
   * ```yaml
   * day_of_month:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.getDate}"]
   * ```
   */
  getDate(date: DateInput): number {
    return DateHandler.getDateInstance(date).getDate();
  }

  /**
   * Returns the day of the week (0--6, where 0 is Sunday) for the specified
   * date according to local time.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {number} The day of the week (0 = Sunday, 6 = Saturday).
   * @example
   * ```yaml
   * day_of_week:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.getDay}"]
   * ```
   */
  getDay(date: DateInput): number {
    return DateHandler.getDateInstance(date).getDay();
  }

  /**
   * Returns the full year (4 digits) for the specified date according to
   * local time.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {number} The four-digit year.
   * @example
   * ```yaml
   * full_year:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.getFullYear}"]
   * ```
   */
  getFullYear(date: DateInput): number {
    return DateHandler.getDateInstance(date).getFullYear();
  }

  /**
   * Returns the hour (0--23) for the specified date according to local time.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {number} The hour (0--23).
   * @example
   * ```yaml
   * hour:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.getHours}"]
   * ```
   */
  getHours(date: DateInput): number {
    return DateHandler.getDateInstance(date).getHours();
  }

  /**
   * Returns the milliseconds (0--999) for the specified date according to
   * local time.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {number} The milliseconds component (0--999).
   * @example
   * ```yaml
   * milliseconds:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.getMilliseconds}"]
   * ```
   */
  getMilliseconds(date: DateInput): number {
    return DateHandler.getDateInstance(date).getMilliseconds();
  }

  /**
   * Returns the minutes (0--59) for the specified date according to local
   * time.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {number} The minutes component (0--59).
   * @example
   * ```yaml
   * minutes:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.getMinutes}"]
   * ```
   */
  getMinutes(date: DateInput): number {
    return DateHandler.getDateInstance(date).getMinutes();
  }

  /**
   * Returns the month (0--11, where 0 is January) for the specified date
   * according to local time.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {number} The month (0 = January, 11 = December).
   * @example
   * ```yaml
   * month:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.getMonth}"]
   * ```
   */
  getMonth(date: DateInput): number {
    return DateHandler.getDateInstance(date).getMonth();
  }

  /**
   * Returns the seconds (0--59) for the specified date according to local
   * time.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {number} The seconds component (0--59).
   * @example
   * ```yaml
   * seconds:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.getSeconds}"]
   * ```
   */
  getSeconds(date: DateInput): number {
    return DateHandler.getDateInstance(date).getSeconds();
  }

  /**
   * Returns the number of milliseconds since January 1, 1970, 00:00:00 UTC
   * for the specified date.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {number} Milliseconds since the Unix epoch.
   * @example
   * ```yaml
   * time:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.getTime}"]
   * ```
   */
  getTime(date: DateInput): number {
    return DateHandler.getDateInstance(date).getTime();
  }

  /**
   * Returns the time zone difference, in minutes, from the current locale
   * (host system settings) to UTC.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {number} The timezone offset in minutes.
   * @example
   * ```yaml
   * timezone_offset:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.getTimezoneOffset}"]
   * ```
   */
  getTimezoneOffset(date: DateInput): number {
    return DateHandler.getDateInstance(date).getTimezoneOffset();
  }

  /**
   * Returns the day of the month (1--31) for the specified date according to
   * universal time (UTC).
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {number} The UTC day of the month (1--31).
   * @example
   * ```yaml
   * utc_day:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.getUTCDate}"]
   * ```
   */
  getUTCDate(date: DateInput): number {
    return DateHandler.getDateInstance(date).getUTCDate();
  }

  /**
   * Returns the day of the week (0--6, where 0 is Sunday) for the specified
   * date according to universal time (UTC).
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {number} The UTC day of the week (0 = Sunday, 6 = Saturday).
   * @example
   * ```yaml
   * utc_weekday:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.getUTCDay}"]
   * ```
   */
  getUTCDay(date: DateInput): number {
    return DateHandler.getDateInstance(date).getUTCDay();
  }

  /**
   * Returns the year of the specified date according to universal time (UTC).
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {number} The four-digit UTC year.
   * @example
   * ```yaml
   * utc_year:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.getUTCFullYear}"]
   * ```
   */
  getUTCFullYear(date: DateInput): number {
    return DateHandler.getDateInstance(date).getUTCFullYear();
  }

  /**
   * Returns the hours (0--23) of the specified date according to universal
   * time (UTC).
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {number} The UTC hours (0--23).
   * @example
   * ```yaml
   * utc_hours:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.getUTCHours}"]
   * ```
   */
  getUTCHours(date: DateInput): number {
    return DateHandler.getDateInstance(date).getUTCHours();
  }

  /**
   * Returns the milliseconds (0--999) of the specified date according to
   * universal time (UTC).
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {number} The UTC milliseconds (0--999).
   * @example
   * ```yaml
   * utc_milliseconds:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.getUTCMilliseconds}"]
   * ```
   */
  getUTCMilliseconds(date: DateInput): number {
    return DateHandler.getDateInstance(date).getUTCMilliseconds();
  }

  /**
   * Returns the minutes (0--59) of the specified date according to universal
   * time (UTC).
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {number} The UTC minutes (0--59).
   * @example
   * ```yaml
   * utc_minutes:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.getUTCMinutes}"]
   * ```
   */
  getUTCMinutes(date: DateInput): number {
    return DateHandler.getDateInstance(date).getUTCMinutes();
  }

  /**
   * Returns the month (0--11) of the specified date according to universal
   * time (UTC), where 0 is January and 11 is December.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {number} The UTC month (0 = January, 11 = December).
   * @example
   * ```yaml
   * utc_month:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.getUTCMonth}"]
   * ```
   */
  getUTCMonth(date: DateInput): number {
    return DateHandler.getDateInstance(date).getUTCMonth();
  }

  /**
   * Returns the seconds (0--59) of the specified date according to universal
   * time (UTC).
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {number} The UTC seconds (0--59).
   * @example
   * ```yaml
   * utc_seconds:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.getUTCSeconds}"]
   * ```
   */
  getUTCSeconds(date: DateInput): number {
    return DateHandler.getDateInstance(date).getUTCSeconds();
  }

  /**
   * Sets the milliseconds value (0--999) of a date object according to local
   * time and returns the new timestamp in milliseconds.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @param {number} ms - The milliseconds value to set (0--999).
   * @returns {number} The updated timestamp in milliseconds since epoch.
   * @example
   * ```yaml
   * new_date:
   *   "@pipe":
   *     - ["{a.output.data.date}", 123]
   *     - ["{@date.setMilliseconds}"]
   * ```
   */
  setMilliseconds(date: DateInput, ms: number): number {
    return DateHandler.getDateInstance(date).setMilliseconds(ms);
  }

  /**
   * Sets the minutes value (0--59) of a date object according to local time,
   * with optional seconds and milliseconds. Returns the new timestamp in
   * milliseconds.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @param {number} minutes - The minutes value to set (0--59).
   * @param {number} [seconds] - Optional seconds value to set (0--59).
   * @param {number} [ms] - Optional milliseconds value to set (0--999).
   * @returns {number} The updated timestamp in milliseconds since epoch.
   * @example
   * ```yaml
   * new_date:
   *   "@pipe":
   *     - ["{a.output.data.date}", 45, 30]
   *     - ["{@date.setMinutes}"]
   * ```
   */
  setMinutes(
    date: DateInput,
    minutes: number,
    seconds?: number,
    ms?: number,
  ): number {
    return DateHandler.getDateInstance(date).setMinutes(minutes, seconds, ms);
  }

  /**
   * Sets the month value (0--11) of a date object according to local time,
   * with an optional day of the month. Returns the new timestamp in
   * milliseconds.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @param {number} month - The month value to set (0 = January, 11 = December).
   * @param {number} [day] - Optional day of the month to set (1--31).
   * @returns {number} The updated timestamp in milliseconds since epoch.
   * @example
   * ```yaml
   * new_date:
   *   "@pipe":
   *     - ["{a.output.data.date}", 7, 15]
   *     - ["{@date.setMonth}"]
   * ```
   */
  setMonth(date: DateInput, month: number, day?: number): number {
    return DateHandler.getDateInstance(date).setMonth(month, day);
  }

  /**
   * Sets the seconds value (0--59) of a date object according to local time,
   * with optional milliseconds. Returns the new timestamp in milliseconds.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @param {number} seconds - The seconds value to set (0--59).
   * @param {number} [ms] - Optional milliseconds value to set (0--999).
   * @returns {number} The updated timestamp in milliseconds since epoch.
   * @example
   * ```yaml
   * new_date:
   *   "@pipe":
   *     - ["{a.output.data.date}", 30, 123]
   *     - ["{@date.setSeconds}"]
   * ```
   */
  setSeconds(date: DateInput, seconds: number, ms?: number): number {
    return DateHandler.getDateInstance(date).setSeconds(seconds, ms);
  }

  /**
   * Sets the date object to the time represented by the number of
   * milliseconds since January 1, 1970, 00:00:00 UTC. Returns the new
   * timestamp.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @param {number} time - The number of milliseconds since the Unix epoch.
   * @returns {number} The updated timestamp in milliseconds since epoch.
   * @example
   * ```yaml
   * new_date:
   *   "@pipe":
   *     - ["{a.output.data.date}", 1620000000000]
   *     - ["{@date.setTime}"]
   * ```
   */
  setTime(date: DateInput, time: number): number {
    return DateHandler.getDateInstance(date).setTime(time);
  }

  /**
   * Sets the day of the month (1--31) of a date object according to
   * universal time (UTC). Returns the new timestamp in milliseconds.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @param {number} day - The UTC day of the month to set (1--31).
   * @returns {number} The updated timestamp in milliseconds since epoch.
   * @example
   * ```yaml
   * new_date:
   *   "@pipe":
   *     - ["{a.output.data.date}", 15]
   *     - ["{@date.setUTCDate}"]
   * ```
   */
  setUTCDate(date: DateInput, day: number): number {
    return DateHandler.getDateInstance(date).setUTCDate(day);
  }

  /**
   * Sets the full year of a date object according to universal time (UTC),
   * with optional month and day parameters. Returns the new timestamp in
   * milliseconds.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @param {number} year - The UTC full year value to set.
   * @param {number} [month] - Optional UTC month to set (0--11).
   * @param {number} [day] - Optional UTC day of the month to set (1--31).
   * @returns {number} The updated timestamp in milliseconds since epoch.
   * @example
   * ```yaml
   * new_date:
   *   "@pipe":
   *     - ["{a.output.data.date}", 2025]
   *     - ["{@date.setUTCFullYear}"]
   * ```
   */
  setUTCFullYear(
    date: DateInput,
    year: number,
    month?: number,
    day?: number,
  ): number {
    return DateHandler.getDateInstance(date).setUTCFullYear(year, month, day);
  }

  /**
   * Sets the hours (0--23) of a date object according to universal time
   * (UTC), with optional minutes, seconds, and milliseconds. Returns the new
   * timestamp in milliseconds.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @param {number} hours - The UTC hours value to set (0--23).
   * @param {number} [minutes] - Optional UTC minutes to set (0--59).
   * @param {number} [seconds] - Optional UTC seconds to set (0--59).
   * @param {number} [ms] - Optional UTC milliseconds to set (0--999).
   * @returns {number} The updated timestamp in milliseconds since epoch.
   * @example
   * ```yaml
   * new_date:
   *   "@pipe":
   *     - ["{a.output.data.date}", 18]
   *     - ["{@date.setUTCHours}"]
   * ```
   */
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

  /**
   * Sets the milliseconds value (0--999) of a date object according to
   * universal time (UTC). Returns the new timestamp in milliseconds.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @param {number} ms - The UTC milliseconds value to set (0--999).
   * @returns {number} The updated timestamp in milliseconds since epoch.
   * @example
   * ```yaml
   * new_date:
   *   "@pipe":
   *     - ["{a.output.data.date}", 500]
   *     - ["{@date.setUTCMilliseconds}"]
   * ```
   */
  setUTCMilliseconds(date: DateInput, ms: number): number {
    return DateHandler.getDateInstance(date).setUTCMilliseconds(ms);
  }

  /**
   * Sets the minutes value (0--59) of a date object according to universal
   * time (UTC), with optional seconds and milliseconds. Returns the new
   * timestamp in milliseconds.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @param {number} minutes - The UTC minutes value to set (0--59).
   * @param {number} [seconds] - Optional UTC seconds to set (0--59).
   * @param {number} [ms] - Optional UTC milliseconds to set (0--999).
   * @returns {number} The updated timestamp in milliseconds since epoch.
   * @example
   * ```yaml
   * new_date:
   *   "@pipe":
   *     - ["{a.output.data.date}", 45]
   *     - ["{@date.setUTCMinutes}"]
   * ```
   */
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

  /**
   * Sets the month value (0--11) of a date object according to universal
   * time (UTC), with an optional day parameter. Returns the new timestamp in
   * milliseconds.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @param {number} month - The UTC month value to set (0 = January, 11 = December).
   * @param {number} [day] - Optional UTC day of the month to set (1--31).
   * @returns {number} The updated timestamp in milliseconds since epoch.
   * @example
   * ```yaml
   * new_date:
   *   "@pipe":
   *     - ["{a.output.data.date}", 2]
   *     - ["{@date.setUTCMonth}"]
   * ```
   */
  setUTCMonth(date: DateInput, month: number, day?: number): number {
    return DateHandler.getDateInstance(date).setUTCMonth(month, day);
  }

  /**
   * Sets the seconds value (0--59) of a date object according to universal
   * time (UTC), with an optional milliseconds parameter. Returns the new
   * timestamp in milliseconds.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @param {number} seconds - The UTC seconds value to set (0--59).
   * @param {number} [ms] - Optional UTC milliseconds to set (0--999).
   * @returns {number} The updated timestamp in milliseconds since epoch.
   * @example
   * ```yaml
   * new_date:
   *   "@pipe":
   *     - ["{a.output.data.date}", 30]
   *     - ["{@date.setUTCSeconds}"]
   * ```
   */
  setUTCSeconds(date: DateInput, seconds: number, ms?: number): number {
    return DateHandler.getDateInstance(date).setUTCSeconds(seconds, ms);
  }

  /**
   * Sets the day of the month (1--31) of a date object according to local
   * time. Returns the new timestamp in milliseconds.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @param {number} day - The day of the month to set (1--31).
   * @returns {number} The updated timestamp in milliseconds since epoch.
   * @example
   * ```yaml
   * new_date:
   *   "@pipe":
   *     - ["{a.output.data.date}", 15]
   *     - ["{@date.setDate}"]
   * ```
   */
  setDate(date: DateInput, day: number): number {
    return DateHandler.getDateInstance(date).setDate(day);
  }

  /**
   * Sets the full year of a date object according to local time, with
   * optional month and day parameters. Returns the new timestamp in
   * milliseconds.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @param {number} year - The full year value to set.
   * @param {number} [month] - Optional month to set (0--11).
   * @param {number} [day] - Optional day of the month to set (1--31).
   * @returns {number} The updated timestamp in milliseconds since epoch.
   * @example
   * ```yaml
   * new_date:
   *   "@pipe":
   *     - ["{a.output.data.date}", 2024]
   *     - ["{@date.setFullYear}"]
   * ```
   */
  setFullYear(
    date: DateInput,
    year: number,
    month?: number,
    day?: number,
  ): number {
    return DateHandler.getDateInstance(date).setFullYear(year, month, day);
  }

  /**
   * Sets the hours (0--23) of a date object according to local time, with
   * optional minutes, seconds, and milliseconds. Returns the new timestamp
   * in milliseconds.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @param {number} hours - The hours value to set (0--23).
   * @param {number} [minutes] - Optional minutes to set (0--59).
   * @param {number} [seconds] - Optional seconds to set (0--59).
   * @param {number} [ms] - Optional milliseconds to set (0--999).
   * @returns {number} The updated timestamp in milliseconds since epoch.
   * @example
   * ```yaml
   * new_date:
   *   "@pipe":
   *     - ["{a.output.data.date}", 15]
   *     - ["{@date.setHours}"]
   * ```
   */
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

  /**
   * Returns the date portion of a date object in a human-readable form as a
   * string (e.g., `"Sun Apr 23 2023"`).
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {string} The date portion as a human-readable string.
   * @example
   * ```yaml
   * date_string:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.toDateString}"]
   * ```
   */
  toDateString(date: DateInput): string {
    return DateHandler.getDateInstance(date).toDateString();
  }

  /**
   * Returns the date object as a string in ISO 8601 format
   * (e.g., `"2023-04-23T12:34:56.000Z"`).
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {string} The date as an ISO 8601 string.
   * @example
   * ```yaml
   * iso_date:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.toISOString}"]
   * ```
   */
  toISOString(date: DateInput): string {
    return DateHandler.getDateInstance(date).toISOString();
  }

  /**
   * Returns an ISO date (or current date if none provided) as a string
   * formatted as a compact decimal (e.g., `"20240423123456.789"`). This is
   * useful for sorting dates in string format while keeping the output more
   * human-friendly than `date.valueOf`. Uses the `formatISODate` utility
   * internally.
   *
   * @param {DateInput} [date] - Optional date value (ISO string, milliseconds, or Date). Defaults to now.
   * @returns {string} The date formatted as a compact decimal string.
   * @example
   * ```yaml
   * formatted_date:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.toISOXString}"]
   * ```
   */
  toISOXString(date?: DateInput): string {
    return formatISODate(date ? DateHandler.getDateInstance(date) : new Date());
  }

  /**
   * Returns the date object as a string in a JSON-compatible format, which
   * is similar to the ISO 8601 format.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {string} The date as a JSON-compatible string.
   * @example
   * ```yaml
   * json_date:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.toJSON}"]
   * ```
   */
  toJSON(date: DateInput): string {
    return DateHandler.getDateInstance(date).toJSON();
  }

  /**
   * Returns the date object as a string formatted according to the given
   * locale(s) and formatting options, showing only the date portion.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @param {string | string[]} [locales] - Optional locale string or array of locale strings (e.g., `"en-US"`).
   * @param {Intl.DateTimeFormatOptions} [options] - Optional formatting options.
   * @returns {string} The localized date string.
   * @example
   * ```yaml
   * localized_date:
   *   "@pipe":
   *     - ["{a.output.data.date}", "en-US"]
   *     - ["{@date.toLocaleDateString}"]
   * ```
   */
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

  /**
   * Returns the date object as a string formatted according to the given
   * locale(s) and formatting options, including both date and time portions.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @param {string | string[]} [locales] - Optional locale string or array of locale strings (e.g., `"en-US"`).
   * @param {Intl.DateTimeFormatOptions} [options] - Optional formatting options.
   * @returns {string} The localized date and time string.
   * @example
   * ```yaml
   * localized_date_time:
   *   "@pipe":
   *     - ["{a.output.data.date}", "en-US"]
   *     - ["{@date.toLocaleString}"]
   * ```
   */
  toLocaleString(
    date: DateInput,
    locales?: string | string[],
    options?: Intl.DateTimeFormatOptions,
  ): string {
    return DateHandler.getDateInstance(date).toLocaleString(locales, options);
  }

  /**
   * Returns the time portion of a date object as a string formatted
   * according to the given locale(s) and formatting options.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @param {string | string[]} [locales] - Optional locale string or array of locale strings (e.g., `"en-US"`).
   * @param {Intl.DateTimeFormatOptions} [options] - Optional formatting options.
   * @returns {string} The localized time string.
   * @example
   * ```yaml
   * localized_time:
   *   "@pipe":
   *     - ["{a.output.data.date}", "en-US"]
   *     - ["{@date.toLocaleTimeString}"]
   * ```
   */
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

  /**
   * Converts a date object to a string using the default formatting for the
   * local time zone.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {string} The date and time as a string in the local time zone.
   * @example
   * ```yaml
   * date_string:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.toString}"]
   * ```
   */
  toString(date: DateInput): string {
    return DateHandler.getDateInstance(date).toString();
  }

  /**
   * Returns the number of milliseconds since January 1, 1970, 00:00:00 UTC
   * for the given date components. Accepts between two and seven parameters.
   *
   * @param {number} year - The full year (e.g., 2023).
   * @param {number} month - The month (0 = January, 11 = December).
   * @param {number} [date] - Optional day of the month (1--31).
   * @param {number} [hours] - Optional hours (0--23).
   * @param {number} [minutes] - Optional minutes (0--59).
   * @param {number} [seconds] - Optional seconds (0--59).
   * @param {number} [ms] - Optional milliseconds (0--999).
   * @returns {number} Milliseconds since the Unix epoch for the given UTC date components.
   * @example
   * ```yaml
   * milliseconds_since_epoch:
   *   "@pipe":
   *     - ["{a.output.data.year}", "{a.output.data.month}", "{a.output.data.day}"]
   *     - ["{@date.UTC}"]
   * ```
   */
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

  /**
   * Returns the numeric value of the specified date object as the number of
   * milliseconds since January 1, 1970, 00:00:00 UTC.
   *
   * @param {DateInput} date - A date value (ISO string, milliseconds, or Date).
   * @returns {number} Milliseconds since the Unix epoch.
   * @example
   * ```yaml
   * milliseconds_since_epoch:
   *   "@pipe":
   *     - ["{a.output.data.date}"]
   *     - ["{@date.valueOf}"]
   * ```
   */
  valueOf(date: DateInput): number {
    return DateHandler.getDateInstance(date).valueOf();
  }
}

export { DateHandler };
