# Date Functions

In this section, various Date functions will be explored, which are available for use in HotMesh mapping rules. Although inspired by JavaScript, they have been adapted to follow a functional approach. Each transformation is a function that expects one or more input parameters from the prior row in the @pipe structure.

Many of the Date functions listed here accept various input formats (`Date`, `string`, and `number`), implicitly casting to dates as necessary. The *ISO 8601 Extended Format* is supported which includes date strings like `YYYY-MM-DD`, `YYYY-MM-DDTHH:mm:ss`, and =`YYYY-MM-DDTHH:mm:ss.sss`. 

The following regular expression is used to identify valid ISO 8601 date strings:

```javascript
/^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z)?$/
```

The Date functions can also recognize strings or numbers representing the number of milliseconds since January 1, 1970, 00:00:00 UTC, allowing for Unix timestamps.

**Table of Contents**
- [date.fromISOString](#datefromisostring)
- [date.now](#datenow)
- [date.parse](#dateparse)
- [date.getDate](#dategetdate)
- [date.getDay](#dategetday)
- [date.getFullYear](#dategetfullyear)
- [date.getHours](#dategethours)
- [date.getMilliseconds](#dategetmilliseconds)
- [date.getMinutes](#dategetminutes)
- [date.getMonth](#dategetmonth)
- [date.getSeconds](#dategetseconds)
- [date.getTime](#dategettime)
- [date.getTimezoneOffset](#dategettimezoneoffset)
- [date.getUTCDate](#dategetutcdate)
- [date.getUTCDay](#dategetutcday)
- [date.getUTCFullYear](#dategetutcfullyear)
- [date.getUTCHours](#dategetutchours)
- [date.getUTCMilliseconds](#dategetutcmilliseconds)
- [date.getUTCMinutes](#dategetutcminutes)
- [date.getUTCMonth](#dategetutcmonth)
- [date.getUTCSeconds](#dategetutcseconds)
- [date.setMilliseconds](#datesetmilliseconds)
- [date.setMinutes](#datesetminutes)
- [date.setMonth](#datesetmonth)
- [date.setSeconds](#datesetseconds)
- [date.setTime](#datesettime)
- [date.setUTCDate](#datesetutcdate)
- [date.setUTCFullYear](#datesetutcfullyear)
- [date.setUTCHours](#datesetutchours)
- [date.setUTCMilliseconds](#datesetutcmilliseconds)
- [date.setUTCMinutes](#datesetutcminutes)
- [date.setUTCMonth](#datesetutcmonth)
- [date.setUTCSeconds](#datesetutcseconds)
- [date.setDate](#datesetdate)
- [date.setFullYear](#datesetfullyear)
- [date.setHours](#datesethours)
- [date.toDateString](#datetodatestring)
- [date.toISOString](#datetoisostring)
- [date.toJSON](#datetojson)
- [date.toLocaleDateString](#datetolocaledatestring)
- [date.toLocaleString](#datetolocalestring)
- [date.toLocaleTimeString](#datetolocaletimestring)
- [date.toString](#datetostring)
- [date.UTC](#dateutc)
- [date.valueOf](#datevalueof)

## date.fromISOString

The `date.fromISOString` function creates a new Date object from a string representation of a date in ISO 8601 format. It takes one parameter: the ISO string representing the date.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "isoDate": "2023-04-23T12:00:00.000Z"
    }
  }
}
```

The goal is to create a new object with the date object created from the ISO string. The `date.fromISOString` function can be used in the mapping rules as follows:

```yaml
date_obj: 
  "@pipe":
    - ["{a.output.data.isoDate}"]
    - ["{@date.fromISOString}"]
```

After executing the mapping rules, the resulting JSON object will be:

```json
{
  "date_obj": "2023-04-23T12:00:00.000Z"
}
```

## date.now

The `date.now` function returns the current time in milliseconds since the Unix epoch (January 1, 1970 00:00:00 UTC). It does not take any parameters.

### Example

The goal is to create a new object with the current time in milliseconds. The `date.now` function can be used in the mapping rules as follows:

```yaml
current_time: 
  "@pipe":
    - ["{@date.now}"]
```

After executing the mapping rules, the resulting JSON object will contain the current time in milliseconds, for example:

```json
{
  "current_time": 1677986754231
}
```

## date.parse

The `date.parse` function parses a string representation of a date and returns the number of milliseconds since the Unix epoch (January 1, 1970 00:00:00 UTC). It takes one parameter: the date string to parse.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "dateString": "April 23, 2023 12:00:00"
    }
  }
}
```

The goal is to create a new object with the number of milliseconds since the Unix epoch for the given date string. The `date.parse` function can be used in the mapping rules as follows:

```yaml
date_milliseconds: 
  "@pipe":
    - ["{a.output.data.dateString}"]
    - ["{@date.parse}"]
```

After executing the mapping rules, the resulting JSON object will contain the number of milliseconds since the Unix epoch for the given date, for example:

```json
{
  "date_milliseconds": 1677982800000
}
```

## date.getDate

The `date.getDate` function returns the day of the month (from 1 to 31) for the specified date. It takes one parameter: the date object for which to get the day of the month.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:00:00.000Z"
    }
  }
}
```

The goal is to create a new object with the day of the month for the given date. The `date.getDate` function can be used in the mapping rules as follows:

```yaml
day_of_month: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.getDate}"]
```

After executing the mapping rules, the resulting JSON object will contain the day of the month for the given date:

```json
{
  "day_of_month": 23
}
```

## date.getDay

The `date.getDay` function returns the day of the week (from 0 to 6, where 0 represents Sunday) for the specified date. It takes one parameter: the date object for which to get the day of the week.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:00:00.000Z"
    }
  }
}
```

The goal is to create a new object with the day of the week for the given date. The `date.getDay` function can be used in the mapping rules as follows:

```yaml
day_of_week: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.getDay}"]
```

After executing the mapping rules, the resulting JSON object will contain the day of the week for the given date:

```json
{
  "day_of_week": 0
}
```

## date.getFullYear

The `date.getFullYear` function returns the full year (4 digits) for the specified date. It takes one parameter: the date object for which to get the full year.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:00:00.000Z"
    }
  }
}
```

The goal is to create a new object with the full year for the given date. The `date.getFullYear` function can be used in the mapping rules as follows:

```yaml
full_year: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.getFullYear}"]
```

After executing the mapping rules, the resulting JSON object will contain the full year for the given date:

```json
{
  "full_year": 2023
}
```

## date.getHours

The `date.getHours` function returns the hour (from 0 to 23) for the specified date. It takes one parameter: the date object for which to get the hour.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:00:00.000Z"
    }
  }
}
```

The goal is to create a new object with the hour for the given date. The `date.getHours` function can be used in the mapping rules as follows:

```yaml
hour: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.getHours}"]
```

After executing the mapping rules, the resulting JSON object will contain the hour for the given date:

```json
{
  "hour": 12
}
```

## date.getMilliseconds

The `date.getMilliseconds` function returns the milliseconds (from 0 to 999) for the specified date. It takes one parameter: the date object for which to get the milliseconds.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:00:00.123Z"
    }
  }
}
```

The goal is to create a new object with the milliseconds for the given date. The `date.getMilliseconds` function can be used in the mapping rules as follows:

```yaml
milliseconds: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.getMilliseconds}"]
```

After executing the mapping rules, the resulting JSON object will contain the milliseconds for the given date:

```json
{
  "milliseconds": 123
}
```

## date.getMinutes

The `date.getMinutes` function returns the minutes (from 0 to 59) for the specified date. It takes one parameter: the date object for which to get the minutes.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:00.000Z"
    }
  }
}
```

The goal is to create a new object with the minutes for the given date. The `date.getMinutes` function can be used in the mapping rules as follows:

```yaml
minutes: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.getMinutes}"]
```

After executing the mapping rules, the resulting JSON object will contain the minutes for the given date:

```json
{
  "minutes": 34
}
```

## date.getMonth

The `date.getMonth` function returns the month (from 0 to 11) for the specified date, where 0 corresponds to January and 11 corresponds to December. It takes one parameter: the date object for which to get the month.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:00.000Z"
    }
  }
}
```

The goal is to create a new object with the month for the given date. The `date.getMonth` function can be used in the mapping rules as follows:

```yaml
month: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.getMonth}"]
```

After executing the mapping rules, the resulting JSON object will contain the month for the given date:

```json
{
  "month": 3
}
```

## date.getSeconds

The `date.getSeconds` function returns the seconds (from 0 to 59) for the specified date. It takes one parameter: the date object for which to get the seconds.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the seconds for the given date. The `date.getSeconds` function can be used in the mapping rules as follows:

```yaml
seconds: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.getSeconds}"]
```

After executing the mapping rules, the resulting JSON object will contain the seconds for the given date:

```json
{
  "seconds": 56
}
```

## date.getTime

The `date.getTime` function returns the number of milliseconds since January 1, 1970, 00:00:00 UTC for the specified date. It takes one parameter: the date object for which to get the time value.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the time value for the given date. The `date.getTime` function can be used in the mapping rules as follows:

```yaml
time: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.getTime}"]
```

After executing the mapping rules, the resulting JSON object will contain the time value for the given date:

```json
{
  "time": 1672380896000
}
```

## date.getTimezoneOffset

The `date.getTimezoneOffset` function returns the time zone difference, in minutes, from the current locale (host system settings) to UTC. It takes one parameter: the date object for which to get the time zone offset.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the time zone offset for the given date. The `date.getTimezoneOffset` function can be used in the mapping rules as follows:

```yaml
timezone_offset: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.getTimezoneOffset}"]
```

After executing the mapping rules, the resulting JSON object will contain the time zone offset for the given date:

```json
{
  "timezone_offset": -240
}
```

## date.getUTCDate

The `date.getUTCDate` function returns the day of the month, from 1 to 31, for the specified date according to universal time (UTC). It takes one parameter: the date object for which to get the day of the month.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the UTC day of the month for the given date. The `date.getUTCDate` function can be used in the mapping rules as follows:

```yaml
utc_day: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.getUTCDate}"]
```

After executing the mapping rules, the resulting JSON object will contain the UTC day of the month for the given date:

```json
{
  "utc_day": 23
}
```

## date.getUTCDay

The `date.getUTCDay` function returns the day of the week, from 0 (Sunday) to 6 (Saturday), for the specified date according to universal time (UTC). It takes one parameter: the date object for which to get the day of the week.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the UTC day of the week for the given date. The `date.getUTCDay` function can be used in the mapping rules as follows:

```yaml
utc_weekday: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.getUTCDay}"]
```

After executing the mapping rules, the resulting JSON object will contain the UTC day of the week for the given date:

```json
{
  "utc_weekday": 0
}
```

## date.getUTCFullYear

The `date.getUTCFullYear` function returns the year of the specified date according to universal time (UTC). It takes one parameter: the date object for which to get the year.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the UTC year for the given date. The `date.getUTCFullYear` function can be used in the mapping rules as follows:

```yaml
utc_year: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.getUTCFullYear}"]
```

After executing the mapping rules, the resulting JSON object will contain the UTC year for the given date:

```json
{
  "utc_year": 2023
}
```

## date.getUTCHours

The `date.getUTCHours` function returns the hours of the specified date according to universal time (UTC), where 0 represents midnight and 23 represents 11 PM. It takes one parameter: the date object for which to get the hours.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the UTC hours for the given date. The `date.getUTCHours` function can be used in the mapping rules as follows:

```yaml
utc_hours: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.getUTCHours}"]
```

After executing the mapping rules, the resulting JSON object will contain the UTC hours for the given date:

```json
{
  "utc_hours": 12
}
```

## date.getUTCMilliseconds

The `date.getUTCMilliseconds` function returns the milliseconds of the specified date according to universal time (UTC), ranging from 0 to 999. It takes one parameter: the date object for which to get the milliseconds.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.789Z"
    }
  }
}
```

The goal is to create a new object with the UTC milliseconds for the given date. The `date.getUTCMilliseconds` function can be used in the mapping rules as follows:

```yaml
utc_milliseconds: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.getUTCMilliseconds}"]
```

After executing the mapping rules, the resulting JSON object will contain the UTC milliseconds for the given date:

```json
{
  "utc_milliseconds": 789
}
```

## date.getUTCMinutes

The `date.getUTCMinutes` function returns the minutes of the specified date according to universal time (UTC), ranging from 0 to 59. It takes one parameter: the date object for which to get the minutes.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the UTC minutes for the given date. The `date.getUTCMinutes` function can be used in the mapping rules as follows:

```yaml
utc_minutes: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.getUTCMinutes}"]
```

After executing the mapping rules, the resulting JSON object will contain the UTC minutes for the given date:

```json
{
  "utc_minutes": 34
}
```

## date.getUTCMonth

The `date.getUTCMonth` function returns the month of the specified date according to universal time (UTC), ranging from 0 (January) to 11 (December). It takes one parameter: the date object for which to get the month.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the UTC month for the given date. The `date.getUTCMonth` function can be used in the mapping rules as follows:

```yaml
utc_month: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.getUTCMonth}"]
```

After executing the mapping rules, the resulting JSON object will contain the UTC month for the given date:

```json
{
  "utc_month": 3
}
```

## date.getUTCSeconds

The `date.getUTCSeconds` function returns the seconds of the specified date according to universal time (UTC), ranging from 0 to 59. It takes one parameter: the date object for which to get the seconds.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the UTC seconds for the given date. The `date.getUTCSeconds` function can be used in the mapping rules as follows:

```yaml
utc_seconds: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.getUTCSeconds}"]
```

After executing the mapping rules, the resulting JSON object will contain the UTC seconds for the given date:

```json
{
  "utc_seconds": 56
}
```

## date.setMilliseconds

The `date.setMilliseconds` function sets the milliseconds value of a date object, ranging from 0 to 999. It takes two parameters: the date object for which to set the milliseconds and the milliseconds value to set.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date after setting the milliseconds value to 123. The `date.setMilliseconds` function can be used in the mapping rules as follows:

```yaml
new_date: 
  "@pipe":
    - ["{a.output.data.date}", 123]
    - ["{@date.setMilliseconds}"]
```

After executing the mapping rules, the resulting JSON object will contain the updated date:

```json
{
  "new_date": "2023-04-23T12:34:56.123Z"
}
```

## date.setMinutes

The `date.setMinutes` function sets the minutes value of a date object, ranging from 0 to 59. It takes four parameters: the date object for which to set the minutes, the minutes value to set, and optionally, the seconds and milliseconds values to set.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date after setting the minutes value to 45 and the seconds value to 30. The `date.setMinutes` function can be used in the mapping rules as follows:

```yaml
new_date: 
  "@pipe":
    - ["{a.output.data.date}", 45, 30]
    - ["{@date.setMinutes}"]
```

After executing the mapping rules, the resulting JSON object will contain the updated date:

```json
{
  "new_date": "2023-04-23T12:45:30.000Z"
}
```

## date.setMonth

The `date.setMonth` function sets the month value of a date object, ranging from 0 to 11, where 0 is January and 11 is December. It takes three parameters: the date object for which to set the month, the month value to set, and optionally, the day of the month value to set.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date after setting the month value to 7 (August) and the day value to 15. The `date.setMonth` function can be used in the mapping rules as follows:

```yaml
new_date: 
  "@pipe":
    - ["{a.output.data.date}", 7, 15]
    - ["{@date.setMonth}"]
```

After executing the mapping rules, the resulting JSON object will contain the updated date:

```json
{
  "new_date": "2023-08-15T12:34:56.000Z"
}
```

## date.setSeconds

The `date.setSeconds` function sets the seconds value of a date object, ranging from 0 to 59. It takes three parameters: the date object for which to set the seconds, the seconds value to set, and optionally, the milliseconds value to set.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date after setting the seconds value to 30 and the milliseconds value to 123. The `date.setSeconds` function can be used in the mapping rules as follows:

```yaml
new_date: 
  "@pipe":
    - ["{a.output.data.date}", 30, 123]
    - ["{@date.setSeconds}"]
```

After executing the mapping rules, the resulting JSON object will contain the updated date:

```json
{
  "new_date": "2023-04-23T12:34:30.123Z"
}
```

## date.setTime

The `date.setTime` function sets the date object to the time represented by the number of milliseconds since January 1, 1970, 00:00:00 UTC. It takes two parameters: the date object for which to set the time and the number of milliseconds since the Unix epoch.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date after setting the time to a specific number of milliseconds since the Unix epoch. The `date.setTime` function can be used in the mapping rules as follows:

```yaml
new_date: 
  "@pipe":
    - ["{a.output.data.date}", 1620000000000]
    - ["{@date.setTime}"]
```

After executing the mapping rules, the resulting JSON object will contain the updated date:

```json
{
  "new_date": "2021-05-02T21:20:00.000Z"
}
```

## date.setUTCDate

The `date.setUTCDate` function sets the day of the month value of a date object, ranging from 1 to 31, according to universal time. It takes two parameters: the date object for which to set the UTC day of the month and the day of the month value to set.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date after setting the UTC day of the month value to 15. The `date.setUTCDate` function can be used in the mapping rules as follows:

```yaml
new_date: 
  "@pipe":
    - ["{a.output.data.date}", 15]
    - ["{@date.setUTCDate}"]
```

After executing the mapping rules, the resulting JSON object will contain the updated date:

```json
{
  "new_date": "2023-04-15T12:34:56.000Z"
}
```

## date.setUTCFullYear

The `date.setUTCFullYear` function sets the full year value of a date object, according to universal time. It takes four parameters: the date object for which to set the UTC full year value, the full year value to set, and optional parameters for the month and day of the month values.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date after setting the UTC full year value to 2025. The `date.setUTCFullYear` function can be used in the mapping rules as follows:

```yaml
new_date: 
  "@pipe":
    - ["{a.output.data.date}", 2025]
    - ["{@date.setUTCFullYear}"]
```

After executing the mapping rules, the resulting JSON object will contain the updated date:

```json
{
  "new_date": "2025-04-23T12:34:56.000Z"
}
```

## date.setUTCHours

The `date.setUTCHours` function sets the hours value of a date object, ranging from 0 to 23, according to universal time. It takes five parameters: the date object for which to set the UTC hours value, the hours value to set, and optional parameters for the minutes, seconds, and milliseconds values.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date after setting the UTC hours value to 18. The `date.setUTCHours` function can be used in the mapping rules as follows:

```yaml
new_date: 
  "@pipe":
    - ["{a.output.data.date}", 18]
    - ["{@date.setUTCHours}"]
```

After executing the mapping rules, the resulting JSON object will contain the updated date:

```json
{
  "new_date": "2023-04-23T18:34:56.000Z"
}
```

## date.setUTCMilliseconds

The `date.setUTCMilliseconds` function sets the milliseconds value of a date object, ranging from 0 to 999, according to universal time. It takes two parameters: the date object for which to set the UTC milliseconds value, and the milliseconds value to set.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date after setting the UTC milliseconds value to 500. The `date.setUTCMilliseconds` function can be used in the mapping rules as follows:

```yaml
new_date: 
  "@pipe":
    - ["{a.output.data.date}", 500]
    - ["{@date.setUTCMilliseconds}"]
```

After executing the mapping rules, the resulting JSON object will contain the updated date:

```json
{
  "new_date": "2023-04-23T12:34:56.500Z"
}
```

## date.setUTCMinutes

The `date.setUTCMinutes` function sets the minutes value of a date object, ranging from 0 to 59, according to universal time. It takes four parameters: the date object for which to set the UTC minutes value, the minutes value to set, and optional parameters for the seconds and milliseconds values.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date after setting the UTC minutes value to 45. The `date.setUTCMinutes` function can be used in the mapping rules as follows:

```yaml
new_date: 
  "@pipe":
    - ["{a.output.data.date}", 45]
    - ["{@date.setUTCMinutes}"]
```

After executing the mapping rules, the resulting JSON object will contain the updated date:

```json
{
  "new_date": "2023-04-23T12:45:56.000Z"
}
```

## date.setUTCMonth

The `date.setUTCMonth` function sets the month value of a date object, ranging from 0 (January) to 11 (December), according to universal time. It takes three parameters: the date object for which to set the UTC month value, the month value to set, and an optional parameter for the day value.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date after setting the UTC month value to 2 (March). The `date.setUTCMonth` function can be used in the mapping rules as follows:

```yaml
new_date: 
  "@pipe":
    - ["{a.output.data.date}", 2]
    - ["{@date.setUTCMonth}"]
```

After executing the mapping rules, the resulting JSON object will contain the updated date:

```json
{
  "new_date": "2023-03-23T12:34:56.000Z"
}
```

## date.setUTCSeconds

The `date.setUTCSeconds` function sets the seconds value of a date object, ranging from 0 to 59, according to universal time. It takes three parameters: the date object for which to set the UTC seconds value, the seconds value to set, and an optional parameter for the milliseconds value.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date after setting the UTC seconds value to 30. The `date.setUTCSeconds` function can be used in the mapping rules as follows:

```yaml
new_date: 
  "@pipe":
    - ["{a.output.data.date}", 30]
    - ["{@date.setUTCSeconds}"]
```

After executing the mapping rules, the resulting JSON object will contain the updated date:

```json
{
  "new_date": "2023-04-23T12:34:30.000Z"
}
```

## date.setDate

The `date.setDate` function sets the day of the month value of a date object, ranging from 1 to 31, according to local time. It takes two parameters: the date object for which to set the day of the month value, and the day value to set.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date after setting the day of the month value to 15. The `date.setDate` function can be used in the mapping rules as follows:

```yaml
new_date: 
  "@pipe":
    - ["{a.output.data.date}", 15]
    - ["{@date.setDate}"]
```

After executing the mapping rules, the resulting JSON object will contain the updated date:

```json
{
  "new_date": "2023-04-15T12:34:56.000Z"
}
```

## date.setFullYear

The `date.setFullYear` function sets the full year value of a date object, according to local time. It takes four parameters: the date object for which to set the full year value, the year value to set, and optional parameters for the month and day values.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date after setting the full year value to 2024. The `date.setFullYear` function can be used in the mapping rules as follows:

```yaml
new_date: 
  "@pipe":
    - ["{a.output.data.date}", 2024]
    - ["{@date.setFullYear}"]
```

After executing the mapping rules, the resulting JSON object will contain the updated date:

```json
{
  "new_date": "2024-04-23T12:34:56.000Z"
}
```

## date.setHours

The `date.setHours` function sets the hours value of a date object, according to local time. It takes up to five parameters: the date object for which to set the hours value, the hours value to set, and optional parameters for the minutes, seconds, and milliseconds values.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date after setting the hours value to 15. The `date.setHours` function can be used in the mapping rules as follows:

```yaml
new_date: 
  "@pipe":
    - ["{a.output.data.date}", 15]
    - ["{@date.setHours}"]
```

After executing the mapping rules, the resulting JSON object will contain the updated date:

```json
{
  "new_date": "2023-04-23T15:34:56.000Z"
}
```

## date.toDateString

The `date.toDateString` function returns the date portion of a date object in a human-readable form as a string. It takes one parameter: the date object for which to return the date portion string.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date portion of the date as a string. The `date.toDateString` function can be used in the mapping rules as follows:

```yaml
date_string: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.toDateString}"]
```

After executing the mapping rules, the resulting JSON object will contain the date portion string:

```json
{
  "date_string": "Sun Apr 23 2023"
}
```

## date.toISOString

The `date.toISOString` function returns the date object as a string in ISO 8601 format, which is a standardized format for representing date and time. It takes one parameter: the date object to convert to an ISO 8601 string.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date represented as an ISO 8601 string. The `date.toISOString` function can be used in the mapping rules as follows:

```yaml
iso_date: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.toISOString}"]
```

After executing the mapping rules, the resulting JSON object will contain the date as an ISO 8601 string:

```json
{
  "iso_date": "2023-04-23T12:34:56.000Z"
}
```

## date.toJSON

The `date.toJSON` function returns the date object as a string in a JSON-compatible format, which is similar to the ISO 8601 format. It takes one parameter: the date object to convert to a JSON-compatible string.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date represented as a JSON-compatible string. The `date.toJSON` function can be used in the mapping rules as follows:

```yaml
json_date: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.toJSON}"]
```

After executing the mapping rules, the resulting JSON object will contain the date as a JSON-compatible string:

```json
{
  "json_date": "2023-04-23T12:34:56.000Z"
}
```

## date.toLocaleDateString

The `date.toLocaleDateString` function returns the date object as a string, formatted according to the given locale(s) and formatting options. It takes up to three parameters: the date object to convert to a localized date string, an optional locale(s) parameter, and an optional options object.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date represented as a localized date string for the 'en-US' locale. The `date.toLocaleDateString` function can be used in the mapping rules as follows:

```yaml
localized_date: 
  "@pipe":
    - ["{a.output.data.date}", "en-US"]
    - ["{@date.toLocaleDateString}"]
```

After executing the mapping rules, the resulting JSON object will contain the date as a localized date string:

```json
{
  "localized_date": "4/23/2023"
}
```

## date.toLocaleString

The `date.toLocaleString` function returns the date object as a string, formatted according to the given locale(s) and formatting options. It takes up to three parameters: the date object to convert to a localized date and time string, an optional locale(s) parameter, and an optional options object.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date and time represented as a localized string for the 'en-US' locale. The `date.toLocaleString` function can be used in the mapping rules as follows:

```yaml
localized_date_time: 
  "@pipe":
    - ["{a.output.data.date}", "en-US"]
    - ["{@date.toLocaleString}"]
```

After executing the mapping rules, the resulting JSON object will contain the date and time as a localized string:

```json
{
  "localized_date_time": "4/23/2023, 12:34:56 PM"
}
```

## date.toLocaleTimeString

The `date.toLocaleTimeString` function returns the time portion of a date object as a string, formatted according to the given locale(s) and formatting options. It takes up to three parameters: the date object to convert to a localized time string, an optional locale(s) parameter, and an optional options object.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the time portion represented as a localized string for the 'en-US' locale. The `date.toLocaleTimeString` function can be used in the mapping rules as follows:

```yaml
localized_time: 
  "@pipe":
    - ["{a.output.data.date}", "en-US"]
    - ["{@date.toLocaleTimeString}"]
```

After executing the mapping rules, the resulting JSON object will contain the time portion as a localized string:

```json
{
  "localized_time": "12:34:56 PM"
}
```

## date.toString

The `date.toString` function converts a date object to a string, using the default formatting for the local time zone. It takes one parameter: the date object to be converted to a string.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the date and time represented as a string in the local time zone. The `date.toString` function can be used in the mapping rules as follows:

```yaml
date_string: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.toString}"]
```

After executing the mapping rules, the resulting JSON object will contain the date and time as a string in the local time zone:

```json
{
  "date_string": "Sun Apr 23 2023 12:34:56 GMT+0000 (Coordinated Universal Time)"
}
```

## date.UTC

The `date.UTC` function returns the number of milliseconds since January 1, 1970, 00:00:00 UTC for the given date components. It takes between two and seven parameters: year, month, an optional date, optional hours, optional minutes, optional seconds, and optional milliseconds.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "year": 2023,
      "month": 3,
      "day": 23
    }
  }
}
```

The goal is to create a new object with the number of milliseconds since January 1, 1970, 00:00:00 UTC for the provided date components. The `date.UTC` function can be used in the mapping rules as follows:

```yaml
milliseconds_since_epoch: 
  "@pipe":
    - ["{a.output.data.year}", "{a.output.data.month}", "{a.output.data.day}"]
    - ["{@date.UTC}"]
```

After executing the mapping rules, the resulting JSON object will contain the number of milliseconds since January 1, 1970, 00:00:00 UTC for the provided date components:

```json
{
  "milliseconds_since_epoch": 1670121600000
}
```

## date.valueOf

The `date.valueOf` function returns the numeric value of the specified date object as the number of milliseconds since January 1, 1970, 00:00:00 UTC. It takes one parameter: the date object to retrieve the value from.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2023-04-23T12:34:56.000Z"
    }
  }
}
```

The goal is to create a new object with the number of milliseconds since January 1, 1970, 00:00:00 UTC for the given date object. The `date.valueOf` function can be used in the mapping rules as follows:

```yaml
milliseconds_since_epoch: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.valueOf}"]
```

After executing the mapping rules, the resulting JSON object will contain the number of milliseconds since January 1, 1970, 00:00:00 UTC for the given date object:

```json
{
  "milliseconds_since_epoch": 1670213696000
}
```

## date.toISOXString

Returns an ISO date (or now) as a string formatted as a decimal. This is useful for sorting dates in a string format, while keeping the output more human-friendly than `Date.valueOf`.

### Example

Suppose there is the following input JSON object:

**Object A:**
```json
{
  "output": {
    "data": {
      "date": "2024-04-23T12:34:56.789Z"
    }
  }
}
```
The goal is to create a new string, formatted as a shortened ISO date:

```yaml
current_time: 
  "@pipe":
    - ["{a.output.data.date}"]
    - ["{@date.toISOXString}"]
```

After executing the mapping rules, the resulting JSON object will contain the current time in milliseconds, for example:

```json
{
  "current_time": "20240423123456.789"
}
```
