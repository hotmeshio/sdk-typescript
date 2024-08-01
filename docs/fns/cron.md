# Cron Functions

Cron functions are based on the [cron](https://en.wikipedia.org/wiki/Cron) format.

**Table of Contents**
- [cron.nextDelay](#cronnextdelay)

## cron.nextDelay

The `cron.nextDelay` function calculates the next date and time (per the system clock) that satisfies the cron expression. It then returns the value in seconds from now (the delay).

### Example

Suppose there is the following input JSON with a cron expression that runs every day at midnight:

**Object A:**
```json
{
  "expressions": {
    "cron": "0 0 * * *"
  }
}
```

The goal is to create a new object with the value of the converted expression, namely when to run the cron expression next. The `cron.nextDelay` function can be used in the mapping rules as follows to return the number of seconds until the next cron expression:

```yaml
cron_next_result:
  "@pipe":
    - ["{a.expressions.cron}"]
    - ["{@cron.nextDelay}"]
```

After executing the mapping rules, the resulting JSON object will be as follows (1 day in seconds):

```json
{
  "cron_next_result": 86400
}
```
