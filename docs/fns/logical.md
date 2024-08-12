# Logical Operators

Logical operators are used to combine multiple boolean expressions and return a single boolean value. The following logical operators are supported:

**Table of Contents**
- [logical.and](#logicaland)
- [logical.or](#logicalor)

## logical.and

The `logical.and` operator returns true if both boolean expressions evaluate to true.

### Example

Consider the following input JavaScript object:

**Object A:**
```javascript
{
  user: {
    isActive: true,
    hasPaid: true
  }
}
```

The goal is to check if the user is active and has made a payment. The `logical.and` function can be used in the mapping rules as follows:

```yaml
is_active_and_paid:
  "@pipe":
    - ["{a.user.isActive}", "{a.user.hasPaid}"]
    - ["{@logical.and}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  is_active_and_paid: true
}
```

## logical.or

The `logical.or` operator returns true if at least one of the boolean expressions evaluate to true.

### Example

Suppose there is the following input JavaScript object:

**Object B:**
```javascript
{
  user: {
    hasTrial: true,
    hasSubscription: false
  }
}
```

The objective is to determine if the user has either a trial or a paid subscription. The `logical.or` function can be employed in the mapping rules as follows:

```yaml
has_trial_or_subscription:
  "@pipe":
    - ["{b.user.hasTrial}", "{b.user.hasSubscription}"]
    - ["{@logical.or}"]
```

After executing the mapping rules, the resulting JavaScript object will be:

```javascript
{
  has_trial_or_subscription: true
}
```