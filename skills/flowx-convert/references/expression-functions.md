# ADF Expression Function Translation Reference

This reference documents how all 84 ADF expression functions are translated by `flowx.parser.expression_parser`.

## Translation Categories

- **notebook_code** — Translated to Python code embedded in the generated notebook body.
- **dab_ref** — Mapped to a DAB dynamic value reference (rare for functions; primarily for `@pipeline()`, `@activity()`, `@variables()`, `@item()`).
- **agentic** — Too complex for deterministic translation; returns `None` and requires LLM-assisted translation.

## String Functions (12) — notebook_code

| ADF Function | Python Translation | Notes |
|---|---|---|
| `concat(a, b, ...)` | `str(a) + str(b) + ...` | Variadic; handled by both dedicated resolver and dispatch table |
| `endsWith(text, search)` | `str(text).endswith(str(search))` | |
| `guid()` | `str(uuid4())` | |
| `guid('N')` | `str(uuid4()).replace('-', '')` | No-dash format |
| `indexOf(text, search)` | `str(text).lower().find(str(search).lower())` | ADF is case-insensitive |
| `lastIndexOf(text, search)` | `str(text).lower().rfind(str(search).lower())` | ADF is case-insensitive |
| `replace(text, old, new)` | `str(text).replace(str(old), str(new))` | |
| `split(text, delim)` | `str(text).split(str(delim))` | |
| `startsWith(text, search)` | `str(text).lower().startswith(str(search).lower())` | ADF is case-insensitive |
| `substring(text, start, length)` | `str(text)[int(start):int(start)+int(length)]` | |
| `toLower(text)` | `str(text).lower()` | |
| `toUpper(text)` | `str(text).upper()` | |
| `trim(text)` | `str(text).strip()` | |

## Collection Functions (10) — notebook_code

| ADF Function | Python Translation |
|---|---|
| `contains(collection, value)` | `(value in collection)` |
| `empty(collection)` | `(len(collection) == 0)` |
| `first(collection)` | `collection[0]` |
| `intersection(c1, c2, ...)` | `list(set(c1) & set(c2) & ...)` |
| `join(array, delim)` | `str(delim).join(str(x) for x in array)` |
| `last(collection)` | `collection[-1]` |
| `length(collection)` | `len(collection)` |
| `skip(collection, count)` | `collection[int(count):]` |
| `take(collection, count)` | `collection[:int(count)]` |
| `union(c1, c2, ...)` | `list(set(c1) \| set(c2) \| ...)` |

## Logical Functions (9) — notebook_code

| ADF Function | Python Translation |
|---|---|
| `and(a, b)` | `(a and b)` |
| `equals(a, b)` | `(a == b)` |
| `greater(a, b)` | `(a > b)` |
| `greaterOrEquals(a, b)` | `(a >= b)` |
| `if(expr, trueVal, falseVal)` | `(trueVal if expr else falseVal)` |
| `less(a, b)` | `(a < b)` |
| `lessOrEquals(a, b)` | `(a <= b)` |
| `not(expr)` | `(not expr)` |
| `or(a, b)` | `(a or b)` |

## Conversion Functions (24) — notebook_code / agentic

| ADF Function | Python Translation | Status |
|---|---|---|
| `array(value)` | `[value]` | notebook_code |
| `base64(value)` | `base64.b64encode(str(value).encode()).decode()` | notebook_code |
| `base64ToBinary(value)` | `base64.b64decode(value)` | notebook_code |
| `base64ToString(value)` | `base64.b64decode(value).decode()` | notebook_code |
| `binary(value)` | `str(value).encode()` | notebook_code |
| `bool(value)` | `bool(value)` | notebook_code |
| `coalesce(a, b, ...)` | `next((x for x in [a, b, ...] if x is not None), None)` | notebook_code |
| `createArray(a, b, ...)` | `[a, b, ...]` | notebook_code |
| `dataUri(value)` | — | **agentic** (rare, complex encoding) |
| `dataUriToBinary(value)` | — | **agentic** |
| `dataUriToString(value)` | — | **agentic** |
| `decodeBase64(value)` | `base64.b64decode(value).decode()` | notebook_code (alias of base64ToString) |
| `decodeDataUri(value)` | — | **agentic** |
| `decodeUriComponent(value)` | `urllib.parse.unquote(value)` | notebook_code |
| `encodeUriComponent(value)` | `urllib.parse.quote(str(value), safe='')` | notebook_code |
| `float(value)` | `float(value)` | notebook_code |
| `int(value)` | `int(value)` | notebook_code |
| `json(value)` | `json.loads(value)` | notebook_code |
| `string(value)` | `str(value)` | notebook_code |
| `uriComponent(value)` | `urllib.parse.quote(str(value), safe='')` | notebook_code (alias of encodeUriComponent) |
| `uriComponentToBinary(value)` | — | **agentic** |
| `uriComponentToString(value)` | `urllib.parse.unquote(value)` | notebook_code (alias of decodeUriComponent) |
| `xml(value)` | — | **agentic** (XML handling complex) |
| `xpath(xml, expr)` | — | **agentic** (XPath complex) |

## Math Functions (9) — notebook_code

| ADF Function | Python Translation |
|---|---|
| `add(a, b)` | `(a + b)` |
| `div(a, b)` | `(a // b)` (integer division, matching ADF semantics) |
| `max(a, b, ...)` | `max(a, b, ...)` |
| `min(a, b, ...)` | `min(a, b, ...)` |
| `mod(a, b)` | `(a % b)` |
| `mul(a, b)` | `(a * b)` |
| `rand(min, max)` | `random.randint(min, max-1)` |
| `range(start, count)` | `list(range(start, start + count))` |
| `sub(a, b)` | `(a - b)` |

## Date/Time Functions (20) — notebook_code / agentic

All date functions use `from datetime import datetime, timezone, timedelta`.

| ADF Function | Python Translation | Status |
|---|---|---|
| `addDays(ts, days, fmt?)` | `(datetime.fromisoformat(ts) + timedelta(days=days)).strftime(fmt)` | notebook_code |
| `addHours(ts, hours, fmt?)` | `(datetime.fromisoformat(ts) + timedelta(hours=hours)).strftime(fmt)` | notebook_code |
| `addMinutes(ts, minutes, fmt?)` | `(datetime.fromisoformat(ts) + timedelta(minutes=minutes)).strftime(fmt)` | notebook_code |
| `addSeconds(ts, seconds, fmt?)` | `(datetime.fromisoformat(ts) + timedelta(seconds=seconds)).strftime(fmt)` | notebook_code |
| `addToTime(ts, interval, unit, fmt?)` | `(datetime.fromisoformat(ts) + timedelta(**{unit}=interval)).strftime(fmt)` | notebook_code |
| `convertFromUtc(ts, tz, fmt?)` | — | **agentic** (timezone handling complex) |
| `convertTimeZone(ts, srcTz, destTz, fmt?)` | — | **agentic** (timezone handling complex) |
| `convertToUtc(ts, srcTz, fmt?)` | — | **agentic** (timezone handling complex) |
| `dayOfMonth(ts)` | `datetime.fromisoformat(ts).day` | notebook_code |
| `dayOfWeek(ts)` | `datetime.fromisoformat(ts).isoweekday() % 7` | notebook_code (ADF: 0=Sunday) |
| `dayOfYear(ts)` | `datetime.fromisoformat(ts).timetuple().tm_yday` | notebook_code |
| `formatDateTime(ts, fmt?)` | `datetime.fromisoformat(ts).strftime(converted_fmt)` | notebook_code |
| `getFutureTime(interval, unit, fmt?)` | `(datetime.now(timezone.utc) + timedelta(...)).strftime(fmt)` | notebook_code |
| `getPastTime(interval, unit, fmt?)` | `(datetime.now(timezone.utc) - timedelta(...)).strftime(fmt)` | notebook_code |
| `startOfDay(ts, fmt?)` | `datetime.fromisoformat(ts).replace(hour=0,...).strftime(fmt)` | notebook_code |
| `startOfHour(ts, fmt?)` | `datetime.fromisoformat(ts).replace(minute=0,...).strftime(fmt)` | notebook_code |
| `startOfMonth(ts, fmt?)` | `datetime.fromisoformat(ts).replace(day=1,...).strftime(fmt)` | notebook_code |
| `subtractFromTime(ts, interval, unit, fmt?)` | `(datetime.fromisoformat(ts) - timedelta(...)).strftime(fmt)` | notebook_code |
| `ticks(ts)` | — | **agentic** (.NET ticks conversion complex) |
| `utcNow(fmt?)` | `datetime.now(timezone.utc).strftime(fmt)` | notebook_code (dedicated handler) |

## Summary

| Category | Total | notebook_code | agentic |
|---|---|---|---|
| String | 12 | 12 | 0 |
| Collection | 10 | 10 | 0 |
| Logical | 9 | 9 | 0 |
| Conversion | 24 | 17 | 7 |
| Math | 9 | 9 | 0 |
| Date/Time | 20 | 16 | 4 |
| **Total** | **84** | **73** | **11** |

## Agentic Functions (11 total)

These functions return `None` from `resolve_expression()` and require LLM-assisted translation:

1. `dataUri` — Data URI encoding (rare in ADF pipelines)
2. `dataUriToBinary` — Data URI to binary conversion
3. `dataUriToString` — Data URI to string conversion
4. `decodeDataUri` — Data URI decoding
5. `uriComponentToBinary` — URI component to binary
6. `xml` — XML parsing (complex DOM handling)
7. `xpath` — XPath evaluation (requires XML context)
8. `convertFromUtc` — UTC to timezone conversion (Windows timezone names)
9. `convertTimeZone` — Timezone conversion (Windows timezone names)
10. `convertToUtc` — Timezone to UTC conversion (Windows timezone names)
11. `ticks` — .NET DateTime ticks (100-nanosecond intervals since 0001-01-01)
