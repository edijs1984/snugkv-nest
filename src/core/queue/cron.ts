const MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
const DAYS = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];

interface Field {
  set: Set<number>;
  star: boolean;
}

interface Parsed {
  sec: Field;
  min: Field;
  hour: Field;
  dom: Field;
  month: Field;
  dow: Field;
  hasSeconds: boolean;
}

function parseField(text: string, min: number, max: number, names?: string[], nameBase = 0): Field {
  const set = new Set<number>();
  let star = false;
  const value = (v: string): number => {
    const i = names ? names.indexOf(v.toLowerCase()) : -1;
    const n = i >= 0 ? i + nameBase : Number(v);
    if (!Number.isInteger(n)) throw new Error(`bad cron value "${v}"`);
    return n;
  };
  for (const part of text.split(',')) {
    const [range, stepText] = part.split('/');
    const step = stepText === undefined ? 1 : Number(stepText);
    if (!Number.isInteger(step) || step < 1) throw new Error(`bad cron step in "${part}"`);
    let lo: number;
    let hi: number;
    if (range === '*' || range === '?') {
      lo = min;
      hi = max;
      if (stepText === undefined) star = true;
    } else if (range!.includes('-')) {
      const [a, b] = range!.split('-');
      lo = value(a!);
      hi = value(b!);
    } else {
      lo = value(range!);
      hi = stepText === undefined ? lo : max;
    }
    if (lo < min || hi > max + (max === 6 ? 1 : 0) || lo > hi) throw new Error(`cron value out of range in "${part}"`);
    for (let v = lo; v <= hi; v += step) set.add(v);
  }
  return { set, star };
}

/** Parses 5 fields (minute hour day-of-month month day-of-week) or 6 with leading seconds. */
export function parseCron(pattern: string): Parsed {
  const parts = pattern.trim().split(/\s+/);
  if (parts.length !== 5 && parts.length !== 6) throw new Error(`cron pattern needs 5 or 6 fields: "${pattern}"`);
  const hasSeconds = parts.length === 6;
  const f = hasSeconds ? parts : ['0', ...parts];
  const dow = parseField(f[5]!, 0, 6, DAYS);
  if (dow.set.has(7)) {
    dow.set.delete(7);
    dow.set.add(0);
  }
  return {
    sec: parseField(f[0]!, 0, 59),
    min: parseField(f[1]!, 0, 59),
    hour: parseField(f[2]!, 0, 23),
    dom: parseField(f[3]!, 1, 31),
    month: parseField(f[4]!, 1, 12, MONTHS, 1),
    dow,
    hasSeconds,
  };
}

const formatters = new Map<string, Intl.DateTimeFormat>();

function offsetMs(instant: number, tz: string): number {
  if (tz === 'UTC') return 0;
  let f = formatters.get(tz);
  if (!f) {
    f = new Intl.DateTimeFormat('en-US', {
      timeZone: tz,
      hourCycle: 'h23',
      year: 'numeric',
      month: 'numeric',
      day: 'numeric',
      hour: 'numeric',
      minute: 'numeric',
      second: 'numeric',
    });
    formatters.set(tz, f);
  }
  const p: Record<string, number> = {};
  for (const part of f.formatToParts(new Date(instant))) if (part.type !== 'literal') p[part.type] = Number(part.value);
  const asUtc = Date.UTC(p.year!, p.month! - 1, p.day!, p.hour! % 24, p.minute!, p.second!);
  return asUtc - Math.floor(instant / 1000) * 1000;
}

/** Local wall-clock time of `instant` in `tz`, expressed as if it were UTC. */
function toNaive(instant: number, tz: string): number {
  return instant + offsetMs(instant, tz);
}

function fromNaive(naive: number, tz: string): number {
  if (tz === 'UTC') return naive;
  const guess = naive - offsetMs(naive, tz);
  return naive - offsetMs(guess, tz);
}

/**
 * The first time after `from` (ms since epoch) that matches the pattern, in the given IANA time zone
 * (default UTC). Throws on a bad pattern or when nothing matches within 8 years.
 */
export function nextCronTime(pattern: string, from: number, tz = 'UTC'): number {
  const c = parseCron(pattern);
  const step = c.hasSeconds ? 1000 : 60_000;
  let t = Math.floor(toNaive(from, tz) / step) * step + step;
  const limit = t + 8 * 366 * 86_400_000;
  while (t < limit) {
    const d = new Date(t);
    if (!c.month.set.has(d.getUTCMonth() + 1)) {
      t = Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 1);
      continue;
    }
    const domOk = c.dom.set.has(d.getUTCDate());
    const dowOk = c.dow.set.has(d.getUTCDay());
    const dayOk = !c.dom.star && !c.dow.star ? domOk || dowOk : domOk && dowOk;
    if (!dayOk) {
      t = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() + 1);
      continue;
    }
    if (!c.hour.set.has(d.getUTCHours())) {
      t = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), d.getUTCHours() + 1);
      continue;
    }
    if (!c.min.set.has(d.getUTCMinutes())) {
      t = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), d.getUTCHours(), d.getUTCMinutes() + 1);
      continue;
    }
    if (!c.sec.set.has(d.getUTCSeconds())) {
      t += 1000;
      continue;
    }
    const result = fromNaive(t, tz);
    if (result > from) return result;
    t += step;
  }
  throw new Error(`cron pattern "${pattern}" never matches`);
}
