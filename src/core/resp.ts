/** Minimal RESP2 encoder and incremental parser. */

export class RespError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'RespError';
  }
  /** The error prefix, e.g. `NOGROUP` or `BUSYGROUP`. */
  get code(): string {
    const i = this.message.indexOf(' ');
    return i < 0 ? this.message : this.message.slice(0, i);
  }
}

export type Reply = string | number | null | RespError | Reply[];
export type Arg = string | number | Buffer;

const CRLF = Buffer.from('\r\n');
const EMPTY = Buffer.alloc(0);
const INCOMPLETE = Symbol('incomplete');

export function encodeCommand(args: ReadonlyArray<Arg>): Buffer {
  const parts: Buffer[] = [Buffer.from(`*${args.length}\r\n`)];
  for (const a of args) {
    const b = Buffer.isBuffer(a) ? a : Buffer.from(String(a), 'utf8');
    parts.push(Buffer.from(`$${b.length}\r\n`), b, CRLF);
  }
  return Buffer.concat(parts);
}

export class RespParser {
  private buf: Buffer = EMPTY;
  private pos = 0;

  push(chunk: Buffer, onReply: (reply: Reply) => void): void {
    this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk;
    this.pos = 0;
    for (;;) {
      const start = this.pos;
      if (start >= this.buf.length) break;
      const value = this.read();
      if (value === INCOMPLETE) {
        this.pos = start;
        break;
      }
      onReply(value);
    }
    this.buf = this.pos >= this.buf.length ? EMPTY : this.buf.subarray(this.pos);
    this.pos = 0;
  }

  private read(): Reply | typeof INCOMPLETE {
    const b = this.buf;
    const p = this.pos;
    if (p >= b.length) return INCOMPLETE;
    const type = b[p];
    const eol = b.indexOf(CRLF, p + 1);
    if (eol < 0) return INCOMPLETE;
    const line = b.toString('utf8', p + 1, eol);
    switch (type) {
      case 43: // +
        this.pos = eol + 2;
        return line;
      case 45: // -
        this.pos = eol + 2;
        return new RespError(line);
      case 58: // :
        this.pos = eol + 2;
        return Number(line);
      case 36: { // $
        const n = parseInt(line, 10);
        if (n < 0) {
          this.pos = eol + 2;
          return null;
        }
        const end = eol + 2 + n;
        if (b.length < end + 2) return INCOMPLETE;
        this.pos = end + 2;
        return b.toString('utf8', eol + 2, end);
      }
      case 42: { // *
        const n = parseInt(line, 10);
        this.pos = eol + 2;
        if (n < 0) return null;
        const out: Reply[] = new Array(n);
        for (let i = 0; i < n; i++) {
          const v = this.read();
          if (v === INCOMPLETE) return INCOMPLETE;
          out[i] = v;
        }
        return out;
      }
      default:
        throw new Error(`protocol error: unexpected byte 0x${(type ?? 0).toString(16)}`);
    }
  }
}
