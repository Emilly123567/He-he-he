// src/server/main.ts
import http from "node:http";
import fs2 from "node:fs";
import path2 from "node:path";

// src/server/WebSocket.ts
import { createHash } from "node:crypto";
var GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
var WsConnection = class {
  remote;
  onmessage = null;
  onclose = null;
  buf = Buffer.alloc(0);
  /** Ganchos internos de encerramento (o consumidor usa onclose). */
  finishHooks = [];
  fragments = [];
  fragmentBytes = 0;
  closed = false;
  alive = true;
  socket;
  max;
  constructor(socket, remote, max2) {
    this.socket = socket;
    this.remote = remote;
    this.max = max2;
    socket.on("data", (d) => this.onData(d));
    socket.on("close", () => this.finish(1006, "conex\xE3o perdida"));
    socket.on("error", () => this.finish(1006, "erro de socket"));
  }
  get open() {
    return !this.closed;
  }
  send(text) {
    if (this.closed) return;
    this.write(1, Buffer.from(text, "utf8"));
  }
  close(code = 1e3, reason = "") {
    if (this.closed) return;
    const r = Buffer.from(reason.slice(0, 100), "utf8");
    const payload = Buffer.alloc(2 + r.length);
    payload.writeUInt16BE(code, 0);
    r.copy(payload, 2);
    this.write(8, payload);
    this.socket.end();
    this.finish(code, reason);
  }
  /** Keepalive: retorna false se o par não respondeu ao último ping. */
  heartbeat() {
    if (this.closed) return false;
    if (!this.alive) {
      this.socket.destroy();
      this.finish(1006, "sem resposta");
      return false;
    }
    this.alive = false;
    this.write(9, Buffer.alloc(0));
    return true;
  }
  finish(code, reason) {
    if (this.closed) return;
    this.closed = true;
    for (const f of this.finishHooks) f();
    this.onclose?.(code, reason);
  }
  write(opcode, payload) {
    if (this.closed && opcode !== 8) return;
    if (!this.socket.writable) return;
    const len = payload.length;
    let head;
    if (len < 126) {
      head = Buffer.from([128 | opcode, len]);
    } else if (len < 65536) {
      head = Buffer.alloc(4);
      head[0] = 128 | opcode;
      head[1] = 126;
      head.writeUInt16BE(len, 2);
    } else {
      head = Buffer.alloc(10);
      head[0] = 128 | opcode;
      head[1] = 127;
      head.writeBigUInt64BE(BigInt(len), 2);
    }
    this.socket.write(Buffer.concat([head, payload]));
  }
  onData(d) {
    try {
      this.parse(d);
    } catch (e) {
      console.error("[erro] ws quadro:", e?.message ?? e);
      this.socket.destroy();
      this.finish(1011, "erro interno");
    }
  }
  parse(d) {
    this.alive = true;
    this.buf = this.buf.length ? Buffer.concat([this.buf, d]) : d;
    if (this.buf.length > this.max * 2 + 14) return this.close(1009, "mensagem grande demais");
    for (; ; ) {
      if (this.closed || this.buf.length < 2) return;
      const b0 = this.buf[0], b1 = this.buf[1];
      const fin = (b0 & 128) !== 0;
      const opcode = b0 & 15;
      const masked = (b1 & 128) !== 0;
      let len = b1 & 127;
      let off = 2;
      if (len === 126) {
        if (this.buf.length < 4) return;
        len = this.buf.readUInt16BE(2);
        off = 4;
      } else if (len === 127) {
        if (this.buf.length < 10) return;
        const big = this.buf.readBigUInt64BE(2);
        if (big > BigInt(this.max)) return this.close(1009, "mensagem grande demais");
        len = Number(big);
        off = 10;
      }
      if (!masked) return this.close(1002, "quadro do cliente sem m\xE1scara");
      if (len > this.max) return this.close(1009, "mensagem grande demais");
      if (this.buf.length < off + 4 + len) return;
      const mask = this.buf.subarray(off, off + 4);
      const payload = Buffer.from(this.buf.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      this.buf = this.buf.subarray(off + 4 + len);
      this.frame(fin, opcode, payload);
    }
  }
  frame(fin, opcode, payload) {
    switch (opcode) {
      case 0:
      case 1: {
        if (opcode === 1 && this.fragments.length) return this.close(1002, "fragmenta\xE7\xE3o inv\xE1lida");
        if (opcode === 0 && !this.fragments.length) return this.close(1002, "continua\xE7\xE3o sem in\xEDcio");
        this.fragments.push(payload);
        this.fragmentBytes += payload.length;
        if (this.fragmentBytes > this.max) return this.close(1009, "mensagem grande demais");
        if (!fin) return;
        const text = Buffer.concat(this.fragments).toString("utf8");
        this.fragments = [];
        this.fragmentBytes = 0;
        try {
          this.onmessage?.(text);
        } catch (e) {
          console.error("[erro] ws mensagem:", e?.message ?? e);
        }
        return;
      }
      case 2:
        return this.close(1003, "apenas texto");
      case 8:
        return this.close(1e3, "tchau");
      case 9:
        this.write(10, payload);
        return;
      case 10:
        return;
      default:
        return this.close(1002, "opcode desconhecido");
    }
  }
};
function attachWebSocket(server2, opts, onConnection) {
  const all = /* @__PURE__ */ new Set();
  server2.on("upgrade", (req, socket) => {
    socket.on("error", () => socket.destroy());
    const url = (req.url ?? "").split("?")[0];
    const key = req.headers["sec-websocket-key"];
    if (url !== opts.path || typeof key !== "string" || req.headers.upgrade?.toLowerCase() !== "websocket") {
      socket.end("HTTP/1.1 400 Bad Request\r\n\r\n");
      return;
    }
    const accept = createHash("sha1").update(key + GUID).digest("base64");
    socket.write(["HTTP/1.1 101 Switching Protocols", "Upgrade: websocket", "Connection: Upgrade", `Sec-WebSocket-Accept: ${accept}`, "", ""].join("\r\n"));
    socket.setNoDelay?.(true);
    const ws2 = new WsConnection(socket, req.socket.remoteAddress ?? "?", opts.maxMessageBytes);
    all.add(ws2);
    ws2.finishHooks.push(() => all.delete(ws2));
    onConnection(ws2, req);
  });
  const timer = setInterval(() => {
    for (const ws2 of all) ws2.heartbeat();
  }, opts.pingIntervalMs ?? 2e4);
  timer.unref();
  return {
    close() {
      clearInterval(timer);
      for (const ws2 of all) ws2.close(1001, "servidor encerrando");
    }
  };
}

// src/server/GameServer.ts
import { createHash as createHash2, createHmac, randomBytes, timingSafeEqual } from "node:crypto";

// src/math/MathError.ts
var MathError = class extends Error {
  code;
  details;
  constructor(code, message, details = {}) {
    super(`[${code}] ${message}`);
    this.name = "MathError";
    this.code = code;
    this.details = Object.freeze({ ...details });
  }
};

// src/math/MathDiagnostics.ts
var counters = {
  overflow: 0,
  underflow: 0,
  "precision-loss": 0,
  "saturated-conversion": 0
};
var listeners = /* @__PURE__ */ new Set();
var MathDiagnostics = {
  report(kind, operation, detail) {
    counters[kind]++;
    if (listeners.size === 0) return;
    const event = { kind, operation, detail };
    for (const l of listeners) {
      try {
        l(event);
      } catch {
      }
    }
  },
  subscribe(listener) {
    listeners.add(listener);
    return () => listeners.delete(listener);
  },
  snapshot() {
    return { ...counters };
  },
  reset() {
    for (const k of Object.keys(counters)) counters[k] = 0;
  }
};

// src/math/BigNum.ts
var BIGNUM_FORMAT_VERSION = 1;
var MAX_EXPONENT = 1e308;
var MIN_EXPONENT = -1e308;
var PRECISE_EXPONENT_LIMIT = Number.MAX_SAFE_INTEGER;
var MANTISSA_DIGITS = 15;
var POW10 = Array.from({ length: 23 }, (_, i) => Number(`1e${i}`));
var BigNum = class _BigNum {
  sign;
  mantissa;
  exponent;
  /** Use BigNum.normalize / NumberEngine.from. O construtor assume valores já normalizados. */
  constructor(sign, mantissa, exponent) {
    this.sign = sign;
    this.mantissa = mantissa;
    this.exponent = exponent;
    Object.freeze(this);
  }
  /** @internal Constrói sem validar. Uso restrito ao motor. */
  static unsafeCreate(sign, mantissa, exponent) {
    return new _BigNum(sign, mantissa, exponent);
  }
  static ZERO = new _BigNum(0, 0, 0);
  static ONE = new _BigNum(1, 1, 0);
  static NEG_ONE = new _BigNum(-1, 1, 0);
  static MAX = new _BigNum(1, 9.999999999999998, MAX_EXPONENT);
  static MIN = new _BigNum(-1, 9.999999999999998, MAX_EXPONENT);
  /**
   * Normaliza (sign, m, e) para a forma canônica.
   * Aceita mantissas fora de [1,10), negativas e expoentes fracionários.
   * Satura em overflow e zera em underflow, sempre reportando ao MathDiagnostics.
   */
  static normalize(sign, m, e, operation = "normalize") {
    if (Number.isNaN(m) || Number.isNaN(e) || Number.isNaN(sign)) {
      throw new MathError("INVALID_STATE", `NaN em ${operation}`, { sign, m, e });
    }
    if (sign === 0 || m === 0) return _BigNum.ZERO;
    if (!Number.isFinite(m)) {
      throw new MathError("INVALID_STATE", `mantissa infinita em ${operation}`, { sign, m, e });
    }
    let s = sign < 0 ? -1 : 1;
    if (m < 0) {
      m = -m;
      s = s === 1 ? -1 : 1;
    }
    if (Number.isFinite(e) && !Number.isInteger(e)) {
      if (Math.abs(e) < PRECISE_EXPONENT_LIMIT) {
        const fl = Math.floor(e);
        m *= Math.pow(10, e - fl);
        e = fl;
      } else {
        e = Math.round(e);
      }
    }
    if (m < 1 || m >= 10) {
      const shift = Math.floor(Math.log10(m));
      m = scaleByPow10(m, -shift);
      e += shift;
      if (m >= 10) {
        m /= 10;
        e += 1;
      } else if (m < 1) {
        m *= 10;
        e -= 1;
      }
    }
    if (e === Infinity || e > MAX_EXPONENT) {
      MathDiagnostics.report("overflow", operation, `expoente ${e}`);
      return s === 1 ? _BigNum.MAX : _BigNum.MIN;
    }
    if (e < MIN_EXPONENT) {
      MathDiagnostics.report("underflow", operation, `expoente ${e}`);
      return _BigNum.ZERO;
    }
    return new _BigNum(s, m, e);
  }
  /** Converte um Number finito. Lança INVALID_INPUT para NaN/Infinity. */
  static fromNumber(n) {
    if (!Number.isFinite(n)) {
      throw new MathError("INVALID_INPUT", `Number n\xE3o finito: ${n}`);
    }
    if (n === 0) return _BigNum.ZERO;
    if (Number.isInteger(n) && n < 1e15 && n > -1e15) {
      const a = n < 0 ? -n : n;
      let e2 = 0;
      while (e2 < 15 && a >= POW10[e2 + 1]) e2++;
      return new _BigNum(n < 0 ? -1 : 1, a / POW10[e2], e2);
    }
    const str = n.toExponential();
    const idx2 = str.indexOf("e");
    const m = Number(str.slice(0, idx2));
    const e = Number(str.slice(idx2 + 1));
    return _BigNum.normalize(m < 0 ? -1 : 1, Math.abs(m), e, "fromNumber");
  }
  /** 10^log, com sinal. */
  static fromLog10(log2, sign = 1) {
    if (Number.isNaN(log2)) throw new MathError("INVALID_STATE", "log10 NaN em fromLog10");
    if (sign === 0) return _BigNum.ZERO;
    if (log2 === Infinity || log2 > MAX_EXPONENT) {
      MathDiagnostics.report("overflow", "fromLog10", `log10 ${log2}`);
      return sign === 1 ? _BigNum.MAX : _BigNum.MIN;
    }
    if (log2 === -Infinity || log2 < MIN_EXPONENT) {
      MathDiagnostics.report("underflow", "fromLog10", `log10 ${log2}`);
      return _BigNum.ZERO;
    }
    const e = Math.floor(log2);
    const m = Math.pow(10, log2 - e);
    return _BigNum.normalize(sign, m, e, "fromLog10");
  }
  isZero() {
    return this.sign === 0;
  }
  /** Forma canônica em notação científica: "-1.25e1000". */
  toString() {
    if (this.sign === 0) return "0";
    return `${this.sign < 0 ? "-" : ""}${this.mantissa}e${this.exponent}`;
  }
  toJSON() {
    return { v: BIGNUM_FORMAT_VERSION, s: this.sign, m: this.mantissa, e: this.exponent };
  }
};
function scaleByPow10(m, k) {
  if (k === 0) return m;
  if (k > 300) return m * 1e300 * Math.pow(10, k - 300);
  if (k < -300) return m * 1e-300 * Math.pow(10, k + 300);
  return k > 0 ? m * Math.pow(10, k) : m / Math.pow(10, -k);
}

// src/math/MathValidator.ts
function validateParts(s, m, e) {
  const issues = [];
  if (s !== -1 && s !== 0 && s !== 1) issues.push(`sinal inv\xE1lido: ${String(s)}`);
  if (typeof m !== "number" || !Number.isFinite(m)) issues.push(`mantissa n\xE3o finita: ${String(m)}`);
  if (typeof e !== "number" || !Number.isFinite(e)) issues.push(`expoente n\xE3o finito: ${String(e)}`);
  if (issues.length === 0) {
    const mm = m;
    const ee = e;
    if (s === 0) {
      if (mm !== 0 || ee !== 0) issues.push("zero n\xE3o can\xF4nico (mantissa/expoente devem ser 0)");
    } else {
      if (mm < 1 || mm >= 10) issues.push(`mantissa fora de [1,10): ${mm}`);
      if (!Number.isInteger(ee)) issues.push(`expoente n\xE3o inteiro: ${ee}`);
      if (ee > MAX_EXPONENT || ee < MIN_EXPONENT) issues.push(`expoente fora da faixa: ${ee}`);
    }
  }
  return { ok: issues.length === 0, issues };
}
function validate(value) {
  if (value instanceof BigNum) return validateParts(value.sign, value.mantissa, value.exponent);
  if (typeof value === "object" && value !== null) {
    const o = value;
    if (o.v !== 1) return { ok: false, issues: [`vers\xE3o de formato desconhecida: ${String(o.v)}`] };
    return validateParts(o.s, o.m, o.e);
  }
  return { ok: false, issues: [`n\xE3o \xE9 BigNum: ${typeof value}`] };
}
function assertValid(value, context = "valor") {
  const r = validate(value);
  if (!r.ok) throw new MathError("INVALID_STATE", `${context} inv\xE1lido: ${r.issues.join("; ")}`);
}
function fromJSON(json) {
  const r = validate(json);
  if (!r.ok) throw new MathError("INVALID_INPUT", `BigNumJSON inv\xE1lido: ${r.issues.join("; ")}`, { json });
  const o = json;
  return o.s === 0 ? BigNum.ZERO : BigNum.unsafeCreate(o.s, o.m, o.e);
}
var MathValidator = { validate, validateParts, assertValid, fromJSON };

// src/math/PrecisionEngine.ts
var SNAP_EPSILON = 1e-9;
function snap(x) {
  const r = Math.round(x);
  return Math.abs(x - r) <= SNAP_EPSILON * Math.max(1, Math.abs(x)) ? r : x;
}
function roundNumber(x, mode) {
  const v = snap(x);
  switch (mode) {
    case "trunc":
      return Math.trunc(v);
    case "floor":
      return Math.floor(v);
    case "ceil":
      return Math.ceil(v);
    case "halfAwayFromZero":
      return v < 0 ? -Math.floor(-v + 0.5) : Math.floor(v + 0.5);
    case "halfEven": {
      const fl = Math.floor(v);
      const diff = v - fl;
      if (diff > 0.5) return fl + 1;
      if (diff < 0.5) return fl;
      return fl % 2 === 0 ? fl : fl + 1;
    }
  }
}
function roundToDecimals(x, decimals, mode) {
  if (decimals <= 0) return roundNumber(x, mode);
  const f = Math.pow(10, decimals);
  return roundNumber(x * f, mode) / f;
}
function roundBigToSignificant(b, digits, mode) {
  if (b.sign === 0) return b;
  const d = Math.max(1, Math.min(MANTISSA_DIGITS, Math.floor(digits)));
  const signedM = b.sign * b.mantissa;
  const r = roundToDecimals(signedM, d - 1, mode);
  return BigNum.normalize(r < 0 ? -1 : 1, Math.abs(r), b.exponent, "roundBigToSignificant");
}

// src/math/ScientificNotation.ts
var SCI_RE = /^([+-])?(?:(\d+)(?:\.(\d*))?|\.(\d+))(?:[eE]([+-]?\d+(?:\.\d+)?))?$/;
function parseScientific(input) {
  const str = input.trim().replace(/_/g, "");
  const match = SCI_RE.exec(str);
  if (!match) return null;
  const signStr = match[1];
  const intPart = (match[2] ?? "").replace(/^0+/, "");
  const fracPart = match[3] ?? match[4] ?? "";
  const expPart = match[5] !== void 0 ? Number(match[5]) : 0;
  if (!Number.isFinite(expPart)) return null;
  const sign = signStr === "-" ? -1 : 1;
  let m;
  let e;
  if (intPart.length > 0) {
    m = Number(`${intPart[0]}.${(intPart.slice(1) + fracPart).slice(0, 20) || "0"}`);
    e = intPart.length - 1;
  } else {
    const z = fracPart.search(/[1-9]/);
    if (z < 0) return BigNum.ZERO;
    m = Number(`${fracPart[z]}.${fracPart.slice(z + 1, z + 21) || "0"}`);
    e = -(z + 1);
  }
  return BigNum.normalize(sign, m, e + expPart, "parseScientific");
}
function formatScientific(b, significantDigits = 3, mode = "trunc", trimZeros = true) {
  if (b.sign === 0) return "0";
  let m = roundToDecimals(b.mantissa, significantDigits - 1, mode);
  let e = b.exponent;
  if (m >= 10) {
    m /= 10;
    e += 1;
  }
  let ms = m.toFixed(Math.max(0, significantDigits - 1));
  if (trimZeros && ms.includes(".")) ms = ms.replace(/\.?0+$/, "");
  return `${b.sign < 0 ? "-" : ""}${ms}e${e}`;
}

// src/math/abbreviations/AbbreviationRegistry.ts
function latinComposite(n, t) {
  const u = t.units[n % 10] ?? "";
  const d = t.tens[Math.floor(n / 10) % 10] ?? "";
  const h = t.hundreds[Math.floor(n / 100) % 10] ?? "";
  return u + d + h;
}
var LatinFamily = class {
  id;
  minIndex;
  maxIndex;
  tables;
  reverse = null;
  constructor(id, minIndex, maxIndex, tables) {
    if (maxIndex > 999) throw new MathError("INVALID_CONFIG", "LatinFamily cobre no m\xE1ximo o \xEDndice 999");
    this.id = id;
    this.minIndex = minIndex;
    this.maxIndex = maxIndex;
    this.tables = tables;
  }
  nameFor(i) {
    if (i < 10) {
      const s = this.tables.short[i];
      if (s === void 0) throw new MathError("INVALID_CONFIG", `sigla curta ausente para i=${i}`);
      return s;
    }
    return latinComposite(i, this.tables);
  }
  indexFor(name) {
    if (!this.reverse) {
      this.reverse = /* @__PURE__ */ new Map();
      for (let i = this.minIndex; i <= this.maxIndex; i++) this.reverse.set(this.nameFor(i), i);
    }
    return this.reverse.get(name) ?? null;
  }
};
var MilliFamily = class {
  id = "milli";
  minIndex = 1e3;
  maxIndex;
  tables;
  separators;
  groupReverse = null;
  constructor(tables, separators, maxGroups) {
    if (maxGroups < 2) throw new MathError("INVALID_CONFIG", "MilliFamily precisa de maxGroups >= 2");
    if (separators.length < maxGroups) {
      throw new MathError("INVALID_CONFIG", `separadores insuficientes para ${maxGroups} grupos`);
    }
    this.tables = tables;
    this.separators = separators;
    this.maxIndex = Math.pow(1e3, maxGroups) - 1;
  }
  nameFor(i) {
    const groups = [];
    let x = i;
    while (x > 0) {
      groups.push(x % 1e3);
      x = Math.floor(x / 1e3);
    }
    const parts = [];
    for (let level = groups.length - 1; level >= 0; level--) {
      const g = groups[level] ?? 0;
      if (g === 0) continue;
      parts.push(latinComposite(g, this.tables) + (this.separators[level] ?? ""));
    }
    return parts.join("-");
  }
  indexFor(name) {
    if (!this.groupReverse) {
      this.groupReverse = /* @__PURE__ */ new Map();
      for (let g = 1; g <= 999; g++) this.groupReverse.set(latinComposite(g, this.tables), g);
    }
    let total = 0;
    let lastLevel = Infinity;
    for (const part of name.split("-")) {
      let level = 0;
      let body = part;
      for (let l = this.separators.length - 1; l >= 1; l--) {
        const sep = this.separators[l] ?? "";
        if (sep && part.endsWith(sep)) {
          level = l;
          body = part.slice(0, -sep.length);
          break;
        }
      }
      const g = this.groupReverse.get(body);
      if (g === void 0 || level >= lastLevel) return null;
      lastLevel = level;
      total += g * Math.pow(1e3, level);
    }
    if (total < this.minIndex || total > this.maxIndex) return null;
    return this.nameFor(total) === name ? total : null;
  }
};
var AbbreviationRegistry = class {
  families = [];
  register(family) {
    for (const f of this.families) {
      if (family.minIndex <= f.maxIndex && f.minIndex <= family.maxIndex) {
        throw new MathError("INVALID_CONFIG", `fam\xEDlia ${family.id} sobrep\xF5e ${f.id}`);
      }
    }
    this.families.push(family);
    this.families.sort((a, b) => a.minIndex - b.minIndex);
    return this;
  }
  /** Maior índice coberto continuamente a partir de 0. */
  get maxIndex() {
    let max2 = -1;
    for (const f of this.families) {
      if (f.minIndex !== max2 + 1) break;
      max2 = f.maxIndex;
    }
    return max2;
  }
  familyFor(index) {
    for (const f of this.families) if (index >= f.minIndex && index <= f.maxIndex) return f;
    return null;
  }
  nameFor(index) {
    return this.familyFor(index)?.nameFor(index) ?? null;
  }
  indexFor(name) {
    for (const f of this.families) {
      const i = f.indexFor(name);
      if (i !== null) return i;
    }
    return null;
  }
  list() {
    return this.families;
  }
};
function createDefaultRegistry(settings) {
  const t = settings.latin;
  return new AbbreviationRegistry().register(new LatinFamily("latin-classic", 0, t.classicMaxIndex, t)).register(new LatinFamily("latin-extended", t.classicMaxIndex + 1, t.extendedMaxIndex, t)).register(new MilliFamily(t, settings.milli.separators, settings.milli.maxGroups));
}

// src/config/NumberFormatConfig.ts
var NUMBER_FORMAT_CONFIG = {
  notation: "abbreviation",
  /** 3 dígitos significativos: 1.25K, 25.7M, 3.84Qa, 12.5Oc (exemplos da especificação). */
  significantDigits: 3,
  /** Abaixo deste valor absoluto, usa notação científica (ex.: 1.25e-5). */
  smallThreshold: 1e-3,
  trimTrailingZeros: true,
  decimalSeparator: ".",
  latin: {
    /** i = 0..9 */
    short: ["K", "M", "B", "T", "Qa", "Qi", "Sx", "Sp", "Oc", "No"],
    units: ["", "U", "D", "T", "Qa", "Qi", "Sx", "Sp", "Oc", "No"],
    tens: ["", "Dc", "Vg", "Tg", "Qag", "Qig", "Sxg", "Spg", "Ocg", "Nog"],
    /** Centenas (só a família estendida usa 2..9). Tokens distintos para não colidir com unidades. */
    hundreds: ["", "Ce", "Dct", "Trc", "Qdc", "Qnc", "Ssc", "Spc", "Otc", "Nnc"],
    /** 1ª hierarquia (tradicional): até QaCe = 1e315. */
    classicMaxIndex: 104,
    /** Hierarquia latina estendida: até NoNogNnc ≈ 1e3000. */
    extendedMaxIndex: 999
  },
  /** Família "Milli": índices ≥ 1000, agrupados em base 1000 com separadores de grupo. */
  milli: {
    separators: ["", "Mi", "Mc", "Na", "Pc", "Fm", "At", "Zp"],
    /** Número máximo de grupos de 1000 no índice. 2 → índices até 999 999 (≈ 1e3 000 000). */
    maxGroups: 2
  },
  /**
   * Hiperescala: acima da última família, mostra o logaritmo decimal do valor
   * formatado com as mesmas abreviações. "Ω3.45M" = 10^(3,45 milhões).
   */
  hyperscale: {
    symbol: "\u03A9",
    innerSignificantDigits: 4
  },
  roman: {
    /** vinculum: barra (×1000) e barra dupla (×1000²); tiers ≥ 3 usam [XII]³. */
    style: "vinculum",
    /** Grupos de 1000 exibidos; grupos inferiores omitidos recebem o marcador "+". */
    maxGroups: 3,
    zeroSymbol: "N"
  }
};

// src/math/NumberParser.ts
var ABBR_RE = /^(\d+(?:[.,]\d+)?(?:[eE][+-]?\d+)?)\s*([A-Za-z][A-Za-z-]*)$/;
var registry = createDefaultRegistry(NUMBER_FORMAT_CONFIG);
var hyperSymbol = NUMBER_FORMAT_CONFIG.hyperscale.symbol;
function getRegistry() {
  return registry;
}
function tryParse(input) {
  let str = input.trim();
  if (str.length === 0) return null;
  let sign = 1;
  if (str[0] === "-" || str[0] === "+") {
    if (str[0] === "-") sign = -1;
    str = str.slice(1).trim();
  }
  if (str.startsWith(hyperSymbol)) {
    const inner = tryParse(str.slice(hyperSymbol.length));
    if (!inner) return null;
    const log2 = inner.sign * Number(`${inner.mantissa}e${Math.min(inner.exponent, 309)}`);
    return BigNum.fromLog10(log2, sign);
  }
  const sci = parseScientific(str);
  if (sci) return sign === 1 ? sci : BigNum.normalize(-sci.sign, sci.mantissa, sci.exponent, "parse");
  const m = ABBR_RE.exec(str);
  if (!m || m[1] === void 0 || m[2] === void 0) return null;
  const base = parseScientific(m[1].replace(",", "."));
  const idx2 = registry.indexFor(m[2]);
  if (!base || idx2 === null) return null;
  if (base.sign === 0) return BigNum.ZERO;
  return BigNum.normalize(sign * base.sign, base.mantissa, base.exponent + 3 * (idx2 + 1), "parse");
}
function parse(input) {
  const r = tryParse(input);
  if (!r) throw new MathError("PARSE_ERROR", `n\xE3o foi poss\xEDvel interpretar "${input}"`);
  return r;
}

// src/math/coerce.ts
var PARSED = /* @__PURE__ */ new Map();
var PARSED_MAX = 4096;
function toBig(x) {
  if (x instanceof BigNum) return x;
  if (typeof x === "number") return BigNum.fromNumber(x);
  if (typeof x === "string") {
    let b = PARSED.get(x);
    if (!b) {
      b = parse(x);
      if (PARSED.size >= PARSED_MAX) PARSED.clear();
      PARSED.set(x, b);
    }
    return b;
  }
  if (typeof x === "object" && x !== null) return fromJSON(x);
  throw new MathError("INVALID_INPUT", `entrada n\xE3o suportada: ${typeof x}`);
}

// src/math/NumberFormatter.ts
var MAX_SMALL_DECIMALS = 6;
var NumberFormatter = class {
  settings;
  registry;
  constructor(settings = NUMBER_FORMAT_CONFIG, registry3 = getRegistry()) {
    this.settings = settings;
    this.registry = registry3;
  }
  format(value, options = {}) {
    const b = toBig(value);
    if (b.sign === 0) return "0";
    const o = {
      notation: options.notation ?? this.settings.notation,
      sig: Math.max(1, Math.min(15, options.significantDigits ?? this.settings.significantDigits)),
      trim: options.trimTrailingZeros ?? this.settings.trimTrailingZeros,
      sep: options.decimalSeparator ?? this.settings.decimalSeparator,
      small: options.smallThreshold ?? this.settings.smallThreshold
    };
    const sign = b.sign < 0 ? "-" : "";
    const e = b.exponent;
    const hyperLimit = 3 * (this.registry.maxIndex + 2);
    if (e >= hyperLimit || e <= -hyperLimit) {
      const log2 = Math.abs(e) >= PRECISE_EXPONENT_LIMIT ? e : e + Math.log10(b.mantissa);
      const inner = this.format(BigNum.fromNumber(log2), {
        ...options,
        notation: "abbreviation",
        significantDigits: this.settings.hyperscale.innerSignificantDigits
      });
      return `${sign}${this.settings.hyperscale.symbol}${inner}`;
    }
    const smallLog = Math.log10(o.small);
    if (e < smallLog) return this.sci(b, o.sig, o.trim, o.sep);
    if (e < 3) {
      const v = b.mantissa * Math.pow(10, e);
      const text2 = this.fixed(v, o.sig, o.trim, o.sep);
      if (text2 !== null) return sign + text2;
      return this.format(BigNum.normalize(b.sign, 1, 3), options);
    }
    if (o.notation === "scientific") return this.sci(b, o.sig, o.trim, o.sep);
    let group = Math.floor(e / 3);
    let scaled = b.mantissa * Math.pow(10, e - 3 * group);
    let text = this.fixed(scaled, o.sig, o.trim, o.sep);
    if (text === null) {
      group += 1;
      scaled = 1;
      text = this.fixed(1, o.sig, o.trim, o.sep) ?? "1";
    }
    if (o.notation === "engineering") return `${sign}${text}e${3 * group}`;
    const name = this.registry.nameFor(group - 1);
    if (name === null) return this.sci(b, o.sig, o.trim, o.sep);
    return `${sign}${text}${name}`;
  }
  /** Decimal com `sig` dígitos significativos, truncado. null se truncar resultar em ≥ 1000. */
  fixed(v, sig, trim, sep) {
    const intDigits = Math.floor(Math.log10(v)) + 1;
    const decimals = Math.max(0, Math.min(MAX_SMALL_DECIMALS, sig - intDigits));
    const t = roundToDecimals(v, decimals, "trunc");
    if (t >= 1e3) return null;
    let s = t.toFixed(decimals);
    if (trim && s.includes(".")) s = s.replace(/\.?0+$/, "");
    return sep === "." ? s : s.replace(".", sep);
  }
  sci(b, sig, trim, sep) {
    const s = formatScientific(b, sig, "trunc", trim);
    return sep === "." ? s : s.replace(".", sep);
  }
};
var defaultFormatter = new NumberFormatter();
function format(value, options) {
  return defaultFormatter.format(value, options);
}

// src/math/NumberEngine.ts
var from = toBig;
var NATIVE_EXP_LIMIT = 290;
var NATIVE_MIN_RESULT = 1e-290;
function nativeOk(b) {
  return b.sign === 0 || b.exponent < NATIVE_EXP_LIMIT && b.exponent > -NATIVE_EXP_LIMIT;
}
function nativeOf(b) {
  return b.sign === 0 ? 0 : Number(`${b.sign * b.mantissa}e${b.exponent}`);
}
function fromNative(r) {
  if (r === 0) return BigNum.ZERO;
  if (!Number.isFinite(r) || Math.abs(r) < NATIVE_MIN_RESULT) return null;
  return BigNum.fromNumber(r);
}
function log10Abs(b) {
  if (b.sign === 0) throw new MathError("LOG_OF_NON_POSITIVE", "log10 de zero");
  return Math.abs(b.exponent) >= PRECISE_EXPONENT_LIMIT ? b.exponent : b.exponent + Math.log10(b.mantissa);
}
function negate(x) {
  const b = from(x);
  return b.sign === 0 ? b : BigNum.unsafeCreate(-b.sign, b.mantissa, b.exponent);
}
function abs(x) {
  const b = from(x);
  return b.sign >= 0 ? b : BigNum.unsafeCreate(1, b.mantissa, b.exponent);
}
function add(x, y) {
  const a = from(x);
  const b = from(y);
  if (a.sign === 0) return b;
  if (b.sign === 0) return a;
  if (nativeOk(a) && nativeOk(b)) {
    const r = fromNative(nativeOf(a) + nativeOf(b));
    if (r) return r;
  }
  let big = a;
  let small = b;
  if (a.exponent < b.exponent || a.exponent === b.exponent && a.mantissa < b.mantissa) {
    big = b;
    small = a;
  }
  const diff = big.exponent - small.exponent;
  if (diff > MANTISSA_DIGITS + 2) return big;
  const m = big.sign * big.mantissa + small.sign * small.mantissa / Math.pow(10, diff);
  if (Math.abs(m) < big.mantissa * 1e-15) return BigNum.ZERO;
  return BigNum.normalize(1, m, big.exponent, "add");
}
function subtract(x, y) {
  return add(x, negate(y));
}
function multiply(x, y) {
  const a = from(x);
  const b = from(y);
  if (a.sign === 0 || b.sign === 0) return BigNum.ZERO;
  if (nativeOk(a) && nativeOk(b)) {
    const r = fromNative(nativeOf(a) * nativeOf(b));
    if (r) return r;
  }
  return BigNum.normalize(a.sign * b.sign, a.mantissa * b.mantissa, a.exponent + b.exponent, "multiply");
}
function divide(x, y) {
  const a = from(x);
  const b = from(y);
  if (b.sign === 0) throw new MathError("DIVISION_BY_ZERO", "divis\xE3o por zero", { dividend: a.toString() });
  if (a.sign === 0) return BigNum.ZERO;
  if (nativeOk(a) && nativeOk(b)) {
    const r = fromNative(nativeOf(a) / nativeOf(b));
    if (r) return r;
  }
  return BigNum.normalize(a.sign * b.sign, a.mantissa / b.mantissa, a.exponent - b.exponent, "divide");
}
function reciprocal(x) {
  return divide(BigNum.ONE, x);
}
function pow(x, p) {
  const a = from(x);
  const pb = from(p);
  if (pb.sign === 0) return BigNum.ONE;
  if (a.sign === 0) {
    if (pb.sign < 0) throw new MathError("DIVISION_BY_ZERO", "0 elevado a expoente negativo");
    return BigNum.ZERO;
  }
  if (pb.exponent > 308) {
    const la = log10Abs(a);
    if (a.sign < 0) throw new MathError("COMPLEX_RESULT", "base negativa com expoente al\xE9m da precis\xE3o inteira");
    if (la === 0) return BigNum.ONE;
    const grows = la > 0 === pb.sign > 0;
    if (grows) {
      MathDiagnostics.report("overflow", "pow", `expoente ${pb.toString()}`);
      return BigNum.MAX;
    }
    MathDiagnostics.report("underflow", "pow", `expoente ${pb.toString()}`);
    return BigNum.ZERO;
  }
  const pn = toNumber(pb);
  let sign = 1;
  if (a.sign < 0) {
    if (!Number.isInteger(pn)) {
      throw new MathError("COMPLEX_RESULT", "base negativa com expoente n\xE3o inteiro", { base: a.toString(), p: pn });
    }
    sign = Math.abs(pn) < PRECISE_EXPONENT_LIMIT && Math.abs(pn % 2) === 1 ? -1 : 1;
  }
  const L = pn * log10Abs(a);
  if (Math.abs(L) < 300 && Math.abs(a.exponent) < 300) {
    const base = Number(`${a.mantissa}e${a.exponent}`);
    const r = Math.pow(base, pn);
    if (Number.isFinite(r) && r !== 0) return BigNum.normalize(sign, r, 0, "pow");
  }
  return BigNum.fromLog10(L, sign);
}
function root(x, n) {
  const a = from(x);
  if (!Number.isFinite(n) || n === 0) throw new MathError("INVALID_INPUT", `\xEDndice de raiz inv\xE1lido: ${n}`);
  if (a.sign < 0) {
    if (!Number.isInteger(n) || n % 2 === 0) throw new MathError("COMPLEX_RESULT", "raiz par de n\xFAmero negativo");
    return negate(pow(abs(a), 1 / n));
  }
  return pow(a, 1 / n);
}
function sqrt(x) {
  return root(x, 2);
}
function exp10(x) {
  return pow(10, x);
}
function log10(x) {
  const b = from(x);
  if (b.sign <= 0) throw new MathError("LOG_OF_NON_POSITIVE", `log10 de ${b.toString()}`);
  return log10Abs(b);
}
function ln(x) {
  return log10(x) * Math.LN10;
}
function log(x, base) {
  const lb = log10(base);
  if (lb === 0) throw new MathError("INVALID_INPUT", "base de logaritmo igual a 1");
  return log10(x) / lb;
}
function tetrate(base, height) {
  if (!Number.isInteger(height) || height < 0) throw new MathError("INVALID_INPUT", "altura de tetra\xE7\xE3o inv\xE1lida");
  const b = from(base);
  let r = BigNum.ONE;
  for (let i = 0; i < height; i++) {
    r = pow(b, r);
    if (r === BigNum.MAX) break;
  }
  return r;
}
function compare(x, y) {
  const a = from(x);
  const b = from(y);
  if (a.sign !== b.sign) return a.sign < b.sign ? -1 : 1;
  if (a.sign === 0) return 0;
  let c = 0;
  if (a.exponent !== b.exponent) c = a.exponent < b.exponent ? -1 : 1;
  else if (a.mantissa !== b.mantissa) c = a.mantissa < b.mantissa ? -1 : 1;
  if (c === 0) return 0;
  return a.sign > 0 ? c : c === 1 ? -1 : 1;
}
var equals = (a, b) => compare(a, b) === 0;
var lt = (a, b) => compare(a, b) < 0;
var lte = (a, b) => compare(a, b) <= 0;
var gt = (a, b) => compare(a, b) > 0;
var gte = (a, b) => compare(a, b) >= 0;
function approxEquals(x, y, relTol = 1e-12) {
  const a = from(x);
  const b = from(y);
  if (a.sign === 0 && b.sign === 0) return true;
  const diff = abs(subtract(a, b));
  if (diff.sign === 0) return true;
  const scale = gt(abs(a), abs(b)) ? abs(a) : abs(b);
  return lte(diff, multiply(scale, relTol));
}
function min(...xs) {
  if (xs.length === 0) throw new MathError("INVALID_INPUT", "min sem argumentos");
  let r = from(xs[0]);
  for (let i = 1; i < xs.length; i++) {
    const v = from(xs[i]);
    if (lt(v, r)) r = v;
  }
  return r;
}
function max(...xs) {
  if (xs.length === 0) throw new MathError("INVALID_INPUT", "max sem argumentos");
  let r = from(xs[0]);
  for (let i = 1; i < xs.length; i++) {
    const v = from(xs[i]);
    if (gt(v, r)) r = v;
  }
  return r;
}
function clamp(x, lo, hi) {
  if (gt(lo, hi)) throw new MathError("INVALID_INPUT", "clamp com limite inferior > superior");
  return min(max(x, lo), hi);
}
function sum(xs) {
  let r = BigNum.ZERO;
  for (const x of xs) r = add(r, x);
  return r;
}
function product(xs) {
  let r = BigNum.ONE;
  for (const x of xs) r = multiply(r, x);
  return r;
}
function round(x, mode = "halfAwayFromZero") {
  const b = from(x);
  if (b.sign === 0 || b.exponent >= MANTISSA_DIGITS) return b;
  const n = roundNumber(Number(`${b.sign * b.mantissa}e${b.exponent}`), mode);
  return n === 0 ? BigNum.ZERO : BigNum.fromNumber(n);
}
var floor = (x) => round(x, "floor");
var ceil = (x) => round(x, "ceil");
var trunc = (x) => round(x, "trunc");
function isInteger(x) {
  const b = from(x);
  if (b.sign === 0 || b.exponent >= MANTISSA_DIGITS) return true;
  if (b.exponent < 0) return false;
  return Number.isInteger(Number(`${b.mantissa}e${b.exponent}`));
}
function toNumber(x) {
  const b = from(x);
  if (b.sign === 0) return 0;
  if (b.exponent > 308) {
    MathDiagnostics.report("saturated-conversion", "toNumber", b.toString());
    return b.sign * Number.MAX_VALUE;
  }
  if (b.exponent < -400) return 0;
  const n = Number(`${b.sign * b.mantissa}e${b.exponent}`);
  if (!Number.isFinite(n)) {
    MathDiagnostics.report("saturated-conversion", "toNumber", b.toString());
    return b.sign * Number.MAX_VALUE;
  }
  return n;
}
function toNumberStrict(x) {
  const b = from(x);
  const n = b.sign === 0 ? 0 : Number(`${b.sign * b.mantissa}e${b.exponent}`);
  if (!Number.isFinite(n)) throw new MathError("INVALID_STATE", `valor ${b.toString()} n\xE3o cabe em Number`);
  return n;
}
var NumberEngine = {
  // construção
  from,
  fromNumber: (n) => BigNum.fromNumber(n),
  fromLog10: (l, sign = 1) => BigNum.fromLog10(l, sign),
  fromJSON,
  toJSON: (x) => from(x).toJSON(),
  isBigNum: (x) => x instanceof BigNum,
  ZERO: BigNum.ZERO,
  ONE: BigNum.ONE,
  MAX: BigNum.MAX,
  MIN: BigNum.MIN,
  MAX_EXPONENT,
  // aritmética
  add,
  subtract,
  multiply,
  divide,
  reciprocal,
  negate,
  abs,
  pow,
  root,
  sqrt,
  exp10,
  tetrate,
  sum,
  product,
  // logaritmos
  log10,
  ln,
  log,
  // comparação
  compare,
  equals,
  approxEquals,
  lt,
  lte,
  gt,
  gte,
  min,
  max,
  clamp,
  // sinal
  sign: (x) => from(x).sign,
  isZero: (x) => from(x).sign === 0,
  isNegative: (x) => from(x).sign < 0,
  isPositive: (x) => from(x).sign > 0,
  // arredondamento
  round,
  floor,
  ceil,
  trunc,
  isInteger,
  roundToSignificant: (x, digits, mode = "halfAwayFromZero") => roundBigToSignificant(from(x), digits, mode),
  // conversão
  toNumber,
  toNumberStrict,
  toString: (x) => from(x).toString(),
  format: (x, options) => format(x, options)
};

// src/math/NegativeNumberEngine.ts
var PURE_NEGATION = (_id, v) => NumberEngine.negate(v);
function negateAll(record, policy = PURE_NEGATION) {
  const out = {};
  for (const key of Object.keys(record)) {
    out[key] = policy(key, record[key]);
  }
  return out;
}
function signedLog10(x) {
  const b = NumberEngine.from(x);
  if (b.sign === 0) return 0;
  const mag = NumberEngine.abs(b);
  const l = b.exponent > 15 ? NumberEngine.log10(mag) : Math.log10(1 + NumberEngine.toNumber(mag));
  return b.sign * l;
}
function compareMagnitude(a, b) {
  return NumberEngine.compare(NumberEngine.abs(a), NumberEngine.abs(b));
}
function hasNegative(values) {
  for (const v of values) if (v.sign < 0) return true;
  return false;
}
var hooks = [];
function registerReverseHook(hook) {
  if (hooks.some((h) => h.id === hook.id)) throw new MathError("INVALID_CONFIG", `hook duplicado: ${hook.id}`);
  hooks.push(hook);
  hooks.sort((a, b) => a.priority - b.priority || a.id.localeCompare(b.id));
  return () => {
    const i = hooks.indexOf(hook);
    if (i >= 0) hooks.splice(i, 1);
  };
}
function applyReverseHooks(factor, ctx) {
  let f = NumberEngine.from(factor);
  for (const h of hooks) if (h.appliesTo(ctx)) f = h.transform(f, ctx);
  return f;
}
function listReverseHooks() {
  return hooks.map((h) => h.id);
}
var NegativeNumberEngine = {
  negateAll,
  signedLog10,
  compareMagnitude,
  hasNegative,
  registerReverseHook,
  applyReverseHooks,
  listReverseHooks,
  PURE_NEGATION
};

// src/config/ProgressionConfig.ts
var LEVEL_CONFIG = {
  startLevel: "0.01",
  levelStep: "0.01",
  xpBase: "100",
  xpGrowth: "1.25",
  polySwitchAt: 11,
  polyPower: 4.5,
  hpPerLevel: "0.25",
  hpStacking: "additive",
  resistancePerLevel: "0.03",
  resistanceStacking: "additive",
  baseHp: "100",
  baseResistance: "1"
};
var LEVEL_EFFECTS = {
  /** O bônus do nível também multiplica as unidades por compra das camadas. */
  // revisão v1.0.5: o bônus vale para XP e Letras, mas NÃO multiplica as unidades das camadas
  // (nas 6 camadas ao mesmo tempo ele se acumulava e levava o jogo do nível 1T ao infinito em ~6 min)
  applyToUnits: false,
  allGainPercentPerLevel: "0.00000000001",
  /** additive: 1 + n·p · compound: (1 + p)^n */
  allGainMode: "additive",
  letterCycleReductionSeconds: 0.2,
  letterCycleReductionPerLevel: 0.01,
  /** O ciclo nunca fica abaixo disto (pedido do autor: 1,5 s). */
  minLetterCycleSeconds: 1.5
};
var LETTERS_CONFIG = {
  cycleSeconds: 20,
  baseAmountPerCycle: "1",
  xpBonusPerLetter: "0.000001",
  xpBonusFormula: "linear",
  xpBonusLogScale: "1"
};
var RESET_LAYER_ORDER = [
  "multiplier",
  "rank",
  "rebirth",
  "ultraRebirth",
  "prestige",
  "ultraPrestige"
];
var add2 = "additive";
var RESET_LAYERS = {
  multiplier: {
    id: "multiplier",
    displayName: "Multiplicador",
    // revisão do autor: requisito cresce 68% por unidade (antes 75%); +0,85x de ganho de Letras por unidade (antes 0,5x)
    requirements: [{ resource: "letters", base: "100", growth: "1.68", costMode: "cumulative" }],
    rewards: [
      { channel: "letterGain", amountPerUnit: "0.85", stacking: add2 },
      { channel: "letterSpeed", amountPerUnit: "2", stacking: add2 },
      // revisão v1.0.5: o bônus de XP do Multiplicador é elevado ao CUBO: (1 + 0,5·M)³
      { channel: "xpGain", amountPerUnit: "0.5", stacking: add2, power: 3 }
    ],
    bulk: { enabledByDefault: false, unlockEffectId: "bulk:multiplier" },
    automation: { unlockEffectId: "auto:multiplier" },
    costIndex: "purchases",
    // revisão v1.0.5: preço por compra (os bônus de ganho dão mais unidades pelo mesmo preço)
    display: "abbreviation"
  },
  rank: {
    id: "rank",
    displayName: "Rank",
    // revisão do autor: 10 Multiplicadores, crescendo 1,7x (antes 15x), sempre inteiro: 10, 17, 29, 50, 84…
    requirements: [{ resource: "multiplier", base: "10", growth: "1.7", costMode: "cumulative", integer: true }],
    rewards: [
      { channel: "letterGain", amountPerUnit: "10", stacking: add2 },
      // revisão v1.0.5: cada COMPRA de Rank multiplica o ganho de Multiplicadores por 1,6 — um pouco abaixo dos 1,7x
      // do preço do Rank: comprar só Ranks para sempre não vira bola de neve (é preciso avançar para o ReB)
      { channel: "multiplierGain", amountPerUnit: "0", stacking: add2, perPurchase: "1.6" }
    ],
    bulk: { enabledByDefault: false, unlockEffectId: "bulk:rank" },
    automation: { unlockEffectId: "auto:rank" },
    costIndex: "purchases",
    // revisão v1.0.5: preço por compra (os bônus de ganho dão mais unidades pelo mesmo preço)
    display: "roman"
  },
  rebirth: {
    id: "rebirth",
    displayName: "ReB",
    // revisão do autor: 5 Ranks, crescendo 3x (antes 5 000 e 20x): 5, 15, 45, 135…
    requirements: [{ resource: "rank", base: "5", growth: "3", costMode: "cumulative", integer: true }],
    rewards: [
      { channel: "letterGain", amountPerUnit: "250", stacking: add2 },
      { channel: "multiplierGain", amountPerUnit: "67", stacking: add2 },
      // revisão v1.0.5: cada compra de ReB multiplica o ganho de Ranks por 3 (= quanto o preço do ReB sobe)
      { channel: "rankGain", amountPerUnit: "0", stacking: add2, perPurchase: "3" }
    ],
    bulk: { enabledByDefault: false, unlockEffectId: "bulk:rebirth" },
    automation: { unlockEffectId: "auto:rebirth" },
    costIndex: "purchases",
    // revisão v1.0.5: preço por compra (os bônus de ganho dão mais unidades pelo mesmo preço)
    display: "abbreviation"
  },
  ultraRebirth: {
    id: "ultraRebirth",
    displayName: "Ultra ReB",
    // revisão do autor: 15 ReB, crescendo 2x (antes 50 e 5x): 15, 30, 60, 120…
    requirements: [{ resource: "rebirth", base: "15", growth: "2", costMode: "cumulative", integer: true }],
    rewards: [
      { channel: "letterGain", amountPerUnit: "30e3", stacking: add2 },
      { channel: "multiplierGain", amountPerUnit: "2e3", stacking: add2 },
      { channel: "rankGain", amountPerUnit: "670", stacking: add2 },
      // revisão v1.0.5: cada compra de Ultra ReB multiplica o ganho de ReB por 2 (= quanto o preço do Ultra ReB sobe)
      { channel: "rebirthGain", amountPerUnit: "0", stacking: add2, perPurchase: "2" }
    ],
    bulk: { enabledByDefault: true, unlockEffectId: "bulk:ultraRebirth" },
    automation: { unlockEffectId: "auto:ultraRebirth" },
    costIndex: "purchases",
    // revisão v1.0.5: preço por compra (os bônus de ganho dão mais unidades pelo mesmo preço)
    display: "abbreviation"
  },
  prestige: {
    id: "prestige",
    displayName: "Prest\xEDgio",
    // revisão do autor: crescimento 2x (antes 20x): 20, 40, 80, 160…
    requirements: [{ resource: "ultraRebirth", base: "20", growth: "2", costMode: "cumulative", integer: true }],
    rewards: [
      { channel: "letterGain", amountPerUnit: "780e3", stacking: add2 },
      { channel: "multiplierGain", amountPerUnit: "278e3", stacking: add2 },
      { channel: "rankGain", amountPerUnit: "125e3", stacking: add2 },
      { channel: "rebirthGain", amountPerUnit: "45", stacking: add2 },
      // revisão v1.0.5: cada compra de Prestígio multiplica o ganho de Ultra ReB por 2 (= quanto o preço do Prestígio sobe)
      { channel: "ultraRebirthGain", amountPerUnit: "0", stacking: add2, perPurchase: "2" }
    ],
    bulk: { enabledByDefault: true, unlockEffectId: "bulk:prestige" },
    automation: { unlockEffectId: "auto:prestige" },
    costIndex: "purchases",
    // revisão v1.0.5: preço por compra (os bônus de ganho dão mais unidades pelo mesmo preço)
    display: "abbreviation"
  },
  ultraPrestige: {
    id: "ultraPrestige",
    displayName: "Ultra Prest\xEDgio",
    requirements: [
      // 1T = 1e12 (revisado pelo autor; antes 1 Vg). [DECISÃO D8] o nível é um limiar (não é consumido).
      // crescimento aleatório de 70x a 95x por unidade (revisado pelo autor; antes 70x fixo)
      { resource: "level", base: "1e12", growth: "70", costMode: "threshold", growthRange: { min: 70, max: 95 } },
      { resource: "prestige", base: "5", growth: "70", costMode: "cumulative", growthRange: { min: 70, max: 95 } }
    ],
    // após o 1º Ultra Prestígio, todos os multiplicadores dele são elevados ao quadrado (autor)
    rewardPowerAfterFirst: 2,
    rewards: [
      { channel: "letterGain", amountPerUnit: "3.56e6", stacking: add2 },
      { channel: "multiplierGain", amountPerUnit: "967e3", stacking: add2 },
      { channel: "rankGain", amountPerUnit: "634e3", stacking: add2 },
      { channel: "rebirthGain", amountPerUnit: "1.5e3", stacking: add2 },
      { channel: "ultraRebirthGain", amountPerUnit: "15", stacking: add2 },
      // revisão v1.0.5: cada compra de Ultra Prestígio multiplica o ganho de Prestígio por 8 (64 após o 1º, pelo ²),
      // um pouco abaixo dos 70–95x do preço: cada Ultra Prestígio fica um pouco mais difícil que o anterior
      { channel: "prestigeGain", amountPerUnit: "0", stacking: add2, perPurchase: "8" }
    ],
    bulk: { enabledByDefault: true, unlockEffectId: "bulk:ultraPrestige" },
    automation: { unlockEffectId: "auto:ultraPrestige" },
    costIndex: "purchases",
    // revisão v1.0.5: preço por compra (os bônus de ganho dão mais unidades pelo mesmo preço)
    display: "abbreviation"
  }
};

// src/config/WorldConfig.ts
var STAIRCASE_CONFIG = {
  stepsPerPlatform: 20,
  // [ESPEC]
  difficultyGrowthPerFlight: "2.5",
  // [ESPEC]
  defaultRenderDistance: 16,
  // [ESPEC] blocos de escada
  minRenderDistance: 4,
  // [PROPOSTO]
  maxRenderDistance: 64,
  // [PROPOSTO] limitado também pelo perfil do dispositivo
  lobbySize: 24,
  // [ESPEC] blocos
  /**
   * [ESPEC revisada] Cada pedestal libera o AUTOMÁTICO do próximo "prestígio" (reset),
   * na ordem da hierarquia: 1º pedestal → Multiplicador, 2º → Rank, ... 6º → Ultra Prestígio.
   * Pedestais além do 6º não liberam automação (só aumentam a dificuldade).
   */
  pedestalUnlocksAutomation: true,
  /**
   * [ESPEC do autor] XP da escada:
   *  - o 1º degrau dá 5 XP a cada 100 ms;
   *  - cada degrau do lance soma 75% do valor do 1º degrau do lance;
   *  - após cada pedestal, o 1º degrau do próximo lance vale 1,5x o ÚLTIMO degrau do lance
   *    anterior (revisão do autor: antes era 1,5x o 1º degrau, o que fazia o XP CAIR ao passar
   *    do pedestal e prendia os jogadores), e a mesma lógica recomeça.
   *    Resultado: o XP nunca diminui subindo a escada.
   * O 2,5x por lance NÃO afeta o XP: só o peso do Miasma (resistência exigida 2,5x maior a cada pedestal).
   */
  stepXpBase: "5",
  stepXpIntervalSeconds: 0.1,
  stepXpGrowthPerStep: "0.75",
  /** additive: base × (1 + 0,75·j)  ·  compound: base × 1,75^j   (j = degrau dentro do lance, 0..19) */
  stepXpGrowthMode: "additive",
  flightXpMultiplier: "1.5",
  /** Semente da geração procedural determinística. [PROPOSTO] */
  worldSeed: 1592632769,
  /** Geometria (metros). [PROPOSTO] Degrau baixo o bastante para subir andando. */
  stepHeight: 0.3,
  stepDepth: 0.9,
  /** Bem mais larga que o jogador (≈ 15 ombros): espaço para contornar o pedestal e para várias pessoas. */
  stepWidth: 9,
  platformDepth: 5,
  /** Velocidade máxima legítima (m/s) e salto vertical máximo por segundo, para validar posições. */
  maxHorizontalSpeed: 9.5,
  maxVerticalSpeed: 10
};
var MIASMA_CONFIG = {
  deathThreshold: 15,
  // [ESPEC] negativação total na 15ª morte
  /** Resistência exigida no degrau n: base × growth^n × dificuldadeDoLance. [PROPOSTO] */
  resistanceBase: "1",
  resistanceGrowthPerStep: "1.02",
  /**
   * Dano por segundo, em fração do HP máximo [PROPOSTO]:
   *   fração = min(máx, escala × log10(exigida / resistência) × (1 + intensidade))
   * Logarítmico: a exigência cresce 2,5x por lance; um dano linear tornaria a
   * escada intransponível já no 2º lance.
   */
  damageScale: "0.08",
  maxDamageFractionPerSecond: "1",
  /** Regeneração por segundo (fração do HP máximo) quando a resistência é suficiente ou no lobby. [PROPOSTO] */
  regenFractionPerSecond: "0.06",
  /** Onde o jogador renasce após morrer pelo Miasma. [PROPOSTO] */
  respawn: "lobby",
  /** [DECISÃO D11] contador de mortes zera após a negativação. */
  resetDeathCounterAfterNegation: true,
  /** [ESPEC revisada] A negativação é permanente: novas 15 mortes não revertem o estado. */
  negationWhenAlreadyNegative: "keep"
};
var MULTIPLAYER_CONFIG = {
  maxRealPlayersPerRoom: 5,
  // [ESPEC]
  leaderboardCategories: [
    "level",
    "letters",
    "multiplier",
    "rank",
    "rebirth",
    "ultraRebirth",
    "prestige",
    "ultraPrestige"
  ],
  // [ESPEC] 8 placares
  leaderboardSize: 100,
  // [ESPEC]
  /**
   * [ESPEC revisada] Jogadores negativados competem normalmente: a posição usa a
   * MAGNITUDE do valor (−1M fica acima de 500K) e o valor é exibido com o sinal.
   */
  leaderboardRanking: "magnitude",
  leaderboardRefreshMs: 3e4,
  // [PROPOSTO]
  reconnectBackoffMs: [1e3, 2e3, 5e3, 1e4, 3e4],
  // [PROPOSTO]
  /**
   * Servidor oficial (Render). Usado quando o jogo é aberto como ARQUIVO (o HTML
   * único) ou hospedado em outro lugar; quando a página vem do próprio servidor,
   * a conexão é com ele mesmo.
   */
  officialServer: "https://he-he-he-j548.onrender.com",
  /** Plano gratuito do Render "dorme": tempo máximo esperando ele acordar ao apertar START. */
  wakeTimeoutMs: 75e3
};
var BOT_CONFIG = {
  totalBots: 100,
  // [ESPEC]
  physicalBotsPerRoom: 2,
  // [ESPEC]
  simulationIntervalMs: 1e3,
  // [PROPOSTO]
  simulationBatchSize: 25,
  // [PROPOSTO] bots processados por fatia
  strategyProfiles: ["velocista", "matematico", "resistente", "longo-prazo", "oportunista", "abissal"],
  // [PROPOSTO]
  /** [ESPEC revisada] IAs também podem seguir o caminho negativo ('abissal' o busca de propósito). */
  allowNegativePath: true
};
var ITEM_CONFIG = {
  /** Potência relativa por raridade (multiplica o % base do buff). [PROPOSTO] */
  itemRarities: [
    { id: "comum", power: 1 },
    { id: "incomum", power: 1.5 },
    { id: "raro", power: 2.25 },
    { id: "epico", power: 3.4 },
    { id: "lendario", power: 5 },
    { id: "mitico", power: 7.5 },
    { id: "divino", power: 11 },
    { id: "celestial", power: 16 },
    { id: "astral", power: 24 },
    { id: "prismatico", power: 36 }
  ],
  cardRarities: [
    { id: "comum", power: 1 },
    { id: "incomum", power: 1.5 },
    { id: "raro", power: 2.25 },
    { id: "epico", power: 3.4 },
    { id: "lendario", power: 5 },
    { id: "mitico", power: 7.5 },
    { id: "divino", power: 11 },
    { id: "celestial", power: 16 },
    { id: "astral", power: 24 },
    { id: "corrompido", power: 36 }
  ],
  buildSlots: 9,
  // [ESPEC]
  buffTypes: ["xpGain", "letterGain", "letterSpeed", "hp", "resistance"]
};

// src/inventory/RaritySystem.ts
var LABELS = {
  comum: "Comum",
  incomum: "Incomum",
  raro: "Raro",
  epico: "\xC9pico",
  lendario: "Lend\xE1rio",
  mitico: "M\xEDtico",
  divino: "Divino",
  celestial: "Celestial",
  astral: "Astral",
  prismatico: "Prism\xE1tico",
  corrompido: "Corrompido"
};
var COLORS = {
  comum: "#b8bccc",
  incomum: "#5ee08a",
  raro: "#4ca8ff",
  epico: "#b467ff",
  lendario: "#ffb340",
  mitico: "#ff5d73",
  divino: "#fff1a8",
  celestial: "#8ff3ff",
  astral: "#7a7bff",
  prismatico: "#ffffff",
  corrompido: "#ff1f5b"
};
var STYLES = ["plain", "soft", "shine", "glow", "flare", "aura", "radiant", "celestial", "astral"];
var WEIGHTS = [55, 25, 10, 5, 2.5, 1.2, 0.6, 0.3, 0.15, 0.05];
function build(list) {
  return list.map((r, tier) => ({
    id: r.id,
    label: LABELS[r.id] ?? r.id,
    tier,
    power: r.power,
    color: COLORS[r.id] ?? "#ffffff",
    style: r.id === "prismatico" ? "prismatic" : r.id === "corrompido" ? "corrupted" : STYLES[tier] ?? "astral",
    weight: WEIGHTS[tier] ?? 0.05
  }));
}
var ITEM_RARITIES = build(ITEM_CONFIG.itemRarities);
var CARD_RARITIES = build(ITEM_CONFIG.cardRarities);
function raritiesOf(kind) {
  return kind === "item" ? ITEM_RARITIES : CARD_RARITIES;
}
function dropWeights(kind, flight) {
  const boost = 1 + (kind === "card" ? 0.2 : 0.15) * Math.max(0, flight);
  return raritiesOf(kind).map((r) => r.weight * Math.pow(boost, r.tier));
}
function pickRarity(kind, flight, u, minTier = 0) {
  const list = raritiesOf(kind);
  const w = dropWeights(kind, flight).map((x, i) => i < minTier ? 0 : x);
  const total = w.reduce((a, b) => a + b, 0);
  let acc = 0;
  for (let i = 0; i < w.length; i++) {
    acc += w[i] / total;
    if (u < acc) return list[i];
  }
  return list[list.length - 1];
}

// src/inventory/ObjectDatabase.ts
var FEMININE = {
  comum: "Comum",
  incomum: "Incomum",
  raro: "Rara",
  epico: "\xC9pica",
  lendario: "Lend\xE1ria",
  mitico: "M\xEDtica",
  divino: "Divina",
  celestial: "Celestial",
  astral: "Astral",
  prismatico: "Prism\xE1tica",
  corrompido: "Corrompida"
};
var CHANNEL_TEXT = {
  xpGain: "XP",
  resistance: "resist\xEAncia",
  hp: "HP",
  letterGain: "ganho de Letras",
  letterSpeed: "velocidade de Letras"
};
var CONSUMABLES = [
  { key: "elixir-degrau", name: "Elixir do Degrau", icon: "\u2697\uFE0F", text: "Destilado dos primeiros degraus.", buffs: [{ channel: "xpGain", percent: 20 }], animation: "beber", sound: "gole" },
  { key: "tonico-bastiao", name: "T\xF4nico do Basti\xE3o", icon: "\u{1F6E1}\uFE0F", text: "Endurece o corpo contra o Miasma.", buffs: [{ channel: "resistance", percent: 25 }], animation: "beber", sound: "gole" },
  { key: "pocao-vital", name: "Po\xE7\xE3o Vital", icon: "\u2764\uFE0F", text: "Expande o limite de vida.", buffs: [{ channel: "hp", percent: 30 }], animation: "beber", sound: "gole", feminine: true },
  { key: "po-escriba", name: "P\xF3 do Escriba", icon: "\u2712\uFE0F", text: "Tinta antiga que atrai Letras.", buffs: [{ channel: "letterGain", percent: 20 }], animation: "esmagar", sound: "sopro" },
  { key: "cristal-veloz", name: "Cristal Veloz", icon: "\u26A1", text: "Acelera o ciclo das Letras.", buffs: [{ channel: "letterSpeed", percent: 15 }], animation: "esmagar", sound: "cristal" },
  { key: "incenso-ascensao", name: "Incenso da Ascens\xE3o", icon: "\u{1F56F}\uFE0F", text: "Fuma\xE7a sagrada que fortalece e ensina.", buffs: [{ channel: "xpGain", percent: 10 }, { channel: "resistance", percent: 10 }], animation: "absorver", sound: "sopro" }
];
var CARDS = [
  { key: "andarilho", name: "Carta do Andarilho", icon: "\u{1F97E}", text: "Quem sobe aprende.", buffs: [{ channel: "xpGain", percent: 8 }], animation: "invocar", sound: "carta", corruptedPenalty: { channel: "hp", percent: 30 } },
  { key: "bastiao", name: "Carta do Basti\xE3o", icon: "\u{1F3F0}", text: "Muralha contra o Miasma.", buffs: [{ channel: "resistance", percent: 10 }], animation: "invocar", sound: "carta", corruptedPenalty: { channel: "xpGain", percent: 25 } },
  { key: "coracao", name: "Carta do Cora\xE7\xE3o Eterno", icon: "\u{1F497}", text: "Um cora\xE7\xE3o que n\xE3o para.", buffs: [{ channel: "hp", percent: 12 }], animation: "invocar", sound: "carta", corruptedPenalty: { channel: "resistance", percent: 20 } },
  { key: "escriba", name: "Carta do Escriba", icon: "\u{1F4DC}", text: "Cada Letra conta.", buffs: [{ channel: "letterGain", percent: 8 }], animation: "invocar", sound: "carta", corruptedPenalty: { channel: "letterSpeed", percent: 25 } },
  { key: "relampago", name: "Carta do Rel\xE2mpago", icon: "\u{1F329}\uFE0F", text: "O tempo se curva.", buffs: [{ channel: "letterSpeed", percent: 6 }], animation: "invocar", sound: "carta", corruptedPenalty: { channel: "letterGain", percent: 25 } },
  { key: "equilibrio", name: "Carta do Equil\xEDbrio", icon: "\u262F\uFE0F", text: "Um pouco de tudo.", buffs: [{ channel: "xpGain", percent: 4 }, { channel: "resistance", percent: 4 }, { channel: "letterGain", percent: 4 }], animation: "invocar", sound: "carta", corruptedPenalty: { channel: "hp", percent: 15 } }
];
var registry2 = /* @__PURE__ */ new Map();
function describe(buffs, penalties, duration) {
  const b = buffs.map((x) => `+${x.percent}% ${CHANNEL_TEXT[x.channel] ?? x.channel}`);
  const p = penalties.map((x) => `\u2212${x.percent}% ${CHANNEL_TEXT[x.channel] ?? x.channel}`);
  return [...b, ...p].join(" \xB7 ") + (duration > 0 ? ` \xB7 ${Math.round(duration)} s` : " \xB7 enquanto equipada");
}
function round2(v) {
  return v >= 100 ? Math.round(v) : Math.round(v * 10) / 10;
}
function make(t, r, kind) {
  const buffs = t.buffs.map((b) => ({ channel: b.channel, percent: round2(b.percent * r.power) }));
  const penalties = kind === "card" && r.style === "corrupted" && t.corruptedPenalty ? [t.corruptedPenalty] : [];
  const durationSec = kind === "consumable" ? 60 * (1 + r.tier * 0.35) : 0;
  return {
    id: `${kind === "card" ? "carta" : "item"}:${t.key}:${r.id}`,
    kind,
    template: t.key,
    name: `${t.name} ${t.feminine || kind === "card" ? FEMININE[r.id] ?? r.label : r.label}`,
    rarityLabel: t.feminine || kind === "card" ? FEMININE[r.id] ?? r.label : r.label,
    description: `${t.text} ${describe(buffs, penalties, durationSec)}`,
    rarity: r,
    icon: t.icon,
    buffs,
    penalties,
    durationSec,
    animation: t.animation,
    sound: t.sound
  };
}
for (const t of CONSUMABLES) for (const r of ITEM_RARITIES) registerObject(make(t, r, "consumable"));
for (const t of CARDS) for (const r of CARD_RARITIES) registerObject(make(t, r, "card"));
function registerObject(def) {
  if (registry2.has(def.id)) throw new Error(`objeto duplicado: ${def.id}`);
  if ([...def.buffs, ...def.penalties].some((b) => !(b.percent > 0))) throw new Error(`buffs devem ser percentuais positivos: ${def.id}`);
  registry2.set(def.id, def);
}
function getObject(id) {
  return registry2.get(id);
}
var CONSUMABLE_TEMPLATES = CONSUMABLES.map((t) => t.key);
var CARD_TEMPLATES = CARDS.map((t) => t.key);

// src/inventory/Inventory.ts
var BUILD_SLOTS = ITEM_CONFIG.buildSlots;
var STARTER_KIT = {
  "item:elixir-degrau:comum": 3,
  "item:tonico-bastiao:comum": 2,
  "carta:bastiao:comum": 1
};
function emptyInventory() {
  return { counts: {}, build: Array.from({ length: BUILD_SLOTS }, () => null), active: [], unseen: [] };
}
function starterInventory() {
  const inv = { ...emptyInventory(), counts: { ...STARTER_KIT } };
  return { ...inv, build: inv.build.map((_, i) => i === 0 ? { kind: "consumable", id: "item:elixir-degrau:comum" } : null), unseen: Object.keys(STARTER_KIT) };
}
function count(inv, id) {
  return inv.counts[id] ?? 0;
}
function equippedCopies(inv, id) {
  return inv.build.filter((s) => s?.kind === "card" && s.id === id).length;
}
function addObject(inv, id, n = 1) {
  if (!getObject(id)) throw new Error(`objeto desconhecido: ${id}`);
  if (!Number.isInteger(n) || n <= 0) throw new Error("quantidade inv\xE1lida");
  return {
    ...inv,
    counts: { ...inv.counts, [id]: count(inv, id) + n },
    unseen: inv.unseen.includes(id) ? inv.unseen : [...inv.unseen, id]
  };
}
function markSeen(inv) {
  return inv.unseen.length ? { ...inv, unseen: [] } : inv;
}
function canEquip(inv, slot, id) {
  if (!Number.isInteger(slot) || slot < 0 || slot >= BUILD_SLOTS) return "slot-invalido";
  const def = getObject(id);
  if (!def) return "desconhecido";
  if (def.kind === "card") {
    const already = inv.build[slot]?.kind === "card" && inv.build[slot]?.id === id ? 1 : 0;
    if (equippedCopies(inv, id) - already >= count(inv, id)) return "sem-copias";
  } else if (count(inv, id) <= 0) return "sem-copias";
  return null;
}
function equip(inv, slot, id) {
  const err = canEquip(inv, slot, id);
  if (err) throw new Error(err);
  const def = getObject(id);
  const build2 = [...inv.build];
  build2[slot] = { kind: def.kind, id };
  return { ...inv, build: build2 };
}
function unequip(inv, slot) {
  if (!inv.build[slot]) return inv;
  const build2 = [...inv.build];
  build2[slot] = null;
  return { ...inv, build: build2 };
}
function useObject(inv, id) {
  const def = getObject(id);
  if (!def || def.kind !== "consumable" || count(inv, id) <= 0) return { inv, used: null };
  const left = count(inv, id) - 1;
  const counts = { ...inv.counts };
  if (left > 0) counts[id] = left;
  else delete counts[id];
  const build2 = left > 0 ? inv.build : inv.build.map((s) => s?.id === id ? null : s);
  const prev = inv.active.find((a) => a.id === id);
  const active = [...inv.active.filter((a) => a.id !== id), { id, remaining: Math.max(prev?.remaining ?? 0, def.durationSec) }];
  return { inv: { ...inv, counts, build: build2, active }, used: def };
}
function useSlot(inv, slot) {
  const s = inv.build[slot];
  if (!s || s.kind !== "consumable") return { inv, used: null };
  return useObject(inv, s.id);
}
function tickInventory(inv, dt) {
  if (!inv.active.length || !(dt > 0)) return { inv, expired: [] };
  const expired = [];
  const active = [];
  for (const a of inv.active) {
    const r = a.remaining - dt;
    if (r > 0) active.push({ id: a.id, remaining: r });
    else expired.push(a.id);
  }
  return { inv: { ...inv, active }, expired };
}
function buffSources(inv) {
  const out = [];
  const push = (def, tag) => {
    for (const b of def.buffs) out.push({ id: `${tag}:${def.id}:${b.channel}`, label: def.name, channel: b.channel, kind: "percent", value: NumberEngine.from(b.percent) });
    for (const p of def.penalties) out.push({ id: `${tag}:${def.id}:${p.channel}:pen`, label: def.name, channel: p.channel, kind: "penalty", value: NumberEngine.from(p.percent) });
  };
  inv.build.forEach((s, i) => {
    if (s?.kind !== "card") return;
    const def = getObject(s.id);
    if (def) push(def, `slot${i}`);
  });
  for (const a of inv.active) {
    const def = getObject(a.id);
    if (def) push(def, "ativo");
  }
  return out;
}
function normalizeInventory(raw) {
  const base = emptyInventory();
  if (typeof raw !== "object" || raw === null) return base;
  const x = raw;
  const counts = {};
  for (const [id, n] of Object.entries(x.counts ?? {})) {
    if (getObject(id) && Number.isInteger(n) && n > 0 && n < 1e9) counts[id] = n;
  }
  let inv = { ...base, counts };
  const build2 = Array.isArray(x.build) ? x.build : [];
  for (let i = 0; i < BUILD_SLOTS; i++) {
    const s = build2[i];
    if (s && typeof s.id === "string" && canEquip(inv, i, s.id) === null) inv = equip(inv, i, s.id);
  }
  const active = (Array.isArray(x.active) ? x.active : []).filter((a) => a && getObject(a.id)?.kind === "consumable" && typeof a.remaining === "number" && a.remaining > 0).map((a) => ({ id: a.id, remaining: Math.min(a.remaining, getObject(a.id).durationSec) }));
  const unseen = (Array.isArray(x.unseen) ? x.unseen : []).filter((id) => typeof id === "string" && counts[id]);
  return { ...inv, active, unseen };
}

// src/core/GameState.ts
var SAVE_SCHEMA_VERSION = 5;
var PROGRESSION_STATS = [
  "level",
  "xp",
  "letters",
  "multiplier",
  "rank",
  "rebirth",
  "ultraRebirth",
  "prestige",
  "ultraPrestige"
];
function createInitialProgress() {
  const stats = {};
  for (const s of PROGRESSION_STATS) stats[s] = NumberEngine.ZERO;
  stats.level = NumberEngine.from(LEVEL_CONFIG.startLevel);
  return {
    stats,
    levelUps: NumberEngine.ZERO,
    highestStep: NumberEngine.ZERO,
    pedestalsPassed: NumberEngine.ZERO,
    miasmaDeaths: 0,
    negations: 0,
    unlockedEffects: [],
    polarity: 1,
    automation: {},
    letterTimer: 0,
    dropTimer: 0,
    dropRolls: 0,
    inventory: starterInventory()
  };
}
function applyTotalNegation(state) {
  const base = {
    ...state,
    negations: state.negations + 1,
    miasmaDeaths: MIASMA_CONFIG.resetDeathCounterAfterNegation ? 0 : state.miasmaDeaths,
    unlockedEffects: [...state.unlockedEffects],
    automation: { ...state.automation }
  };
  if (state.polarity === -1 && MIASMA_CONFIG.negationWhenAlreadyNegative === "keep") return base;
  return {
    ...base,
    stats: NegativeNumberEngine.negateAll(state.stats),
    polarity: state.polarity === 1 ? -1 : 1
  };
}
function checksum(text) {
  let h = 2166136261;
  for (let i = 0; i < text.length; i++) {
    h ^= text.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return (h >>> 0).toString(16).padStart(8, "0");
}
function toSerialized(s) {
  const stats = {};
  for (const k of PROGRESSION_STATS) stats[k] = s.stats[k].toJSON();
  return {
    stats,
    levelUps: s.levelUps.toJSON(),
    highestStep: s.highestStep.toJSON(),
    pedestalsPassed: s.pedestalsPassed.toJSON(),
    miasmaDeaths: s.miasmaDeaths,
    negations: s.negations,
    unlockedEffects: [...s.unlockedEffects],
    polarity: s.polarity,
    automation: { ...s.automation },
    letterTimer: s.letterTimer,
    dropTimer: s.dropTimer,
    dropRolls: s.dropRolls,
    inventory: JSON.parse(JSON.stringify(s.inventory))
  };
}
function serializeProgress(state, now = /* @__PURE__ */ new Date()) {
  const payload = toSerialized(state);
  const envelope = {
    schema: SAVE_SCHEMA_VERSION,
    savedAt: now.toISOString(),
    checksum: checksum(JSON.stringify(payload)),
    payload
  };
  return JSON.stringify(envelope);
}
var migrations = {
  // v1 → v2: estado de polaridade (espelho negativo) e automáticos do Livro de Prestígios.
  1: (payload) => {
    const p = payload;
    const negative = Object.values(p.stats ?? {}).some((v) => v && v.s < 0);
    return { ...p, polarity: negative ? -1 : 1, automation: {} };
  },
  // v2 → v3: cronômetro do ciclo de Letras (Fase 4).
  2: (payload) => ({ ...payload, letterTimer: 0 }),
  // v3 → v4: inventário/build (Fase 6) — saves antigos recebem o kit inicial.
  3: (payload) => ({ ...payload, inventory: starterInventory() }),
  // v4 → v5: relógio de drops (3 objetos a cada 2:30).
  4: (payload) => ({ ...payload, dropTimer: 0, dropRolls: 0 })
};
function deserializeProgress(text) {
  let env;
  try {
    env = JSON.parse(text);
  } catch {
    throw new MathError("INVALID_INPUT", "save corrompido: JSON inv\xE1lido");
  }
  if (typeof env !== "object" || env === null || typeof env.schema !== "number") {
    throw new MathError("INVALID_INPUT", "save corrompido: envelope ausente");
  }
  if (env.schema > SAVE_SCHEMA_VERSION) {
    throw new MathError("INVALID_INPUT", `save de vers\xE3o futura (${env.schema}); atualize o jogo`);
  }
  if (checksum(JSON.stringify(env.payload)) !== env.checksum) {
    throw new MathError("INVALID_INPUT", "save corrompido: checksum n\xE3o confere");
  }
  let payload = env.payload;
  for (let v = env.schema; v < SAVE_SCHEMA_VERSION; v++) {
    const m = migrations[v];
    if (!m) throw new MathError("INVALID_INPUT", `sem migra\xE7\xE3o do esquema ${v}`);
    payload = m(payload);
  }
  const p = payload;
  const stats = {};
  for (const k of PROGRESSION_STATS) stats[k] = MathValidator.fromJSON(p.stats?.[k]);
  if (!Number.isInteger(p.miasmaDeaths) || p.miasmaDeaths < 0) throw new MathError("INVALID_INPUT", "miasmaDeaths inv\xE1lido");
  if (!Number.isInteger(p.negations) || p.negations < 0) throw new MathError("INVALID_INPUT", "negations inv\xE1lido");
  if (p.polarity !== 1 && p.polarity !== -1) throw new MathError("INVALID_INPUT", "polarity inv\xE1lido");
  if (!Array.isArray(p.unlockedEffects) || p.unlockedEffects.some((e) => typeof e !== "string")) {
    throw new MathError("INVALID_INPUT", "unlockedEffects inv\xE1lido");
  }
  return {
    stats,
    levelUps: MathValidator.fromJSON(p.levelUps),
    highestStep: MathValidator.fromJSON(p.highestStep),
    pedestalsPassed: MathValidator.fromJSON(p.pedestalsPassed),
    miasmaDeaths: p.miasmaDeaths,
    negations: p.negations,
    unlockedEffects: [...p.unlockedEffects],
    polarity: p.polarity === -1 ? -1 : 1,
    automation: { ...p.automation ?? {} },
    letterTimer: typeof p.letterTimer === "number" && p.letterTimer >= 0 && p.letterTimer < 1e6 ? p.letterTimer : 0,
    dropTimer: typeof p.dropTimer === "number" && p.dropTimer >= 0 && p.dropTimer < 1e6 ? p.dropTimer : 0,
    dropRolls: Number.isSafeInteger(p.dropRolls) && p.dropRolls >= 0 ? p.dropRolls : 0,
    inventory: normalizeInventory(p.inventory)
  };
}

// src/player/CharacterCatalog.ts
var catalog = {
  bodyType: [
    { id: "atletico", label: "Atl\xE9tico", params: { height: 1, shoulder: 1.05, hip: 1, limb: 1.05, head: 1 } },
    { id: "esguio", label: "Esguio", params: { height: 1.04, shoulder: 0.9, hip: 0.92, limb: 0.86, head: 0.97 } },
    { id: "robusto", label: "Robusto", params: { height: 1.02, shoulder: 1.18, hip: 1.1, limb: 1.25, head: 1.02 } },
    { id: "compacto", label: "Compacto", params: { height: 0.9, shoulder: 0.98, hip: 1.02, limb: 1.05, head: 1.1 } },
    { id: "heroico", label: "Heroico", params: { height: 1.08, shoulder: 1.22, hip: 0.95, limb: 1.12, head: 0.94 } }
  ],
  faceShape: [
    { id: "oval", label: "Oval", params: { width: 1, length: 1.06, jaw: 0.82, chin: 0.25 } },
    { id: "angular", label: "Angular", params: { width: 0.97, length: 1.08, jaw: 0.92, chin: 0.45 } },
    { id: "redondo", label: "Redondo", params: { width: 1.06, length: 0.98, jaw: 0.95, chin: 0.1 } },
    { id: "coracao", label: "Cora\xE7\xE3o", params: { width: 1.03, length: 1.04, jaw: 0.7, chin: 0.4 } },
    { id: "quadrado", label: "Quadrado", params: { width: 1.04, length: 1.02, jaw: 1.02, chin: 0.2 } },
    { id: "fino", label: "Fino", params: { width: 0.92, length: 1.1, jaw: 0.75, chin: 0.35 } }
  ],
  mouth: [
    { id: "sorriso", label: "Sorriso", params: { curve: 0.5, width: 1, open: 0 } },
    { id: "neutro", label: "Neutro", params: { curve: 0.05, width: 0.9, open: 0 } },
    { id: "confiante", label: "Confiante", params: { curve: 0.3, width: 0.95, open: 0, smirk: 1 } },
    { id: "alegre", label: "Alegre", params: { curve: 0.7, width: 1.05, open: 1 } },
    { id: "serio", label: "S\xE9rio", params: { curve: -0.15, width: 0.85, open: 0 } }
  ],
  eyeShape: [
    { id: "amendoado", label: "Amendoado", params: { w: 1.15, h: 0.85, tilt: 0.08, lid: 0.2 } },
    { id: "redondo", label: "Redondo", params: { w: 1, h: 1.05, tilt: 0, lid: 0.05 } },
    { id: "felino", label: "Felino", params: { w: 1.2, h: 0.8, tilt: 0.28, lid: 0.25 } },
    { id: "sereno", label: "Sereno", params: { w: 1.1, h: 0.85, tilt: -0.04, lid: 0.45 } },
    { id: "determinado", label: "Determinado", params: { w: 1.1, h: 0.82, tilt: -0.15, lid: 0.35 } }
  ],
  brows: [
    { id: "retas", label: "Retas", params: { arch: 0.05, thick: 1, tilt: 0 } },
    { id: "arqueadas", label: "Arqueadas", params: { arch: 0.35, thick: 0.9, tilt: 0.05 } },
    { id: "grossas", label: "Grossas", params: { arch: 0.12, thick: 1.6, tilt: -0.05 } },
    { id: "finas", label: "Finas", params: { arch: 0.25, thick: 0.55, tilt: 0.05 } },
    { id: "angulares", label: "Angulares", params: { arch: 0.2, thick: 1.1, tilt: -0.22 } },
    { id: "nenhuma", label: "Nenhuma", params: { arch: 0, thick: 0, tilt: 0 } }
  ],
  hair: [
    { id: "curto", label: "Curto" },
    { id: "franja", label: "Franja Lateral" },
    { id: "espetado", label: "Espetado" },
    { id: "moicano", label: "Moicano" },
    { id: "longo", label: "Longo" },
    { id: "rabo", label: "Rabo de Cavalo" },
    { id: "coque", label: "Coque" },
    { id: "afro", label: "Black Power" },
    { id: "tranca", label: "Tran\xE7a" },
    { id: "chanel", label: "Chanel" },
    { id: "raspado", label: "Raspado" },
    { id: "careca", label: "Careca" }
  ],
  top: [
    { id: "camiseta", label: "Camiseta" },
    { id: "regata", label: "Regata" },
    { id: "jaqueta", label: "Jaqueta" },
    { id: "moletom", label: "Moletom com Capuz" },
    { id: "armadura", label: "Armadura Leve" },
    { id: "sobretudo", label: "Sobretudo" }
  ],
  bottom: [
    { id: "calca", label: "Cal\xE7a" },
    { id: "cargo", label: "Cal\xE7a Cargo" },
    { id: "shorts", label: "Shorts" },
    { id: "saia", label: "Saia" },
    { id: "legging", label: "Legging" }
  ],
  shoes: [
    { id: "tenis", label: "T\xEAnis" },
    { id: "botas", label: "Botas" },
    { id: "coturno", label: "Coturno" },
    { id: "neon", label: "Sapatilha Neon" },
    { id: "descalco", label: "Descal\xE7o" }
  ],
  accessory: [
    { id: "oculos", label: "\xD3culos" },
    { id: "viseira", label: "Viseira Cibern\xE9tica" },
    { id: "fones", label: "Fones" },
    { id: "cachecol", label: "Cachecol" },
    { id: "colar", label: "Colar de Cristal" },
    { id: "brincos", label: "Brincos" },
    { id: "aureola", label: "Aur\xE9ola de Luz" },
    { id: "capa", label: "Capa" }
  ],
  detail: [
    { id: "sardas", label: "Sardas" },
    { id: "cicatriz", label: "Cicatriz" },
    { id: "pintura", label: "Pintura Facial" },
    { id: "marca", label: "Marca Brilhante" },
    { id: "rubor", label: "Rubor" }
  ]
};
var ACCESSORY_SLOTS = {
  oculos: "olhos",
  viseira: "olhos",
  fones: "orelhas",
  brincos: "orelhas-joia",
  cachecol: "pescoco",
  colar: "pescoco-joia",
  aureola: "acima",
  capa: "costas"
};
var SKIN_TONES = [
  "#fbe3d0",
  "#f5d0b5",
  "#eabf9c",
  "#e0ab86",
  "#c98e66",
  "#b07650",
  "#935d3c",
  "#7a4a2e",
  "#5f3822",
  "#4a2a19",
  "#c9b7f0",
  "#9fd8c9"
];
var HAIR_COLORS = [
  "#141018",
  "#3b2618",
  "#6b4226",
  "#a0683a",
  "#d9a65e",
  "#f1dca0",
  "#e8e6ef",
  "#b8322f",
  "#e86a2a",
  "#7b3fe4",
  "#2f7bff",
  "#1fd1a8",
  "#ff5fa2",
  "#9aa3b5"
];
var EYE_COLORS = [
  "#3a2416",
  "#6b4423",
  "#2e6b4f",
  "#3c7bd6",
  "#8ab6d6",
  "#7f7f7f",
  "#c48a2b",
  "#8a2be2",
  "#e23a5b",
  "#19d4c0",
  "#ffd23f",
  "#f5f5f5"
];
var OUTFIT_COLORS = [
  "#101014",
  "#2b2d42",
  "#4a4e69",
  "#e9ecef",
  "#c1121f",
  "#ff6b35",
  "#ffd166",
  "#06d6a0",
  "#118ab2",
  "#3a0ca3",
  "#7209b7",
  "#f72585",
  "#5c4033",
  "#2d6a4f",
  "#bc6c25",
  "#adb5bd"
];
var PALETTES = [
  { id: "aurora", label: "Aurora", top: "#3a0ca3", bottom: "#1b1b3a", shoes: "#e9ecef", accent: "#4cc9f0" },
  { id: "abismo", label: "Abismo", top: "#101014", bottom: "#2b2d42", shoes: "#101014", accent: "#7209b7" },
  { id: "solar", label: "Solar", top: "#ff6b35", bottom: "#5c4033", shoes: "#ffd166", accent: "#ffd166" },
  { id: "neon", label: "Neon", top: "#101014", bottom: "#101014", shoes: "#06d6a0", accent: "#f72585" },
  { id: "monocromo", label: "Monocromo", top: "#e9ecef", bottom: "#4a4e69", shoes: "#101014", accent: "#adb5bd" },
  { id: "floresta", label: "Floresta", top: "#2d6a4f", bottom: "#5c4033", shoes: "#bc6c25", accent: "#95d5b2" },
  { id: "realeza", label: "Realeza", top: "#7209b7", bottom: "#101014", shoes: "#ffd166", accent: "#ffd166" },
  { id: "corrompido", label: "Corrompido", top: "#1a0b1f", bottom: "#2b0a0a", shoes: "#101014", accent: "#ff1744" }
];
function items(category) {
  return catalog[category];
}
function findItem(category, id) {
  return catalog[category].find((i) => i.id === id);
}
function hasItem(category, id) {
  return findItem(category, id) !== void 0;
}

// src/titles/TitleCatalog.ts
var CATEGORIES = [
  { id: "degraus", label: "Primeiros Degraus", lore: "Os primeiros passos na pedra fria.", effect: "none", animation: "none", font: "clean", sound: "chime", ornament: ["\xB7", "\xB7"] },
  { id: "ascendentes", label: "Ascendentes", lore: "Quem j\xE1 sentiu o Miasma e continuou subindo.", effect: "glow", animation: "breathe", font: "clean", sound: "chime", ornament: ["\u2726", "\u2726"] },
  { id: "herdeiros", label: "Herdeiros", lore: "Os que herdaram a vontade da escada.", effect: "shimmer", animation: "sweep", font: "serif", sound: "bells", ornament: ["\u2756", "\u2756"] },
  { id: "guardioes", label: "Guardi\xF5es", lore: "Sentinelas das grandes hierarquias.", effect: "flame", animation: "float", font: "serif", sound: "fanfare", ornament: ["\u269C", "\u269C"] },
  { id: "soberanos", label: "Soberanos", lore: "Reinam acima dos n\xFAmeros comuns.", effect: "aurora", animation: "pulse", font: "display", sound: "fanfare", ornament: ["\u265B", "\u265B"] },
  { id: "transcendentes", label: "Transcendentes", lore: "J\xE1 n\xE3o cabem nas nota\xE7\xF5es conhecidas.", effect: "prism", animation: "orbit", font: "display", sound: "choir", ornament: ["\u2736", "\u2736"] },
  { id: "primordiais", label: "Primordiais", lore: "Anteriores \xE0s pr\xF3prias abrevia\xE7\xF5es.", effect: "starfield", animation: "orbit", font: "runic", sound: "cosmic", ornament: ["\u2604", "\u2604"] },
  { id: "entidades", label: "Entidades", lore: "Al\xE9m da \xFAltima hierarquia. Al\xE9m do \u03A9.", effect: "void", animation: "storm", font: "runic", sound: "void", ornament: ["\u27C1", "\u27C1"] }
];
var ROWS = {
  degraus: [
    ["0.01", "Aprendiz do Primeiro Degrau", "Todo infinito come\xE7a com um passo em falso.", "#c9c5e8", "#9d97c9"],
    ["0.02", "Pisante da Pedra Fria", "Sentiu o frio da escada e n\xE3o voltou atr\xE1s.", "#b8d4e8", "#8fb3cf"],
    ["0.03", "Contador de Degraus", "Perdeu a conta no vig\xE9simo e come\xE7ou de novo.", "#c2e0c6", "#93bf99"],
    ["0.05", "F\xF4lego da Subida", "Aprendeu a respirar entre um lance e outro.", "#e6d9b8", "#c7b48a"],
    ["0.08", "Sentinela do Corrim\xE3o", "Conhece cada poste de luz do primeiro lance.", "#d7c2ee", "#b096d8"],
    ["0.12", "Errante da Escadaria Violeta", "Caminha onde o c\xE9u come\xE7a a escurecer.", "#c7a8ff", "#9a74f0"],
    ["0.18", "Portador da Primeira Chama", "Acendeu a primeira luz no Miasma raso.", "#ffcf9e", "#f2a65e"],
    ["0.25", "Vigia do Quarto Patamar", "J\xE1 viu quatro pedestais acenderem.", "#a8e6ff", "#6cc8f0"]
  ],
  ascendentes: [
    ["0.35", "Ascendente de Passo Firme", "O degrau treme; o passo, n\xE3o.", "#8fd0ff", "#4ca8ff"],
    ["0.5", "Disc\xEDpulo do Horizonte", "Metade de um n\xEDvel inteiro, e o horizonte ainda foge.", "#9ef0d4", "#3ddc97"],
    ["0.65", "Rastreador de Pedestais", "Encontra o pr\xF3ximo pedestal pelo brilho no Miasma.", "#ffe08a", "#ffb340"],
    ["0.8", "Andarilho do Miasma Raso", "A n\xE9voa j\xE1 o reconhece e abre passagem.", "#d29bff", "#a05cff"],
    ["1", "Escriba das Letras Vivas", "Alcan\xE7ou o primeiro n\xEDvel inteiro: as Letras respondem ao seu nome.", "#fff1a8", "#ffd166"],
    ["1.25", "L\xE2mina da Escada Infinita", "Corta o ar dos lances como quem conhece o fim.", "#b8f3ff", "#4cc9f0"],
    ["1.5", "Cora\xE7\xE3o de M\xE1rmore Estelar", "Pulsa no mesmo ritmo das estrelas sobre a escada.", "#ffc2e0", "#ff7ab8"],
    ["2", "Arauto do Degrau Sem Nome", "Anuncia degraus que ningu\xE9m pisou antes.", "#c4ffb0", "#7ee05e"]
  ],
  herdeiros: [
    ["2.5", "Herdeiro da Ascens\xE3o Infinita", "A escada o escolheu como continua\xE7\xE3o de si mesma.", "#ffd9a0", "#ff9f43"],
    ["3", "Tecel\xE3o de Multiplicadores", "Tran\xE7a ganhos sobre ganhos at\xE9 virarem tape\xE7aria.", "#a0e7ff", "#38b6ff"],
    ["4", "Guardi\xE3o do Patamar Perdido", "Protege um patamar que s\xF3 aparece para quem sobe demais.", "#d8b4ff", "#8f5bff"],
    ["5", "Domador da N\xE9voa Rubra", "O Miasma vermelho deita a seus p\xE9s.", "#ff9aa8", "#ff4d6d"],
    ["6.5", "Cronista dos Mil Degraus", "Escreveu a hist\xF3ria de cada lance que venceu.", "#e8ffb0", "#b6e65a"],
    ["8", "Portador do Selo Romano", "Carrega um Rank que s\xF3 os antigos sabiam escrever.", "#ffe6b0", "#e0b45a"],
    ["10", "Alquimista do Renascimento", "Transforma recome\xE7os em pot\xEAncia pura.", "#b0fff0", "#2ee6c4"],
    ["12.5", "Cavaleiro do Abismo Ascendente", "Sobe pelo abismo como quem desce por um rio.", "#c8c8ff", "#6f6fff"]
  ],
  guardioes: [
    ["15", "Sentinela das Hierarquias", "Vigia a fronteira entre milh\xF5es e bilh\xF5es.", "#ffb870", "#ff7b2e"],
    ["20", "Mestre da Torre Sem Teto", "Descobriu que a escada n\xE3o termina \u2014 e sorriu.", "#ffd47a", "#ffa600"],
    ["25", "Senhor dos Ultra Renascimentos", "Renasceu tantas vezes que a morte pede licen\xE7a.", "#ff9de2", "#e040fb"],
    ["35", "Vidente da Curva Exponencial", "Enxerga onde cada curva de custo vai explodir.", "#9dfff5", "#18ffff"],
    ["50", "Paladino do Prest\xEDgio Eterno", "Jurou nunca recusar um Prest\xEDgio justo.", "#fff59d", "#ffd600"],
    ["75", "Regente das Letras Ancestrais", "As Letras malucas obedecem \xE0 sua caligrafia.", "#ffab91", "#ff3d00"],
    ["100", "Guardi\xE3o do \xDAltimo Corrim\xE3o", "N\xEDvel cem: o corrim\xE3o termina aqui; a escada, n\xE3o.", "#ff8a80", "#d50000"],
    ["150", "Juiz das Escalas Imposs\xEDveis", "Decide o que \xE9 grande demais \u2014 e nada \xE9.", "#ffd180", "#ff6d00"]
  ],
  soberanos: [
    ["250", "Soberano do Horizonte Imposs\xEDvel", "O horizonte fugiu por tempo demais. Agora \xE9 seu.", "#b388ff", "#00e5ff"],
    ["500", "Imperador da Escadaria Celeste", "Cada degrau celeste paga tributo ao seu passo.", "#82b1ff", "#e040fb"],
    ["1e3", "Monarca do Milhar de N\xEDveis", "Mil n\xEDveis inteiros, contados um a um.", "#ffe57f", "#ff4081"],
    ["1e4", "Patriarca das Ordens de Grandeza", "Fundou uma linhagem de expoentes.", "#a7ffeb", "#7c4dff"],
    ["1e5", "Arquiteto do Infinito Cont\xE1vel", "Constr\xF3i o infinito com tijolos que ainda d\xE1 para contar.", "#ffd740", "#00b0ff"],
    ["1e6", "Soberano das Nota\xE7\xF5es Perdidas", "Usa abrevia\xE7\xF5es que os dicion\xE1rios esqueceram.", "#ea80fc", "#18ffff"],
    ["1e9", "Trono Acima dos Bilh\xF5es", "Senta-se num trono feito de bilh\xF5es de degraus.", "#ff80ab", "#ffd740"],
    ["1e12", "Coroa do Trilh\xE3o Silencioso", "O trilh\xE3o n\xE3o faz barulho quando \xE9 dominado.", "#b9f6ca", "#651fff"]
  ],
  transcendentes: [
    ["1e15", "Transcendente da Mat\xE9ria Num\xE9rica", "Os n\xFAmeros deixaram de ser medida e viraram mat\xE9ria.", "#ffffff", "#b388ff"],
    ["1e18", "Eco Al\xE9m do Quintilh\xE3o", "Sua voz ainda ecoa onde o quintilh\xE3o acaba.", "#e0f7fa", "#00e5ff"],
    ["1e24", "Peregrino Entre Gal\xE1xias de D\xEDgitos", "Viaja entre gal\xE1xias feitas s\xF3 de algarismos.", "#f3e5f5", "#d500f9"],
    ["1e33", "Or\xE1culo do Decilh\xE3o", "Prev\xEA o pr\xF3ximo decilh\xE3o antes que ele exista.", "#fffde7", "#ffea00"],
    ["1e50", "Viajante da Torre de Expoentes", "Sobe uma torre onde cada andar \xE9 um expoente.", "#e8eaf6", "#3d5afe"],
    ["1e75", "Forjador de Constela\xE7\xF5es Aritm\xE9ticas", "Bate estrelas na bigorna at\xE9 virarem equa\xE7\xF5es.", "#fff3e0", "#ff9100"],
    ["1e100", "Senhor do Googol Desperto", "Acordou o googol \u2014 e ele agora o segue.", "#e0f2f1", "#1de9b6"],
    ["1e150", "Ascendido Al\xE9m da Contagem", "Nenhum contador humano alcan\xE7a onde est\xE1.", "#fce4ec", "#ff1744"]
  ],
  primordiais: [
    ["1e200", "Primordial das Abrevia\xE7\xF5es Esquecidas", "Fala a l\xEDngua anterior a todas as abrevia\xE7\xF5es.", "#ffecb3", "#ff6f00"],
    ["1e308", "Devorador do Limite Flutuante", "Engoliu o maior n\xFAmero que uma m\xE1quina comum conhece.", "#d1c4e9", "#6200ea"],
    ["1e500", "Arconte das Hierarquias Superiores", "Governa hierarquias que s\xF3 o motor central enxerga.", "#b2ebf2", "#00b8d4"],
    ["1e1000", "Sombra do Mil\xE9simo Expoente", "Projeta sombra sobre mil casas decimais.", "#cfd8dc", "#263238"],
    ["1e10000", "Tirano das Torres de Dez", "Empilha dez sobre dez at\xE9 o c\xE9u dobrar.", "#ffcdd2", "#b71c1c"],
    ["1e1000000", "Primog\xEAnito do Expoente Infinito", "Nasceu onde o expoente tem um milh\xE3o de d\xEDgitos.", "#f8bbd0", "#880e4f"]
  ],
  entidades: [
    ["1e1000000000", "Entidade Al\xE9m da \xDAltima Hierarquia", "J\xE1 n\xE3o \xE9 jogador: \xE9 um evento na escada.", "#ffffff", "#7c4dff"],
    ["1e1000000000000", "Consci\xEAncia da Escada Sem Fim", "A escada pensa \u2014 e pensa com a sua voz.", "#e1f5fe", "#00e5ff"],
    ["1e1e30", "Vazio Que Conta a Si Mesmo", "Um sil\xEAncio que soma, multiplica e n\xE3o termina.", "#eceff1", "#000000"],
    ["1e1e100", "Aquele que o Googolplex Teme", "O googolplex desvia o olhar quando passa.", "#fff8e1", "#ff3d00"],
    ["1e1e200", "Singularidade do \xDAltimo Degrau", "Todo degrau converge para um \xFAnico ponto: voc\xEA.", "#f3e5f5", "#aa00ff"],
    ["1e1e300", "\u03A9 \u2014 O Inomin\xE1vel Acima do \xD4mega", "O motor central precisou inventar um s\xEDmbolo para escrev\xEA-lo.", "#ffffff", "#ff00e6"]
  ]
};
function slug(s) {
  return s.normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}
var TITLES = (() => {
  const out = [];
  for (const cat of CATEGORIES) {
    const rows = ROWS[cat.id];
    rows.forEach(([lvl, name, desc, color, colorB], i) => {
      const tower = /^1e1e(\d+)$/.exec(lvl);
      const requirement = tower ? NumberEngine.fromLog10(10 ** Number(tower[1])) : NumberEngine.from(lvl);
      out.push({
        id: slug(name),
        index: out.length,
        name,
        description: desc,
        category: cat,
        requirement,
        requirementText: `N\xEDvel ${NumberEngine.format(requirement)}`,
        color,
        colorB,
        effect: cat.effect,
        animation: cat.animation,
        font: cat.font,
        sound: cat.sound,
        intensity: rows.length > 1 ? i / (rows.length - 1) : 1
      });
    });
  }
  return out;
})();
var BY_ID = new Map(TITLES.map((t) => [t.id, t]));
var EFFECTS = ["none", "glow", "shimmer", "flame", "aurora", "prism", "starfield", "glitch", "void"];
var ANIMS = ["none", "breathe", "sweep", "float", "pulse", "orbit", "storm"];
var FONTS = ["clean", "serif", "display", "runic", "mono"];
function normalizeTitleChoice(x) {
  if (typeof x !== "object" || x === null) return void 0;
  const c = x;
  const out = {};
  if (typeof c.id === "string" && BY_ID.has(c.id)) out.id = c.id;
  if (typeof c.color === "string" && /^#[0-9a-f]{6}$/i.test(c.color)) out.color = c.color.toLowerCase();
  if (EFFECTS.includes(c.effect)) out.effect = c.effect;
  if (ANIMS.includes(c.animation)) out.animation = c.animation;
  if (FONTS.includes(c.font)) out.font = c.font;
  return Object.keys(out).length ? out : void 0;
}

// src/player/CharacterAppearance.ts
var APPEARANCE_VERSION = 1;
var NAME_MIN = 3;
var NAME_MAX = 16;
var NAME_RE = /^[\p{L}\p{N} _.-]+$/u;
var HEX_RE = /^#[0-9a-f]{6}$/i;
function defaultAppearance() {
  return {
    version: APPEARANCE_VERSION,
    name: "Ascendente",
    body: { type: "atletico", height: 0.5, build: 0.45, silhouette: 0.35 },
    face: { shape: "oval", mouth: "confiante" },
    skin: "#e0ab86",
    eyes: { shape: "amendoado", color: "#3c7bd6", colorRight: "#3c7bd6" },
    brows: { style: "arqueadas", color: "#3b2618" },
    hair: { style: "franja", color: "#3b2618", tip: "#a0683a" },
    outfit: {
      top: "jaqueta",
      topColor: "#3a0ca3",
      bottom: "cargo",
      bottomColor: "#1b1b3a",
      shoes: "tenis",
      shoeColor: "#e9ecef",
      accent: "#4cc9f0"
    },
    accessories: ["fones"],
    details: [],
    palette: "aurora"
  };
}
function validateName(name) {
  if (typeof name !== "string") return "nome ausente";
  const t = name.trim();
  if (t.length < NAME_MIN) return `o nome precisa de pelo menos ${NAME_MIN} caracteres`;
  if (t.length > NAME_MAX) return `o nome pode ter no m\xE1ximo ${NAME_MAX} caracteres`;
  if (!NAME_RE.test(t)) return "use apenas letras, n\xFAmeros, espa\xE7o, ponto, h\xEDfen ou sublinhado";
  return null;
}
function normalizeAppearance(input) {
  const d = defaultAppearance();
  if (typeof input !== "object" || input === null) return d;
  const x = input;
  const pick = (cat, v, fb) => typeof v === "string" && hasItem(cat, v) ? v : fb;
  const col = (v, fb) => typeof v === "string" && HEX_RE.test(v) ? v.toLowerCase() : fb;
  const unit = (v, fb) => typeof v === "number" && Number.isFinite(v) ? Math.min(1, Math.max(0, v)) : fb;
  const name = typeof x.name === "string" && validateName(x.name) === null ? x.name.trim() : d.name;
  const slots = /* @__PURE__ */ new Set();
  const accessories = (Array.isArray(x.accessories) ? x.accessories : d.accessories).filter((a) => {
    if (typeof a !== "string" || !hasItem("accessory", a)) return false;
    const slot = ACCESSORY_SLOTS[a];
    if (slot && slots.has(slot)) return false;
    if (slot) slots.add(slot);
    return true;
  });
  const details = [...new Set((Array.isArray(x.details) ? x.details : d.details).filter((v) => typeof v === "string" && hasItem("detail", v)))];
  return {
    version: APPEARANCE_VERSION,
    name,
    body: {
      type: pick("bodyType", x.body?.type, d.body.type),
      height: unit(x.body?.height, d.body.height),
      build: unit(x.body?.build, d.body.build),
      silhouette: unit(x.body?.silhouette, d.body.silhouette)
    },
    face: { shape: pick("faceShape", x.face?.shape, d.face.shape), mouth: pick("mouth", x.face?.mouth, d.face.mouth) },
    skin: col(x.skin, d.skin),
    eyes: {
      shape: pick("eyeShape", x.eyes?.shape, d.eyes.shape),
      color: col(x.eyes?.color, d.eyes.color),
      colorRight: col(x.eyes?.colorRight, col(x.eyes?.color, d.eyes.colorRight))
    },
    brows: { style: pick("brows", x.brows?.style, d.brows.style), color: col(x.brows?.color, d.brows.color) },
    hair: { style: pick("hair", x.hair?.style, d.hair.style), color: col(x.hair?.color, d.hair.color), tip: col(x.hair?.tip, d.hair.tip) },
    outfit: {
      top: pick("top", x.outfit?.top, d.outfit.top),
      topColor: col(x.outfit?.topColor, d.outfit.topColor),
      bottom: pick("bottom", x.outfit?.bottom, d.outfit.bottom),
      bottomColor: col(x.outfit?.bottomColor, d.outfit.bottomColor),
      shoes: pick("shoes", x.outfit?.shoes, d.outfit.shoes),
      shoeColor: col(x.outfit?.shoeColor, d.outfit.shoeColor),
      accent: col(x.outfit?.accent, d.outfit.accent)
    },
    accessories,
    details,
    palette: typeof x.palette === "string" && PALETTES.some((p) => p.id === x.palette) ? x.palette : null,
    ...normalizeTitleChoice(x.title) ? { title: normalizeTitleChoice(x.title) } : {}
  };
}
function applyPalette(a, paletteId) {
  const p = PALETTES.find((x) => x.id === paletteId);
  if (!p) return a;
  return { ...a, palette: p.id, outfit: { ...a.outfit, topColor: p.top, bottomColor: p.bottom, shoeColor: p.shoes, accent: p.accent } };
}
function randomAppearance(rand = Math.random, name = "Ascendente") {
  const one = (arr) => arr[Math.floor(rand() * arr.length)];
  const id = (cat) => one(items(cat)).id;
  const hair = one(HAIR_COLORS);
  const eye = one(EYE_COLORS);
  const palette = one(PALETTES);
  const accPool = items("accessory").map((i) => i.id);
  const accessories = [];
  const used = /* @__PURE__ */ new Set();
  for (let i = 0; i < 2; i++) {
    const a = one(accPool);
    const slot = ACCESSORY_SLOTS[a] ?? a;
    if (rand() < 0.6 && !used.has(slot)) {
      used.add(slot);
      accessories.push(a);
    }
  }
  const base = {
    version: APPEARANCE_VERSION,
    name,
    body: { type: id("bodyType"), height: rand(), build: rand(), silhouette: rand() },
    face: { shape: id("faceShape"), mouth: id("mouth") },
    skin: one(SKIN_TONES.slice(0, 10)),
    eyes: { shape: id("eyeShape"), color: eye, colorRight: rand() < 0.1 ? one(EYE_COLORS) : eye },
    brows: { style: id("brows"), color: hair },
    hair: { style: one(items("hair").filter((h) => h.id !== "careca")).id, color: hair, tip: rand() < 0.4 ? one(HAIR_COLORS) : hair },
    outfit: {
      top: id("top"),
      topColor: one(OUTFIT_COLORS),
      bottom: id("bottom"),
      bottomColor: one(OUTFIT_COLORS),
      shoes: id("shoes"),
      shoeColor: one(OUTFIT_COLORS),
      accent: one(OUTFIT_COLORS)
    },
    accessories,
    details: items("detail").map((d) => d.id).filter(() => rand() < 0.15),
    palette: null
  };
  return rand() < 0.5 ? applyPalette(base, palette.id) : base;
}

// src/multiplayer/Protocol.ts
var PROTOCOL_VERSION = 1;
var MAX_MESSAGE_BYTES = 256 * 1024;
var CHAT_COLORS = [
  "#ff6b6b",
  "#ffa94d",
  "#ffd43b",
  "#a9e34b",
  "#51cf66",
  "#20c997",
  "#3bc9db",
  "#4dabf7",
  "#748ffc",
  "#9775fa",
  "#da77f2",
  "#f783ac",
  "#ffffff",
  "#e8c39e",
  "#94d82d"
];
var CHAT_MAX_CHARS = 160;
var CHAT_HISTORY = 40;
var ANIMS2 = ["idle", "walk", "run", "air"];
var LAYERS = ["multiplier", "rank", "rebirth", "ultraRebirth", "prestige", "ultraPrestige"];
var isNum = (v) => typeof v === "number" && Number.isFinite(v);
var isInt = (v, lo, hi) => Number.isInteger(v) && v >= lo && v <= hi;
var isStr = (v, max2) => typeof v === "string" && v.length <= max2;
function parsePos(v) {
  if (typeof v !== "object" || v === null) return null;
  const p = v;
  if (!isInt(p.seg, 0, 1e15) || !isNum(p.x) || !isNum(p.y) || !isNum(p.s)) return null;
  if (Math.abs(p.x) > 1e4 || Math.abs(p.y) > 1e4 || Math.abs(p.s) > 1e4) return null;
  return { seg: p.seg, x: p.x, y: p.y, s: p.s };
}
function parseCommand(v) {
  if (typeof v !== "object" || v === null) return null;
  const c = v;
  switch (c.kind) {
    case "prestige":
      return LAYERS.includes(c.layer) ? { kind: "prestige", layer: c.layer, single: c.single === true } : null;
    case "automation":
      return LAYERS.includes(c.layer) ? { kind: "automation", layer: c.layer, on: c.on === true } : null;
    case "equip":
      return isInt(c.slot, 0, 8) && isStr(c.id, 80) ? { kind: "equip", slot: c.slot, id: c.id } : null;
    case "unequip":
      return isInt(c.slot, 0, 8) ? { kind: "unequip", slot: c.slot } : null;
    case "use":
      return isStr(c.id, 80) ? { kind: "use", id: c.id } : null;
    case "useSlot":
      return isInt(c.slot, 0, 8) ? { kind: "useSlot", slot: c.slot } : null;
    case "markSeen":
      return { kind: "markSeen" };
    case "appearance":
      return { kind: "appearance", appearance: c.appearance };
    default:
      return null;
  }
}
function parseClientMessage(text) {
  if (text.length > MAX_MESSAGE_BYTES) return null;
  let m;
  try {
    m = JSON.parse(text);
  } catch {
    return null;
  }
  if (typeof m !== "object" || m === null) return null;
  switch (m.t) {
    case "info":
    case "part":
      return { t: m.t };
    case "ping":
      return isNum(m.ct) ? { t: "ping", ct: m.ct } : null;
    case "chat":
      return isStr(m.text, CHAT_MAX_CHARS * 4) && isStr(m.nonce, 40) ? { t: "chat", text: m.text, nonce: m.nonce } : null;
    case "join":
      if (!isInt(m.v, 0, 1e6) || !isStr(m.name, 64)) return null;
      if (m.token !== null && !(isStr(m.token, 128) && /^[a-f0-9]{32,128}$/.test(m.token))) return null;
      if (m.save !== void 0 && !isStr(m.save, MAX_MESSAGE_BYTES)) return null;
      {
        const sg = m.signed;
        const signed = sg && isStr(sg.save, MAX_MESSAGE_BYTES) && isStr(sg.sig, 128) ? { save: sg.save, sig: sg.sig } : null;
        return {
          t: "join",
          v: m.v,
          token: m.token ?? null,
          name: m.name,
          appearance: m.appearance,
          pos: m.pos == null ? null : parsePos(m.pos),
          ...typeof m.save === "string" ? { save: m.save } : {},
          signed
        };
      }
    case "move": {
      const pos = parsePos(m.pos);
      if (!pos || !isNum(m.ct) || !isNum(m.yaw)) return null;
      return { t: "move", ct: m.ct, pos, g: m.g === true, yaw: m.yaw, a: ANIMS2.includes(m.a) ? m.a : "idle" };
    }
    case "cmd": {
      const cmd = parseCommand(m.cmd);
      return cmd && isInt(m.seq, 0, Number.MAX_SAFE_INTEGER) ? { t: "cmd", seq: m.seq, cmd } : null;
    }
    default:
      return null;
  }
}
function packPos(p, yaw, a) {
  const r = (v) => Math.round(v * 1e3) / 1e3;
  return [p.seg, r(p.x), r(p.y), r(p.s), Math.round(yaw * 100) / 100, a];
}
function cleanChatText(raw) {
  return raw.replace(/[\u0000-\u001f\u007f-\u009f\u200b-\u200f\u202a-\u202e\u2066-\u2069]/g, " ").replace(/\s+/g, " ").trim().slice(0, CHAT_MAX_CHARS);
}

// src/multiplayer/LeaderboardRanking.ts
function compareEntries(a, b, mode = MULTIPLAYER_CONFIG.leaderboardRanking) {
  const c = mode === "magnitude" ? NumberEngine.compare(NumberEngine.abs(b.value), NumberEngine.abs(a.value)) : NumberEngine.compare(b.value, a.value);
  if (c !== 0) return c;
  if (a.achievedAt !== b.achievedAt) return a.achievedAt - b.achievedAt;
  return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
}
function rankTop(entries, size = MULTIPLAYER_CONFIG.leaderboardSize) {
  return [...entries].sort((a, b) => compareEntries(a, b)).slice(0, size);
}

// src/multiplayer/LeaderboardService.ts
var CATEGORIES2 = MULTIPLAYER_CONFIG.leaderboardCategories;
function valuesOf(state) {
  const out = {};
  for (const c of CATEGORIES2) out[c] = state.stats[c];
  return out;
}
var HISTORY_LENGTH = 48;
var LeaderboardService = class {
  competitors = /* @__PURE__ */ new Map();
  /** Histórico de posições por categoria (mais recente no fim; null = fora do top). */
  history = /* @__PURE__ */ new Map();
  best = /* @__PURE__ */ new Map();
  /** Registra/atualiza um competidor; só valores que mudaram renovam a data. */
  upsert(id, name, values, now, isBot = false, achievedAt) {
    let c = this.competitors.get(id);
    if (!c) {
      const at = {};
      for (const k of CATEGORIES2) at[k] = achievedAt?.[k] ?? now;
      c = { id, name, isBot, values: { ...values }, achievedAt: at };
      this.competitors.set(id, c);
      return c;
    }
    c.name = name;
    for (const k of CATEGORIES2) {
      if (!NumberEngine.equals(c.values[k], values[k])) {
        c.values[k] = values[k];
        c.achievedAt[k] = now;
      }
    }
    return c;
  }
  get(id) {
    return this.competitors.get(id);
  }
  remove(id) {
    this.competitors.delete(id);
    this.history.delete(id);
    this.best.delete(id);
  }
  /** Posições atuais (1-based) de todos no top de uma categoria. */
  positions(category) {
    const m = /* @__PURE__ */ new Map();
    this.top(category).forEach((e, i) => m.set(e.id, i + 1));
    return m;
  }
  /**
   * Registra um instantâneo de posições (histórico). Chamado a cada
   * publicação das tabelas — não a cada pequena alteração.
   */
  record() {
    for (const c of CATEGORIES2) {
      const pos = this.positions(c);
      for (const id of this.competitors.keys()) {
        const h = this.history.get(id) ?? {};
        const list = h[c] ?? [];
        const p = pos.get(id) ?? null;
        if (list.length === 0 && p === null) continue;
        list.push(p);
        if (list.length > HISTORY_LENGTH) list.shift();
        h[c] = list;
        this.history.set(id, h);
        if (p !== null) {
          const b = this.best.get(id) ?? {};
          if (b[c] === void 0 || p < b[c]) b[c] = p;
          this.best.set(id, b);
        }
      }
    }
  }
  /** Histórico de posições de um competidor numa categoria. */
  historyOf(id, category) {
    return this.history.get(id)?.[category] ?? [];
  }
  get size() {
    return this.competitors.size;
  }
  top(category, size = MULTIPLAYER_CONFIG.leaderboardSize) {
    const entries = [];
    for (const c of this.competitors.values()) {
      if (NumberEngine.isZero(c.values[category])) continue;
      entries.push({ id: c.id, name: c.name, value: c.values[category], achievedAt: c.achievedAt[category], isBot: c.isBot });
    }
    return rankTop(entries, size);
  }
  /** Posição oficial (1-based) de um competidor numa categoria, ou null se fora do top. */
  positionOf(id, category) {
    const i = this.top(category).findIndex((e) => e.id === id);
    return i < 0 ? null : i + 1;
  }
  snapshot() {
    const out = {};
    for (const c of CATEGORIES2) {
      out[c] = this.top(c).map((e) => {
        const h = this.history.get(e.id)?.[c];
        const prev = h && h.length >= 2 ? h[h.length - 2] ?? null : null;
        return { id: e.id, name: e.name, v: e.value.toJSON(), at: e.achievedAt, bot: e.isBot, prev, best: this.best.get(e.id)?.[c] ?? null };
      });
    }
    return out;
  }
};

// src/multiplayer/RoomManager.ts
var RoomManager = class {
  rooms = /* @__PURE__ */ new Map();
  where = /* @__PURE__ */ new Map();
  next = 1;
  capacity;
  constructor(capacity = MULTIPLAYER_CONFIG.maxRealPlayersPerRoom) {
    this.capacity = capacity;
  }
  /** Coloca o jogador numa sala (ou mantém a atual). */
  assign(playerId) {
    const cur = this.roomOf(playerId);
    if (cur) return cur;
    let best = null;
    for (const r of this.rooms.values()) {
      if (r.players.size >= this.capacity) continue;
      if (!best || r.players.size > best.players.size) best = r;
    }
    if (!best) {
      best = { id: `sala-${this.next++}`, players: /* @__PURE__ */ new Set(), botSlots: BOT_CONFIG.physicalBotsPerRoom };
      this.rooms.set(best.id, best);
    }
    best.players.add(playerId);
    this.where.set(playerId, best.id);
    return best;
  }
  remove(playerId) {
    const r = this.roomOf(playerId);
    if (!r) return null;
    r.players.delete(playerId);
    this.where.delete(playerId);
    if (r.players.size === 0) this.rooms.delete(r.id);
    return r;
  }
  roomOf(playerId) {
    const id = this.where.get(playerId);
    return id ? this.rooms.get(id) ?? null : null;
  }
  /** Outros jogadores da mesma sala. */
  mates(playerId) {
    const r = this.roomOf(playerId);
    return r ? [...r.players].filter((p) => p !== playerId) : [];
  }
  all() {
    return [...this.rooms.values()];
  }
  get playerCount() {
    return this.where.size;
  }
};

// src/world/Staircase.ts
var C = STAIRCASE_CONFIG;
var STEPS_PER_SEGMENT = C.stepsPerPlatform;
var SEGMENT_LENGTH = STEPS_PER_SEGMENT * C.stepDepth + C.platformDepth;
var SEGMENT_RISE = STEPS_PER_SEGMENT * C.stepHeight;
var LOBBY_RADIUS = C.lobbySize / 2;
var LOBBY_BRIDGE = 2.5;
function hash32(n) {
  let h = (n ^ C.worldSeed) >>> 0;
  h = Math.imul(h ^ h >>> 16, 2246822507) >>> 0;
  h = Math.imul(h ^ h >>> 13, 3266489909) >>> 0;
  return (h ^ h >>> 16) >>> 0;
}
function variation(step, salt = 0) {
  return hash32(step * 31 + salt * 7919) / 4294967296;
}
function surfaceAt(seg, x, s) {
  if (s < 0) {
    if (seg !== 0) {
      const r = surfaceAt(seg - 1, x, s + SEGMENT_LENGTH);
      return { ...r, top: r.top - SEGMENT_RISE };
    }
    const dz = s + LOBBY_RADIUS;
    return Math.hypot(x, dz) <= LOBBY_RADIUS + 0.05 || Math.abs(x) <= C.stepWidth / 2 && s > -LOBBY_BRIDGE ? { kind: "lobby", step: 0, top: 0 } : { kind: "void", step: 0, top: -Infinity };
  }
  if (s >= SEGMENT_LENGTH) {
    const r = surfaceAt(seg + 1, x, s - SEGMENT_LENGTH);
    return { ...r, top: r.top + SEGMENT_RISE };
  }
  if (Math.abs(x) > C.stepWidth / 2) return { kind: "void", step: 0, top: -Infinity };
  const base = seg * STEPS_PER_SEGMENT;
  const stairLen = STEPS_PER_SEGMENT * C.stepDepth;
  if (s < stairLen) {
    const local = Math.floor(s / C.stepDepth) + 1;
    return { kind: "step", step: base + local, top: local * C.stepHeight };
  }
  return { kind: "platform", step: base + STEPS_PER_SEGMENT, top: SEGMENT_RISE };
}
function flightOf(step) {
  return step <= 0 ? 0 : Math.floor((step - 1) / STEPS_PER_SEGMENT);
}
function difficultyOf(flight) {
  return NumberEngine.pow(C.difficultyGrowthPerFlight, flight);
}
function inFlightGrowth(j) {
  return C.stepXpGrowthMode === "additive" ? NumberEngine.add(1, NumberEngine.multiply(C.stepXpGrowthPerStep, j)) : NumberEngine.pow(NumberEngine.add(1, C.stepXpGrowthPerStep), j);
}
var FLIGHT_XP_FACTOR = NumberEngine.multiply(C.flightXpMultiplier, inFlightGrowth(STEPS_PER_SEGMENT - 1));
function flightFirstStepXp(flight) {
  return NumberEngine.multiply(C.stepXpBase, NumberEngine.pow(FLIGHT_XP_FACTOR, flight));
}
function stepXp(step) {
  if (step <= 0) return NumberEngine.ZERO;
  const flight = flightOf(step);
  const j = (step - 1) % STEPS_PER_SEGMENT;
  return NumberEngine.multiply(flightFirstStepXp(flight), inFlightGrowth(j));
}
function stepProps(step) {
  const flight = flightOf(step);
  const difficulty = difficultyOf(flight);
  return {
    step,
    flight,
    pedestalsBelow: flight,
    xp: stepXp(step),
    xpPerSecond: NumberEngine.divide(stepXp(step), C.stepXpIntervalSeconds),
    difficulty,
    resistanceRequired: NumberEngine.multiply(NumberEngine.multiply(MIASMA_CONFIG.resistanceBase, NumberEngine.pow(MIASMA_CONFIG.resistanceGrowthPerStep, step)), difficulty),
    miasma: 1 - Math.exp(-step / 400)
  };
}
var LOG_RES_BASE = Math.log10(Number(MIASMA_CONFIG.resistanceBase));
var LOG_RES_GROWTH = Math.log10(Number(MIASMA_CONFIG.resistanceGrowthPerStep));
var LOG_DIFFICULTY = Math.log10(Number(C.difficultyGrowthPerFlight));
function log10RequiredResistance(step) {
  return LOG_RES_BASE + step * LOG_RES_GROWTH + flightOf(step) * LOG_DIFFICULTY;
}

// src/world/MovementValidator.ts
var SLACK = 1.35;
var BASE_SLACK_M = 0.6;
function delta(a, b) {
  const ds = (b.seg - a.seg) * SEGMENT_LENGTH + (b.s - a.s);
  const dy = (b.seg - a.seg) * SEGMENT_RISE + (b.y - a.y);
  return { horizontal: Math.hypot(ds, b.x - a.x), vertical: dy };
}
function validateMove(prev, next) {
  const dt = next.t - prev.t;
  if (!(dt > 0)) return { ok: false, reason: "amostra fora de ordem" };
  const d = delta(prev.pos, next.pos);
  const maxH = STAIRCASE_CONFIG.maxHorizontalSpeed * dt * SLACK + BASE_SLACK_M;
  const maxUp = STAIRCASE_CONFIG.maxVerticalSpeed * dt * SLACK + BASE_SLACK_M;
  if (d.horizontal > maxH) return { ok: false, reason: `deslocamento horizontal ${d.horizontal.toFixed(1)} m > ${maxH.toFixed(1)} m` };
  if (d.vertical > maxUp) return { ok: false, reason: `subida ${d.vertical.toFixed(1)} m > ${maxUp.toFixed(1)} m` };
  return { ok: true };
}

// src/math/CalculationEngine.ts
function geometricSum(first, ratio, count2) {
  const n = NumberEngine.from(count2);
  if (NumberEngine.isZero(n)) return NumberEngine.ZERO;
  if (NumberEngine.isNegative(n)) throw new MathError("INVALID_INPUT", "geometricSum com quantidade negativa");
  const r = NumberEngine.from(ratio);
  if (NumberEngine.equals(r, 1)) return NumberEngine.multiply(first, n);
  const rn = NumberEngine.pow(r, n);
  return NumberEngine.divide(NumberEngine.multiply(first, NumberEngine.subtract(rn, 1)), NumberEngine.subtract(r, 1));
}
function stackFactor(mode, amountPerUnit, count2) {
  if (mode === "additive") return NumberEngine.add(1, NumberEngine.multiply(amountPerUnit, count2));
  return NumberEngine.pow(NumberEngine.add(1, amountPerUnit), count2);
}
function applyPercent(value, percent) {
  return NumberEngine.multiply(value, NumberEngine.add(1, NumberEngine.divide(percent, 100)));
}
function lerp(a, b, t) {
  return NumberEngine.add(a, NumberEngine.multiply(NumberEngine.subtract(b, a), t));
}
var CalculationEngine = { geometricSum, stackFactor, applyPercent, lerp };

// src/math/RandomGrowth.ts
var MEMO_CAP = 2e6;
var TAIL_TERMS = 12;
function unitRoll(j) {
  let h = (Math.floor(j) ^ 2654435769) >>> 0;
  h = Math.imul(h ^ h >>> 16, 2246822507) >>> 0;
  h = Math.imul(h ^ h >>> 13, 3266489909) >>> 0;
  h = (h ^ h >>> 16) >>> 0;
  const hi = Math.floor(j / 4294967296);
  if (hi) h = (Math.imul(h ^ hi, 668265263) ^ h >>> 15) >>> 0;
  return h / 4294967296;
}
function growthAt(range, j) {
  return range.min + (range.max - range.min) * unitRoll(j);
}
var memos = /* @__PURE__ */ new Map();
function memoFor(range) {
  const key = `${range.min}:${range.max}`;
  let m = memos.get(key);
  if (!m) {
    const a = range.min, b = range.max;
    const mean = b === a ? Math.log10(a) : (b * Math.log(b) - b - (a * Math.log(a) - a)) / (b - a) / Math.LN10;
    m = { prefix: new Float64Array(1024), filled: 0, mean };
    memos.set(key, m);
  }
  return { key, m };
}
function logProduct(range, index) {
  if (!(index > 0)) return 0;
  const { m } = memoFor(range);
  const i = Math.floor(index);
  const upto = Math.min(i, MEMO_CAP);
  if (upto > m.filled) {
    if (upto >= m.prefix.length) {
      const next = new Float64Array(Math.min(MEMO_CAP + 1, Math.max(upto + 1, m.prefix.length * 2)));
      next.set(m.prefix);
      m.prefix = next;
    }
    for (let j = m.filled; j < upto; j++) m.prefix[j + 1] = m.prefix[j] + Math.log10(growthAt(range, j));
    m.filled = upto;
  }
  const base = m.prefix[upto];
  return i > MEMO_CAP ? base + (i - MEMO_CAP) * m.mean : base;
}
function idx(i) {
  const v = NumberEngine.toNumber(NumberEngine.max(0, i));
  return Number.isFinite(v) ? v : Number.MAX_VALUE;
}
function requirementAtRandom(base, range, index) {
  return NumberEngine.multiply(base, NumberEngine.fromLog10(logProduct(range, idx(index))));
}
function costForRandom(base, range, owned, count2, u, mode) {
  if (count2 <= 0) return NumberEngine.ZERO;
  const o = idx(owned);
  const at = (k) => requirementAtRandom(base, range, o + k * u);
  if (mode === "threshold") return at(count2 - 1);
  let sum2 = NumberEngine.ZERO;
  for (let k = Math.max(0, count2 - TAIL_TERMS); k < count2; k++) sum2 = NumberEngine.add(sum2, at(k));
  return sum2;
}
function maxAffordableRandom(base, range, owned, budget, u, mode, limit) {
  const R = NumberEngine.from(budget);
  const fits = (n) => n === 0 || NumberEngine.lte(costForRandom(base, range, owned, n, u, mode), R);
  if (!fits(1)) return 0;
  const cap = limit ?? Number.MAX_SAFE_INTEGER;
  let lo = 1, hi = 2;
  while (hi <= cap && fits(hi)) {
    lo = hi;
    hi *= 2;
  }
  hi = Math.min(hi, cap + 1);
  while (hi - lo > 1) {
    const mid = Math.floor((lo + hi) / 2);
    if (fits(mid)) lo = mid;
    else hi = mid;
  }
  return Math.min(lo, cap);
}

// src/math/ProgressionCalculator.ts
function assertCurve(c) {
  if (!NumberEngine.isPositive(c.base)) throw new MathError("INVALID_CONFIG", `base de custo deve ser > 0 (${String(c.base)})`);
  if (NumberEngine.lt(c.growth, 1)) throw new MathError("INVALID_CONFIG", `crescimento deve ser \u2265 1 (${String(c.growth)})`);
}
function requirementAt(curve, index) {
  assertCurve(curve);
  return NumberEngine.multiply(curve.base, NumberEngine.pow(curve.growth, index));
}
function cumulativeCost(curve, owned, count2) {
  return CalculationEngine.geometricSum(requirementAt(curve, owned), curve.growth, count2);
}
function costFor(curve, owned, count2, mode) {
  if (NumberEngine.isZero(count2)) return NumberEngine.ZERO;
  if (mode === "cumulative") return cumulativeCost(curve, owned, count2);
  return requirementAt(curve, NumberEngine.add(owned, NumberEngine.subtract(count2, 1)));
}
function maxAffordable(curve, owned, budget, mode = "cumulative", options = {}) {
  assertCurve(curve);
  const R = NumberEngine.from(budget);
  const first = requirementAt(curve, owned);
  if (!NumberEngine.isPositive(R) || NumberEngine.lt(R, first)) return NumberEngine.ZERO;
  const limit = options.limit !== void 0 ? NumberEngine.from(options.limit) : null;
  if (limit !== null && !NumberEngine.isPositive(limit)) return NumberEngine.ZERO;
  const g = NumberEngine.from(curve.growth);
  let n;
  if (NumberEngine.equals(g, 1)) {
    if (mode === "threshold") {
      if (limit === null) throw new MathError("INVALID_CONFIG", "crescimento 1 em modo threshold exige limite");
      return limit;
    }
    n = NumberEngine.floor(NumberEngine.divide(R, first));
  } else {
    const logG = NumberEngine.log10(g);
    const raw = mode === "cumulative" ? NumberEngine.log10(NumberEngine.add(NumberEngine.divide(NumberEngine.multiply(R, NumberEngine.subtract(g, 1)), first), 1)) / logG : NumberEngine.log10(NumberEngine.divide(R, first)) / logG + 1;
    n = NumberEngine.floor(NumberEngine.fromNumber(Math.max(0, raw)));
  }
  if (NumberEngine.lt(n, PRECISE_EXPONENT_LIMIT)) {
    const fits = (k) => NumberEngine.isZero(k) || NumberEngine.lte(costFor(curve, owned, k, mode), R);
    for (let i = 0; i < 4 && fits(NumberEngine.add(n, 1)); i++) n = NumberEngine.add(n, 1);
    for (let i = 0; i < 4 && !fits(n); i++) n = NumberEngine.subtract(n, 1);
  }
  if (limit !== null && NumberEngine.gt(n, limit)) n = limit;
  return n;
}
function maxAffordableMonotonic(requirement, owned, budget, maxCount = Number.MAX_SAFE_INTEGER - owned) {
  const ok = (n) => n === 0 || NumberEngine.lte(requirement(owned + n - 1), budget);
  if (!ok(1)) return 0;
  let lo = 1;
  let hi = 2;
  while (hi <= maxCount && ok(hi)) {
    lo = hi;
    hi *= 2;
  }
  hi = Math.min(hi, maxCount + 1);
  while (hi - lo > 1) {
    const mid = Math.floor((lo + hi) / 2);
    if (ok(mid)) lo = mid;
    else hi = mid;
  }
  return lo;
}
function planBulk(q) {
  const u = NumberEngine.from(q.unitsPerPurchase ?? 1);
  if (!NumberEngine.isPositive(u)) throw new MathError("INVALID_INPUT", "unitsPerPurchase deve ser > 0");
  const perPurchase = q.costIndex === "units" ? { base: requirementAt(q.curve, q.ownedIndex), growth: NumberEngine.pow(q.curve.growth, u) } : { base: requirementAt(q.curve, q.ownedIndex), growth: q.curve.growth };
  const purchases = maxAffordable(perPurchase, 0, q.budget, q.mode, q.limit !== void 0 ? { limit: q.limit } : {});
  const unitsGained = NumberEngine.multiply(purchases, u);
  const advance = q.costIndex === "units" ? unitsGained : purchases;
  return {
    purchases,
    unitsGained,
    totalCost: costFor(perPurchase, 0, purchases, q.mode),
    nextRequirement: requirementAt(q.curve, NumberEngine.add(q.ownedIndex, advance))
  };
}
function curveOf(req) {
  return { base: req.base, growth: req.growth };
}
function isBulkAllowed(layer, unlockedEffects) {
  return layer.bulk.enabledByDefault || unlockedEffects.has(layer.bulk.unlockEffectId);
}
function planLayerPurchase(layerId, input) {
  if (input.polarity === -1) {
    const mags = {};
    for (const [k, v] of Object.entries(input.stats)) mags[k] = NumberEngine.abs(v);
    const p2 = planLayerPurchase(layerId, { ...input, stats: mags, polarity: 1 });
    return {
      ...p2,
      unitsGained: NumberEngine.negate(p2.unitsGained),
      costs: p2.costs.map((c) => ({ ...c, amount: NumberEngine.negate(c.amount) })),
      nextRequirements: p2.nextRequirements.map((r) => ({ ...r, amount: NumberEngine.negate(r.amount) }))
    };
  }
  const layer = RESET_LAYERS[layerId];
  const owned = input.stats[layerId] ?? NumberEngine.ZERO;
  const u = NumberEngine.from(input.unitsPerPurchase ?? 1);
  const bulk = !input.single && isBulkAllowed(layer, input.unlockedEffects ?? /* @__PURE__ */ new Set());
  const ownedIndex = costIndexOf(layerId, owned, u);
  const uNum = NumberEngine.toNumber(u);
  let purchases = null;
  for (const req of layer.requirements) {
    let reqPurchases;
    if (req.growthRange) {
      const n = maxAffordableRandom(req.base, req.growthRange, ownedIndex, input.stats[req.resource] ?? NumberEngine.ZERO, layer.costIndex === "units" ? uNum : 1, req.costMode, bulk ? void 0 : 1);
      reqPurchases = NumberEngine.from(n);
    } else {
      reqPurchases = planBulk({
        curve: curveOf(req),
        ownedIndex,
        budget: input.stats[req.resource] ?? NumberEngine.ZERO,
        mode: req.costMode,
        unitsPerPurchase: u,
        costIndex: layer.costIndex,
        ...bulk ? {} : { limit: 1 }
      }).purchases;
    }
    if (req.integer && !req.growthRange) reqPurchases = fitIntegerPurchases(req, ownedIndex, u, layer.costIndex, input.stats[req.resource] ?? NumberEngine.ZERO, reqPurchases);
    purchases = purchases === null ? reqPurchases : NumberEngine.min(purchases, reqPurchases);
  }
  const p = purchases ?? NumberEngine.ZERO;
  const unitsGained = NumberEngine.multiply(p, u);
  const advance = layer.costIndex === "units" ? unitsGained : p;
  const perPurchaseGrowth = (req) => layer.costIndex === "units" ? NumberEngine.pow(req.growth, u) : NumberEngine.from(req.growth);
  return {
    layer: layerId,
    purchases: p,
    unitsGained,
    costs: layer.requirements.map((req) => ({
      resource: req.resource,
      mode: req.costMode,
      amount: req.growthRange ? costForRandom(req.base, req.growthRange, ownedIndex, NumberEngine.toNumber(p), layer.costIndex === "units" ? uNum : 1, req.costMode) : req.integer ? integerCost(req, ownedIndex, u, layer.costIndex, p) : costFor({ base: requirementAt(curveOf(req), ownedIndex), growth: perPurchaseGrowth(req) }, 0, p, req.costMode)
    })),
    nextRequirements: layer.requirements.map((req) => ({
      resource: req.resource,
      amount: requirementOf(req, NumberEngine.add(ownedIndex, advance))
    }))
  };
}
var INTEGER_EXACT_LIMIT = 2e3;
function integerTerm(req, index) {
  return NumberEngine.ceil(requirementAt(curveOf(req), index));
}
function integerCost(req, ownedIndex, u, costIndex, purchases) {
  const p = NumberEngine.toNumber(purchases);
  const step = costIndex === "units" ? u : NumberEngine.ONE;
  if (!(p > 0)) return NumberEngine.ZERO;
  if (p > INTEGER_EXACT_LIMIT) {
    return costFor({ base: requirementAt(curveOf(req), ownedIndex), growth: NumberEngine.pow(req.growth, step) }, 0, purchases, req.costMode);
  }
  if (req.costMode !== "cumulative") return integerTerm(req, NumberEngine.add(ownedIndex, NumberEngine.multiply(step, p - 1)));
  let sum2 = NumberEngine.ZERO;
  for (let k = 0; k < p; k++) sum2 = NumberEngine.add(sum2, integerTerm(req, NumberEngine.add(ownedIndex, NumberEngine.multiply(step, k))));
  return sum2;
}
function fitIntegerPurchases(req, ownedIndex, u, costIndex, budget, n) {
  let p = n;
  for (let i = 0; i < 64 && NumberEngine.gt(p, 0) && NumberEngine.lte(p, INTEGER_EXACT_LIMIT); i++) {
    if (NumberEngine.lte(integerCost(req, ownedIndex, u, costIndex, p), budget)) return p;
    p = NumberEngine.subtract(p, 1);
  }
  return p;
}
function requirementOf(req, index) {
  if (req.growthRange) return requirementAtRandom(req.base, req.growthRange, index);
  return req.integer ? integerTerm(req, index) : requirementAt(curveOf(req), index);
}
var EXP_CURVE = () => ({ base: LEVEL_CONFIG.xpBase, growth: LEVEL_CONFIG.xpGrowth });
function polyParams() {
  const n0 = LEVEL_CONFIG.polySwitchAt;
  const p = LEVEL_CONFIG.polyPower;
  if (n0 === void 0 || p === void 0) return null;
  const r0 = requirementAt(EXP_CURVE(), n0);
  return { n0, p, k: NumberEngine.divide(r0, NumberEngine.multiply(NumberEngine.pow(n0 + 1, p), p + 1)) };
}
function polyCost(a, c, pp) {
  const q = pp.p + 1;
  const A2 = NumberEngine.add(a, 0.5);
  const ratio = NumberEngine.divide(c, A2);
  const base = NumberEngine.multiply(pp.k, NumberEngine.pow(A2, q));
  if (NumberEngine.lt(ratio, 1e-9)) return NumberEngine.multiply(base, NumberEngine.multiply(ratio, q));
  const x = NumberEngine.toNumber(ratio);
  if (!Number.isFinite(x) || x > 1e12) return NumberEngine.subtract(NumberEngine.multiply(pp.k, NumberEngine.pow(NumberEngine.add(A2, c), q)), base);
  return NumberEngine.multiply(base, Math.expm1(q * Math.log1p(x)));
}
function levelXpCost(levelUps, count2) {
  const a = NumberEngine.from(levelUps);
  const c = NumberEngine.from(count2);
  if (!NumberEngine.isPositive(c)) return NumberEngine.ZERO;
  const pp = polyParams();
  if (!pp) return cumulativeCost(EXP_CURVE(), a, c);
  const b = NumberEngine.add(a, c);
  let total = NumberEngine.ZERO;
  if (NumberEngine.lt(a, pp.n0)) total = NumberEngine.add(total, cumulativeCost(EXP_CURVE(), a, NumberEngine.subtract(NumberEngine.min(b, pp.n0), a)));
  if (NumberEngine.gt(b, pp.n0)) {
    if (NumberEngine.gte(a, pp.n0)) total = NumberEngine.add(total, polyCost(a, c, pp));
    else total = NumberEngine.add(total, polyCost(pp.n0, NumberEngine.subtract(b, pp.n0), pp));
  }
  return total;
}
function levelXpRequirement(levelUps) {
  return levelXpCost(levelUps, 1);
}
function levelUpsAffordable(levelUps, xp) {
  try {
    return levelUpsAffordableRaw(levelUps, xp);
  } catch {
    return NumberEngine.ZERO;
  }
}
function levelUpsAffordableRaw(levelUps, xp) {
  const pp = polyParams();
  if (!pp) return maxAffordable(EXP_CURVE(), levelUps, xp, "cumulative");
  const a = NumberEngine.from(levelUps);
  const budget = NumberEngine.abs(xp);
  let bought = NumberEngine.ZERO;
  let left = budget;
  let at = a;
  if (NumberEngine.lt(a, pp.n0)) {
    const room = NumberEngine.subtract(pp.n0, a);
    const e = NumberEngine.min(maxAffordable(EXP_CURVE(), a, budget, "cumulative"), room);
    bought = e;
    left = NumberEngine.subtract(budget, cumulativeCost(EXP_CURVE(), a, e));
    at = NumberEngine.add(a, e);
    if (NumberEngine.lt(e, room)) return bought;
  }
  const q = pp.p + 1;
  const A2 = NumberEngine.add(at, 0.5);
  const r = NumberEngine.divide(left, NumberEngine.multiply(pp.k, NumberEngine.pow(A2, q)));
  const rn = NumberEngine.toNumber(r);
  let c;
  if (NumberEngine.lt(r, 1e-9)) c = NumberEngine.floor(NumberEngine.divide(NumberEngine.multiply(A2, r), q));
  else if (Number.isFinite(rn) && rn < 1e15) c = NumberEngine.floor(NumberEngine.multiply(A2, Math.expm1(Math.log1p(rn) / q)));
  else {
    const lb = NumberEngine.divide(NumberEngine.log10(NumberEngine.add(NumberEngine.divide(left, pp.k), NumberEngine.pow(A2, q))), q);
    c = NumberEngine.floor(NumberEngine.max(0, NumberEngine.subtract(NumberEngine.exp10(lb), A2)));
  }
  if (NumberEngine.isNegative(c)) c = NumberEngine.ZERO;
  for (let i = 0; i < 6 && NumberEngine.isPositive(c) && NumberEngine.gt(polyCost(at, c, pp), left); i++) c = NumberEngine.subtract(c, NumberEngine.max(1, NumberEngine.multiply(c, 1e-12)));
  for (let i = 0; i < 6 && NumberEngine.lt(c, 9e15) && NumberEngine.lte(polyCost(at, NumberEngine.add(c, 1), pp), left); i++) c = NumberEngine.add(c, 1);
  const b = NumberEngine.add(at, c);
  return NumberEngine.add(bought, NumberEngine.subtract(b, at));
}
function lettersPerCycle(gainFactor, speedFactor, polarity = 1) {
  return NumberEngine.multiply(NumberEngine.multiply(NumberEngine.multiply(LETTERS_CONFIG.baseAmountPerCycle, gainFactor), speedFactor), polarity);
}
function costIndexOf(layerId, owned, unitsPerPurchase = 1) {
  const o = NumberEngine.from(owned);
  if (RESET_LAYERS[layerId].costIndex === "units") return o;
  const u = NumberEngine.max(1, NumberEngine.from(unitsPerPurchase));
  const idx2 = NumberEngine.floor(NumberEngine.divide(NumberEngine.abs(o), u));
  return NumberEngine.isNegative(o) ? NumberEngine.negate(idx2) : idx2;
}
function calculateNextRequirement(type, owned, unitsPerPurchase = 1) {
  if (type === "level") return levelXpRequirement(owned);
  const req = RESET_LAYERS[type].requirements[0];
  if (!req) throw new MathError("INVALID_CONFIG", `camada ${type} sem requisitos`);
  return requirementOf(req, costIndexOf(type, owned, unitsPerPurchase));
}
var ProgressionCalculator = {
  requirementAt,
  requirementOf,
  cumulativeCost,
  costFor,
  maxAffordable,
  maxAffordableMonotonic,
  planBulk,
  planLayerPurchase,
  isBulkAllowed,
  levelXpRequirement,
  levelUpsAffordable,
  levelXpCost,
  costIndexOf,
  calculateNextRequirement,
  lettersPerCycle
};

// src/progression/EffectEngine.ts
var EFFECT_ORDER = ["base", "layers", "letters", "level", "additivePercent", "penalties", "reverse"];
var LEVEL_GAIN_CHANNELS = [
  "xpGain",
  "letterGain",
  "multiplierGain",
  "rankGain",
  "rebirthGain",
  "ultraRebirthGain",
  "prestigeGain",
  "ultraPrestigeGain"
];
function levelGainFactor(level) {
  const n = NumberEngine.abs(level);
  const p = NumberEngine.divide(LEVEL_EFFECTS.allGainPercentPerLevel, 100);
  return LEVEL_EFFECTS.allGainMode === "compound" ? NumberEngine.pow(NumberEngine.add(1, p), n) : NumberEngine.add(1, NumberEngine.multiply(p, n));
}
function letterCycleSeconds(level) {
  const base = LETTERS_CONFIG.cycleSeconds;
  const lv = NumberEngine.toNumber(NumberEngine.min(NumberEngine.abs(level), 1e9));
  const steps = Math.floor(lv / LEVEL_EFFECTS.letterCycleReductionPerLevel + 1e-9);
  return Math.max(LEVEL_EFFECTS.minLetterCycleSeconds, base - steps * LEVEL_EFFECTS.letterCycleReductionSeconds);
}
var CHANNELS = [
  "xpGain",
  "letterGain",
  "letterSpeed",
  "multiplierGain",
  "rankGain",
  "rebirthGain",
  "ultraRebirthGain",
  "prestigeGain",
  "ultraPrestigeGain",
  "hp",
  "resistance"
];
function letterXpFactor(letters) {
  const L = NumberEngine.abs(letters);
  const per = LETTERS_CONFIG.xpBonusPerLetter;
  if (LETTERS_CONFIG.xpBonusFormula === "linear") return NumberEngine.add(1, NumberEngine.multiply(per, L));
  return NumberEngine.add(1, NumberEngine.multiply(NumberEngine.multiply(per, NumberEngine.log10(NumberEngine.add(1, L))), LETTERS_CONFIG.xpBonusLogScale));
}
var PENALTY_FLOOR = 0.1;
function computeEffects(state, sources = [], now = 0) {
  const factors = {};
  const breakdown = {};
  for (const c of CHANNELS) {
    factors[c] = NumberEngine.ONE;
    breakdown[c] = [];
  }
  const all = state.inventory ? [...sources, ...buffSources(state.inventory)] : sources;
  const live = all.filter((s) => s.expiresAt === void 0 || s.expiresAt > now);
  for (const stage of EFFECT_ORDER) {
    if (stage === "layers") {
      const lvl = LEVEL_EFFECTS.applyToUnits ? levelGainFactor(state.stats.level) : NumberEngine.ONE;
      const unitF = {};
      for (const id of [...RESET_LAYER_ORDER].reverse()) {
        const owned = NumberEngine.abs(state.stats[id]);
        if (NumberEngine.isZero(owned)) continue;
        const ch = UNITS_CHANNEL[id];
        const u = NumberEngine.max(1, NumberEngine.floor(NumberEngine.multiply(unitF[ch] ?? NumberEngine.ONE, lvl)));
        const purchases = NumberEngine.floor(NumberEngine.divide(owned, u));
        const power2 = RESET_LAYERS[id].rewardPowerAfterFirst;
        const squared = power2 !== void 0 && power2 !== 1 && NumberEngine.gte(owned, 1);
        for (const r of RESET_LAYERS[id].rewards) {
          const isUnitGain = UNIT_GAIN_CHANNELS.has(r.channel);
          let f = CalculationEngine.stackFactor(r.stacking, r.amountPerUnit, isUnitGain ? purchases : owned);
          if (r.perPurchase !== void 0) f = NumberEngine.multiply(f, NumberEngine.pow(r.perPurchase, purchases));
          if (isUnitGain) {
            const g = r.power !== void 0 && r.power !== 1 ? NumberEngine.pow(f, r.power) : f;
            unitF[r.channel] = NumberEngine.multiply(unitF[r.channel] ?? NumberEngine.ONE, squared ? NumberEngine.pow(g, power2) : g);
          }
          if (r.power !== void 0 && r.power !== 1) f = NumberEngine.pow(f, r.power);
          if (squared) f = NumberEngine.pow(f, power2);
          const sup = r.power !== void 0 && r.power !== 1 ? r.power === 2 ? "\xB2" : r.power === 3 ? "\xB3" : `^${r.power}` : "";
          factors[r.channel] = NumberEngine.multiply(factors[r.channel], f);
          breakdown[r.channel].push({ stage, label: `${RESET_LAYERS[id].displayName}${sup ? ` (${sup})` : ""}${squared ? " (\xB2)" : ""}`, factor: f });
        }
      }
    } else if (stage === "letters") {
      const f = letterXpFactor(state.stats.letters);
      if (NumberEngine.gt(f, 1)) {
        factors.xpGain = NumberEngine.multiply(factors.xpGain, f);
        breakdown.xpGain.push({ stage, label: "Letras", factor: f });
      }
    } else if (stage === "level") {
      const f = levelGainFactor(state.stats.level);
      if (NumberEngine.gt(f, 1)) {
        for (const c of LEVEL_GAIN_CHANNELS) {
          if (!LEVEL_EFFECTS.applyToUnits && c !== "xpGain" && c !== "letterGain") continue;
          factors[c] = NumberEngine.multiply(factors[c], f);
          breakdown[c].push({ stage, label: "N\xEDvel", factor: f });
        }
      }
    } else if (stage === "additivePercent" || stage === "penalties") {
      const kind = stage === "additivePercent" ? "percent" : "penalty";
      for (const c of CHANNELS) {
        const mine = live.filter((s) => s.channel === c && s.kind === kind);
        if (!mine.length) continue;
        const total = NumberEngine.sum(mine.map((s) => s.value));
        const f = kind === "percent" ? NumberEngine.add(1, NumberEngine.divide(total, 100)) : NumberEngine.max(PENALTY_FLOOR, NumberEngine.subtract(1, NumberEngine.divide(total, 100)));
        factors[c] = NumberEngine.multiply(factors[c], f);
        breakdown[c].push({ stage, label: kind === "percent" ? `Buffs (+${NumberEngine.format(total)}%)` : `Penalidades (\u2212${NumberEngine.format(total)}%)`, factor: f });
      }
    } else if (stage === "reverse") {
      for (const c of CHANNELS) {
        const f = NegativeNumberEngine.applyReverseHooks(factors[c], { channel: c, source: "effects", negatedState: state.polarity === -1 });
        if (!NumberEngine.equals(f, factors[c])) {
          breakdown[c].push({ stage, label: "Efeitos reversos", factor: NumberEngine.divide(f, factors[c]) });
          factors[c] = f;
        }
      }
    }
  }
  return { factors, breakdown };
}
var UNIT_GAIN_CHANNELS = /* @__PURE__ */ new Set(["multiplierGain", "rankGain", "rebirthGain", "ultraRebirthGain", "prestigeGain", "ultraPrestigeGain"]);
var UNITS_CHANNEL = {
  multiplier: "multiplierGain",
  rank: "rankGain",
  rebirth: "rebirthGain",
  ultraRebirth: "ultraRebirthGain",
  prestige: "prestigeGain",
  ultraPrestige: "ultraPrestigeGain"
};

// src/progression/PrestigeBook.ts
function automationUnlocksFor(pedestals) {
  const n = NumberEngine.toNumber(NumberEngine.min(NumberEngine.floor(NumberEngine.abs(pedestals)), RESET_LAYER_ORDER.length));
  return RESET_LAYER_ORDER.slice(0, n).map((id) => RESET_LAYERS[id].automation.unlockEffectId);
}
function applyPedestalUnlocks(state) {
  const set = new Set(state.unlockedEffects);
  for (const id of automationUnlocksFor(state.pedestalsPassed)) set.add(id);
  return { ...state, unlockedEffects: [...set] };
}
function isAutomationUnlocked(state, layer) {
  return state.unlockedEffects.includes(RESET_LAYERS[layer].automation.unlockEffectId);
}
function setAutomation(state, layer, enabled) {
  if (enabled && !isAutomationUnlocked(state, layer)) {
    throw new Error(`autom\xE1tico de ${RESET_LAYERS[layer].displayName} ainda n\xE3o foi liberado`);
  }
  return { ...state, automation: { ...state.automation, [layer]: enabled } };
}

// src/inventory/DropSystem.ts
var CARD_SHARE = 0.25;
var TIMED_DROP_INTERVAL_SECONDS = 150;
var TIMED_DROP_COUNT = 3;
function cardShareAt(flight) {
  return Math.min(0.65, CARD_SHARE + 0.02 * flight);
}
function minTierAt(flight) {
  return Math.min(8, Math.floor(flight / 4));
}
function dropForRoll(roll, flight) {
  const f = Math.max(0, Math.floor(flight));
  const isCard = variation(roll, 403) < cardShareAt(f);
  const r = pickRarity(isCard ? "card" : "item", f, variation(roll, 407), minTierAt(f));
  const pool = isCard ? CARD_TEMPLATES : CONSUMABLE_TEMPLATES;
  const t = pool[Math.floor(variation(roll, 409) * pool.length)];
  return `${isCard ? "carta" : "item"}:${t}:${r.id}`;
}
function dropFlightOf(step) {
  return step > 0 ? flightOf(step) : 0;
}
function dropForPedestal(k) {
  const minTier = Math.min(8, Math.floor((k - 1) / 2));
  const r = pickRarity("card", k, variation(k * 7919, 211), minTier);
  const t = CARD_TEMPLATES[Math.floor(variation(k, 223) * CARD_TEMPLATES.length)];
  return `carta:${t}:${r.id}`;
}

// src/world/PedestalTracker.ts
function applyClimb(state, step, onPlatformOf) {
  const events = [];
  let next = state;
  if (NumberEngine.gt(step, next.highestStep)) next = { ...next, highestStep: NumberEngine.from(step) };
  const reached = onPlatformOf ?? Math.max(0, Math.floor((step - 1) / STEPS_PER_SEGMENT));
  const passed = NumberEngine.toNumber(next.pedestalsPassed);
  if (reached > passed) {
    for (let k = passed + 1; k <= reached; k++) {
      const before = new Set(next.unlockedEffects);
      next = applyPedestalUnlocks({ ...next, pedestalsPassed: NumberEngine.from(k) });
      const reward = dropForPedestal(k);
      if (next.inventory) next = { ...next, inventory: addObject(next.inventory, reward) };
      events.push({ kind: "pedestal", pedestal: k, unlocked: next.unlockedEffects.filter((e) => !before.has(e)), reward });
    }
  }
  return { state: next, events };
}

// src/config/ResetTable.ts
var RESET_TABLE = {
  multiplier: ["letters"],
  rank: ["letters", "multiplier"],
  rebirth: ["letters", "multiplier", "rank"],
  ultraRebirth: ["letters", "multiplier", "rank", "rebirth"],
  prestige: ["letters", "multiplier", "rank", "rebirth", "ultraRebirth"],
  // [DECISÃO D9] Ultra Prestígio zera até Prestígio; Nível e XP são preservados.
  ultraPrestige: ["letters", "multiplier", "rank", "rebirth", "ultraRebirth", "prestige"]
};

// src/progression/ProgressionSystem.ts
var MOVING_XP_BONUS = 1;
var AUTOMATION_INTERVAL = 0.5;
function levelValue(levelUps, polarity) {
  return NumberEngine.multiply(polarity, NumberEngine.add(LEVEL_CONFIG.startLevel, NumberEngine.multiply(levelUps, LEVEL_CONFIG.levelStep)));
}
function xpRate(state, effects, step, moving) {
  if (step <= 0) return NumberEngine.ZERO;
  const base = NumberEngine.multiply(stepProps(step).xpPerSecond, effects.factors.xpGain);
  return NumberEngine.multiply(NumberEngine.multiply(base, moving ? MOVING_XP_BONUS : 1), state.polarity);
}
function lettersPerCycleOf(state, effects) {
  return ProgressionCalculator.lettersPerCycle(effects.factors.letterGain, effects.factors.letterSpeed, state.polarity);
}
function applyLevelUps(state) {
  const n = ProgressionCalculator.levelUpsAffordable(state.levelUps, NumberEngine.abs(state.stats.xp));
  if (NumberEngine.isZero(n)) return { state, gained: NumberEngine.ZERO };
  const cost = ProgressionCalculator.levelXpCost(state.levelUps, n);
  let xp = NumberEngine.subtract(NumberEngine.abs(state.stats.xp), cost);
  if (NumberEngine.isNegative(xp)) xp = NumberEngine.ZERO;
  const levelUps = NumberEngine.add(state.levelUps, n);
  return {
    state: { ...state, levelUps, stats: { ...state.stats, xp: NumberEngine.multiply(xp, state.polarity), level: levelValue(levelUps, state.polarity) } },
    gained: n
  };
}
function unitsPerPurchaseOf(effects, layer) {
  return NumberEngine.max(1, NumberEngine.floor(effects.factors[UNITS_CHANNEL[layer] ?? "prestigeGain"]));
}
function performPrestige(state, layer, opts = {}) {
  const effects = opts.effects ?? computeEffects(state, opts.sources);
  const units = unitsPerPurchaseOf(effects, layer);
  const plan = ProgressionCalculator.planLayerPurchase(layer, {
    stats: state.stats,
    unitsPerPurchase: units,
    unlockedEffects: new Set(state.unlockedEffects),
    single: opts.single,
    polarity: state.polarity
  });
  if (NumberEngine.isZero(plan.purchases)) return { state, event: null };
  const stats = { ...state.stats };
  const reset = new Set(RESET_TABLE[layer]);
  for (const c of plan.costs) {
    if (c.mode === "cumulative" && !reset.has(c.resource)) stats[c.resource] = NumberEngine.subtract(stats[c.resource], c.amount);
  }
  for (const r of reset) stats[r] = NumberEngine.ZERO;
  stats[layer] = NumberEngine.add(stats[layer], plan.unitsGained);
  return {
    state: { ...state, stats },
    event: { kind: "purchase", layer, purchases: plan.purchases, units: plan.unitsGained, auto: !!opts.auto }
  };
}
function previewPrestige(state, layer, effects, single = false) {
  return ProgressionCalculator.planLayerPurchase(layer, {
    stats: state.stats,
    unitsPerPurchase: unitsPerPurchaseOf(effects, layer),
    unlockedEffects: new Set(state.unlockedEffects),
    single,
    polarity: state.polarity
  });
}
function tick(state, dt, ctx, automationClock = { t: 0 }) {
  const events = [];
  const safeDt = Number.isFinite(dt) && dt > 0 ? dt : 0;
  let s = state;
  let expiredIds = [];
  let effects = computeEffects(s, ctx.sources, ctx.now);
  let timer = s.letterTimer + safeDt;
  const cycle = letterCycleSeconds(s.stats.level);
  const cycles = Math.floor(timer / cycle);
  timer -= cycles * cycle;
  if (cycles > 0) {
    const amount = NumberEngine.multiply(lettersPerCycleOf(s, effects), cycles);
    s = { ...s, stats: { ...s.stats, letters: NumberEngine.add(s.stats.letters, amount) } };
    events.push({ kind: "letters", amount, cycles });
  }
  s = { ...s, letterTimer: timer };
  let dropTimer = (s.dropTimer ?? 0) + safeDt;
  if (dropTimer >= TIMED_DROP_INTERVAL_SECONDS && s.inventory) {
    const waves = Math.floor(dropTimer / TIMED_DROP_INTERVAL_SECONDS);
    dropTimer -= waves * TIMED_DROP_INTERVAL_SECONDS;
    const flight = dropFlightOf(ctx.onStair ? ctx.step : 0);
    let inv = s.inventory;
    let roll = s.dropRolls ?? 0;
    const ids = [];
    for (let w = 0; w < Math.min(waves, 10); w++) {
      for (let i = 0; i < TIMED_DROP_COUNT; i++) {
        const id = dropForRoll(roll++, flight);
        inv = addObject(inv, id);
        ids.push(id);
      }
    }
    s = { ...s, inventory: inv, dropRolls: roll };
    events.push({ kind: "loot", ids });
  }
  s = { ...s, dropTimer };
  const rate = ctx.onStair ? xpRate(s, effects, ctx.step, ctx.moving) : NumberEngine.ZERO;
  if (!NumberEngine.isZero(rate)) {
    s = { ...s, stats: { ...s.stats, xp: NumberEngine.add(s.stats.xp, NumberEngine.multiply(rate, safeDt)) } };
    const lv = applyLevelUps(s);
    if (!NumberEngine.isZero(lv.gained)) events.push({ kind: "levelUp", count: lv.gained, level: lv.state.stats.level });
    s = lv.state;
  }
  automationClock.t += safeDt;
  if (automationClock.t >= AUTOMATION_INTERVAL) {
    automationClock.t = 0;
    for (const layer of [...RESET_LAYER_ORDER].reverse()) {
      if (!s.automation[layer] || !isAutomationUnlocked(s, layer)) continue;
      const r = performPrestige(s, layer, { auto: true, sources: ctx.sources });
      if (r.event) {
        s = r.state;
        events.push(r.event);
      }
    }
  }
  if (s.inventory?.active.length) {
    const t = tickInventory(s.inventory, safeDt);
    expiredIds = t.expired;
    s = { ...s, inventory: t.inv };
    if (expiredIds.length) events.push({ kind: "buffExpired", ids: expiredIds });
  }
  if (s !== state) effects = computeEffects(s, ctx.sources, ctx.now);
  return { state: s, events, effects, rates: { xpPerSecond: ctx.onStair ? xpRate(s, effects, ctx.step, ctx.moving) : NumberEngine.ZERO, lettersPerCycle: lettersPerCycleOf(s, effects), cycleSeconds: letterCycleSeconds(s.stats.level), nextDropSeconds: TIMED_DROP_INTERVAL_SECONDS - dropTimer } };
}

// src/progression/PlayerVitals.ts
function vitalsForLevel(level) {
  const lv = NumberEngine.abs(level);
  return {
    maxHp: NumberEngine.multiply(LEVEL_CONFIG.baseHp, CalculationEngine.stackFactor(LEVEL_CONFIG.hpStacking, LEVEL_CONFIG.hpPerLevel, lv)),
    resistance: NumberEngine.multiply(
      LEVEL_CONFIG.baseResistance,
      CalculationEngine.stackFactor(LEVEL_CONFIG.resistanceStacking, LEVEL_CONFIG.resistancePerLevel, lv)
    )
  };
}

// src/progression/Survival.ts
function maxHpOf(state, effects) {
  return NumberEngine.multiply(vitalsForLevel(state.stats.level).maxHp, effects.factors.hp);
}
function resistanceOf(state, effects) {
  return NumberEngine.multiply(vitalsForLevel(state.stats.level).resistance, effects.factors.resistance);
}
function readSurvival(ctx) {
  const maxHp = maxHpOf(ctx.state, ctx.effects);
  const resistance = resistanceOf(ctx.state, ctx.effects);
  if (ctx.step <= 0) return { maxHp, resistance, required: NumberEngine.ZERO, ratio: NumberEngine.ZERO, damageFraction: 0, intensity: 0 };
  const props = stepProps(ctx.step);
  const ratio = NumberEngine.isZero(resistance) ? NumberEngine.MAX : NumberEngine.divide(props.resistanceRequired, resistance);
  let damageFraction = 0;
  if (NumberEngine.gt(ratio, 1)) {
    const raw = NumberEngine.multiply(NumberEngine.multiply(MIASMA_CONFIG.damageScale, NumberEngine.log10(ratio)), 1 + props.miasma);
    damageFraction = NumberEngine.toNumber(NumberEngine.min(raw, MIASMA_CONFIG.maxDamageFractionPerSecond));
  }
  return { maxHp, resistance, required: props.resistanceRequired, ratio, damageFraction, intensity: props.miasma };
}
function survivalTick(hp, dt, ctx) {
  const reading = readSurvival(ctx);
  const safeDt = Number.isFinite(dt) && dt > 0 ? dt : 0;
  let next;
  if (reading.damageFraction > 0) {
    next = NumberEngine.subtract(hp, NumberEngine.multiply(reading.maxHp, reading.damageFraction * safeDt));
  } else {
    next = NumberEngine.add(hp, NumberEngine.multiply(reading.maxHp, NumberEngine.multiply(MIASMA_CONFIG.regenFractionPerSecond, safeDt)));
  }
  next = NumberEngine.min(next, reading.maxHp);
  const dead = ctx.step > 0 && !NumberEngine.isPositive(next);
  if (!dead && !NumberEngine.isPositive(next)) next = NumberEngine.max(reading.maxHp, 1);
  return { hp: dead ? NumberEngine.ZERO : next, reading, dead };
}
function applyMiasmaDeath(state) {
  const deaths = state.miasmaDeaths + 1;
  if (deaths >= MIASMA_CONFIG.deathThreshold) {
    const wasPositive = state.polarity === 1;
    const neg = applyTotalNegation({ ...state, miasmaDeaths: deaths });
    return { state: neg, negated: wasPositive, deathsBefore: state.miasmaDeaths };
  }
  return { state: { ...state, miasmaDeaths: deaths }, negated: false, deathsBefore: state.miasmaDeaths };
}

// src/multiplayer/ServerPlayer.ts
var SPAWN = { seg: 0, x: 0, y: 0, s: -LOBBY_RADIUS + 1.5 };
var CLOCK_AHEAD_TOLERANCE = 0.6;
var RESYNC_RADIUS = 2;
var GROUND_TOLERANCE = 0.35;
function sanitizeStart(pos, highestStep) {
  if (!pos) return { ...SPAWN };
  const surf = surfaceAt(pos.seg, pos.x, pos.s);
  const highest = NumberEngine.toNumber(NumberEngine.min(highestStep, Number.MAX_SAFE_INTEGER));
  if (surf.kind === "void" || surf.step > highest + 1) return { ...SPAWN };
  return { seg: pos.seg, x: pos.x, y: surf.top, s: pos.s };
}
var ServerPlayer = class {
  state;
  pos;
  yaw = Math.PI;
  anim = "idle";
  /** Último degrau com os pés no chão (o Miasma usa este). */
  step = 0;
  grounded = true;
  hp = null;
  lastCmdSeq = 0;
  rejected = 0;
  /** Enquanto morto (tela de morte), as amostras são ignoradas. */
  deadUntil = 0;
  last = null;
  /** Ponto conhecido de onde o cliente deve retomar (início, correção, renascimento). */
  anchor;
  lastSafe;
  clockOffset = null;
  autoClock = { t: 0 };
  moving = false;
  constructor(state, start) {
    this.state = state;
    this.pos = sanitizeStart(start, state.highestStep);
    this.anchor = { ...this.pos };
    this.lastSafe = { ...this.pos };
    const surf = surfaceAt(this.pos.seg, this.pos.x, this.pos.s);
    this.step = surf.step;
  }
  get dead() {
    return this.deadUntil > 0;
  }
  /** Processa uma amostra de posição do cliente (`now` em segundos do servidor). */
  handleMove(m, now) {
    this.yaw = m.yaw;
    this.anim = m.a;
    if (this.deadUntil > 0) {
      if (now < this.deadUntil) return { ok: true, events: [] };
      this.deadUntil = 0;
    }
    if (this.clockOffset === null) this.clockOffset = now - m.ct;
    const t = m.ct + this.clockOffset;
    if (t > now + CLOCK_AHEAD_TOLERANCE) return this.reject("rel\xF3gio do cliente adiantado (acelera\xE7\xE3o)");
    if (this.last && t <= this.last.t) return { ok: true, events: [] };
    const surf = surfaceAt(m.pos.seg, m.pos.x, m.pos.s);
    if (!this.last) {
      if (!this.near(m.pos, this.anchor) && surf.kind !== "lobby") return this.reject("posi\xE7\xE3o inicial divergente");
    } else {
      const v = validateMove(this.last, { pos: m.pos, t });
      if (!v.ok) {
        const toLobby = surf.kind === "lobby" && this.near(m.pos, SPAWN, LOBBY_RADIUS);
        const toSafe = this.near(m.pos, this.lastSafe);
        if (!toLobby && !toSafe) return this.reject(v.reason ?? "movimento inv\xE1lido");
      }
    }
    const prev = this.last;
    this.last = { pos: { ...m.pos }, t };
    this.moving = !!prev && t > prev.t && delta(prev.pos, m.pos).horizontal / (t - prev.t) > 0.6;
    this.pos = { ...m.pos };
    this.grounded = m.g && surf.kind !== "void" && Math.abs(m.pos.y - surf.top) < GROUND_TOLERANCE;
    if (!this.grounded) return { ok: true, events: [] };
    this.step = surf.step;
    this.lastSafe = { ...m.pos };
    if (surf.step <= 0) return { ok: true, events: [] };
    const platformOf = surf.kind === "platform" ? surf.step / STEPS_PER_SEGMENT : null;
    if (!NumberEngine.gt(surf.step, this.state.highestStep) && !(platformOf !== null && platformOf > NumberEngine.toNumber(this.state.pedestalsPassed))) {
      return { ok: true, events: [] };
    }
    const r = applyClimb(this.state, surf.step, platformOf);
    this.state = r.state;
    return { ok: true, events: r.events };
  }
  near(a, b, radius = RESYNC_RADIUS) {
    const d = delta(b, a);
    return d.horizontal <= radius && Math.abs(d.vertical) <= radius;
  }
  reject(reason) {
    this.rejected++;
    const back = this.last ? { ...this.last.pos } : { ...this.anchor };
    this.anchor = back;
    return { ok: false, correction: { pos: back, reason }, events: [] };
  }
  /** Avança a simulação `dt` segundos (laço do servidor). */
  tick(dt, now) {
    const onStair = !this.dead && this.grounded && this.step > 0;
    const pr = tick(this.state, dt, { step: onStair ? this.step : 0, onStair, moving: onStair && this.moving }, this.autoClock);
    this.state = pr.state;
    if (this.dead) return { events: pr.events, death: null };
    const sv = survivalTick(this.hp ?? maxHpOf(pr.state, pr.effects), dt, { state: pr.state, effects: pr.effects, step: this.step });
    this.hp = sv.hp;
    if (!sv.dead) return { events: pr.events, death: null };
    const death = applyMiasmaDeath(this.state);
    this.state = death.state;
    this.deadUntil = now + (death.negated ? 4.2 : 2.4);
    this.respawn();
    return { events: pr.events, death };
  }
  /** Volta ao lobby com HP cheio; a próxima amostra precisa partir do lobby. */
  respawn() {
    this.pos = { ...SPAWN };
    this.anchor = { ...SPAWN };
    this.lastSafe = { ...SPAWN };
    this.last = null;
    this.step = 0;
    this.grounded = true;
    this.hp = maxHpOf(this.state, computeEffects(this.state));
  }
  /** Reconexão: retoma da posição enviada se for legítima. */
  resume(pos) {
    this.pos = sanitizeStart(pos, this.state.highestStep);
    this.anchor = { ...this.pos };
    this.lastSafe = { ...this.pos };
    this.last = null;
    this.clockOffset = null;
    return this.pos;
  }
  /** Comandos do jogador, revalidados com as regras do jogo. */
  command(seq, cmd) {
    if (seq > this.lastCmdSeq) this.lastCmdSeq = seq;
    const s = this.state;
    const fail = (error) => ({ ok: false, error, events: [] });
    switch (cmd.kind) {
      case "prestige": {
        const r = performPrestige(s, cmd.layer, { single: cmd.single });
        if (!r.event) return fail("requisitos n\xE3o atendidos");
        this.state = r.state;
        return { ok: true, events: [r.event] };
      }
      case "automation":
        if (cmd.on && !isAutomationUnlocked(s, cmd.layer)) return fail("autom\xE1tico ainda n\xE3o liberado");
        this.state = setAutomation(s, cmd.layer, cmd.on);
        return { ok: true, events: [] };
      case "equip": {
        const err = canEquip(s.inventory, cmd.slot, cmd.id);
        if (err) return fail(err);
        this.state = { ...s, inventory: equip(s.inventory, cmd.slot, cmd.id) };
        return { ok: true, events: [] };
      }
      case "unequip":
        this.state = { ...s, inventory: unequip(s.inventory, cmd.slot) };
        return { ok: true, events: [] };
      case "use":
      case "useSlot": {
        const r = cmd.kind === "use" ? useObject(s.inventory, cmd.id) : useSlot(s.inventory, cmd.slot);
        if (!r.used) return fail("nada para usar");
        this.state = { ...s, inventory: r.inv };
        return { ok: true, events: [] };
      }
      case "markSeen":
        this.state = { ...s, inventory: markSeen(s.inventory) };
        return { ok: true, events: [] };
      case "appearance":
        return { ok: true, events: [] };
    }
  }
};

// src/ai/BotBrain.ts
var PROFILES = {
  velocista: { id: "velocista", label: "Velocista", speed: 7.6, risk: 0.06, buyRatio: 0, autoUpTo: 6, activity: 0.32, corrupted: true, seeksNegation: false, dashBudget: 0.6 },
  matematico: { id: "matematico", label: "Matem\xE1tico", speed: 6.2, risk: 0.035, buyRatio: 0.5, autoUpTo: 3, activity: 0.38, corrupted: true, seeksNegation: false, dashBudget: 0.45 },
  resistente: { id: "resistente", label: "Resistente", speed: 5.6, risk: 0.02, buyRatio: 0.75, autoUpTo: 2, activity: 0.42, corrupted: false, seeksNegation: false, dashBudget: 0.3 },
  "longo-prazo": { id: "longo-prazo", label: "Longo prazo", speed: 6, risk: 0.03, buyRatio: 1.5, autoUpTo: 1, activity: 0.45, corrupted: false, seeksNegation: false, dashBudget: 0.4 },
  oportunista: { id: "oportunista", label: "Oportunista", speed: 7, risk: 0.045, buyRatio: 0.25, autoUpTo: 4, activity: 0.3, corrupted: true, seeksNegation: false, dashBudget: 0.55 },
  abissal: { id: "abissal", label: "Abissal", speed: 7.2, risk: 0.05, buyRatio: 0, autoUpTo: 6, activity: 0.33, corrupted: true, seeksNegation: true, dashBudget: 0.6 }
};
var MAX_STEP = 1e13;
var DAMAGE_SCALE = Number(MIASMA_CONFIG.damageScale);
var MAX_DAMAGE = Number(MIASMA_CONFIG.maxDamageFractionPerSecond);
function highestStepWhere(ok) {
  if (!ok(1)) return 0;
  let lo = 1, hi = 2;
  while (hi < MAX_STEP && ok(hi)) {
    lo = hi;
    hi *= 2;
  }
  if (hi >= MAX_STEP && ok(MAX_STEP)) return MAX_STEP;
  while (hi - lo > 1) {
    const mid = Math.floor((lo + hi) / 2);
    if (ok(mid)) lo = mid;
    else hi = mid;
  }
  return lo;
}
function planClimb(state, effects, profile) {
  const lr = NumberEngine.toNumber(NumberEngine.log10(NumberEngine.max(resistanceOf(state, effects), "1e-300")));
  const dmg = (step) => {
    const over = log10RequiredResistance(step) - lr;
    if (over <= 0) return 0;
    return Math.min(MAX_DAMAGE, DAMAGE_SCALE * over * (1 + (1 - Math.exp(-step / 400))));
  };
  const safe = highestStepWhere((s) => dmg(s) <= 0);
  const perStep = STAIRCASE_CONFIG.stepDepth / profile.speed;
  const dash = highestStepWhere((s) => s <= safe || dmg(s) < MAX_DAMAGE && dmg(s) * 2 * (s - safe) * perStep <= profile.dashBudget);
  const steady = highestStepWhere((s) => dmg(s) <= profile.risk);
  const explore = Math.max(safe, dash, steady);
  let farm = safe;
  const f = flightOf(Math.max(1, safe));
  const prevTop = f * STEPS_PER_SEGMENT;
  if (safe > 0 && prevTop > 0 && prevTop < safe && NumberEngine.gt(stepXp(prevTop), stepXp(safe))) farm = prevTop;
  return { safe, explore, farm };
}
var SKILL_RANGE = [0.55, 0.95];
function decidePurchases(state, profile, rand) {
  let s = state;
  const events = [];
  const ratio = profile.id === "oportunista" ? [0, 0.25, 0.5, 1][Math.floor(rand() * 4)] : profile.buyRatio;
  for (const layer of [...RESET_LAYER_ORDER].reverse()) {
    const effects = computeEffects(s);
    const plan = previewPrestige(s, layer, effects);
    if (NumberEngine.isZero(plan.purchases)) continue;
    const have = NumberEngine.abs(s.stats[layer]);
    if (ratio > 0 && !NumberEngine.isZero(have) && NumberEngine.lt(NumberEngine.abs(plan.unitsGained), NumberEngine.multiply(have, ratio))) continue;
    const r = performPrestige(s, layer, { effects });
    if (r.event) {
      s = r.state;
      events.push(r.event);
    }
  }
  return { state: s, events };
}
function decideAutomation(state, profile) {
  let s = state;
  RESET_LAYER_ORDER.forEach((layer, i) => {
    const want = i < profile.autoUpTo;
    if (isAutomationUnlocked(s, layer) && s.automation[layer] === true !== want) s = setAutomation(s, layer, want);
  });
  return s;
}
var power = (d) => d.buffs.reduce((a, b) => a + b.percent, 0) - d.penalties.reduce((a, b) => a + b.percent, 0) * 0.5;
function decideInventory(state, profile, exploring) {
  let inv = state.inventory;
  const cards = [];
  for (const [id, n] of Object.entries(inv.counts)) {
    const d = getObject(id);
    if (!d || d.kind !== "card") continue;
    if (d.penalties.length && !profile.corrupted) continue;
    for (let i = 0; i < n; i++) cards.push(d);
  }
  cards.sort((a, b) => power(b) - power(a));
  const want = cards.slice(0, BUILD_SLOTS).map((d) => d.id);
  const have = inv.build.map((s) => s?.kind === "card" ? s.id : null).filter(Boolean);
  if (want.join() !== have.join()) {
    let next = { ...emptyInventory(), counts: inv.counts, active: inv.active, unseen: [] };
    want.forEach((id, i) => next = equip(next, i, id));
    inv = next;
  } else if (inv.unseen.length) {
    inv = { ...inv, unseen: [] };
  }
  const best = /* @__PURE__ */ new Map();
  for (const id of Object.keys(inv.counts)) {
    const d = getObject(id);
    if (!d || d.kind !== "consumable") continue;
    const cur = best.get(d.template);
    if (!cur || d.rarity.tier > cur.rarity.tier) best.set(d.template, d);
  }
  for (const d of best.values()) {
    if (inv.active.some((a) => getObject(a.id)?.template === d.template)) continue;
    const forClimb = d.buffs.some((b) => b.channel === "resistance" || b.channel === "hp");
    if (forClimb && !exploring) continue;
    inv = useObject(inv, d.id).inv;
  }
  return inv === state.inventory ? state : { ...state, inventory: inv };
}

// src/ai/Random.ts
function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a = a + 1831565813 >>> 0;
    let t = a;
    t = Math.imul(t ^ t >>> 15, t | 1);
    t ^= t + Math.imul(t ^ t >>> 7, t | 61);
    return ((t ^ t >>> 14) >>> 0) / 4294967296;
  };
}
function between(r, lo, hi) {
  return lo + (hi - lo) * r();
}

// src/ai/BotNames.ts
var A = ["Kael", "Lyra", "Nyx", "Orion", "Vega", "Zeph", "Iris", "Rune", "Sol", "Atlas", "Nova", "Echo", "Juno", "Kira", "Lux", "Mira", "Onyx", "Pyra", "Rhea", "Sable", "Taro", "Vex", "Wren", "Yuri", "Zara", "Dante", "\xCDris", "Caio", "Bia", "Theo", "L\xFAcio", "Ravi", "Nina", "Gael", "Luma", "Saga", "Tita", "Brisa", "Aurora", "Cosmo"];
var B = ["", "", "_", ".", "X", "Zero", "Prime", "Sombra", "Alto", "C\xE9u", "Lume", "Mist", "Byte", "Ray", "Fox", "Wolf", "Rex", "Kun", "Jr", "BR"];
function botNames(count2, seed) {
  const r = mulberry32(seed ^ 20907);
  const used = /* @__PURE__ */ new Set();
  const out = [];
  let guard = 0;
  while (out.length < count2 && guard++ < count2 * 50) {
    const a = A[Math.floor(r() * A.length)];
    const b = B[Math.floor(r() * B.length)];
    const n = r() < 0.45 ? String(Math.floor(r() * 99) + 1) : "";
    let name = `${a}${b}${n}`;
    if (name.length < 3) name += "x";
    name = name.slice(0, 16);
    if (used.has(name)) continue;
    used.add(name);
    out.push(name);
  }
  for (let i = out.length; i < count2; i++) out.push(`Ascendente${i + 1}`);
  return out;
}

// src/ai/BotWorld.ts
var LOBBY_RUN = LOBBY_RADIUS - 1.5;
var STAIR_LEN = STEPS_PER_SEGMENT * STAIRCASE_CONFIG.stepDepth;
var LANES = [2.4, -2.4, 3.2, -3.2, 1.9, -1.9];
var DECIDE_EVERY = 5;
var BOT_SPAWN_SIDE = 3.4;
function tauOfStep(step) {
  if (step <= 0) return 0;
  const seg = Math.floor((step - 1) / STEPS_PER_SEGMENT);
  const j = step - 1 - seg * STEPS_PER_SEGMENT;
  return LOBBY_RUN + seg * SEGMENT_LENGTH + (j + 0.5) * STAIRCASE_CONFIG.stepDepth;
}
function posOfTau(tau, lane) {
  if (tau < LOBBY_RUN) {
    const k = Math.max(0, tau / LOBBY_RUN);
    const startX = Math.sign(lane || 1) * BOT_SPAWN_SIDE;
    return { seg: 0, x: startX + (lane - startX) * k, y: 0, s: tau - LOBBY_RUN };
  }
  const u = tau - LOBBY_RUN;
  const seg = Math.floor(u / SEGMENT_LENGTH);
  const s = u - seg * SEGMENT_LENGTH;
  return { seg, x: lane, y: surfaceAt(seg, lane, s).top, s };
}
function stepAtTau(tau, lane) {
  const p = posOfTau(tau, lane);
  const surf = surfaceAt(p.seg, p.x, p.s);
  return { step: surf.step, platformOf: surf.kind === "platform" ? surf.step / STEPS_PER_SEGMENT : null };
}
function speedOf(b) {
  return Math.min(7.6, b.profile.speed * (0.75 + 0.25 * b.skill));
}
function tauAt(b, t) {
  const d = b.target - b.tau0;
  const run = speedOf(b) * Math.max(0, t - b.t0);
  return Math.abs(d) <= run ? b.target : b.tau0 + Math.sign(d) * run;
}
function moveTo(b, t, target) {
  b.tau0 = tauAt(b, t);
  b.t0 = t;
  b.target = target;
}
var BotWorld = class {
  bots = [];
  seed;
  intervalS;
  batch;
  embodied = /* @__PURE__ */ new Set();
  cursor = 0;
  /** IAs ainda aquecendo: [índice, instante simulado, instante final]. */
  pending = [];
  warmDt = 5;
  constructor(nowS, opts = {}, saved = null) {
    this.seed = saved?.seed ?? opts.seed ?? 20261125;
    this.intervalS = (opts.intervalMs ?? BOT_CONFIG.simulationIntervalMs) / 1e3;
    this.batch = opts.batch ?? BOT_CONFIG.simulationBatchSize;
    const count2 = saved?.bots.length ?? opts.count ?? BOT_CONFIG.totalBots;
    const names = botNames(count2, this.seed);
    const profiles = BOT_CONFIG.strategyProfiles;
    for (let i = 0; i < count2; i++) {
      const rand = mulberry32(this.seed + i * 7919);
      const skill = SKILL_RANGE[0] + (SKILL_RANGE[1] - SKILL_RANGE[0]) * mulberry32(this.seed + i * 31337)();
      const id = `ia${String(i + 1).padStart(3, "0")}`;
      const s = saved?.bots.find((b) => b.id === id);
      const profile = PROFILES[s?.profile ?? profiles[i % profiles.length]] ?? PROFILES.matematico;
      const appearance = normalizeAppearance({ ...randomAppearance(mulberry32(this.seed ^ i * 104729), names[i]), name: names[i] });
      let state = createInitialProgress();
      if (s) {
        try {
          state = deserializeProgress(s.save);
        } catch {
        }
      }
      let hp = null;
      try {
        hp = s?.hp ? MathValidator.fromJSON(s.hp) : null;
      } catch {
        hp = null;
      }
      const tau = s ? Math.max(0, s.tau) : 0;
      this.bots.push({
        id,
        name: names[i],
        profile,
        appearance,
        lane: LANES[i % LANES.length],
        state,
        hp,
        tau0: tau,
        t0: nowS,
        target: tau,
        mode: s?.mode ?? "explore",
        active: s ? s.active : rand() < profile.activity,
        nextSwitch: nowS + (s ? s.nextSwitchIn : between(rand, 60, 1800)),
        deadUntil: 0,
        lastSim: nowS - between(rand, 0, this.intervalS),
        lastDecide: -Infinity,
        autoClock: { t: 0 },
        rand,
        drive: 1,
        skill
      });
    }
    if (!saved && opts.warmupMinutes) {
      this.warmDt = opts.warmupStep ?? 5;
      const [lo, hi] = opts.warmupMinutes;
      this.bots.forEach((b, i) => {
        const start = nowS - between(b.rand, lo, hi) * 60;
        b.t0 = start;
        b.lastSim = start;
        b.active = true;
        b.nextSwitch = Infinity;
        this.pending.push({ i, t: start, end: nowS });
      });
      if (!opts.lazyWarmup) this.warmupSome(Infinity);
    }
  }
  /** Aquecendo? (antiguidade inicial ainda sendo simulada) */
  get warming() {
    return this.pending.length > 0;
  }
  /**
   * Antiguidade: cada IA já jogou alguns minutos quando o mundo nasce (mundo
   * vivo desde o início). Simula até `budgetMs` de CPU; retorna true quando termina.
   */
  warmupSome(budgetMs, clock = () => performance.now()) {
    const start = clock();
    while (this.pending.length && clock() - start < budgetMs) {
      const w = this.pending[0];
      const b = this.bots[w.i];
      w.t = Math.min(w.end, w.t + this.warmDt);
      this.simulate(b, w.t);
      if (w.t >= w.end) {
        this.pending.shift();
        b.active = b.rand() < b.profile.activity;
        b.nextSwitch = w.end + between(b.rand, 60, 1800);
      }
    }
    return this.pending.length === 0;
  }
  /**
   * Conta de teste (offline): faz as IAs viverem `seconds` a mais, como se tivessem jogado
   * esse tempo (desloca os relógios delas para trás e simula até agora).
   */
  fastForward(seconds, nowS, stepS = 20) {
    const span = Math.max(0, Math.min(seconds, 7 * 24 * 3600));
    for (const b of this.bots) {
      b.lastSim -= span;
      b.t0 -= span;
      b.nextSwitch -= span;
      if (b.deadUntil > 0) b.deadUntil -= span;
      if (Number.isFinite(b.lastDecide)) b.lastDecide -= span;
      for (let t = b.lastSim + stepS; t < nowS; t += stepS) this.simulate(b, t);
      this.simulate(b, nowS);
    }
  }
  /** Conta de teste (offline): aplica uma mudança direta ao progresso de todas as IAs. */
  adjustAll(fn) {
    for (const b of this.bots) {
      b.state = fn(b.state);
      b.hp = null;
    }
  }
  /** Atualiza as IAs cujo intervalo venceu (no máximo `batch`). Retorna as atualizadas. */
  update(nowS) {
    const out = [];
    if (this.pending.length) return out;
    const n = this.bots.length;
    for (let k = 0; k < n && out.length < this.batch; k++) {
      const b = this.bots[(this.cursor + k) % n];
      if (nowS - b.lastSim < this.intervalS) continue;
      this.simulate(b, nowS);
      out.push(b);
    }
    this.cursor = (this.cursor + Math.max(1, out.length)) % n;
    return out;
  }
  /** Um passo de simulação da IA até o instante `t` (s). */
  simulate(b, t) {
    const dt = Math.min(60, Math.max(0, t - b.lastSim));
    b.lastSim = t;
    const embodied = this.embodied.has(b.id);
    if (t >= b.nextSwitch && !embodied) {
      b.active = !b.active;
      const on = between(b.rand, 20, 90) * 60;
      b.nextSwitch = t + (b.active ? on : on * (1 - b.profile.activity) / b.profile.activity);
      if (!b.active) {
        b.tau0 = b.target = 0;
        b.t0 = t;
      }
    }
    if (!b.active && !embodied) return;
    if (b.deadUntil > 0) {
      if (t < b.deadUntil) return;
      b.deadUntil = 0;
    }
    const tau = tauAt(b, t);
    const { step: stepNow, platformOf } = stepAtTau(tau, b.lane);
    if (stepNow > 0 && (NumberEngine.gt(stepNow, b.state.highestStep) || platformOf !== null && platformOf > NumberEngine.toNumber(b.state.pedestalsPassed))) {
      b.state = applyClimb(b.state, stepNow, platformOf).state;
    }
    const stepThen = stepAtTau(tauAt(b, Math.max(b.t0, t - dt)), b.lane).step;
    const step = Math.min(stepNow, stepThen);
    const moving = tau !== b.target;
    const pr = tick(b.state, dt, { step, onStair: step > 0, moving }, b.autoClock);
    b.state = pr.state;
    const sv = survivalTick(b.hp ?? maxHpOf(pr.state, pr.effects), dt, { state: pr.state, effects: pr.effects, step });
    b.hp = sv.hp;
    if (sv.dead) {
      b.state = applyMiasmaDeath(b.state).state;
      b.deadUntil = t + 2.4;
      b.tau0 = b.target = 0;
      b.t0 = t;
      b.hp = maxHpOf(b.state, computeEffects(b.state));
      b.mode = "farm";
      b.lastDecide = -Infinity;
      return;
    }
    const hpFrac = NumberEngine.toNumber(NumberEngine.divide(sv.hp, sv.reading.maxHp));
    const retreat = b.mode === "explore" && hpFrac < 0.45 && !(b.profile.seeksNegation && b.state.polarity === 1);
    if (t - b.lastDecide >= DECIDE_EVERY + 6 * (1 - b.skill) || retreat) this.decide(b, t, hpFrac);
  }
  decide(b, t, hpFrac) {
    b.lastDecide = t;
    let s = b.rand() < 0.55 + 0.45 * b.skill ? decidePurchases(b.state, b.profile, b.rand).state : b.state;
    s = decideAutomation(s, b.profile);
    const effects = computeEffects(s);
    const drive = b.drive;
    const plan = planClimb(s, effects, { speed: speedOf(b), risk: b.profile.risk * drive, dashBudget: Math.min(0.75, b.profile.dashBudget * drive * b.skill) });
    if (b.mode === "explore" && hpFrac < 0.45) b.mode = "farm";
    else if (b.mode === "farm" && hpFrac >= 0.95 && plan.explore > plan.farm + 2) b.mode = "explore";
    if (b.rand() < b.skill) s = decideInventory(s, b.profile, b.mode === "explore");
    b.state = s;
    const slack = Math.floor((1 - b.skill) * 6 * b.rand());
    let targetStep = b.mode === "explore" ? plan.explore : Math.max(plan.farm > 0 ? 1 : 0, plan.farm - slack);
    if (b.profile.seeksNegation && s.polarity === 1) targetStep = plan.explore + 3;
    if (this.embodied.has(b.id) && b.mode === "farm" && targetStep > 3 && Math.floor(t / DECIDE_EVERY) % 2 === 1) targetStep -= 2;
    const tau = targetStep > 0 && targetStep % STEPS_PER_SEGMENT === 0 && b.mode === "explore" ? LOBBY_RUN + (targetStep / STEPS_PER_SEGMENT - 1) * SEGMENT_LENGTH + STAIR_LEN + 1.2 : tauOfStep(targetStep);
    if (Math.abs(tau - b.target) > 0.01) moveTo(b, t, tau);
  }
  /**
   * Compara o desempenho de cada IA com o de todos (posição no Nível):
   * fora do top 100 ou na metade de baixo → mais agressiva; no top 5 → mais cautelosa.
   */
  compare(levelPositions) {
    for (const b of this.bots) {
      const p = levelPositions.get(b.id) ?? 101;
      b.drive = p > 100 ? 1.2 : p > 50 ? 1.1 : p <= 5 ? 0.9 : 1;
    }
  }
  // ---------------------------------------------------------------- IAs físicas
  /** Escolhe `n` IAs para andarem fisicamente numa sala (ativas, não usadas). */
  embody(n) {
    const free = this.bots.filter((b) => !this.embodied.has(b.id));
    const pick = [];
    const pref = [...free].sort((a, b) => Number(b.active) - Number(a.active) || a.rand() - 0.5);
    for (const b of pref.slice(0, n)) {
      this.embodied.add(b.id);
      if (!b.active) {
        b.active = true;
        b.tau0 = b.target = 0;
      }
      pick.push(b.id);
    }
    return pick;
  }
  release(id) {
    this.embodied.delete(id);
  }
  get(id) {
    return this.bots.find((b) => b.id === id);
  }
  packed(b, t) {
    const tau = tauAt(b, t);
    const p = posOfTau(tau, b.lane);
    const moving = tau !== b.target && b.deadUntil <= 0;
    const anim = moving ? b.profile.speed > 6.5 ? "run" : "walk" : "idle";
    const yaw = moving && b.target < tau ? 0 : Math.PI;
    return packPos(b.deadUntil > 0 ? posOfTau(0, b.lane) : p, yaw, anim);
  }
  info(b) {
    return { id: b.id, name: b.name, appearance: b.appearance, polarity: b.state.polarity, level: b.state.stats.level.toJSON(), bot: true };
  }
  toJSON(nowS) {
    return {
      v: 1,
      seed: this.seed,
      bots: this.bots.map((b) => ({
        id: b.id,
        profile: b.profile.id,
        save: serializeProgress(b.state),
        hp: b.hp ? b.hp.toJSON() : null,
        tau: tauAt(b, nowS),
        mode: b.mode,
        active: b.active,
        nextSwitchIn: Math.max(0, b.nextSwitch - nowS)
      }))
    };
  }
};

// src/server/GameServer.ts
var accountIdOf = (token) => `j${createHash2("sha256").update(`conta:${token}`).digest("hex").slice(0, 12)}`;
var SERVER_BOTS = { warmupMinutes: [0, 45], warmupStep: 10, lazyWarmup: true };
var BOTS_SAVE_EVERY_MS = 3e4;
var WARMUP_BUDGET_MS = 20;
var TICK_MS = 100;
var STATE_EVERY_MS = 500;
var PERSIST_EVERY_MS = 5e3;
var RATE_CAPACITY = 60;
var RATE_REFILL_PER_S = 40;
var RATE_ABUSE_LIMIT = 300;
var hashToken = (t) => createHash2("sha256").update(t).digest("hex");
var Session = class {
  sid;
  account = null;
  player = null;
  tokens = RATE_CAPACITY;
  lastRefill;
  dropped = 0;
  transport;
  /** Último nível/polaridade anunciados à sala (nameplates). */
  announced = "";
  /** Cor do jogador no chat (índice em CHAT_COLORS). */
  chatColor = 0;
  /** Instantes das últimas mensagens de chat (limite anti-spam). */
  chatTimes = [];
  constructor(sid, transport, now) {
    this.sid = sid;
    this.transport = transport;
    this.lastRefill = now;
  }
  send(m) {
    this.transport.send(JSON.stringify(m));
  }
  allow(now) {
    this.tokens = Math.min(RATE_CAPACITY, this.tokens + (now - this.lastRefill) / 1e3 * RATE_REFILL_PER_S);
    this.lastRefill = now;
    if (this.tokens < 1) {
      this.dropped++;
      return false;
    }
    this.tokens -= 1;
    return true;
  }
};
var GameServer = class {
  rooms = new RoomManager();
  boards = new LeaderboardService();
  accounts = /* @__PURE__ */ new Map();
  byToken = /* @__PURE__ */ new Map();
  active = /* @__PURE__ */ new Map();
  sessions = /* @__PURE__ */ new Set();
  dirty = /* @__PURE__ */ new Set();
  store;
  clock;
  nextSid = 1;
  stateAcc = 0;
  persistAcc = 0;
  boardsAcc = 0;
  lastTick;
  bots;
  /** IAs físicas de cada sala. */
  roomBots = /* @__PURE__ */ new Map();
  botBadges = /* @__PURE__ */ new Map();
  botsSaveAcc = 0;
  chatLog = [];
  chatSeq = 1;
  warmed = false;
  secret;
  trustLocal;
  constructor(store, clock = Date.now, opts = {}) {
    this.store = store;
    this.clock = clock;
    this.lastTick = clock();
    this.secret = opts.saveSecret ?? randomBytes(32).toString("hex");
    this.trustLocal = opts.trustLocalSaves ?? true;
    this.bots = opts.bots === false ? null : new BotWorld(clock() / 1e3, opts.bots ?? SERVER_BOTS, store.loadBots?.() ?? null);
    if (this.bots && !this.bots.warming) this.publishBots(this.bots.bots);
    for (const a of store.loadAll()) {
      this.accounts.set(a.id, a);
      this.byToken.set(a.tokenHash, a.id);
      try {
        this.boards.upsert(a.id, a.name, valuesOf(deserializeProgress(a.save)), a.updatedAt, false, a.achievedAt);
      } catch {
      }
    }
  }
  publishBots(list) {
    const now = this.clock();
    for (const b of list) this.boards.upsert(b.id, b.name, valuesOf(b.state), now, true);
  }
  /** IAs físicas da sala (2 por sala, escolhidas quando a sala abre). */
  botsOf(roomId) {
    if (!this.bots) return [];
    let ids = this.roomBots.get(roomId);
    if (!ids) {
      ids = this.bots.embody(BOT_CONFIG.physicalBotsPerRoom);
      this.roomBots.set(roomId, ids);
    }
    return ids;
  }
  releaseEmptyRooms() {
    const open = new Set(this.rooms.all().map((r) => r.id));
    for (const [room, ids] of this.roomBots) {
      if (open.has(room)) continue;
      for (const id of ids) this.bots?.release(id);
      this.roomBots.delete(room);
    }
  }
  get onlineCount() {
    return this.active.size;
  }
  connect(transport) {
    const s = new Session(this.nextSid++, transport, this.clock());
    this.sessions.add(s);
    return s;
  }
  disconnect(s) {
    this.part(s);
    this.sessions.delete(s);
  }
  message(s, text) {
    const now = this.clock();
    if (!s.allow(now)) {
      if (s.dropped > RATE_ABUSE_LIMIT) s.transport.close(1008, "mensagens demais");
      return;
    }
    const m = parseClientMessage(text);
    if (!m) {
      s.send({ t: "error", code: "mensagem-invalida", message: "mensagem inv\xE1lida" });
      return;
    }
    this.handle(s, m, now);
  }
  handle(s, m, now) {
    switch (m.t) {
      case "info":
        s.send({ t: "info", v: PROTOCOL_VERSION, online: this.active.size, rooms: this.rooms.all().length, capacity: this.rooms.capacity });
        return;
      case "ping":
        s.send({ t: "pong", ct: m.ct, st: now });
        return;
      case "join":
        return this.join(s, m, now);
      case "chat":
        return this.chat(s, m.text, m.nonce, now);
      case "part":
        return this.part(s);
      case "move": {
        const p = s.player;
        if (!p) return;
        const out = p.handleMove(m, now / 1e3);
        if (out.correction) s.send({ t: "correct", pos: out.correction.pos, reason: out.correction.reason });
        if (out.events.length) this.pushState(s, now);
        return;
      }
      case "cmd": {
        const p = s.player, a = s.account;
        if (!p || !a) return;
        let out;
        if (m.cmd.kind === "appearance") {
          a.appearance = normalizeAppearance({ ...m.cmd.appearance, name: a.name });
          this.dirty.add(a.id);
          for (const id of this.rooms.mates(a.id)) this.active.get(id)?.send({ t: "update", player: this.info(s) });
          out = { ok: true };
        } else {
          out = p.command(m.seq, m.cmd);
        }
        s.send({ t: "ack", seq: m.seq, ok: out.ok, ...out.ok ? {} : { error: out.error } });
        this.dirty.add(a.id);
        this.pushState(s, now);
        return;
      }
    }
  }
  join(s, m, now) {
    if (m.v !== PROTOCOL_VERSION) {
      s.send({ t: "error", code: "versao", message: `vers\xE3o do jogo incompat\xEDvel (cliente ${m.v}, servidor ${PROTOCOL_VERSION}); atualize a p\xE1gina` });
      return;
    }
    if (s.player) this.part(s);
    let token = m.token ?? randomBytes(24).toString("hex");
    let account = this.accounts.get(this.byToken.get(hashToken(token)) ?? "");
    if (!account && this.accounts.has(accountIdOf(token))) token = randomBytes(24).toString("hex");
    if (!account) {
      const id = accountIdOf(token);
      account = {
        id,
        tokenHash: hashToken(token),
        name: "Ascendente",
        appearance: normalizeAppearance(null),
        save: serializeProgress(createInitialProgress()),
        position: null,
        createdAt: now,
        updatedAt: now
      };
      this.accounts.set(id, account);
      this.byToken.set(account.tokenHash, id);
    }
    const name = validateName(m.name) === null ? m.name.trim() : account.name;
    account.name = name;
    account.appearance = normalizeAppearance({ ...typeof m.appearance === "object" && m.appearance ? m.appearance : {}, name });
    const old = this.active.get(account.id);
    if (old && old !== s) {
      old.send({ t: "error", code: "outra-sessao", message: "Sua conta entrou em outro aparelho." });
      this.part(old);
      old.transport.close(4e3, "outra sess\xE3o");
    }
    const state = this.chooseSave(account, m);
    const player = new ServerPlayer(state, m.pos ?? account.position);
    s.account = account;
    s.player = player;
    this.active.set(account.id, s);
    s.chatColor = this.pickChatColor(s);
    const room = this.rooms.assign(account.id);
    this.boards.upsert(account.id, name, valuesOf(state), now);
    s.announced = this.badge(s);
    s.send({
      t: "welcome",
      id: account.id,
      token,
      room: room.id,
      save: serializeProgress(player.state),
      pos: player.pos,
      players: [
        ...this.rooms.mates(account.id).flatMap((id) => {
          const o = this.active.get(id);
          return o ? [{ ...this.info(o), pos: this.packed(o) }] : [];
        }),
        ...this.botsOf(room.id).flatMap((id) => {
          const b = this.bots?.get(id);
          return b ? [{ ...this.bots.info(b), pos: this.bots.packed(b, now / 1e3) }] : [];
        })
      ],
      serverTime: now
    });
    for (const id of this.rooms.mates(account.id)) this.active.get(id)?.send({ t: "joined", player: { ...this.info(s), pos: this.packed(s) } });
    s.send({ t: "boards", at: now, boards: this.boards.snapshot() });
    s.send({ t: "chatHistory", list: this.chatLog });
    this.dirty.add(account.id);
  }
  /** Assinatura de um save (HMAC do conteúdo, ligado à conta). */
  sign(accountId, save) {
    return createHmac("sha256", this.secret).update(`${accountId}|${save}`).digest("hex");
  }
  verify(accountId, save, sig) {
    const a = Buffer.from(this.sign(accountId, save), "hex"), b = Buffer.from(sig, "hex");
    return a.length === b.length && timingSafeEqual(a, b);
  }
  /**
   * Qual save usar ao entrar (backup local): o save do aparelho (padrão) —
   * ele é o save da conta, online e offline. No modo estrito, só um
   * instantâneo com assinatura válida deste servidor; sem ele, o que o servidor já tem.
   */
  chooseSave(account, m) {
    const tryParse2 = (text) => {
      if (!text) return null;
      try {
        return deserializeProgress(text);
      } catch {
        return null;
      }
    };
    const signed = m.signed && this.verify(account.id, m.signed.save, m.signed.sig) ? tryParse2(m.signed.save) : null;
    const local = this.trustLocal ? tryParse2(m.save) : null;
    return local ?? signed ?? tryParse2(account.save) ?? createInitialProgress();
  }
  /** Chat global: entregue na hora a todos os jogadores online (sem esperar o laço). */
  chat(s, raw, nonce, now) {
    const a = s.account, p = s.player;
    if (!a || !p) return;
    const text = cleanChatText(raw);
    const fail = (reason) => s.send({ t: "chatError", nonce, reason });
    if (!text) return fail("mensagem vazia");
    s.chatTimes = s.chatTimes.filter((x) => now - x < 1e4);
    const last = s.chatTimes[s.chatTimes.length - 1] ?? -Infinity;
    if (now - last < 600 || s.chatTimes.length >= 6) return fail("devagar: espere um instante para mandar outra mensagem");
    s.chatTimes.push(now);
    const msg = {
      id: this.chatSeq++,
      from: a.id,
      name: a.name,
      color: s.chatColor,
      text,
      level: p.state.stats.level.toJSON(),
      polarity: p.state.polarity,
      best: this.bestStanding(a.id, p.state),
      at: now
    };
    this.chatLog.push(msg);
    if (this.chatLog.length > CHAT_HISTORY) this.chatLog.shift();
    const forOthers = JSON.stringify({ t: "chat", m: msg });
    for (const o of this.active.values()) {
      if (o === s) o.send({ t: "chat", m: { ...msg, nonce } });
      else o.transport.send(forOthers);
    }
  }
  /** Atributo em que o jogador tem MAIS quantidade e a posição dele nessa tabela. */
  bestStanding(id, state) {
    let best = null;
    for (const c of CATEGORIES2) {
      if (NumberEngine.isZero(state.stats[c])) continue;
      if (!best || NumberEngine.gt(NumberEngine.abs(state.stats[c]), NumberEngine.abs(state.stats[best]))) best = c;
    }
    if (!best) return null;
    this.boards.upsert(id, this.accounts.get(id)?.name ?? "?", valuesOf(state), this.clock());
    return { category: best, position: this.boards.positionOf(id, best) };
  }
  /** Cor de chat menos usada entre quem está online (15 cores). */
  pickChatColor(except) {
    const used = new Array(CHAT_COLORS.length).fill(0);
    for (const o of this.active.values()) if (o !== except) used[o.chatColor]++;
    let best = 0;
    for (let i = 1; i < used.length; i++) if (used[i] < used[best]) best = i;
    return best;
  }
  part(s) {
    const a = s.account, p = s.player;
    if (!a || !p) return;
    this.persistAccount(a, p);
    const mates = this.rooms.mates(a.id);
    this.rooms.remove(a.id);
    this.releaseEmptyRooms();
    if (this.active.get(a.id) === s) this.active.delete(a.id);
    for (const id of mates) this.active.get(id)?.send({ t: "left", id: a.id });
    s.account = null;
    s.player = null;
  }
  info(s) {
    const a = s.account, p = s.player;
    return { id: a.id, name: a.name, appearance: a.appearance, polarity: p.state.polarity, level: p.state.stats.level.toJSON() };
  }
  packed(s) {
    const p = s.player;
    return packPos(p.pos, p.yaw, p.dead ? "idle" : p.anim);
  }
  badge(s) {
    const p = s.player;
    return `${p.state.polarity}|${NumberEngine.format(p.state.stats.level)}`;
  }
  pushState(s, now) {
    const p = s.player;
    if (!p) return;
    const save = serializeProgress(p.state);
    s.send({ t: "state", ack: p.lastCmdSeq, save, hp: p.hp ? p.hp.toJSON() : null, st: now, sig: this.sign(s.account.id, save) });
  }
  persistAccount(a, p) {
    a.save = serializeProgress(p.state);
    a.position = p.dead ? null : { ...p.pos };
    a.updatedAt = this.clock();
    a.achievedAt = { ...this.boards.get(a.id)?.achievedAt ?? {} };
    this.safeSave(a);
    this.dirty.delete(a.id);
  }
  /** Laço do servidor: chame a cada ~100 ms (main.ts) ou manualmente nos testes. */
  update() {
    const now = this.clock();
    const dtMs = Math.min(1e3, Math.max(0, now - this.lastTick));
    this.lastTick = now;
    const dt = dtMs / 1e3;
    for (const s of this.active.values()) {
      const p = s.player, a = s.account;
      let r;
      try {
        r = p.tick(dt, now / 1e3);
      } catch (e) {
        console.error(`[erro] simula\xE7\xE3o de ${a.id}:`, e?.message ?? e);
        continue;
      }
      if (r.death) s.send({ t: "death", deaths: r.death.state.miasmaDeaths, negated: r.death.negated });
      if (r.death || r.events.length) this.dirty.add(a.id);
      this.boards.upsert(a.id, a.name, valuesOf(p.state), now);
      const badge = this.badge(s);
      if (badge !== s.announced) {
        s.announced = badge;
        for (const id of this.rooms.mates(a.id)) this.active.get(id)?.send({ t: "update", player: this.info(s) });
      }
    }
    if (this.bots) {
      try {
        if (this.bots.warming) {
          if (this.bots.warmupSome(WARMUP_BUDGET_MS)) this.publishBots(this.bots.bots);
        } else {
          this.publishBots(this.bots.update(now / 1e3));
          if (!this.warmed) this.warmed = true;
        }
      } catch (e) {
        console.error("[erro] IAs:", e?.message ?? e);
      }
      this.botsSaveAcc += dtMs;
      if (this.botsSaveAcc >= BOTS_SAVE_EVERY_MS) {
        this.botsSaveAcc = 0;
        this.safeSaveBots();
      }
    }
    for (const room of this.rooms.all()) {
      const p = {};
      for (const id of room.players) {
        const s = this.active.get(id);
        if (s?.player) p[id] = this.packed(s);
      }
      for (const id of this.botsOf(room.id)) {
        const b = this.bots?.get(id);
        if (!b) continue;
        p[id] = this.bots.packed(b, now / 1e3);
        const badge = `${b.state.polarity}|${NumberEngine.format(b.state.stats.level)}`;
        if (this.botBadges.get(id) !== badge) {
          this.botBadges.set(id, badge);
          const msg = JSON.stringify({ t: "update", player: this.bots.info(b) });
          for (const pid of room.players) this.active.get(pid)?.transport.send(msg);
        }
      }
      if (room.players.size > 0) {
        const text = JSON.stringify({ t: "room", p });
        for (const id of room.players) this.active.get(id)?.transport.send(text);
      }
    }
    this.stateAcc += dtMs;
    if (this.stateAcc >= STATE_EVERY_MS) {
      this.stateAcc = 0;
      for (const s of this.active.values()) this.pushState(s, now);
    }
    this.boardsAcc += dtMs;
    if (this.boardsAcc >= MULTIPLAYER_CONFIG.leaderboardRefreshMs) {
      this.boardsAcc = 0;
      this.boards.record();
      if (this.bots && !this.bots.warming) this.bots.compare(this.boards.positions("level"));
      const text = JSON.stringify({ t: "boards", at: now, boards: this.boards.snapshot() });
      for (const s of this.active.values()) s.transport.send(text);
    }
    this.persistAcc += dtMs;
    if (this.persistAcc >= PERSIST_EVERY_MS) {
      this.persistAcc = 0;
      this.flush();
    }
  }
  /** Falha de disco (cheio, sem permissão) não derruba o jogo: o backup local dos jogadores segue valendo. */
  safeSave(a) {
    try {
      this.store.save(a);
    } catch (e) {
      console.error("[erro] gravar conta:", e?.message ?? e);
    }
  }
  safeSaveBots() {
    if (!this.bots || this.bots.warming) return;
    try {
      this.store.saveBots?.(this.bots.toJSON(this.clock() / 1e3));
    } catch (e) {
      console.error("[erro] gravar IAs:", e?.message ?? e);
    }
  }
  /** Grava as contas com mudanças. */
  flush() {
    for (const s of this.active.values()) this.persistAccount(s.account, s.player);
    for (const id of this.dirty) {
      const a = this.accounts.get(id);
      if (a) this.safeSave(a);
    }
    this.dirty.clear();
  }
  shutdown() {
    for (const s of [...this.sessions]) {
      this.part(s);
      s.transport.close(1001, "servidor reiniciando");
    }
    this.flush();
    this.safeSaveBots();
  }
  /** Diagnóstico (testes/console do servidor). */
  inspect(id) {
    return this.active.get(id)?.player ?? null;
  }
};

// src/server/FileStore.ts
import fs from "node:fs";
import path from "node:path";
var FileStore = class {
  dir;
  botsFile;
  constructor(dir) {
    this.dir = path.resolve(dir, "contas");
    this.botsFile = path.resolve(dir, "ias.json");
    fs.mkdirSync(this.dir, { recursive: true });
  }
  loadAll() {
    const out = [];
    for (const f of fs.readdirSync(this.dir)) {
      if (!f.endsWith(".json")) continue;
      try {
        const a = JSON.parse(fs.readFileSync(path.join(this.dir, f), "utf8"));
        if (a && typeof a.id === "string" && typeof a.tokenHash === "string" && typeof a.save === "string") out.push(a);
      } catch {
        try {
          fs.renameSync(path.join(this.dir, f), path.join(this.dir, `${f}.corrompido`));
        } catch {
        }
      }
    }
    return out;
  }
  save(a) {
    if (!/^[a-z0-9]+$/.test(a.id)) throw new Error("id inv\xE1lido");
    const file = path.join(this.dir, `${a.id}.json`);
    const tmp = `${file}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(a));
    fs.renameSync(tmp, file);
  }
  loadBots() {
    try {
      const j = JSON.parse(fs.readFileSync(this.botsFile, "utf8"));
      return j && j.v === 1 && Array.isArray(j.bots) ? j : null;
    } catch {
      return null;
    }
  }
  saveBots(json) {
    const tmp = `${this.botsFile}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(json));
    fs.renameSync(tmp, this.botsFile);
  }
};

// src/server/main.ts
import { fileURLToPath } from "node:url";
var port = Number(process.env.PORT) || 8080;
var here = path2.dirname(fileURLToPath(import.meta.url));
var root2 = path2.resolve(process.env.PUBLIC_DIR ?? (fs2.existsSync(path2.resolve("public", "index.html")) ? "public" : path2.join(here, "public")));
var warm = (process.env.BOTS_WARMUP ?? "").split(",").map(Number);
var game = new GameServer(new FileStore(process.env.DATA_DIR ?? "data"), Date.now, {
  saveSecret: process.env.SAVE_SECRET || void 0,
  trustLocalSaves: process.env.TRUST_LOCAL_SAVES !== "0",
  bots: process.env.BOTS === "0" ? false : warm.length === 2 && warm.every((v) => Number.isFinite(v) && v >= 0) ? { ...SERVER_BOTS, warmupMinutes: [warm[0], warm[1]] } : SERVER_BOTS
});
var types = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".map": "application/json",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".json": "application/json",
  ".ico": "image/x-icon"
};
var lastLog = /* @__PURE__ */ new Map();
function logError(where, e) {
  const key = `${where}:${e?.message ?? e}`;
  const now = Date.now();
  if ((lastLog.get(key) ?? 0) > now - 1e4) return;
  lastLog.set(key, now);
  console.error(`[erro] ${where}:`, e?.stack ?? e);
}
function status() {
  return JSON.stringify({
    ok: true,
    protocol: PROTOCOL_VERSION,
    online: game.onlineCount,
    rooms: game.rooms.all().length,
    bots: game.bots?.bots.length ?? 0,
    botsWarming: game.bots?.warming ?? false,
    uptime: Math.round(process.uptime())
  });
}
var server = http.createServer((req, res) => {
  try {
    if (req.method !== "GET" && req.method !== "HEAD") {
      res.writeHead(405, { allow: "GET, HEAD" });
      res.end();
      return;
    }
    let url;
    try {
      url = decodeURIComponent(new URL(req.url ?? "/", "http://x").pathname);
    } catch {
      res.writeHead(400);
      res.end();
      return;
    }
    if (url === "/api/status" || url === "/healthz") {
      res.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
      res.end(req.method === "HEAD" ? void 0 : status());
      return;
    }
    if (url.includes("\0")) {
      res.writeHead(400);
      res.end();
      return;
    }
    let file = path2.join(root2, url === "/" ? "index.html" : url);
    if (file !== root2 && !file.startsWith(root2 + path2.sep)) {
      res.writeHead(403);
      res.end();
      return;
    }
    let st = fs2.existsSync(file) ? fs2.statSync(file) : null;
    if (!st || st.isDirectory()) {
      file = path2.join(root2, "index.html");
      st = fs2.existsSync(file) ? fs2.statSync(file) : null;
    }
    if (!st) {
      res.writeHead(500, { "content-type": "text/plain; charset=utf-8" });
      res.end("Jogo n\xE3o encontrado: a pasta public/ precisa estar ao lado do server.mjs");
      return;
    }
    const ext = path2.extname(file);
    res.writeHead(200, {
      "content-type": types[ext] ?? "application/octet-stream",
      "content-length": st.size,
      "cache-control": ext === ".html" ? "no-cache" : "public, max-age=300",
      "x-content-type-options": "nosniff"
    });
    if (req.method === "HEAD") {
      res.end();
      return;
    }
    const stream = fs2.createReadStream(file);
    stream.on("error", (e) => {
      logError("arquivo", e);
      res.destroy();
    });
    stream.pipe(res);
  } catch (e) {
    logError("http", e);
    if (!res.headersSent) res.writeHead(500);
    res.end();
  }
});
server.on("clientError", (_e, socket) => {
  if (socket.writable) socket.end("HTTP/1.1 400 Bad Request\r\n\r\n");
  else socket.destroy();
});
server.keepAliveTimeout = 65e3;
server.headersTimeout = 66e3;
var ws = attachWebSocket(server, { path: "/ws", maxMessageBytes: MAX_MESSAGE_BYTES }, (conn) => {
  const s = game.connect(conn);
  conn.onmessage = (text) => {
    try {
      game.message(s, text);
    } catch (e) {
      logError("mensagem", e);
    }
  };
  conn.onclose = () => {
    try {
      game.disconnect(s);
    } catch (e) {
      logError("desconex\xE3o", e);
    }
  };
});
var loop = setInterval(() => {
  try {
    game.update();
  } catch (e) {
    logError("la\xE7o do jogo", e);
  }
}, TICK_MS);
var host = process.env.HOST ?? "0.0.0.0";
server.on("error", (e) => {
  console.error(`[erro] n\xE3o foi poss\xEDvel abrir a porta ${port}:`, e.message);
  process.exit(1);
});
server.listen(port, host, () => {
  console.log(`Ascension Infinite \xB7 servidor oficial em http://${host}:${port} (WebSocket /ws)`);
  console.log(`Node ${process.version} \xB7 jogo em ${root2} \xB7 dados em ${path2.resolve(process.env.DATA_DIR ?? "data")} \xB7 IAs: ${game.bots ? game.bots.bots.length : 0} \xB7 saves locais: ${process.env.TRUST_LOCAL_SAVES === "0" ? "modo estrito" : "aceitos"}${process.env.SAVE_SECRET ? "" : " \xB7 aviso: sem SAVE_SECRET (assinaturas mudam a cada rein\xEDcio)"}`);
});
process.on("uncaughtException", (e) => logError("n\xE3o tratado", e));
process.on("unhandledRejection", (e) => logError("promessa", e));
var stopping = false;
var stop = () => {
  if (stopping) return;
  stopping = true;
  clearInterval(loop);
  try {
    game.shutdown();
  } catch (e) {
    logError("encerramento", e);
  }
  ws.close();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 1500).unref();
};
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
export {
  logError
};
