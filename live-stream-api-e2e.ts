import { randomUUID } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import {
  LocalVideoTrack,
  Room,
  RoomEvent,
  TrackKind,
  TrackPublishOptions,
  TrackSource,
  VideoBufferType,
  VideoFrame,
  VideoSource,
  VideoStream,
  dispose,
} from '@livekit/rtc-node';
import type { RemoteTrack } from '@livekit/rtc-node';

const RESULTS_DIR = resolve(process.cwd(), 'live-e2e-results');

const DEFAULTS = {
  usersFile: './users.json',
  apiBase: 'https://tubor-api.onrender.com',
  tokensFile: '',
  streamers: 5,
  viewersPerStream: 10,
  durationSeconds: 120,
  rampMs: 250,
  requestTimeoutMs: 30_000,
  liveTimeoutMs: 45_000,
  mediaTimeoutMs: 30_000,
  minFrames: 15,
  publishWidth: 640,
  publishHeight: 360,
  publishFps: 15,
  maxRateLimitRetries: 10,
  joinConcurrency: 10,
  maxAccounts: 100,
};

type Config = typeof DEFAULTS & {
  keepStreams: boolean;
  viewerAuth: boolean;
  noDecode: boolean;
  takeover: boolean;
  probe: boolean;
  cleanup: boolean;
};

type SeedUser = {
  id?: string;
  identityId?: string;
  email: string;
  password: string;
  username: string;
  displayName?: string;
  roles?: string[];
};

type SeedFile = {
  generatedAt?: string;
  emailDomain?: string;
  seededInto?: Record<string, string>;
  users: SeedUser[];
};

type CleanupResult = { ok: boolean; details: string };

type StreamResult = {
  index: number;
  username: string;
  streamId: string;
  status: 'started' | 'failed';
  resumed?: boolean;
  error?: string;
  timeToLiveMs?: number;
  publisherFramesSent?: number;
  publisherStayedConnected?: boolean;
  currentViewersReportedByApi?: number | null;
  cleanup?: CleanupResult;
};

type ViewerResult = {
  viewer: string;
  streamIndex: number;
  streamId: string;
  mode?: 'livekit' | 'hls';
  status: 'passed' | 'failed';
  /** Time spent on the playback-session call, including any rate-limit waits. */
  playbackSessionMs?: number;
  /** LiveKit connect (or HLS start) until media is flowing; excludes the playback-session call. */
  firstMediaMs?: number;
  /** Everything: ramp-in excluded, rate-limit waits, join queue, connect and first media. */
  totalToFirstMediaMs?: number;
  unitsAtJoin?: number;
  unitsFinal?: number;
  stillReceivingAtEnd?: boolean;
  detail?: Record<string, unknown>;
  error?: string;
};

type ViewerHandle = {
  mode: 'livekit' | 'hls';
  /** Decoded frames (LiveKit) or downloaded segments (HLS). */
  units: () => number;
  minUnits: number;
  progressWindowMs: number;
  staleAfterMs: number;
  lastProgressAt: () => number;
  alive: () => boolean;
  detail: () => Record<string, unknown>;
  stop: () => Promise<void>;
};

let config!: Config;
let stopRequested = false;
let playbackShapeShown = false;
let wakeHold: (() => void) | undefined;
const unhandled: string[] = [];

/**
 * Paces requests that hit a rate limit. After a 429 every waiting caller re-enters through the
 * same gate, one slot apart, so a whole wave of viewers does not retry in the same instant.
 * The gap doubles only if 429s keep coming after the previous wave had time to drain.
 */
class Gate {
  private nextAt = 0;
  private gapMs = 0;
  private lastPenaltyAt = 0;

  async enter(): Promise<void> {
    const now = Date.now();
    const at = Math.max(now, this.nextAt);
    this.nextAt = at + this.gapMs;
    if (at > now) await sleep(at - now);
  }

  penalize(waitMs: number): void {
    const now = Date.now();
    if (now - this.lastPenaltyAt > 3_000) {
      this.gapMs = Math.min(3_000, Math.max(300, this.gapMs * 2));
      this.lastPenaltyAt = now;
    }
    this.nextAt = Math.max(this.nextAt, now + waitMs);
  }
}

const loginGate = new Gate();
const playbackGate = new Gate();

class Semaphore {
  private queue: Array<() => void> = [];
  constructor(private free: number) {}

  async acquire(): Promise<void> {
    if (this.free > 0) {
      this.free -= 1;
      return;
    }
    await new Promise<void>((resolveSlot) => this.queue.push(resolveSlot));
  }

  release(): void {
    const next = this.queue.shift();
    if (next) next();
    else this.free += 1;
  }
}

let joinSlots!: Semaphore;

const stats = { requests: 0, http429: 0, transientRetries: 0, logins: 0, cachedLogins: 0 };

// ---------------------------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------------------------

const sleep = (ms: number) => new Promise<void>((done) => setTimeout(done, ms));
const errorMessage = (error: unknown): string => (error instanceof Error ? error.message : String(error));
const jitter = (maxMs: number): number => Math.floor(Math.random() * maxMs);

function asRecord(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function parseArgs(argv: string[]): Config {
  const values: Record<string, string | boolean> = {};
  for (let i = 0; i < argv.length; i += 1) {
    const part = argv[i]!;
    if (!part.startsWith('--')) continue;
    const equals = part.indexOf('=');
    if (equals > -1) {
      values[part.slice(2, equals)] = part.slice(equals + 1);
      continue;
    }
    const key = part.slice(2);
    const next = argv[i + 1];
    if (next && !next.startsWith('--')) {
      values[key] = next;
      i += 1;
    } else {
      values[key] = true;
    }
  }

  const numberArg = (name: string, fallback: number): number => {
    const raw = values[name];
    if (raw === undefined || raw === true) return fallback;
    const value = Number(raw);
    if (!Number.isInteger(value) || value < 0) throw new Error(`--${name} must be a non-negative integer.`);
    return value;
  };
  const stringArg = (name: string, fallback: string): string =>
    typeof values[name] === 'string' ? (values[name] as string) : fallback;

  const parsed: Config = {
    ...DEFAULTS,
    usersFile: stringArg('users-file', DEFAULTS.usersFile),
    apiBase: stringArg('api-base', DEFAULTS.apiBase).replace(/\/+$/, ''),
    tokensFile: stringArg('tokens-file', DEFAULTS.tokensFile),
    streamers: numberArg('streamers', DEFAULTS.streamers),
    viewersPerStream: numberArg('viewers-per-stream', DEFAULTS.viewersPerStream),
    durationSeconds: numberArg('duration-seconds', DEFAULTS.durationSeconds),
    rampMs: numberArg('ramp-ms', DEFAULTS.rampMs),
    requestTimeoutMs: numberArg('request-timeout-ms', DEFAULTS.requestTimeoutMs),
    liveTimeoutMs: numberArg('live-timeout-ms', DEFAULTS.liveTimeoutMs),
    mediaTimeoutMs: numberArg('media-timeout-ms', DEFAULTS.mediaTimeoutMs),
    minFrames: numberArg('min-frames', DEFAULTS.minFrames),
    publishWidth: numberArg('publish-width', DEFAULTS.publishWidth),
    publishHeight: numberArg('publish-height', DEFAULTS.publishHeight),
    publishFps: numberArg('publish-fps', DEFAULTS.publishFps),
    maxRateLimitRetries: numberArg('max-rate-limit-retries', DEFAULTS.maxRateLimitRetries),
    joinConcurrency: numberArg('join-concurrency', DEFAULTS.joinConcurrency),
    maxAccounts: numberArg('max-accounts', DEFAULTS.maxAccounts),
    keepStreams: Boolean(values['keep-streams']),
    viewerAuth: Boolean(values['viewer-auth']),
    noDecode: Boolean(values['no-decode']),
    takeover: Boolean(values.takeover),
    probe: Boolean(values.probe),
    cleanup: Boolean(values.cleanup),
  };

  if (parsed.joinConcurrency < 1) throw new Error('--join-concurrency must be at least 1.');
  if (parsed.probe) parsed.streamers = 1;
  if (parsed.streamers < 1) throw new Error('--streamers must be at least 1.');
  if (parsed.durationSeconds < 1) throw new Error('--duration-seconds must be at least 1.');
  if (parsed.publishFps < 1 || parsed.publishFps > 60) throw new Error('--publish-fps must be between 1 and 60.');
  if (!/^https?:\/\//i.test(parsed.apiBase)) throw new Error('--api-base must start with http:// or https://.');
  if (!parsed.probe && parsed.viewersPerStream < 1) throw new Error('--viewers-per-stream must be at least 1.');
  return parsed;
}

function loadUsers(filePath: string): { seed: SeedFile; users: SeedUser[]; absolutePath: string } {
  const absolutePath = resolve(process.cwd(), filePath);
  if (!existsSync(absolutePath)) {
    throw new Error(`Could not find users.json at ${absolutePath}. Put it in the working directory or pass --users-file.`);
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(readFileSync(absolutePath, 'utf8')) as unknown;
  } catch (error) {
    throw new Error(`users.json is not valid JSON: ${errorMessage(error)}`);
  }
  const seed = (Array.isArray(parsed) ? { users: parsed } : parsed) as SeedFile;
  if (!seed || !Array.isArray(seed.users)) {
    throw new Error('Expected users.json to contain a top-level "users" array (the SeedFile format).');
  }
  const users = seed.users.filter((user): user is SeedUser =>
    Boolean(
      user &&
        typeof user.email === 'string' && user.email.includes('@') &&
        typeof user.password === 'string' && user.password.length > 0 &&
        typeof user.username === 'string' && user.username.length > 0,
    ),
  );
  if (users.length !== seed.users.length) {
    throw new Error(`users.json has ${seed.users.length} entries, but only ${users.length} have email, password, and username.`);
  }
  if (new Set(users.map((user) => user.email.toLowerCase())).size !== users.length) {
    throw new Error('users.json contains duplicate email addresses; every concurrent session needs a unique account.');
  }
  return { seed, users, absolutePath };
}

function shuffle<T>(items: T[]): T[] {
  const result = [...items];
  for (let i = result.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [result[i], result[j]] = [result[j]!, result[i]!];
  }
  return result;
}

function percentile(values: number[], p: number): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1))]!;
}

// ---------------------------------------------------------------------------------------------
// Response inspection (the publish / playback-session shapes are discovered, not assumed)
// ---------------------------------------------------------------------------------------------

type FoundString = { key: string; path: string; value: string };

const looksLikeJwt = (value: string): boolean => value.length > 50 && /^[\w-]+\.[\w-]+\.[\w-]*$/.test(value);

function collectStrings(value: unknown, path = '$', key = '', depth = 0, into: FoundString[] = []): FoundString[] {
  if (depth > 6) return into;
  if (typeof value === 'string') {
    into.push({ key, path, value });
  } else if (Array.isArray(value)) {
    value.slice(0, 20).forEach((item, index) => collectStrings(item, `${path}[${index}]`, key, depth + 1, into));
  } else if (value !== null && typeof value === 'object') {
    for (const [childKey, child] of Object.entries(value)) {
      collectStrings(child, `${path}.${childKey}`, childKey, depth + 1, into);
    }
  }
  return into;
}

const SECRET_KEY = /token|secret|password|passwd|jwt|authorization|stream_?key|^key$|api_?key/i;

/** A printable copy of a JSON value: secrets and URL query strings are replaced. */
function shapeOf(value: unknown, key = '', depth = 0): unknown {
  if (value === null || typeof value === 'number' || typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    if (SECRET_KEY.test(key) || looksLikeJwt(value)) return `<redacted, ${value.length} chars>`;
    try {
      const url = new URL(value);
      url.username = '';
      url.password = '';
      if (url.search) url.search = '?<redacted>';
      return url.toString().slice(0, 200);
    } catch {
      return value.length > 80 ? `${value.slice(0, 40)}...<${value.length} chars>` : value;
    }
  }
  if (Array.isArray(value)) return depth > 4 ? '[...]' : value.slice(0, 5).map((item) => shapeOf(item, key, depth + 1));
  if (typeof value === 'object') {
    if (depth > 4) return '{...}';
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, shapeOf(v, k, depth + 1)]));
  }
  return String(value);
}

const printable = (value: unknown): string => JSON.stringify(shapeOf(value), null, 2);

function findLiveKitCredentials(payload: unknown): { url: string; token: string } | null {
  const strings = collectStrings(payload);
  const rtcKey = /webrtc|livekit|rtc/i;
  const urlHits = strings.filter(
    (s) =>
      /^wss?:\/\//i.test(s.value) ||
      (/^https?:\/\//i.test(s.value) && /livekit|server_?url|ws_?url|rtc_?url|webrtc/i.test(s.key)),
  );
  const url = urlHits.find((s) => rtcKey.test(s.key)) ?? urlHits[0];
  // A response can carry several JWTs (e.g. an HLS playback token next to a LiveKit one).
  // Prefer the one named for WebRTC/LiveKit; a plain "token" is the fallback.
  const jwts = strings.filter((s) => looksLikeJwt(s.value));
  const token = jwts.find((s) => rtcKey.test(s.key)) ?? jwts.find((s) => /token/i.test(s.key)) ?? jwts[0];
  if (!url || !token) return null;
  try {
    const parsed = new URL(url.value.replace(/^http/i, 'ws'));
    parsed.search = '';
    parsed.hash = '';
    return { url: parsed.toString(), token: token.value };
  } catch {
    return null;
  }
}

function findHlsPlaylist(payload: unknown): string | null {
  const hit = collectStrings(payload).find((s) => /\.m3u8(?:[?#]|$)/i.test(s.value));
  if (!hit) return null;
  try {
    return new URL(hit.value, config.apiBase).toString();
  } catch {
    return null;
  }
}

function pickAccessToken(payload: unknown): string | null {
  const strings = collectStrings(payload).filter((s) => /^(access_?token|token|jwt|id_?token)$/i.test(s.key) && s.value.length > 20);
  const preferred = strings.find((s) => /^access/i.test(s.key)) ?? strings[0];
  return preferred ? preferred.value : null;
}

function decodeJwtExp(token: string): number | null {
  const part = token.split('.')[1];
  if (!part) return null;
  try {
    const payload = JSON.parse(Buffer.from(part, 'base64url').toString('utf8')) as { exp?: unknown };
    return typeof payload.exp === 'number' ? payload.exp : null;
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------------------------
// HTTP client: retries cold starts (502/503/504) and honours 429 + Retry-After
// ---------------------------------------------------------------------------------------------

type ApiResult = { status: number; ok: boolean; json: unknown; text: string };

function retryAfterMs(response: Response): number {
  const header = response.headers.get('retry-after') ?? response.headers.get('ratelimit-reset');
  if (header) {
    const seconds = Number(header);
    if (Number.isFinite(seconds)) return Math.min(90_000, Math.max(1_000, seconds * 1_000));
    const date = Date.parse(header);
    if (Number.isFinite(date)) return Math.min(90_000, Math.max(1_000, date - Date.now()));
  }
  return 15_000;
}

async function api(
  method: 'GET' | 'POST' | 'DELETE',
  path: string,
  options: { token?: string; body?: unknown; idempotent?: boolean; gate?: Gate } = {},
): Promise<ApiResult> {
  const url = `${config.apiBase}${path}`;
  const headers: Record<string, string> = { accept: 'application/json', 'user-agent': 'tubor-api-e2e/1.0' };
  if (options.token) headers.authorization = `Bearer ${options.token}`;

  let body: string | undefined;
  if (method === 'POST') {
    headers['content-type'] = 'application/json';
    body = JSON.stringify(options.body ?? {});
    // One key per logical request, reused across retries, so a retried POST is never applied twice.
    if (options.idempotent) headers['idempotency-key'] = randomUUID();
  }

  let rateLimitRetries = 0;
  let transientRetries = 0;

  for (;;) {
    if (options.gate) await options.gate.enter();

    stats.requests += 1;
    let response: Response;
    try {
      response = await fetch(url, { method, headers, body, signal: AbortSignal.timeout(config.requestTimeoutMs) });
    } catch (error) {
      if (transientRetries < 3) {
        transientRetries += 1;
        stats.transientRetries += 1;
        await sleep(1_500 * transientRetries);
        continue;
      }
      throw new Error(`${method} ${path} failed: ${errorMessage(error)}`);
    }

    const text = await response.text().catch(() => '');

    if ([502, 503, 504].includes(response.status) && transientRetries < 3) {
      transientRetries += 1;
      stats.transientRetries += 1;
      await sleep(1_500 * transientRetries);
      continue;
    }

    if (response.status === 429) {
      stats.http429 += 1;
      if (rateLimitRetries < config.maxRateLimitRetries) {
        rateLimitRetries += 1;
        const wait = retryAfterMs(response);
        if (stats.http429 <= 3 || stats.http429 % 25 === 0) {
          console.warn(`[http] 429 on ${method} ${path.replace(/[0-9a-f-]{36}/gi, ':id')}; waiting ~${Math.round(wait / 1000)}s (retry ${rateLimitRetries}/${config.maxRateLimitRetries}; ${stats.http429} 429s so far)`);
        }
        if (options.gate) {
          // Everyone sharing this gate waits, then re-enters one slot apart.
          options.gate.penalize(wait);
        } else {
          await sleep(wait + jitter(2_000));
        }
        continue;
      }
    }

    let json: unknown = null;
    if (text) {
      try {
        json = JSON.parse(text) as unknown;
      } catch {
        json = null;
      }
    }
    return { status: response.status, ok: response.ok, json, text };
  }
}

const snippet = (result: ApiResult): string => result.text.replace(/\s+/g, ' ').slice(0, 300) || '(empty body)';

// ---------------------------------------------------------------------------------------------
// Login with optional token cache
// ---------------------------------------------------------------------------------------------

type TokenCache = Record<string, { accessToken: string; exp: number | null }>;
let tokenCache: TokenCache = {};

function loadTokenCache(): void {
  if (!config.tokensFile || !existsSync(resolve(process.cwd(), config.tokensFile))) return;
  try {
    tokenCache = JSON.parse(readFileSync(resolve(process.cwd(), config.tokensFile), 'utf8')) as TokenCache;
  } catch {
    tokenCache = {};
  }
}

function saveTokenCache(): void {
  if (!config.tokensFile) return;
  writeFileSync(resolve(process.cwd(), config.tokensFile), `${JSON.stringify(tokenCache, null, 2)}\n`, 'utf8');
}

async function login(user: SeedUser, force = false): Promise<string> {
  const key = user.email.toLowerCase();
  const cached = tokenCache[key];
  if (!force && cached && cached.exp !== null && cached.exp * 1_000 > Date.now() + 90_000) {
    stats.cachedLogins += 1;
    return cached.accessToken;
  }

  const result = await api('POST', '/api/auth/login', {
    body: { email: user.email, password: user.password },
    gate: loginGate,
  });
  stats.logins += 1;

  if (!result.ok) {
    throw new Error(`Login failed for ${user.username}: HTTP ${result.status} ${snippet(result)}`);
  }
  const token = pickAccessToken(result.json);
  if (!token) {
    const mfa = collectStrings(result.json).some((s) => /mfa/i.test(s.key)) || /mfa/i.test(result.text);
    throw new Error(
      mfa
        ? `Login for ${user.username} wants MFA; this test cannot complete MFA challenges. Use a seed account without MFA.`
        : `Login for ${user.username} succeeded (HTTP ${result.status}) but no access token was found. Response shape: ${printable(result.json)}`,
    );
  }

  tokenCache[key] = { accessToken: token, exp: decodeJwtExp(token) };
  saveTokenCache();
  return token;
}

// ---------------------------------------------------------------------------------------------
// Synthetic broadcaster (LiveKit publish)
// ---------------------------------------------------------------------------------------------

class SyntheticPublisher {
  private readonly room = new Room();
  private source: VideoSource | undefined;
  private track: LocalVideoTrack | undefined;
  private timer: NodeJS.Timeout | undefined;
  private frameNo = 0;
  private droppedByServer = false;
  framesSent = 0;

  get stayedConnected(): boolean {
    return !this.droppedByServer && this.room.isConnected;
  }

  async start(url: string, token: string): Promise<void> {
    const { publishWidth: width, publishHeight: height, publishFps: fps } = config;

    this.room.on(RoomEvent.Disconnected, () => {
      this.droppedByServer = true;
    });

    await this.room.connect(url, token, { autoSubscribe: false, dynacast: false });

    this.source = new VideoSource(width, height);
    this.track = LocalVideoTrack.createVideoTrack('tubor-e2e-camera', this.source);
    const options = new TrackPublishOptions({ source: TrackSource.SOURCE_CAMERA });
    await this.room.localParticipant!.publishTrack(this.track, options);

    this.timer = setInterval(() => {
      try {
        this.source?.captureFrame(this.nextFrame(width, height));
        this.framesSent += 1;
      } catch {
        // A closed source during teardown is expected.
      }
    }, Math.round(1_000 / fps));
  }

  /** Moving white bar over a gradient, so the encoder always has real motion to encode. */
  private nextFrame(width: number, height: number): VideoFrame {
    const data = new Uint8Array(width * height * 4);
    const barX = (this.frameNo * 6) % width;
    const tint = this.frameNo % 255;
    this.frameNo += 1;
    for (let y = 0; y < height; y += 1) {
      const green = Math.floor((y / height) * 200);
      for (let x = 0; x < width; x += 1) {
        const i = (y * width + x) * 4;
        const inBar = x >= barX && x < barX + 40;
        data[i] = inBar ? 255 : tint;
        data[i + 1] = inBar ? 255 : green;
        data[i + 2] = inBar ? 255 : 120;
        data[i + 3] = 255;
      }
    }
    return new VideoFrame(data, width, height, VideoBufferType.RGBA);
  }

  async stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    await this.room.disconnect().catch(() => undefined);
    await this.source?.close().catch(() => undefined);
  }
}

// ---------------------------------------------------------------------------------------------
// Viewers: LiveKit subscriber or HLS poller
// ---------------------------------------------------------------------------------------------

async function startLiveKitViewer(url: string, token: string): Promise<ViewerHandle> {
  const room = new Room();
  const state = { frames: 0, lastProgressAt: 0, width: 0, height: 0, videoTracks: 0, dropped: false, consumeError: '' };
  // --no-decode: no VideoStream, so no frame is ever copied into JavaScript. The frame count comes
  // from the WebRTC inbound-rtp stats instead (framesReceived counts frames before decoding).
  const rtp = { packets: 0, bytes: 0, decoded: 0 };
  // What the stats call actually returned, so an empty result is explainable instead of silent.
  const diag = { calls: 0, errors: 0, lastError: '', entries: 0, cases: new Set<string>(), kinds: new Set<string>() };
  let statsTimer: NodeJS.Timeout | undefined;
  let polling = false;

  const pollStats = async (): Promise<void> => {
    if (polling || !room.isConnected) return;
    polling = true;
    try {
      const { publisherStats, subscriberStats } = await room.getRtcStats();
      diag.calls += 1;
      // A viewer publishes nothing, but with a single peer connection the receive stats may be
      // listed under the publisher side, so look at both.
      const all = [...subscriberStats, ...publisherStats];
      diag.entries = all.length;
      let frames = 0;
      let packets = 0;
      let bytes = 0;
      let decoded = 0;
      for (const { stats } of all) {
        diag.cases.add(String(stats.case));
        if (stats.case === 'inboundRtp') diag.kinds.add(String(stats.value.stream?.kind));
        if (stats.case !== 'inboundRtp' || stats.value.stream?.kind !== 'video') continue;
        const { inbound, received } = stats.value;
        frames += Number(inbound?.framesReceived ?? 0n);
        decoded += inbound?.framesDecoded ?? 0;
        bytes += Number(inbound?.bytesReceived ?? 0n);
        packets += Number(received?.packetsReceived ?? 0n);
        state.width = inbound?.frameWidth ?? state.width;
        state.height = inbound?.frameHeight ?? state.height;
      }
      rtp.packets = packets;
      rtp.bytes = bytes;
      rtp.decoded = decoded;
      if (frames > state.frames) {
        state.frames = frames;
        state.lastProgressAt = Date.now();
      }
    } catch (error) {
      // Usually a stats call that raced a disconnect; recorded so a persistent failure shows in the report.
      diag.errors += 1;
      diag.lastError = errorMessage(error).slice(0, 200);
    } finally {
      polling = false;
    }
  };

  const consume = (track: RemoteTrack): void => {
    state.videoTracks += 1;
    void (async () => {
      try {
        // Created inside the try: if the track was already torn down (room dropped while the
        // subscribe event was in flight) the native call throws, and that must not crash the run.
        const reader = new VideoStream(track).getReader();
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          state.frames += 1;
          state.lastProgressAt = Date.now();
          state.width = value.frame.width;
          state.height = value.frame.height;
        }
      } catch (error) {
        // Expected when the room disconnects; also covers a track that vanished before we read it.
        if (!state.dropped && state.frames === 0) state.consumeError = errorMessage(error).slice(0, 200);
      }
    })();
  };

  room.on(RoomEvent.TrackSubscribed, (track) => {
    if (track.kind !== TrackKind.KIND_VIDEO) return;
    if (config.noDecode) state.videoTracks += 1;
    else consume(track);
  });
  room.on(RoomEvent.Disconnected, () => {
    state.dropped = true;
  });

  let timeout: NodeJS.Timeout | undefined;
  try {
    await Promise.race([
      room.connect(url, token, { autoSubscribe: true, dynacast: false }),
      new Promise<never>((_, reject) => {
        timeout = setTimeout(() => reject(new Error(`LiveKit connect timed out after ${config.mediaTimeoutMs}ms`)), config.mediaTimeoutMs);
      }),
    ]);
  } catch (error) {
    await room.disconnect().catch(() => undefined);
    throw error;
  } finally {
    if (timeout) clearTimeout(timeout);
  }

  if (config.noDecode) statsTimer = setInterval(() => void pollStats(), 1_000);

  return {
    mode: 'livekit',
    units: () => state.frames,
    minUnits: config.minFrames,
    progressWindowMs: 2_000,
    staleAfterMs: 5_000,
    lastProgressAt: () => state.lastProgressAt,
    alive: () => room.isConnected && !state.dropped,
    detail: () => ({
      frames: state.frames,
      width: state.width,
      height: state.height,
      videoTracks: state.videoTracks,
      consumeError: state.consumeError || undefined,
      ...(config.noDecode
        ? {
            decode: false,
            rtpPackets: rtp.packets,
            rtpBytes: rtp.bytes,
            framesDecodedNatively: rtp.decoded,
            statsCalls: diag.calls,
            statsErrors: diag.errors,
            statsLastError: diag.lastError || undefined,
            statsEntries: diag.entries,
            statsCases: [...diag.cases],
            inboundKinds: [...diag.kinds],
          }
        : {}),
    }),
    stop: async () => {
      if (statsTimer) clearInterval(statsTimer);
      await room.disconnect().catch(() => undefined);
    },
  };
}

async function fetchBody(url: string): Promise<ArrayBuffer> {
  const response = await fetch(url, { signal: AbortSignal.timeout(config.requestTimeoutMs) });
  if (!response.ok) throw new Error(`HTTP ${response.status} for ${url.split('?')[0]}`);
  return response.arrayBuffer();
}

function startHlsViewer(playlistUrl: string): ViewerHandle {
  const state = { segments: 0, bytes: 0, lastProgressAt: 0, targetDurationMs: 4_000, lastError: '' };
  const seen = new Set<string>();
  let stopped = false;

  const loop = (async () => {
    let mediaUrl = playlistUrl;
    while (!stopped) {
      try {
        const text = new TextDecoder().decode(await fetchBody(mediaUrl));
        if (text.includes('#EXT-X-STREAM-INF')) {
          const lines = text.split(/\r?\n/);
          const at = lines.findIndex((line) => line.startsWith('#EXT-X-STREAM-INF'));
          const uri = lines.slice(at + 1).find((line) => line.trim() && !line.startsWith('#'));
          if (!uri) throw new Error('Master playlist has no variant URI');
          mediaUrl = new URL(uri.trim(), mediaUrl).toString();
          continue;
        }
        const target = /#EXT-X-TARGETDURATION:(\d+)/.exec(text);
        if (target) state.targetDurationMs = Number(target[1]) * 1_000;
        const segments = text.split(/\r?\n/).map((line) => line.trim()).filter((line) => line && !line.startsWith('#'));
        for (const segment of segments.slice(-3)) {
          const segmentUrl = new URL(segment, mediaUrl).toString();
          if (seen.has(segmentUrl)) continue;
          seen.add(segmentUrl);
          const bytes = (await fetchBody(segmentUrl)).byteLength;
          if (bytes > 0) {
            state.segments += 1;
            state.bytes += bytes;
            state.lastProgressAt = Date.now();
          }
        }
        state.lastError = '';
      } catch (error) {
        state.lastError = errorMessage(error);
      }
      await sleep(Math.max(1_000, state.targetDurationMs / 2));
    }
  })();

  return {
    mode: 'hls',
    units: () => state.segments,
    minUnits: 2,
    progressWindowMs: 4_000,
    get staleAfterMs() {
      return state.targetDurationMs * 3 + 5_000;
    },
    lastProgressAt: () => state.lastProgressAt,
    alive: () => !stopped,
    detail: () => ({ segments: state.segments, bytes: state.bytes, targetDurationMs: state.targetDurationMs, lastError: state.lastError }),
    stop: async () => {
      stopped = true;
      await loop.catch(() => undefined);
    },
  };
}

async function verifyViewerMedia(handle: ViewerHandle, startedAt: number): Promise<number> {
  // startedAt is when this viewer began connecting (see runViewer).
  while (Date.now() - startedAt < config.mediaTimeoutMs) {
    if (handle.units() >= handle.minUnits) break;
    await sleep(250);
  }
  if (handle.units() < handle.minUnits) {
    throw new Error(
      `Only ${handle.units()}/${handle.minUnits} ${handle.mode === 'livekit' ? 'video frames' : 'HLS segments'} within ${config.mediaTimeoutMs}ms. ${JSON.stringify(handle.detail())}`,
    );
  }
  const firstMediaMs = Date.now() - startedAt;

  const before = handle.units();
  await sleep(handle.progressWindowMs);
  if (handle.units() <= before) {
    throw new Error(`Media arrived but then stalled (no new ${handle.mode === 'livekit' ? 'frames' : 'segments'} in ${handle.progressWindowMs}ms). ${JSON.stringify(handle.detail())}`);
  }
  return firstMediaMs;
}

async function runViewer(
  user: SeedUser | null,
  label: string,
  streamIndex: number,
  streamId: string,
  rampDelayMs: number,
): Promise<{ result: ViewerResult; handle?: ViewerHandle }> {
  if (rampDelayMs > 0) await sleep(rampDelayMs);

  const result: ViewerResult = { viewer: label, streamIndex, streamId, status: 'failed' };
  if (stopRequested) {
    result.error = 'Skipped: run was interrupted.';
    return { result };
  }

  const startedAt = Date.now();
  let handle: ViewerHandle | undefined;
  let sessionJson: unknown;
  try {
    const token = user ? await login(user) : undefined;

    const session = await api('POST', `/api/live/streams/${encodeURIComponent(streamId)}/playback-session`, { token, gate: playbackGate });
    sessionJson = session.json;
    result.playbackSessionMs = Date.now() - startedAt;
    if (!session.ok) throw new Error(`playback-session returned HTTP ${session.status}: ${snippet(session)}`);

    const creds = findLiveKitCredentials(session.json);
    const hls = findHlsPlaylist(session.json);
    let joinStartedAt = Date.now();
    if (creds) {
      result.mode = 'livekit';
      // Limit simultaneous LiveKit connects; a burst of dozens at once overwhelms the native runtime.
      await joinSlots.acquire();
      joinStartedAt = Date.now();
      try {
        handle = await startLiveKitViewer(creds.url, creds.token);
      } finally {
        joinSlots.release();
      }
    } else if (hls) {
      result.mode = 'hls';
      handle = startHlsViewer(hls);
    } else {
      throw new Error(`playback-session response has neither a LiveKit url+token nor an .m3u8 URL. Shape: ${printable(session.json)}`);
    }

    // The media timeout counts from the moment this viewer actually started connecting,
    // not from time spent waiting out rate limits or the join queue.
    result.firstMediaMs = await verifyViewerMedia(handle, joinStartedAt);
    result.totalToFirstMediaMs = joinStartedAt - startedAt + result.firstMediaMs;
    result.unitsAtJoin = handle.units();
    result.status = 'passed';
    console.log(`[viewer] PASS ${label} -> stream ${streamIndex} (${result.mode}, media after ${result.firstMediaMs}ms; ${result.totalToFirstMediaMs}ms including API/queue waits)`);
    return { result, handle };
  } catch (error) {
    result.error = errorMessage(error);
    if (sessionJson !== undefined && !playbackShapeShown) {
      playbackShapeShown = true;
      console.error(`[viewer] playback-session response shape (secrets redacted, shown once):\n${printable(sessionJson)}`);
    }
    if (handle) {
      result.detail = handle.detail();
      await handle.stop().catch(() => undefined);
    }
    console.error(`[viewer] FAIL ${label} -> stream ${streamIndex}: ${result.error.slice(0, 400)}`);
    return { result };
  }
}

// ---------------------------------------------------------------------------------------------
// Broadcast lifecycle through the API
// ---------------------------------------------------------------------------------------------

type StreamSession = {
  index: number;
  user: SeedUser;
  token: string;
  result: StreamResult;
  publisher: SyntheticPublisher | null;
};

async function readStreamStatus(token: string | undefined, streamId: string): Promise<{ status: string | null; currentViewers: number | null }> {
  const result = await api('GET', `/api/live/streams/${encodeURIComponent(streamId)}`, { token });
  const stream = asRecord(asRecord(result.json).stream);
  return {
    status: typeof stream.status === 'string' ? stream.status : null,
    currentViewers: typeof stream.currentViewers === 'number' ? stream.currentViewers : null,
  };
}

async function waitForStreamStatus(token: string, streamId: string, wanted: string, timeoutMs: number): Promise<string | null> {
  const deadline = Date.now() + timeoutMs;
  let last: string | null = null;
  while (Date.now() < deadline) {
    last = (await readStreamStatus(token, streamId)).status;
    if (last === wanted) return last;
    await sleep(1_000);
  }
  return last;
}

async function endBroadcast(session: StreamSession): Promise<CleanupResult> {
  const { streamId } = session.result;
  if (!streamId) return { ok: true, details: 'No stream was created.' };

  try {
    const before = (await readStreamStatus(session.token, streamId)).status;
    if (before === 'ended') return { ok: true, details: 'The stream was already "ended".' };
    if (before === 'ready' || before === 'scheduled') {
      // Never went on air: the API refuses to "end" it, so delete the unused stream instead.
      const path = `/api/live/streams/${encodeURIComponent(streamId)}`;
      let removed = await api('DELETE', path, { token: session.token });
      if (removed.status === 401) {
        session.token = await login(session.user, true);
        removed = await api('DELETE', path, { token: session.token });
      }
      return removed.ok
        ? { ok: true, details: `The stream never went live (status "${before}"); deleted it.` }
        : { ok: false, details: `Stream ${streamId} never went live (status "${before}") and could not be deleted: HTTP ${removed.status} ${snippet(removed)}` };
    }

    let end = await api('POST', `/api/live/streams/${encodeURIComponent(streamId)}/end`, { token: session.token, idempotent: true });
    if (end.status === 401) {
      session.token = await login(session.user, true);
      end = await api('POST', `/api/live/streams/${encodeURIComponent(streamId)}/end`, { token: session.token, idempotent: true });
    }

    const finalStatus = await waitForStreamStatus(session.token, streamId, 'ended', 15_000);
    if (finalStatus === 'ended') {
      return { ok: true, details: end.ok ? 'Ended through the API and verified as "ended".' : `End call returned HTTP ${end.status}, but the stream is "ended".` };
    }
    return {
      ok: false,
      details: `Stream ${streamId} is still "${finalStatus ?? 'unknown'}" after the end call (HTTP ${end.status}: ${snippet(end)}).`,
    };
  } catch (error) {
    return { ok: false, details: errorMessage(error) };
  }
}

function unsupportedPublishError(publish: unknown): Error {
  const text = JSON.stringify(publish ?? {});
  const hint = /rtmp/i.test(text)
    ? 'It looks like an RTMP ingest. Re-run with the browser source (default) or extend this script to push RTMP with ffmpeg.'
    : /whip/i.test(text)
      ? 'It looks like a WHIP endpoint, which @livekit/rtc-node cannot publish to.'
      : 'No ws(s):// LiveKit URL plus JWT token was found in it.';
  return new Error(`go-live "publish" is not a LiveKit room join. ${hint} Shape: ${printable(publish)}`);
}

async function startBroadcast(index: number, user: SeedUser): Promise<StreamSession> {
  const result: StreamResult = { index, username: user.username, streamId: '', status: 'failed' };
  const session: StreamSession = { index, user, token: '', result, publisher: null };

  try {
    session.token = await login(user);

    const title = `[e2e] Tubor API test ${index} ${new Date().toISOString()}`;
    const goLive = await api('POST', '/api/live/streams/go-live', {
      token: session.token,
      body: { source: 'browser', title, category: 'gaming', visibility: 'public', ...(config.takeover ? { takeover: true } : {}) },
    });
    if (!goLive.ok) {
      const conflict = goLive.status === 409 ? ' The account probably already has an active live; end it or pass --takeover.' : '';
      throw new Error(`go-live returned HTTP ${goLive.status}: ${snippet(goLive)}.${conflict}`);
    }

    const body = asRecord(goLive.json);
    const stream = asRecord(body.stream);
    if (typeof stream.id !== 'string') throw new Error(`go-live response has no stream.id. Shape: ${printable(goLive.json)}`);
    result.streamId = stream.id;
    result.resumed = body.resumed === true;
    if (result.resumed) console.warn(`[stream ${index}] go-live resumed an existing live for ${user.username}.`);

    const creds = findLiveKitCredentials(body.publish);
    if (!creds) throw unsupportedPublishError(body.publish);

    session.publisher = new SyntheticPublisher();
    await session.publisher.start(creds.url, creds.token);

    const publishedAt = Date.now();
    const status = await waitForStreamStatus(session.token, stream.id, 'live', config.liveTimeoutMs);
    if (status !== 'live') {
      throw new Error(`Video was published, but the backend never moved the stream to "live" within ${config.liveTimeoutMs}ms (last status: ${status ?? 'unknown'}).`);
    }
    result.timeToLiveMs = Date.now() - publishedAt;
    result.status = 'started';
    console.log(`[stream ${index}] LIVE as ${user.username}: ${stream.id} (${result.timeToLiveMs}ms from first frame to "live")`);
    return session;
  } catch (error) {
    result.error = errorMessage(error);
    console.error(`[stream ${index}] FAILED for ${user.username}: ${result.error.slice(0, 600)}`);
    await session.publisher?.stop().catch(() => undefined);
    session.publisher = null;
    if (result.streamId) {
      result.cleanup = await endBroadcast(session);
      if (!result.cleanup.ok) result.error += ` | Startup cleanup failed: ${result.cleanup.details}`;
    }
    return session;
  }
}

// ---------------------------------------------------------------------------------------------
// Probe mode: learn the response shapes without running a load test
// ---------------------------------------------------------------------------------------------

async function runProbe(streamer: SeedUser): Promise<void> {
  console.log(`[probe] logging in ${streamer.username}`);
  const token = await login(streamer);
  const goLive = await api('POST', '/api/live/streams/go-live', {
    token,
    body: { source: 'browser', title: `[e2e] probe ${new Date().toISOString()}`, category: 'gaming', visibility: 'public', ...(config.takeover ? { takeover: true } : {}) },
  });
  console.log(`[probe] go-live HTTP ${goLive.status}\n${printable(goLive.json)}`);

  const stream = asRecord(asRecord(goLive.json).stream);
  if (typeof stream.id !== 'string') {
    console.log('[probe] No stream.id; stopping.');
    return;
  }

  const creds = findLiveKitCredentials(asRecord(goLive.json).publish);
  console.log(`[probe] publish contains a LiveKit ws(s) URL + JWT: ${creds ? 'yes' : 'NO'}`);

  // Publish real video so the stream can go live; playback-session is refused until then.
  let publisher: SyntheticPublisher | null = null;
  if (creds) {
    publisher = new SyntheticPublisher();
    try {
      await publisher.start(creds.url, creds.token);
      console.log('[probe] publishing synthetic video, waiting for the backend to mark the stream live...');
      const startedAt = Date.now();
      const status = await waitForStreamStatus(token, stream.id, 'live', config.liveTimeoutMs);
      console.log(`[probe] stream status: ${status ?? 'unknown'} (${Date.now() - startedAt}ms after publish)`);
    } catch (error) {
      console.error(`[probe] publishing failed: ${errorMessage(error)}`);
    }
  }

  const playback = await api('POST', `/api/live/streams/${encodeURIComponent(stream.id)}/playback-session`, {});
  console.log(`[probe] playback-session (anonymous) HTTP ${playback.status}\n${printable(playback.json)}`);
  console.log(
    `[probe] playback is: ${findLiveKitCredentials(playback.json) ? 'LiveKit url+token' : findHlsPlaylist(playback.json) ? 'HLS playlist' : 'UNRECOGNISED'}`,
  );

  const ended = await endBroadcast({ index: 0, user: streamer, token, result: { index: 0, username: streamer.username, streamId: stream.id, status: 'started' }, publisher: null });
  await publisher?.stop().catch(() => undefined);
  await dispose().catch(() => undefined);
  console.log(`[probe] cleanup: ${ended.ok ? 'ok' : 'FAILED'} - ${ended.details}`);
  if (!ended.ok) process.exitCode = 1;
}

// ---------------------------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------------------------

async function main(): Promise<void> {
  config = parseArgs(process.argv.slice(2));
  joinSlots = new Semaphore(config.joinConcurrency);
  mkdirSync(RESULTS_DIR, { recursive: true });
  loadTokenCache();

  const { seed, users, absolutePath } = loadUsers(config.usersFile);
  const viewerCount = config.probe ? 0 : config.streamers * config.viewersPerStream;
  const requiredAccounts = config.streamers + (config.viewerAuth ? viewerCount : 0);

  if (requiredAccounts > users.length) {
    throw new Error(
      `This run needs ${requiredAccounts} distinct accounts, but users.json has ${users.length}. ${config.viewerAuth ? 'Drop --viewer-auth to use anonymous viewers, or reduce --viewers-per-stream.' : 'Reduce --streamers.'}`,
    );
  }
  if (requiredAccounts > config.maxAccounts) {
    throw new Error(`This run needs ${requiredAccounts} accounts, above --max-accounts=${config.maxAccounts}.`);
  }
  if (viewerCount > 60 && !config.noDecode) {
    console.warn(`WARNING: ${viewerCount} LiveKit viewers decode video in this one process; CPU may become the bottleneck rather than the backend. Pass --no-decode to count received frames from WebRTC stats instead.`);
  }

  const health = await api('GET', '/health');
  console.log(`[preflight] GET /health -> ${health.status}`);
  if (!health.ok) throw new Error(`API preflight failed: HTTP ${health.status}. Confirm the Render API is awake at ${config.apiBase}.`);

  const streamerPreferred = users.filter((user) => (user.roles ?? []).some((role) => /streamer/i.test(role)));
  const preferredEmails = new Set(streamerPreferred.map((user) => user.email.toLowerCase()));
  const streamers = [
    ...shuffle(streamerPreferred),
    ...shuffle(users.filter((user) => !preferredEmails.has(user.email.toLowerCase()))),
  ].slice(0, config.streamers);

  if (config.probe) {
    await runProbe(streamers[0]!);
    return;
  }

  const streamerEmails = new Set(streamers.map((user) => user.email.toLowerCase()));
  const viewerPool = config.viewerAuth
    ? shuffle(users.filter((user) => !streamerEmails.has(user.email.toLowerCase()))).slice(0, viewerCount)
    : [];

  const report: Record<string, unknown> = {
    test: 'Tubor EX backend-only live-stream test',
    startedAt: new Date().toISOString(),
    target: config.apiBase,
    usersFile: absolutePath,
    seedGeneratedAt: seed.generatedAt ?? null,
    configuration: {
      streamers: config.streamers,
      viewersPerStream: config.viewersPerStream,
      requestedViewers: viewerCount,
      viewerAuth: config.viewerAuth,
      durationSeconds: config.durationSeconds,
      rampMs: config.rampMs,
      keepStreams: config.keepStreams,
      publish: `${config.publishWidth}x${config.publishHeight}@${config.publishFps}fps synthetic video`,
    },
  };

  const sessions: StreamSession[] = [];
  const viewerResults: ViewerResult[] = [];
  const liveViewers: Array<{ result: ViewerResult; handle: ViewerHandle }> = [];
  let fatalError: string | undefined;

  process.on('SIGINT', () => {
    if (stopRequested) process.exit(130);
    stopRequested = true;
    console.warn('\n[test] Interrupted; cleaning up (press Ctrl+C again to force quit and leave lives running).');
    wakeHold?.();
  });

  try {
    for (let i = 0; i < streamers.length; i += 1) {
      const session = await startBroadcast(i + 1, streamers[i]!);
      sessions.push(session);
      if (session.result.status !== 'started') {
        throw new Error(`Broadcaster ${session.user.username} failed to start: ${session.result.error ?? 'unknown error'}`);
      }
      if (stopRequested) break;
    }

    if (!stopRequested) {
      console.log(`[viewers] ramping ${viewerCount} ${config.viewerAuth ? 'authenticated' : 'anonymous'} viewers at ${config.rampMs}ms each`);
      const tasks: Array<ReturnType<typeof runViewer>> = [];
      let viewerIndex = 0;
      for (const session of sessions) {
        for (let slot = 0; slot < config.viewersPerStream; slot += 1) {
          const user = config.viewerAuth ? viewerPool[viewerIndex]! : null;
          const label = user ? user.username : `anon-${viewerIndex + 1}`;
          tasks.push(runViewer(user, label, session.index, session.result.streamId, viewerIndex * config.rampMs));
          viewerIndex += 1;
        }
      }

      for (const outcome of await Promise.all(tasks)) {
        viewerResults.push(outcome.result);
        if (outcome.handle) liveViewers.push({ result: outcome.result, handle: outcome.handle });
      }

      for (const session of sessions) {
        const info = await readStreamStatus(session.token, session.result.streamId).catch(() => ({ status: null, currentViewers: null }));
        session.result.currentViewersReportedByApi = info.currentViewers;
      }

      console.log(`[hold] ${liveViewers.length}/${viewerCount} viewers connected; holding for ${config.durationSeconds}s`);
      const holdEnd = Date.now() + config.durationSeconds * 1_000;
      while (Date.now() < holdEnd && !stopRequested) {
        await new Promise<void>((done) => {
          wakeHold = done;
          setTimeout(done, Math.min(10_000, Math.max(0, holdEnd - Date.now())));
        });
        const receiving = liveViewers.filter(({ handle }) => handle.alive() && Date.now() - handle.lastProgressAt() < handle.staleAfterMs).length;
        console.log(`[hold] ${receiving}/${liveViewers.length} viewers still receiving media, ${Math.max(0, Math.round((holdEnd - Date.now()) / 1_000))}s left`);
      }

      for (const { result, handle } of liveViewers) {
        result.unitsFinal = handle.units();
        result.stillReceivingAtEnd = handle.alive() && Date.now() - handle.lastProgressAt() < handle.staleAfterMs;
        result.detail = handle.detail();
        if (!result.stillReceivingAtEnd) {
          result.status = 'failed';
          result.error = `Stopped receiving media during the hold period. ${JSON.stringify(result.detail)}`;
        }
      }
    }
  } catch (error) {
    fatalError = errorMessage(error);
    console.error(`[test] FATAL: ${fatalError}`);
  } finally {
    for (const { handle } of liveViewers) await handle.stop().catch(() => undefined);

    for (const session of [...sessions].reverse()) {
      session.result.publisherFramesSent = session.publisher?.framesSent;
      session.result.publisherStayedConnected = session.publisher?.stayedConnected;
      if (!config.keepStreams && session.result.streamId && !session.result.cleanup) {
        session.result.cleanup = await endBroadcast(session);
        if (!session.result.cleanup.ok) console.error(`[cleanup] FAILED for ${session.user.username}: ${session.result.cleanup.details}`);
      }
      await session.publisher?.stop().catch(() => undefined);
    }
    await dispose().catch(() => undefined);

    const streams = sessions.map((session) => session.result);
    const passed = viewerResults.filter((viewer) => viewer.status === 'passed');
    const cleanupFailures = streams.filter((stream) => stream.cleanup && !stream.cleanup.ok).length;
    const droppedPublishers = streams.filter((stream) => stream.publisherStayedConnected === false).length;
    const firstMedia = viewerResults.flatMap((viewer) => (viewer.firstMediaMs !== undefined ? [viewer.firstMediaMs] : []));
    const timeToLive = streams.flatMap((stream) => (stream.timeToLiveMs !== undefined ? [stream.timeToLiveMs] : []));

    report.finishedAt = new Date().toISOString();
    report.fatalError = fatalError ?? null;
    report.http = { ...stats };
    report.streams = streams;
    report.viewers = viewerResults;
    report.summary = {
      broadcastersRequested: config.streamers,
      broadcastersStarted: streams.filter((stream) => stream.status === 'started').length,
      droppedPublishers,
      viewersRequested: viewerCount,
      viewersPassed: passed.length,
      viewersFailed: viewerResults.length - passed.length,
      mediaPassRate: viewerResults.length ? Number((passed.length / viewerResults.length).toFixed(4)) : 0,
      timeToLiveMs: { p50: percentile(timeToLive, 50), max: percentile(timeToLive, 100) },
      viewerFirstMediaMs: { p50: percentile(firstMedia, 50), p95: percentile(firstMedia, 95), max: percentile(firstMedia, 100) },
      http429Responses: stats.http429,
      cleanupFailures,
      result:
        fatalError ||
        stopRequested ||
        streams.some((stream) => stream.status !== 'started') ||
        viewerResults.some((viewer) => viewer.status !== 'passed') ||
        droppedPublishers > 0 ||
        cleanupFailures > 0
          ? 'FAILED'
          : 'PASSED',
    };

    const reportPath = resolve(RESULTS_DIR, 'live-api-e2e-report.json');
    writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
    console.log(`\n[report] ${reportPath}`);
    console.log(JSON.stringify(report.summary, null, 2));
  }

  const summary = report.summary as { result: string } | undefined;
  if (fatalError || summary?.result !== 'PASSED') process.exitCode = 1;
}

main()
  .catch((error: unknown) => {
    mkdirSync(RESULTS_DIR, { recursive: true });
    const message = errorMessage(error);
    writeFileSync(
      resolve(RESULTS_DIR, 'live-api-e2e-report.json'),
      `${JSON.stringify({ test: 'Tubor EX backend-only live-stream test', fatalError: message, finishedAt: new Date().toISOString(), result: 'FAILED' }, null, 2)}\n`,
      'utf8',
    );
    console.error(`[test] FATAL: ${message}`);
    process.exitCode = 1;
  })
  .finally(() => {
    // The native LiveKit runtime can keep the event loop alive after rooms close.
    setTimeout(() => process.exit(process.exitCode ?? 0), 250).unref();
  });