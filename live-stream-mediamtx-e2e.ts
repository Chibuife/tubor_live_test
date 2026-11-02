/**
 * Tubor EX backend-only live-stream test for the MediaMTX backend (no browser, no UI).
 *
 * The pattern follows tubor_v1/load-tests/media/load-test.mjs: real encoders, real viewers, nothing
 * that counts an HTTP 200 as a viewer.
 *
 *   1. POST /api/auth/login                          (streamers; viewers only with --viewer-auth)
 *   2. POST /api/live/streams/go-live                (creates the LIVE and returns `publish`)
 *   3. ffmpeg publishes a test pattern (H.264 + AAC) over RTMP, SRT or WHIP to that target
 *   4. GET  /api/live/streams/:id                    (waits for MediaMTX's event to flip it to "live")
 *   5. POST /api/live/streams/:id/playback-session   (one per viewer; may be anonymous)
 *   6. Each viewer follows the session's LL-HLS playlist the way hls.js does (blocking playlist
 *      reloads, every video and audio part downloaded) and must keep receiving parts for the whole
 *      hold period. Nothing is decoded, so the viewers cost almost no CPU.
 *   7. POST /api/live/streams/:id/end                (cleanup, then verifies status "ended")
 *
 * Not covered: WebRTC (WHEP) playback, which needs a browser. Use tubor_v1/load-tests/media/browser-e2e.mjs.
 *
 * Inputs: users.json (the same seed file as the other tests).
 *
 * Examples:
 *   npx tsx .\live-stream-mediamtx-e2e.ts --probe
 *   npx tsx .\live-stream-mediamtx-e2e.ts --streamers 1 --viewers-per-stream 3 --duration-seconds 15
 *   npx tsx .\live-stream-mediamtx-e2e.ts --streamers 5 --viewers-per-stream 19 --takeover
 *   npx tsx .\live-stream-mediamtx-e2e.ts --api-base http://localhost:4000 --publisher docker
 *
 * Flags:
 *   --probe               Log in one streamer, go live, print the REDACTED shape of the `publish` and
 *                         playback-session responses, end the stream, and exit. Use this first.
 *   --protocol <p>        rtmp (default), srt or whip. rtmp/srt use source "software", whip uses "browser".
 *                         whip needs ffmpeg 8+ (the docker image has it).
 *   --publisher <p>       local (ffmpeg on PATH, default) or docker (runs ffmpeg in --publisher-image;
 *                         localhost in the publish URL is rewritten to host.docker.internal).
 *   --ffmpeg <path>       ffmpeg binary for --publisher local. Default ffmpeg.
 *   --publisher-image <i> Default bluenviron/mediamtx:1.21.1-ffmpeg (the image tubor_v1 pins).
 *   --bitrate <kbit/s>    Video bitrate per stream. Default 1000.
 *   --min-parts <n>       LL-HLS video parts a viewer must receive before it counts as playing. Default 5.
 *   --viewer-auth         Log each viewer in. Default is anonymous viewers (the playback-session route
 *                         accepts anonymous callers on public streams), because /auth/login is rate limited.
 *   --tokens-file <path>  Cache access tokens (JWTs) between runs so repeated runs skip logins.
 *                         The file contains live bearer tokens; keep it out of git.
 *   --takeover            Send takeover:true to go-live if the account already has an active live.
 *   --keep-streams        Do not end the lives at the end.
 *   --api-base <url>      Defaults to https://tubor-api.onrender.com. tubor_v1's dev stack is http://localhost:4000.
 *
 * Besides pass/fail the report records the load generator's own CPU and event-loop delay. If
 * generator.saturated is true, the run measured this machine, not the backend.
 */

import { execFile, spawn } from 'node:child_process';
import type { ChildProcess } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { cpus, freemem } from 'node:os';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import { resolve } from 'node:path';

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
  liveTimeoutMs: 60_000,
  mediaTimeoutMs: 30_000,
  minParts: 5,
  publishWidth: 640,
  publishHeight: 360,
  publishFps: 15,
  bitrateKbps: 1_000,
  protocol: 'rtmp' as 'rtmp' | 'srt' | 'whip',
  publisher: 'local' as 'local' | 'docker',
  ffmpeg: 'ffmpeg',
  publisherImage: 'bluenviron/mediamtx:1.21.1-ffmpeg',
  maxRateLimitRetries: 10,
  maxAccounts: 100,
};

type Config = typeof DEFAULTS & {
  keepStreams: boolean;
  viewerAuth: boolean;
  takeover: boolean;
  probe: boolean;
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
  publisherStayedRunning?: boolean;
  publisherLastStderr?: string;
  currentViewersReportedByApi?: number | null;
  cleanup?: CleanupResult;
};

type ViewerResult = {
  viewer: string;
  streamIndex: number;
  streamId: string;
  status: 'passed' | 'failed';
  /** Time spent on the playback-session call, including any rate-limit waits. */
  playbackSessionMs?: number;
  /** Playlist request until `minParts` video parts arrived; excludes the playback-session call. */
  firstMediaMs?: number;
  /** Everything: ramp-in excluded, rate-limit waits, playback session, first media. */
  totalToFirstMediaMs?: number;
  partsAtJoin?: number;
  partsFinal?: number;
  stillReceivingAtEnd?: boolean;
  detail?: Record<string, unknown>;
  error?: string;
};

type ViewerHandle = {
  /** Video parts downloaded so far. */
  units: () => number;
  minUnits: number;
  progressWindowMs: number;
  staleAfterMs: number;
  lastProgressAt: () => number;
  alive: () => boolean;
  /** Arrival delay of each part after its wall-clock end (PROGRAM-DATE-TIME); includes clock skew with the media host. */
  partDelays: () => number[];
  stalls: () => number;
  bytes: () => number;
  detail: () => Record<string, unknown>;
  stop: () => Promise<void>;
};

let config!: Config;
let stopRequested = false;
let playbackShapeShown = false;
let wakeHold: (() => void) | undefined;

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

const stats = { requests: 0, http429: 0, transientRetries: 0, logins: 0, cachedLogins: 0 };

/** Every media byte (init segments and parts) any viewer downloaded, failed viewers included. */
let totalDownloadedBytes = 0;

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
  const choiceArg = <T extends string>(name: string, allowed: readonly T[], fallback: T): T => {
    const raw = stringArg(name, fallback);
    if (!(allowed as readonly string[]).includes(raw)) throw new Error(`--${name} must be one of: ${allowed.join(', ')}.`);
    return raw as T;
  };

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
    minParts: numberArg('min-parts', DEFAULTS.minParts),
    publishWidth: numberArg('publish-width', DEFAULTS.publishWidth),
    publishHeight: numberArg('publish-height', DEFAULTS.publishHeight),
    publishFps: numberArg('publish-fps', DEFAULTS.publishFps),
    bitrateKbps: numberArg('bitrate', DEFAULTS.bitrateKbps),
    protocol: choiceArg('protocol', ['rtmp', 'srt', 'whip'] as const, DEFAULTS.protocol),
    publisher: choiceArg('publisher', ['local', 'docker'] as const, DEFAULTS.publisher),
    ffmpeg: stringArg('ffmpeg', DEFAULTS.ffmpeg),
    publisherImage: stringArg('publisher-image', DEFAULTS.publisherImage),
    maxRateLimitRetries: numberArg('max-rate-limit-retries', DEFAULTS.maxRateLimitRetries),
    maxAccounts: numberArg('max-accounts', DEFAULTS.maxAccounts),
    keepStreams: Boolean(values['keep-streams']),
    viewerAuth: Boolean(values['viewer-auth']),
    takeover: Boolean(values.takeover),
    probe: Boolean(values.probe),
  };

  if (parsed.probe) parsed.streamers = 1;
  if (parsed.streamers < 1) throw new Error('--streamers must be at least 1.');
  if (parsed.durationSeconds < 1) throw new Error('--duration-seconds must be at least 1.');
  if (parsed.publishFps < 1 || parsed.publishFps > 60) throw new Error('--publish-fps must be between 1 and 60.');
  if (parsed.bitrateKbps < 100) throw new Error('--bitrate must be at least 100 (kbit/s).');
  if (parsed.minParts < 1) throw new Error('--min-parts must be at least 1.');
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
// Response inspection (the publish / playback-session shapes are checked, then printed redacted)
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

const SECRET_KEY = /token|secret|password|passwd|jwt|authorization|stream_?key|^key$|api_?key|streamid/i;

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
  const headers: Record<string, string> = { accept: 'application/json', 'user-agent': 'tubor-mediamtx-e2e/1.0' };
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
// Broadcaster: ffmpeg pushing a test pattern to the publish target from go-live
// ---------------------------------------------------------------------------------------------

type PublishTarget = { protocol: 'rtmp' | 'srt' | 'whip'; url: string; token?: string };

/**
 * Reads the go-live `publish` object. The shape is what tubor_v1's MediaMTX provider returns:
 * rtmp -> url, srt -> srt.url, whip -> url + token. Anything else (for example LiveKit credentials
 * from the previous backend) is an error that prints the redacted shape.
 */
function publishTargetFrom(publish: unknown): PublishTarget {
  const record = asRecord(publish);
  const fail = (why: string): Error =>
    new Error(`go-live "publish" is not usable for --protocol ${config.protocol}: ${why}. Is this API running the MediaMTX backend? Shape: ${printable(publish)}`);

  if (config.protocol === 'srt') {
    const url = asRecord(record.srt).url;
    if (typeof url !== 'string' || !url.startsWith('srt://')) throw fail('no srt.url');
    return { protocol: 'srt', url };
  }
  const url = record.url;
  if (typeof url !== 'string') throw fail('no url');
  if (config.protocol === 'whip') {
    if (typeof record.token !== 'string' || !/^https?:\/\//i.test(url)) throw fail('no WHIP url + token (was go-live called with source "browser"?)');
    return { protocol: 'whip', url, token: record.token };
  }
  if (!url.startsWith('rtmp://') && !url.startsWith('rtmps://')) throw fail('url is not rtmp(s)://');
  return { protocol: 'rtmp', url };
}

function ffmpegArgs(target: PublishTarget): string[] {
  const { publishWidth: width, publishHeight: height, publishFps: fps, bitrateKbps: kbps } = config;
  const input = [
    '-hide_banner', '-loglevel', 'error', '-re',
    '-f', 'lavfi', '-i', `testsrc2=size=${width}x${height}:rate=${fps}`,
    '-f', 'lavfi', '-i', 'sine=frequency=440:sample_rate=48000',
  ];
  // A keyframe every 2 s sets the LL-HLS segment length, as in the real encoders.
  const video = [
    '-c:v', 'libx264', '-preset', 'ultrafast', '-tune', 'zerolatency', '-pix_fmt', 'yuv420p',
    '-g', String(fps * 2), '-bf', '0', '-b:v', `${kbps}k`, '-maxrate', `${kbps}k`, '-bufsize', `${kbps}k`,
  ];
  switch (target.protocol) {
    case 'whip':
      return [...input, ...video, '-profile:v', 'baseline', '-c:a', 'libopus', '-ar', '48000', '-ac', '2', '-f', 'whip', '-authorization', target.token!, target.url];
    case 'srt':
      return [...input, ...video, '-c:a', 'aac', '-b:a', '128k', '-f', 'mpegts', target.url];
    default:
      return [...input, ...video, '-c:a', 'aac', '-b:a', '128k', '-f', 'flv', target.url];
  }
}

class FfmpegPublisher {
  private child: ChildProcess | undefined;
  private readonly containerName = `tubor-e2e-pub-${process.pid}-${randomUUID().slice(0, 8)}`;
  private stderrTail = '';
  private exited = false;

  get stayedRunning(): boolean {
    return !this.exited;
  }

  get lastStderr(): string {
    return this.stderrTail.trim();
  }

  async start(target: PublishTarget): Promise<void> {
    const args = ffmpegArgs(target);
    this.child =
      config.publisher === 'local'
        ? spawn(config.ffmpeg, args, { stdio: ['ignore', 'ignore', 'pipe'] })
        : spawn(
            'docker',
            ['run', '--rm', '--name', this.containerName, '--entrypoint', 'ffmpeg', config.publisherImage, ...args.map(toDockerHost)],
            { stdio: ['ignore', 'ignore', 'pipe'] },
          );
    this.child.stderr?.on('data', (chunk: Buffer) => {
      this.stderrTail = (this.stderrTail + chunk.toString()).slice(-600);
    });
    this.child.once('exit', () => {
      this.exited = true;
    });
    await new Promise<void>((done, fail) => {
      this.child!.once('spawn', () => done());
      this.child!.once('error', (error: Error) => fail(error));
    });
  }

  async stop(): Promise<void> {
    const child = this.child;
    if (!child || this.exited) return;
    if (config.publisher === 'docker') execFile('docker', ['rm', '-f', this.containerName], () => undefined);
    child.kill('SIGINT');
    await Promise.race([new Promise<void>((done) => child.once('exit', () => done())), sleep(5_000)]);
    if (!this.exited) child.kill();
  }
}

/** Inside a container, the host's localhost is host.docker.internal. */
const toDockerHost = (arg: string): string => arg.replace(/\/\/(localhost|127\.0\.0\.1)([:/?])/, '//host.docker.internal$2');

function run(command: string, args: string[]): Promise<{ ok: boolean; output: string }> {
  return new Promise((done) => {
    execFile(command, args, { timeout: 20_000 }, (error: Error | null, stdout: string, stderr: string) =>
      done({ ok: !error, output: `${stdout}${stderr}`.trim() }),
    );
  });
}

async function checkPublisher(): Promise<void> {
  if (config.publisher === 'local') {
    const result = await run(config.ffmpeg, ['-version']);
    if (!result.ok) {
      throw new Error(
        `Could not run "${config.ffmpeg} -version". Install ffmpeg and put it on PATH (Windows: winget install Gyan.FFmpeg, then open a new terminal), pass --ffmpeg <path>, or use --publisher docker.`,
      );
    }
    console.log(`[preflight] ${result.output.split('\n')[0]}`);
    return;
  }
  const result = await run('docker', ['image', 'inspect', config.publisherImage, '--format', '{{.Id}}']);
  if (!result.ok) {
    throw new Error(`Docker image ${config.publisherImage} is not available locally. Run: docker pull ${config.publisherImage}`);
  }
  console.log(`[preflight] docker image ${config.publisherImage} is present`);
}

// ---------------------------------------------------------------------------------------------
// Viewers: an LL-HLS client that downloads every part, without a browser or a decoder
// ---------------------------------------------------------------------------------------------

type MediaPart = { msn: number; part: number; uri: string; duration: number; independent: boolean; startsAt: number | null };
type MediaPlaylist = { parts: MediaPart[]; partTarget: number; map: string | null; nextMsn: number; nextPart: number };

function hlsAttributes(line: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const match of line.matchAll(/([A-Z0-9-]+)=("[^"]*"|[^,]*)/g)) out[match[1]!] = match[2]!.replace(/^"|"$/g, '');
  return out;
}

/** The parts of an LL-HLS media playlist, each with its wall-clock start when the playlist has PROGRAM-DATE-TIME. */
function parseMediaPlaylist(text: string): MediaPlaylist {
  const lines = text.split('\n').map((line) => line.trim());
  let msn = Number(lines.find((line) => line.startsWith('#EXT-X-MEDIA-SEQUENCE:'))?.split(':')[1] ?? 0);
  const partTarget = Number(text.match(/PART-TARGET=([\d.]+)/)?.[1] ?? 0.2);
  const map = text.match(/#EXT-X-MAP:URI="([^"]+)"/)?.[1] ?? null;
  const parts: MediaPart[] = [];
  let partIndex = 0;
  let clock: number | null = null;
  for (const line of lines) {
    if (line.startsWith('#EXT-X-PROGRAM-DATE-TIME:')) {
      clock = Date.parse(line.slice('#EXT-X-PROGRAM-DATE-TIME:'.length));
    } else if (line.startsWith('#EXT-X-PART:')) {
      const attrs = hlsAttributes(line);
      const duration = Number(attrs.DURATION);
      parts.push({ msn, part: partIndex, uri: attrs.URI ?? '', duration, independent: attrs.INDEPENDENT === 'YES', startsAt: clock });
      if (clock !== null) clock += duration * 1000;
      partIndex += 1;
    } else if (line.startsWith('#EXTINF:')) {
      // An older segment listed without its parts still moves the clock.
      if (partIndex === 0 && clock !== null) clock += Number(line.slice(8).split(',')[0]) * 1000;
    } else if (line && !line.startsWith('#')) {
      msn += 1;
      partIndex = 0;
    }
  }
  return { parts, partTarget, map, nextMsn: msn, nextPart: partIndex };
}

function withQuery(url: URL, params: Record<string, string | number>): URL {
  const next = new URL(url);
  for (const [key, value] of Object.entries(params)) next.searchParams.set(key, String(value));
  return next;
}

/**
 * Follows the stream's LL-HLS playlists the way hls.js does (join at the newest independent part,
 * blocking reloads with _HLS_msn/_HLS_part) and downloads every video and audio part, so the media
 * server and network carry a real viewer's load. Throws if the master playlist is refused; after
 * that it runs in the background until stop().
 */
async function startHlsViewer(masterUrl: string): Promise<ViewerHandle> {
  const abort = new AbortController();
  const state = { parts: 0, bytes: 0, lastProgressAt: 0, ttffMs: null as number | null, stalls: 0, delays: [] as number[], errors: [] as string[], running: 0 };
  const startedAt = Date.now();
  const signal = (): AbortSignal => AbortSignal.any([abort.signal, AbortSignal.timeout(config.requestTimeoutMs)]);
  const noteError = (message: string): void => {
    if (state.errors.length < 20) state.errors.push(message);
  };

  const master = await fetch(masterUrl, { signal: signal() });
  if (!master.ok) throw new Error(`master playlist HTTP ${master.status}`);
  const masterText = await master.text();
  const variant = masterText.split('\n').map((line) => line.trim()).find((line) => line && !line.startsWith('#'));
  const audio = masterText.match(/TYPE=AUDIO[^\n]*URI="([^"]+)"/)?.[1];
  if (!variant) throw new Error('no variant in the master playlist');
  const renditions = [new URL(variant, master.url), ...(audio ? [new URL(audio, master.url)] : [])];

  const follow = async (playlistUrl: URL, isVideo: boolean): Promise<void> => {
    state.running += 1;
    try {
      const first = await fetch(playlistUrl, { signal: signal() });
      if (!first.ok) throw new Error(`playlist HTTP ${first.status}`);
      let playlist = parseMediaPlaylist(await first.text());
      if (playlist.map) {
        const init = await fetch(new URL(playlist.map, playlistUrl), { signal: signal() });
        const initBytes = (await init.arrayBuffer()).byteLength;
        state.bytes += initBytes;
        totalDownloadedBytes += initBytes;
      }
      const joinAt = [...playlist.parts].reverse().find((part) => part.independent) ?? playlist.parts.at(-1);
      let downloaded = new Set<string>();
      let pending = joinAt ? playlist.parts.slice(playlist.parts.indexOf(joinAt)) : [];
      let lastNewPartAt = Date.now();
      let consecutiveErrors = 0;

      while (!abort.signal.aborted && consecutiveErrors < 20) {
        try {
          for (const part of pending) {
            const key = `${part.msn}:${part.part}`;
            if (downloaded.has(key)) continue;
            downloaded.add(key);
            const response = await fetch(new URL(part.uri, playlistUrl), { signal: signal() });
            if (!response.ok) {
              noteError(`part HTTP ${response.status}`);
              continue;
            }
            const partBytes = (await response.arrayBuffer()).byteLength;
            state.bytes += partBytes;
            totalDownloadedBytes += partBytes;
            if (isVideo) {
              state.parts += 1;
              state.lastProgressAt = Date.now();
              if (state.ttffMs === null) state.ttffMs = Date.now() - startedAt;
              if (part.startsAt !== null && state.delays.length < 500) state.delays.push(Date.now() - (part.startsAt + part.duration * 1000));
              if (Date.now() - lastNewPartAt > Math.max(1_500, playlist.partTarget * 3_000)) state.stalls += 1;
              lastNewPartAt = Date.now();
            }
          }
          if (downloaded.size > 200) downloaded = new Set([...downloaded].slice(-100));
          // Blocking reload: the server answers when the next part exists.
          const reload = await fetch(withQuery(playlistUrl, { _HLS_msn: playlist.nextMsn, _HLS_part: playlist.nextPart }), { signal: signal() });
          if (!reload.ok) throw new Error(`playlist HTTP ${reload.status}`);
          playlist = parseMediaPlaylist(await reload.text());
          pending = playlist.parts.filter((part) => !downloaded.has(`${part.msn}:${part.part}`)).slice(-6);
          consecutiveErrors = 0;
        } catch (error) {
          if (abort.signal.aborted) break;
          consecutiveErrors += 1;
          noteError(errorMessage(error));
          await sleep(500);
        }
      }
    } catch (error) {
      if (!abort.signal.aborted) noteError(errorMessage(error));
    } finally {
      state.running -= 1;
    }
  };

  const loops = Promise.all(renditions.map((url, index) => follow(url, index === 0)));

  return {
    units: () => state.parts,
    minUnits: config.minParts,
    progressWindowMs: 2_000,
    staleAfterMs: 5_000,
    lastProgressAt: () => state.lastProgressAt,
    alive: () => !abort.signal.aborted && state.running > 0,
    partDelays: () => state.delays,
    stalls: () => state.stalls,
    bytes: () => state.bytes,
    detail: () => ({ videoParts: state.parts, bytes: state.bytes, ttffMs: state.ttffMs, stalls: state.stalls, errors: state.errors.slice(0, 5) }),
    stop: async () => {
      abort.abort();
      await loops.catch(() => undefined);
    },
  };
}

async function verifyViewerMedia(handle: ViewerHandle, startedAt: number): Promise<number> {
  // startedAt is when this viewer began requesting the playlist (see runViewer).
  while (Date.now() - startedAt < config.mediaTimeoutMs) {
    if (handle.units() >= handle.minUnits) break;
    await sleep(250);
  }
  if (handle.units() < handle.minUnits) {
    throw new Error(`Only ${handle.units()}/${handle.minUnits} LL-HLS video parts within ${config.mediaTimeoutMs}ms. ${JSON.stringify(handle.detail())}`);
  }
  const firstMediaMs = Date.now() - startedAt;

  const before = handle.units();
  await sleep(handle.progressWindowMs);
  if (handle.units() <= before) {
    throw new Error(`Media arrived but then stalled (no new video parts in ${handle.progressWindowMs}ms). ${JSON.stringify(handle.detail())}`);
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

    const hlsUrl = asRecord(asRecord(session.json).hls).url;
    if (typeof hlsUrl !== 'string') {
      throw new Error(`playback-session response has no hls.url, so it is not the MediaMTX shape. Shape: ${printable(session.json)}`);
    }

    // The media timeout counts from the moment this viewer actually starts requesting the playlist,
    // not from time spent waiting out rate limits.
    const joinStartedAt = Date.now();
    handle = await startHlsViewer(hlsUrl);
    result.firstMediaMs = await verifyViewerMedia(handle, joinStartedAt);
    result.totalToFirstMediaMs = joinStartedAt - startedAt + result.firstMediaMs;
    result.partsAtJoin = handle.units();
    result.status = 'passed';
    console.log(`[viewer] PASS ${label} -> stream ${streamIndex} (ll-hls, media after ${result.firstMediaMs}ms; ${result.totalToFirstMediaMs}ms including API waits)`);
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
  publisher: FfmpegPublisher | null;
};

async function readStreamStatus(token: string | undefined, streamId: string): Promise<{ status: string | null; currentViewers: number | null }> {
  const result = await api('GET', `/api/live/streams/${encodeURIComponent(streamId)}`, { token });
  const stream = asRecord(asRecord(result.json).stream);
  return {
    status: typeof stream.status === 'string' ? stream.status : null,
    currentViewers: typeof stream.currentViewers === 'number' ? stream.currentViewers : null,
  };
}

/** Polls until the stream has `wanted` status. `giveUpIf` returns a message to stop waiting early. */
async function waitForStreamStatus(
  token: string,
  streamId: string,
  wanted: string,
  timeoutMs: number,
  giveUpIf?: () => string | null,
): Promise<string | null> {
  const deadline = Date.now() + timeoutMs;
  let last: string | null = null;
  while (Date.now() < deadline) {
    last = (await readStreamStatus(token, streamId)).status;
    if (last === wanted) return last;
    const reason = giveUpIf?.();
    if (reason) throw new Error(reason);
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

const goLiveBody = (title: string): Record<string, unknown> => ({
  source: config.protocol === 'whip' ? 'browser' : 'software',
  title,
  category: 'gaming',
  visibility: 'public',
  ...(config.takeover ? { takeover: true } : {}),
});

async function startBroadcast(index: number, user: SeedUser): Promise<StreamSession> {
  const result: StreamResult = { index, username: user.username, streamId: '', status: 'failed' };
  const session: StreamSession = { index, user, token: '', result, publisher: null };

  try {
    session.token = await login(user);

    const title = `[e2e] Tubor MediaMTX test ${index} ${new Date().toISOString()}`;
    const goLive = await api('POST', '/api/live/streams/go-live', { token: session.token, body: goLiveBody(title) });
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

    const target = publishTargetFrom(body.publish);
    const publisher = new FfmpegPublisher();
    session.publisher = publisher;
    await publisher.start(target);

    const publishedAt = Date.now();
    const status = await waitForStreamStatus(session.token, stream.id, 'live', config.liveTimeoutMs, () =>
      publisher.stayedRunning ? null : `ffmpeg exited before the stream went live: ${publisher.lastStderr || '(no output)'}`,
    );
    if (status !== 'live') {
      throw new Error(`Video was published, but the backend never moved the stream to "live" within ${config.liveTimeoutMs}ms (last status: ${status ?? 'unknown'}). ffmpeg said: ${publisher.lastStderr || '(nothing)'}`);
    }
    result.timeToLiveMs = Date.now() - publishedAt;
    result.status = 'started';
    console.log(`[stream ${index}] LIVE as ${user.username}: ${stream.id} (${result.timeToLiveMs}ms from ffmpeg start to "live", ${target.protocol})`);
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
  const goLive = await api('POST', '/api/live/streams/go-live', { token, body: goLiveBody(`[e2e] probe ${new Date().toISOString()}`) });
  console.log(`[probe] go-live HTTP ${goLive.status}\n${printable(goLive.json)}`);

  const stream = asRecord(asRecord(goLive.json).stream);
  if (typeof stream.id !== 'string') {
    console.log('[probe] No stream.id; stopping.');
    return;
  }
  const session: StreamSession = { index: 0, user: streamer, token, result: { index: 0, username: streamer.username, streamId: stream.id, status: 'started' }, publisher: null };

  try {
    const target = publishTargetFrom(asRecord(goLive.json).publish);
    console.log(`[probe] publish target is ${target.protocol}; starting ffmpeg`);
    session.publisher = new FfmpegPublisher();
    await session.publisher.start(target);
    const publisher = session.publisher;
    const startedAt = Date.now();
    const status = await waitForStreamStatus(token, stream.id, 'live', config.liveTimeoutMs, () =>
      publisher.stayedRunning ? null : `ffmpeg exited: ${publisher.lastStderr || '(no output)'}`,
    );
    console.log(`[probe] stream status: ${status ?? 'unknown'} (${Date.now() - startedAt}ms after ffmpeg start)`);

    const playback = await api('POST', `/api/live/streams/${encodeURIComponent(stream.id)}/playback-session`, {});
    console.log(`[probe] playback-session (anonymous) HTTP ${playback.status}\n${printable(playback.json)}`);
    const hlsUrl = asRecord(asRecord(playback.json).hls).url;
    if (typeof hlsUrl === 'string') {
      const viewer = await startHlsViewer(hlsUrl);
      await sleep(5_000);
      console.log(`[probe] LL-HLS viewer after 5s: ${JSON.stringify(viewer.detail())}`);
      await viewer.stop();
    } else {
      console.log('[probe] playback-session has no hls.url');
    }
  } catch (error) {
    console.error(`[probe] ${errorMessage(error)}`);
    process.exitCode = 1;
  }

  const ended = await endBroadcast(session);
  await session.publisher?.stop().catch(() => undefined);
  console.log(`[probe] cleanup: ${ended.ok ? 'ok' : 'FAILED'} - ${ended.details}`);
  if (!ended.ok) process.exitCode = 1;
}

// ---------------------------------------------------------------------------------------------
// Load generator health: if this machine is the bottleneck, the run says nothing about the backend
// ---------------------------------------------------------------------------------------------

function cpuTimes(): { busy: number; total: number } {
  return cpus().reduce(
    (acc: { busy: number; total: number }, cpu: { times: { user: number; nice: number; sys: number; idle: number; irq: number } }) => {
      const t = cpu.times;
      return { busy: acc.busy + t.user + t.sys + t.irq, total: acc.total + t.user + t.nice + t.sys + t.idle + t.irq };
    },
    { busy: 0, total: 0 },
  );
}

type GeneratorHealth = {
  cpuPercentMax: number;
  cpuPercentAvg: number;
  saturated: boolean;
  freeMemMbMin: number;
  eventLoopDelayMs: { p99: number; max: number };
  /** Media download rate over 2 s windows (avg counts only windows with traffic). */
  downloadMbps: { avg: number; peak: number };
};

function startGeneratorSampler(): () => GeneratorHealth {
  const sampleMs = 2_000;
  const loopDelay = monitorEventLoopDelay({ resolution: 20 });
  loopDelay.enable();
  const samples: number[] = [];
  const mbps: number[] = [];
  let freeMemMbMin = Math.round(freemem() / 1e6);
  let previous = cpuTimes();
  let previousBytes = totalDownloadedBytes;
  const timer = setInterval(() => {
    const now = cpuTimes();
    const total = now.total - previous.total;
    if (total > 0) samples.push(Math.round(((now.busy - previous.busy) / total) * 100));
    previous = now;
    mbps.push(((totalDownloadedBytes - previousBytes) * 8) / 1e6 / (sampleMs / 1_000));
    previousBytes = totalDownloadedBytes;
    freeMemMbMin = Math.min(freeMemMbMin, Math.round(freemem() / 1e6));
  }, sampleMs);
  return () => {
    clearInterval(timer);
    loopDelay.disable();
    const max = Math.max(0, ...samples);
    const busy = mbps.filter((value) => value > 0.2);
    return {
      cpuPercentMax: max,
      cpuPercentAvg: samples.length ? Math.round(samples.reduce((a, b) => a + b, 0) / samples.length) : 0,
      // Over ~85 % overall CPU the generator (ffmpeg encoders + this process) is the bottleneck.
      saturated: max > 85,
      freeMemMbMin,
      eventLoopDelayMs: { p99: Number((loopDelay.percentile(99) / 1e6).toFixed(1)), max: Number((loopDelay.max / 1e6).toFixed(1)) },
      downloadMbps: {
        avg: busy.length ? Number((busy.reduce((a, b) => a + b, 0) / busy.length).toFixed(1)) : 0,
        peak: Number(Math.max(0, ...mbps).toFixed(1)),
      },
    };
  };
}

// ---------------------------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------------------------

async function main(): Promise<void> {
  config = parseArgs(process.argv.slice(2));
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
  if (config.streamers > 3) {
    console.warn(`NOTE: ${config.streamers} ffmpeg encoders run on this machine. If the report says generator.saturated, add machines or lower --streamers/--bitrate; the numbers then describe this machine.`);
  }

  // Every viewer downloads the whole stream, so the audience's total bandwidth lands on this machine's connection.
  const perViewerMbps = (config.bitrateKbps + 128) / 1_000;
  const neededMbps = viewerCount * perViewerMbps;
  if (viewerCount > 0) {
    console.log(
      `[preflight] ${viewerCount} LL-HLS viewers pull about ${neededMbps.toFixed(0)} Mbit/s at real time (${perViewerMbps.toFixed(2)} each: video ${config.bitrateKbps} + audio 128 kbit/s). This machine's connection must carry that, or the run measures the connection instead of the backend; check it with a speed test.`,
    );
  }

  await checkPublisher();

  const health = await api('GET', '/health');
  console.log(`[preflight] GET ${config.apiBase}/health -> ${health.status}`);
  if (!health.ok) throw new Error(`API preflight failed: HTTP ${health.status}. Confirm the API is up at ${config.apiBase}.`);

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
    test: 'Tubor EX backend-only live-stream test (MediaMTX)',
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
      publish: `${config.protocol} via ${config.publisher} ffmpeg, ${config.publishWidth}x${config.publishHeight}@${config.publishFps}fps, ${config.bitrateKbps} kbit/s`,
      viewers: `LL-HLS, no decode, >= ${config.minParts} video parts`,
    },
  };

  const sessions: StreamSession[] = [];
  const viewerResults: ViewerResult[] = [];
  const liveViewers: Array<{ result: ViewerResult; handle: ViewerHandle }> = [];
  let fatalError: string | undefined;
  const stopSampler = startGeneratorSampler();

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
      console.log(`[viewers] ramping ${viewerCount} ${config.viewerAuth ? 'authenticated' : 'anonymous'} LL-HLS viewers at ${config.rampMs}ms each`);
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
        const publishersUp = sessions.filter((session) => session.publisher?.stayedRunning).length;
        console.log(`[hold] ${receiving}/${liveViewers.length} viewers still receiving media, ${publishersUp}/${sessions.length} ffmpeg publishers running, ${Math.max(0, Math.round((holdEnd - Date.now()) / 1_000))}s left`);
      }

      for (const { result, handle } of liveViewers) {
        result.partsFinal = handle.units();
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
    const delays = liveViewers.flatMap(({ handle }) => handle.partDelays());
    const stalls = liveViewers.reduce((sum, { handle }) => sum + handle.stalls(), 0);
    const bytes = liveViewers.reduce((sum, { handle }) => sum + handle.bytes(), 0);
    for (const { handle } of liveViewers) await handle.stop().catch(() => undefined);

    for (const session of [...sessions].reverse()) {
      session.result.publisherStayedRunning = session.publisher?.stayedRunning;
      session.result.publisherLastStderr = session.publisher?.lastStderr || undefined;
      if (!config.keepStreams && session.result.streamId && !session.result.cleanup) {
        session.result.cleanup = await endBroadcast(session);
        if (!session.result.cleanup.ok) console.error(`[cleanup] FAILED for ${session.user.username}: ${session.result.cleanup.details}`);
      }
      await session.publisher?.stop().catch(() => undefined);
    }

    const generator = stopSampler();
    const streams = sessions.map((session) => session.result);
    const passed = viewerResults.filter((viewer) => viewer.status === 'passed');
    const cleanupFailures = streams.filter((stream) => stream.cleanup && !stream.cleanup.ok).length;
    const droppedPublishers = streams.filter((stream) => stream.publisherStayedRunning === false).length;
    const firstMedia = viewerResults.flatMap((viewer) => (viewer.firstMediaMs !== undefined ? [viewer.firstMediaMs] : []));
    const timeToLive = streams.flatMap((stream) => (stream.timeToLiveMs !== undefined ? [stream.timeToLiveMs] : []));
    const outcome =
      fatalError ||
      stopRequested ||
      streams.some((stream) => stream.status !== 'started') ||
      viewerResults.some((viewer) => viewer.status !== 'passed') ||
      droppedPublishers > 0 ||
      cleanupFailures > 0
        ? 'FAILED'
        : 'PASSED';

    const notes: string[] = [];
    if (outcome === 'FAILED' && generator.saturated) {
      notes.push('The load generator CPU was saturated (generator.saturated), so failures may be this machine, not the backend.');
    }
    if (outcome === 'FAILED' && neededMbps > 0 && generator.downloadMbps.peak < neededMbps * 0.8) {
      notes.push(
        `Download throughput peaked at ${generator.downloadMbps.peak} Mbit/s while ${viewerCount} viewers need about ${neededMbps.toFixed(0)} at real time, so media was not delivered at full rate. Compare ${generator.downloadMbps.peak} Mbit/s with a speed test of this machine: if it is close, this connection is the limit; if it is well below, the media host or its auth path is not delivering fast enough (re-running with a lower --bitrate and getting the same failures also points at the host).`,
      );
    }

    report.finishedAt = new Date().toISOString();
    report.fatalError = fatalError ?? null;
    report.http = { ...stats };
    report.generator = generator;
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
      hls: {
        partArrivalDelayMs: { p50: percentile(delays, 50), p95: percentile(delays, 95) },
        stallsTotal: stalls,
        megabytesDownloaded: Number((bytes / 1e6).toFixed(1)),
        totalMegabytesDownloadedIncludingFailedViewers: Number((totalDownloadedBytes / 1e6).toFixed(1)),
        neededMbpsAtRealTime: Number(neededMbps.toFixed(1)),
      },
      generator,
      http429Responses: stats.http429,
      cleanupFailures,
      result: outcome,
      note: notes.length ? notes.join(' ') : undefined,
    };

    const reportPath = resolve(RESULTS_DIR, 'live-mediamtx-e2e-report.json');
    writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
    console.log(`\n[report] ${reportPath}`);
    console.log(JSON.stringify(report.summary, null, 2));
  }

  const summary = report.summary as { result: string } | undefined;
  if (fatalError || summary?.result !== 'PASSED') process.exitCode = 1;
}

main().catch((error: unknown) => {
  mkdirSync(RESULTS_DIR, { recursive: true });
  const message = errorMessage(error);
  writeFileSync(
    resolve(RESULTS_DIR, 'live-mediamtx-e2e-report.json'),
    `${JSON.stringify({ test: 'Tubor EX backend-only live-stream test (MediaMTX)', fatalError: message, finishedAt: new Date().toISOString(), result: 'FAILED' }, null, 2)}\n`,
    'utf8',
  );
  console.error(`[test] FATAL: ${message}`);
  process.exitCode = 1;
});
