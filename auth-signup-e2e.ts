/**
 * Tubor EX sign-up / authentication ramp test (no browser, no UI).
 *
 * Runs sign-up waves back to back (default 50, then 70, then 100) over the public API gateway.
 * Every wave creates NEW accounts, so a full default run creates 220 of them:
 *   1. POST /api/auth/register   one per account: { email, password, name } -> 201 { message, userId }
 *   2. POST /api/auth/login      one per created account (skip with --skip-login)
 *
 * Both routes are rate limited (5 per minute per client at the time of writing). A 429 is not a
 * failure: the request waits for Retry-After and tries again, and the wave report counts the 429s
 * and splits latency into "final attempt" and "including rate-limit waits". Expect a wave of N
 * sign-ups from one IP to take roughly N/5 minutes.
 *
 * A login that is refused because the e-mail address is not verified yet is reported separately
 * (loginBlockedUnverified) and does not fail the wave: sign-up works, verification is out of scope.
 *
 * Examples:
 *   npx tsx .\auth-signup-e2e.ts --dry-run
 *   npx tsx .\auth-signup-e2e.ts --waves 1                       (one account: smoke test)
 *   npx tsx .\auth-signup-e2e.ts                                  (50, 70, 100)
 *   npx tsx .\auth-signup-e2e.ts --waves 50,70,100 --api-base http://localhost:3000
 *
 * Flags:
 *   --waves <a,b,c>        Accounts to create per wave. Default 50,70,100.
 *   --concurrency <n>      Sign-ups in flight at once. Default 5.
 *   --skip-login           Only test sign-up.
 *   --email-domain <d>     Domain for the generated e-mail addresses. Default e2e.tubor.test (reserved
 *                          TLD, never deliverable, and easy to clean up by pattern).
 *   --max-rate-limit-retries <n>
 *                          429s tolerated per request before it counts as failed. Default 60.
 *                          Use 0 to see how the rate limiter answers a burst instead of waiting it out.
 *   --dry-run              Print the plan and a sample request body (password redacted); send nothing.
 *   --api-base <url>       Defaults to https://tubor-api.onrender.com
 *
 * Output: live-e2e-results/auth-signup-report.json, and live-e2e-results/auth-signup-accounts-<run>.json
 * (e-mail, username and userId of every account created, no passwords, for cleanup).
 */

import { randomBytes } from 'node:crypto';
import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

const RESULTS_DIR = resolve(process.cwd(), 'live-e2e-results');

const DEFAULTS = {
  apiBase: 'https://tubor-api.onrender.com',
  waves: '50,70,100',
  concurrency: 5,
  emailDomain: 'e2e.tubor.test',
  requestTimeoutMs: 30_000,
  warmupTimeoutMs: 120_000,
  maxRateLimitRetries: 60,
};

type Config = typeof DEFAULTS & {
  wavePlan: number[];
  skipLogin: boolean;
  dryRun: boolean;
};

type AccountResult = {
  wave: number;
  index: number;
  email: string;
  username: string;
  signup: 'created' | 'failed' | 'skipped';
  signupHttp?: number;
  /** Final attempt only. */
  signupAttemptMs?: number;
  /** Everything, including rate-limit waits and retries. */
  signupTotalMs?: number;
  signupHttp429?: number;
  userId?: string;
  signupError?: string;
  login?: 'passed' | 'blocked-unverified' | 'failed';
  loginHttp?: number;
  loginAttemptMs?: number;
  loginTotalMs?: number;
  loginHttp429?: number;
  loginError?: string;
};

type Entry = { result: AccountResult; password: string };

type Spread = { p50: number | null; p95: number | null; max: number | null };

type WaveSummary = {
  wave: number;
  requested: number;
  created: number;
  signupFailed: number;
  signupSkipped: number;
  signupFailuresByStatus: Record<string, number>;
  loginPassed: number | null;
  loginBlockedUnverified: number | null;
  loginFailed: number | null;
  http429: number;
  signupAttemptMs: Spread;
  signupTotalMs: Spread;
  loginAttemptMs: Spread;
  signupPhaseSeconds: number;
  loginPhaseSeconds: number | null;
  signupsPerMinute: number;
  result: 'PASSED' | 'FAILED';
};

let config!: Config;
let stopRequested = false;
const stats = { requests: 0, http429: 0, transientRetries: 0 };

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

function percentile(values: number[], p: number): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1))]!;
}

const spread = (values: number[]): Spread => ({
  p50: percentile(values, 50),
  p95: percentile(values, 95),
  max: percentile(values, 100),
});

const numbers = (values: Array<number | undefined>): number[] => values.filter((value): value is number => value !== undefined);

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

  const waves = stringArg('waves', DEFAULTS.waves);
  const wavePlan = waves.split(',').map((part) => Number(part.trim()));
  if (wavePlan.length === 0 || wavePlan.some((size) => !Number.isInteger(size) || size < 1)) {
    throw new Error('--waves must be a comma-separated list of positive integers, e.g. 50,70,100.');
  }

  const parsed: Config = {
    ...DEFAULTS,
    apiBase: stringArg('api-base', DEFAULTS.apiBase).replace(/\/+$/, ''),
    waves,
    wavePlan,
    concurrency: numberArg('concurrency', DEFAULTS.concurrency),
    emailDomain: stringArg('email-domain', DEFAULTS.emailDomain),
    requestTimeoutMs: numberArg('request-timeout-ms', DEFAULTS.requestTimeoutMs),
    warmupTimeoutMs: numberArg('warmup-timeout-ms', DEFAULTS.warmupTimeoutMs),
    maxRateLimitRetries: numberArg('max-rate-limit-retries', DEFAULTS.maxRateLimitRetries),
    skipLogin: Boolean(values['skip-login']),
    dryRun: Boolean(values['dry-run']),
  };

  if (parsed.concurrency < 1) throw new Error('--concurrency must be at least 1.');
  if (!/^https?:\/\//i.test(parsed.apiBase)) throw new Error('--api-base must start with http:// or https://.');
  if (!/^[a-z0-9.-]+\.[a-z]+$/i.test(parsed.emailDomain)) throw new Error('--email-domain must look like example.test.');
  return parsed;
}

// ---------------------------------------------------------------------------------------------
// HTTP client: retries cold starts (502/503/504) and honours 429 + Retry-After
// ---------------------------------------------------------------------------------------------

/**
 * Paces requests that hit a rate limit. After a 429 every waiting caller re-enters through the
 * same gate, one slot apart, so a whole wave of sign-ups does not retry in the same instant.
 * `hold` is the quiet version: the server said the budget is spent, so nobody enters until it resets.
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

  hold(waitMs: number): void {
    this.nextAt = Math.max(this.nextAt, Date.now() + waitMs);
  }
}

const registerGate = new Gate();
const loginGate = new Gate();

type ApiResult = {
  status: number;
  ok: boolean;
  json: unknown;
  text: string;
  /** The final attempt only. */
  attemptMs: number;
  /** From the first attempt to the answer, rate-limit waits included. */
  totalMs: number;
  /** 429 responses this call received, including a final one. */
  http429: number;
};

function retryAfterMs(response: Response): number {
  const header = response.headers.get('retry-after') ?? response.headers.get('ratelimit-reset') ?? response.headers.get('x-ratelimit-reset');
  if (header) {
    const seconds = Number(header);
    if (Number.isFinite(seconds)) return Math.min(90_000, Math.max(1_000, seconds * 1_000));
    const date = Date.parse(header);
    if (Number.isFinite(date)) return Math.min(90_000, Math.max(1_000, date - Date.now()));
  }
  return 15_000;
}

async function api(
  method: 'GET' | 'POST',
  path: string,
  options: { body?: unknown; gate?: Gate; retryNetworkErrors?: boolean } = {},
): Promise<ApiResult> {
  const url = `${config.apiBase}${path}`;
  const headers: Record<string, string> = { accept: 'application/json', 'user-agent': 'tubor-auth-e2e/1.0' };
  let body: string | undefined;
  if (method === 'POST') {
    headers['content-type'] = 'application/json';
    body = JSON.stringify(options.body ?? {});
  }

  const startedAt = Date.now();
  let rateLimitRetries = 0;
  let transientRetries = 0;
  let http429 = 0;

  for (;;) {
    if (options.gate) await options.gate.enter();
    if (stopRequested) throw new Error('interrupted');

    stats.requests += 1;
    const attemptStartedAt = Date.now();
    let response: Response;
    try {
      response = await fetch(url, { method, headers, body, signal: AbortSignal.timeout(config.requestTimeoutMs) });
    } catch (error) {
      // A sign-up that timed out may still have been applied; only repeat what is safe to repeat.
      if (options.retryNetworkErrors !== false && transientRetries < 3) {
        transientRetries += 1;
        stats.transientRetries += 1;
        await sleep(1_500 * transientRetries);
        continue;
      }
      throw new Error(`${method} ${path} failed: ${errorMessage(error)}`);
    }

    const text = await response.text().catch(() => '');
    const attemptMs = Date.now() - attemptStartedAt;

    if ([502, 503, 504].includes(response.status) && transientRetries < 3) {
      transientRetries += 1;
      stats.transientRetries += 1;
      await sleep(1_500 * transientRetries);
      continue;
    }

    if (response.status === 429) {
      stats.http429 += 1;
      http429 += 1;
      if (rateLimitRetries < config.maxRateLimitRetries) {
        rateLimitRetries += 1;
        const wait = retryAfterMs(response);
        if (stats.http429 <= 3 || stats.http429 % 25 === 0) {
          console.warn(`[http] 429 on ${method} ${path}; waiting ~${Math.round(wait / 1000)}s (${stats.http429} 429s so far)`);
        }
        if (options.gate) options.gate.penalize(wait);
        else await sleep(wait + jitter(2_000));
        continue;
      }
    } else if (options.gate && response.headers.get('x-ratelimit-remaining') === '0') {
      // The budget is spent; hold everyone until it resets instead of provoking a wave of 429s.
      const reset = Number(response.headers.get('x-ratelimit-reset'));
      if (Number.isFinite(reset) && reset > 0) options.gate.hold(Math.min(90_000, reset * 1_000 + 500));
    }

    let json: unknown = null;
    if (text) {
      try {
        json = JSON.parse(text) as unknown;
      } catch {
        json = null;
      }
    }
    return { status: response.status, ok: response.ok, json, text, attemptMs, totalMs: Date.now() - startedAt, http429 };
  }
}

const snippet = (result: ApiResult): string => result.text.replace(/\s+/g, ' ').slice(0, 300) || '(empty body)';

/** Render free instances sleep; wait until the API answers with anything before timing real requests. */
async function warmUp(): Promise<void> {
  const startedAt = Date.now();
  for (;;) {
    try {
      const response = await fetch(`${config.apiBase}/`, { signal: AbortSignal.timeout(config.requestTimeoutMs) });
      await response.text().catch(() => '');
      console.log(`[warmup] API answered HTTP ${response.status} after ${Math.round((Date.now() - startedAt) / 1000)}s`);
      return;
    } catch (error) {
      if (Date.now() - startedAt > config.warmupTimeoutMs) {
        throw new Error(`API at ${config.apiBase} did not answer within ${Math.round(config.warmupTimeoutMs / 1000)}s: ${errorMessage(error)}`);
      }
      await sleep(5_000);
    }
  }
}

// ---------------------------------------------------------------------------------------------
// Sign-up and login
// ---------------------------------------------------------------------------------------------

function pickAccessToken(payload: unknown): string | null {
  const record = asRecord(payload);
  for (const key of ['accessToken', 'access_token', 'token', 'jwt']) {
    const value = record[key];
    if (typeof value === 'string' && value.length > 20) return value;
  }
  return null;
}

async function signUp(entry: Entry): Promise<void> {
  const { result } = entry;
  try {
    const response = await api('POST', '/api/auth/register', {
      body: { email: result.email, password: entry.password, name: result.username },
      gate: registerGate,
      retryNetworkErrors: false,
    });
    result.signupHttp = response.status;
    result.signupAttemptMs = response.attemptMs;
    result.signupTotalMs = response.totalMs;
    result.signupHttp429 = response.http429;
    if (response.ok) {
      result.signup = 'created';
      const userId = asRecord(response.json).userId;
      if (typeof userId === 'string') result.userId = userId;
    } else {
      result.signup = 'failed';
      result.signupError = snippet(response);
    }
  } catch (error) {
    const message = errorMessage(error);
    result.signup = message === 'interrupted' ? 'skipped' : 'failed';
    if (result.signup === 'failed') result.signupError = message;
  }
}

async function logIn(entry: Entry): Promise<void> {
  const { result } = entry;
  try {
    const response = await api('POST', '/api/auth/login', {
      body: { email: result.email, password: entry.password },
      gate: loginGate,
    });
    result.loginHttp = response.status;
    result.loginAttemptMs = response.attemptMs;
    result.loginTotalMs = response.totalMs;
    result.loginHttp429 = response.http429;
    if (response.ok) {
      if (pickAccessToken(response.json)) {
        result.login = 'passed';
      } else {
        result.login = 'failed';
        result.loginError = `HTTP ${response.status} but no access token; response keys: ${Object.keys(asRecord(response.json)).join(', ') || '(none)'}`;
      }
    } else if ([400, 401, 403, 422].includes(response.status) && /verif/i.test(response.text)) {
      result.login = 'blocked-unverified';
      result.loginError = snippet(response);
    } else {
      result.login = 'failed';
      result.loginError = snippet(response);
    }
  } catch (error) {
    result.login = 'failed';
    result.loginError = errorMessage(error);
  }
}

async function runPool<T>(items: T[], size: number, worker: (item: T) => Promise<void>): Promise<void> {
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(size, items.length) }, async () => {
      while (next < items.length) {
        const item = items[next]!;
        next += 1;
        await worker(item);
      }
    }),
  );
}

function makeEntries(runId: string, wave: number, size: number): Entry[] {
  return Array.from({ length: size }, (_, offset) => {
    const index = offset + 1;
    const tag = `${wave}n${String(index).padStart(3, '0')}`;
    return {
      // Unique per run, wave and index, so a re-run never collides with an earlier one.
      password: `Tb1!${randomBytes(9).toString('base64url')}`,
      result: {
        wave,
        index,
        email: `e2e-${runId}-w${tag}@${config.emailDomain}`,
        username: `e2e${runId}w${tag}`,
        signup: 'skipped',
      },
    };
  });
}

async function runWave(wave: number, size: number, runId: string, canary: boolean): Promise<{ summary: WaveSummary; entries: Entry[] }> {
  const entries = makeEntries(runId, wave, size);
  console.log(`\n[wave ${wave}] ${size} sign-ups, concurrency ${config.concurrency}`);

  let done = 0;
  const trackedSignUp = async (entry: Entry): Promise<void> => {
    await signUp(entry);
    done += 1;
    if (done % 10 === 0 || done === size) {
      const created = entries.filter((e) => e.result.signup === 'created').length;
      console.log(`[wave ${wave}] sign-ups ${done}/${size} answered, ${created} created, ${stats.http429} 429s so far`);
    }
  };

  const signupStartedAt = Date.now();
  let rest = entries;
  if (canary) {
    // Fail fast: if the very first account is refused for a reason retrying cannot fix, do not
    // spend the rate-limit budget on the other accounts.
    await trackedSignUp(entries[0]!);
    const first = entries[0]!.result;
    if (first.signup === 'failed' && first.signupHttp !== undefined && first.signupHttp >= 400 && first.signupHttp < 500 && first.signupHttp !== 429) {
      throw new Error(`First sign-up was refused (HTTP ${first.signupHttp}): ${first.signupError}`);
    }
    rest = entries.slice(1);
  }
  await runPool(rest, config.concurrency, trackedSignUp);
  const signupPhaseSeconds = (Date.now() - signupStartedAt) / 1_000;

  const created = entries.filter((entry) => entry.result.signup === 'created');
  let loginPhaseSeconds: number | null = null;
  if (!config.skipLogin && created.length > 0 && !stopRequested) {
    console.log(`[wave ${wave}] logging in ${created.length} created accounts`);
    const loginStartedAt = Date.now();
    await runPool(created, config.concurrency, logIn);
    loginPhaseSeconds = (Date.now() - loginStartedAt) / 1_000;
  }

  const results = entries.map((entry) => entry.result);
  const failuresByStatus: Record<string, number> = {};
  for (const result of results) {
    if (result.signup !== 'failed') continue;
    const key = result.signupHttp !== undefined ? String(result.signupHttp) : 'network-error';
    failuresByStatus[key] = (failuresByStatus[key] ?? 0) + 1;
  }
  const count = (login: AccountResult['login']): number | null =>
    config.skipLogin ? null : results.filter((result) => result.login === login).length;
  const loginFailed = config.skipLogin ? null : created.filter((entry) => entry.result.login !== 'passed' && entry.result.login !== 'blocked-unverified').length;

  const summary: WaveSummary = {
    wave,
    requested: size,
    created: created.length,
    signupFailed: results.filter((result) => result.signup === 'failed').length,
    signupSkipped: results.filter((result) => result.signup === 'skipped').length,
    signupFailuresByStatus: failuresByStatus,
    loginPassed: count('passed'),
    loginBlockedUnverified: count('blocked-unverified'),
    loginFailed,
    http429: results.reduce((sum, result) => sum + (result.signupHttp429 ?? 0) + (result.loginHttp429 ?? 0), 0),
    signupAttemptMs: spread(numbers(results.map((result) => result.signupAttemptMs))),
    signupTotalMs: spread(numbers(results.map((result) => result.signupTotalMs))),
    loginAttemptMs: spread(numbers(results.map((result) => result.loginAttemptMs))),
    signupPhaseSeconds: Number(signupPhaseSeconds.toFixed(1)),
    loginPhaseSeconds: loginPhaseSeconds === null ? null : Number(loginPhaseSeconds.toFixed(1)),
    signupsPerMinute: Number((created.length / Math.max(signupPhaseSeconds / 60, 1 / 60)).toFixed(2)),
    result: created.length === size && (loginFailed ?? 0) === 0 ? 'PASSED' : 'FAILED',
  };
  console.log(`[wave ${wave}] ${summary.result}: ${summary.created}/${size} created, logins ${config.skipLogin ? 'skipped' : `${summary.loginPassed} passed / ${summary.loginBlockedUnverified} blocked (unverified) / ${summary.loginFailed} failed`}, ${summary.http429} 429s, ${summary.signupPhaseSeconds}s`);
  return { summary, entries };
}

// ---------------------------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------------------------

async function main(): Promise<void> {
  config = parseArgs(process.argv.slice(2));
  const runId = randomBytes(3).toString('hex').slice(0, 5);
  const total = config.wavePlan.reduce((sum, size) => sum + size, 0);

  if (config.dryRun) {
    const sample = makeEntries(runId, 1, 1)[0]!;
    console.log(`[dry-run] api base:   ${config.apiBase}`);
    console.log(`[dry-run] waves:      ${config.wavePlan.join(' -> ')} (${total} NEW accounts in total)`);
    console.log(`[dry-run] concurrency ${config.concurrency}, login check ${config.skipLogin ? 'off' : 'on'}, 429 retries per request ${config.maxRateLimitRetries}`);
    console.log(`[dry-run] sample POST /api/auth/register body: ${JSON.stringify({ email: sample.result.email, password: '<redacted>', name: sample.result.username })}`);
    console.log('[dry-run] nothing was sent.');
    return;
  }

  mkdirSync(RESULTS_DIR, { recursive: true });
  process.on('SIGINT', () => {
    if (stopRequested) process.exit(130);
    stopRequested = true;
    console.warn('\n[test] Ctrl-C: finishing in-flight requests, then writing the report. Press again to quit now.');
  });

  const report: Record<string, unknown> = {
    test: 'Tubor EX sign-up / authentication ramp test',
    runId,
    startedAt: new Date().toISOString(),
    apiBase: config.apiBase,
    wavePlan: config.wavePlan,
    loginCheck: !config.skipLogin,
    maxRateLimitRetries: config.maxRateLimitRetries,
  };
  const summaries: WaveSummary[] = [];
  const accounts: Array<Pick<AccountResult, 'wave' | 'email' | 'username' | 'userId'>> = [];
  const failures: AccountResult[] = [];
  let fatalError: string | undefined;

  const writeReport = (): void => {
    report.finishedAt = new Date().toISOString();
    report.fatalError = fatalError ?? null;
    report.interrupted = stopRequested;
    report.http = { ...stats };
    report.waves = summaries;
    // Only the failures, so the report stays readable at 220 accounts; counts are in `waves`.
    report.failedAccounts = failures.slice(0, 50);
    report.result =
      !fatalError && !stopRequested && summaries.length === config.wavePlan.length && summaries.every((wave) => wave.result === 'PASSED')
        ? 'PASSED'
        : 'FAILED';
    writeFileSync(resolve(RESULTS_DIR, 'auth-signup-report.json'), `${JSON.stringify(report, null, 2)}\n`, 'utf8');
    writeFileSync(resolve(RESULTS_DIR, `auth-signup-accounts-${runId}.json`), `${JSON.stringify({ runId, emailDomain: config.emailDomain, accounts }, null, 2)}\n`, 'utf8');
  };

  try {
    console.log(`[test] run ${runId}: waves ${config.wavePlan.join(' -> ')} (${total} new accounts) against ${config.apiBase}`);
    await warmUp();

    for (const [offset, size] of config.wavePlan.entries()) {
      if (stopRequested) break;
      const { summary, entries } = await runWave(offset + 1, size, runId, offset === 0);
      summaries.push(summary);
      for (const { result } of entries) {
        if (result.signup === 'created') accounts.push({ wave: result.wave, email: result.email, username: result.username, userId: result.userId });
        if (result.signup === 'failed' || result.login === 'failed') failures.push(result);
      }
      writeReport();
      if (summary.created === 0) {
        fatalError = `Wave ${summary.wave} created no accounts; not starting the remaining waves.`;
        break;
      }
    }
  } catch (error) {
    fatalError = errorMessage(error);
    console.error(`[test] FATAL: ${fatalError}`);
  } finally {
    writeReport();
    console.log(`\n[report] ${resolve(RESULTS_DIR, 'auth-signup-report.json')}`);
    console.log(JSON.stringify({ result: report.result, fatalError: fatalError ?? null, http: report.http, waves: summaries }, null, 2));
  }

  if (report.result !== 'PASSED') process.exitCode = 1;
}

main().catch((error: unknown) => {
  console.error(`[test] FATAL: ${errorMessage(error)}`);
  process.exitCode = 1;
});
