import { Pool, PoolClient } from "pg";
import { config } from "./config";

type BotConfig = {
  apiBaseUrl: string;
  userAgent: string;
  requestDelayMinMs: number;
  requestDelayMaxMs: number;
  cyclePauseMs: number;
  liveTailWindow: number;
  nonArchivedBatch: number;
  retryErrorBatch: number;
  newIdsPerCycle: number;
  httpTimeoutMs: number;
  maxRetries: number;
  retryBaseMs: number;
  retryMaxMs: number;
  runOnce: boolean;
  dryRun: boolean;
  archiveStatuses: string[];
};

type FetchSuccess = {
  ok: true;
  statusCode: number;
  payload: Record<string, unknown>;
  rawBody: string;
  attempts: number;
};

type FetchFailure = {
  ok: false;
  statusCode: number;
  error: string;
  attempts: number;
};

type FetchResult = FetchSuccess | FetchFailure;

type OrderRow = {
  id: number;
  type: string | null;
  secondaryId: number | null;
  name: string | null;
  clientId: string | null;
  companyId: string | null;
  storeId: string | null;
  tableId: number | null;
  status: string | null;
  paymentType: string | null;
  price: number | null;
  cash: number | null;
  bonus: number | null;
  margin: number | null;
  createdAt: string | null;
  finishedAt: string | null;
  raw: string;
};

type ProductRow = {
  id: number;
  orderId: number;
  storeId: string | null;
  companyId: string | null;
  productId: string | null;
  status: string | null;
  nameRus: string | null;
  nameKaz: string | null;
  nameEng: string | null;
  price: number | null;
  margin: number | null;
  comment: string | null;
  bonus: number | null;
  count: number | null;
  size: number | null;
  terminalProductId: string | null;
};

type TransactionRow = {
  id: number;
  orderId: number;
  type: string | null;
  name: string | null;
  clientId: string | null;
  storeId: string | null;
  companyId: string | null;
  bin: string | null;
  price: number | null;
  cash: number | null;
  bonus: number | null;
  status: string | null;
  token: string | null;
  paymentId: number | null;
  createdAt: string | null;
  updatedAt: string | null;
};

const readRaw = (name: string): string => {
  const value = process.env[name];
  if (typeof value !== "string") {
    return "";
  }
  return value.trim();
};

const readInt = (name: string, fallback: number, min: number, max: number): number => {
  const raw = readRaw(name);
  const value = raw.length > 0 ? Number(raw) : fallback;

  if (!Number.isInteger(value)) {
    throw new Error(`ENV ${name} must be an integer`);
  }
  if (value < min || value > max) {
    throw new Error(`ENV ${name} must be between ${min} and ${max}`);
  }
  return value;
};

const readBool = (name: string, fallback: boolean): boolean => {
  const raw = readRaw(name).toLowerCase();
  if (raw.length === 0) {
    return fallback;
  }
  if (raw === "1" || raw === "true" || raw === "yes" || raw === "on") {
    return true;
  }
  if (raw === "0" || raw === "false" || raw === "no" || raw === "off") {
    return false;
  }
  throw new Error(`ENV ${name} must be boolean`);
};

const readCsvLower = (name: string, fallback: string[]): string[] => {
  const raw = readRaw(name);
  const source = raw.length > 0 ? raw.split(",") : fallback;
  const normalized = source
    .map((item) => item.trim().toLowerCase())
    .filter((item) => item.length > 0);
  const deduped = Array.from(new Set(normalized));
  return deduped.length > 0 ? deduped : fallback;
};

const buildBotConfig = (): BotConfig => {
  const requestDelayMinMs = readInt("BOT_REQUEST_DELAY_MIN_MS", 2500, 250, 120000);
  const requestDelayMaxMs = readInt("BOT_REQUEST_DELAY_MAX_MS", 6500, requestDelayMinMs, 180000);

  return {
    apiBaseUrl: readRaw("MAIDAH_API_BASE_URL") || "http://api.maidah.kz/orders",
    userAgent:
      readRaw("BOT_USER_AGENT") ||
      "maidah-archive-bot/1.0 (+safe-upsert; contact: admin)",
    requestDelayMinMs,
    requestDelayMaxMs,
    cyclePauseMs: readInt("BOT_CYCLE_PAUSE_MS", 120000, 1000, 3600000),
    liveTailWindow: readInt("BOT_LIVE_TAIL_WINDOW", 24, 1, 1000),
    nonArchivedBatch: readInt("BOT_NON_ARCHIVED_BATCH", 24, 1, 500),
    retryErrorBatch: readInt("BOT_RETRY_ERROR_BATCH", 10, 0, 500),
    newIdsPerCycle: readInt("BOT_NEW_IDS_PER_CYCLE", 1, 0, 100),
    httpTimeoutMs: readInt("BOT_HTTP_TIMEOUT_MS", 15000, 1000, 120000),
    maxRetries: readInt("BOT_MAX_RETRIES", 3, 1, 10),
    retryBaseMs: readInt("BOT_RETRY_BASE_MS", 3000, 200, 120000),
    retryMaxMs: readInt("BOT_RETRY_MAX_MS", 30000, 1000, 300000),
    runOnce: readBool("BOT_RUN_ONCE", false),
    dryRun: readBool("BOT_DRY_RUN", false),
    archiveStatuses: readCsvLower("BOT_ARCHIVE_STATUSES", ["archived"]),
  };
};

const isRecord = (value: unknown): value is Record<string, unknown> => {
  return typeof value === "object" && value !== null && !Array.isArray(value);
};

const toInteger = (value: unknown): number | null => {
  if (typeof value === "number" && Number.isFinite(value) && Number.isInteger(value)) {
    return value;
  }
  if (typeof value === "string") {
    const normalized = value.trim();
    if (/^-?\d+$/.test(normalized)) {
      const parsed = Number(normalized);
      if (Number.isFinite(parsed) && Number.isInteger(parsed)) {
        return parsed;
      }
    }
  }
  return null;
};

const toFloat = (value: unknown): number | null => {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const normalized = value.trim();
    if (normalized.length === 0) {
      return null;
    }
    const parsed = Number(normalized);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
};

const toText = (value: unknown): string | null => {
  if (typeof value !== "string") {
    return null;
  }
  const normalized = value.trim();
  if (normalized.length === 0) {
    return null;
  }
  return normalized;
};

const normalizeStatus = (value: unknown): string => {
  const text = toText(value);
  if (!text) {
    return "";
  }
  return text.toLowerCase();
};

const nowTs = (): string => {
  return new Date().toISOString();
};

const sleep = async (ms: number): Promise<void> => {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
};

const randomBetween = (min: number, max: number): number => {
  if (max <= min) {
    return min;
  }
  return min + Math.floor(Math.random() * (max - min + 1));
};

const backoffMs = (base: number, max: number, attempt: number): number => {
  const value = base * Math.pow(2, Math.max(0, attempt - 1));
  return Math.min(max, value);
};

const sanitizeError = (value: string): string => {
  return value.replace(/\s+/g, " ").trim().slice(0, 500);
};

const cloudflareLikely = (body: string): boolean => {
  const lower = body.toLowerCase();
  return (
    lower.includes("cloudflare") ||
    lower.includes("just a moment") ||
    lower.includes("attention required")
  );
};

const shouldRetry = (statusCode: number, body: string, networkError: boolean): boolean => {
  if (networkError) {
    return true;
  }
  if (statusCode === 408 || statusCode === 425 || statusCode === 429) {
    return true;
  }
  if (statusCode >= 500) {
    return true;
  }
  if (cloudflareLikely(body)) {
    return true;
  }
  return false;
};

const extractPayload = (parsed: unknown): Record<string, unknown> | null => {
  if (!isRecord(parsed)) {
    return null;
  }
  if (isRecord(parsed.data) && toInteger(parsed.data.id) !== null) {
    return parsed.data;
  }
  if (toInteger(parsed.id) !== null) {
    return parsed;
  }
  return null;
};

const fetchOrderOnce = async (
  orderId: number,
  botConfig: BotConfig,
): Promise<{ ok: true; statusCode: number; payload: Record<string, unknown>; rawBody: string } | { ok: false; statusCode: number; error: string; retryable: boolean }> => {
  const url = `${botConfig.apiBaseUrl}/${orderId}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), botConfig.httpTimeoutMs);

  let statusCode = 0;
  let body = "";
  let networkError = false;

  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        accept: "application/json, text/plain, */*",
        "user-agent": botConfig.userAgent,
      },
      signal: controller.signal,
    });

    statusCode = response.status;
    body = await response.text();
  } catch (error) {
    networkError = true;
    const message = error instanceof Error ? error.message : String(error);
    clearTimeout(timer);
    return {
      ok: false,
      statusCode,
      error: sanitizeError(message),
      retryable: shouldRetry(statusCode, body, true),
    };
  } finally {
    clearTimeout(timer);
  }

  try {
    const parsed = JSON.parse(body);
    const payload = extractPayload(parsed);
    if (!payload) {
      return {
        ok: false,
        statusCode,
        error: sanitizeError(`invalid_payload_for_${orderId}`),
        retryable: shouldRetry(statusCode, body, networkError),
      };
    }

    const payloadId = toInteger(payload.id);
    if (payloadId !== orderId) {
      return {
        ok: false,
        statusCode,
        error: sanitizeError(`id_mismatch_expected_${orderId}_got_${String(payload.id)}`),
        retryable: shouldRetry(statusCode, body, networkError),
      };
    }

    return {
      ok: true,
      statusCode,
      payload,
      rawBody: body,
    };
  } catch {
    return {
      ok: false,
      statusCode,
      error: sanitizeError(body || `non_json_response_for_${orderId}`),
      retryable: shouldRetry(statusCode, body, networkError),
    };
  }
};

const fetchOrderWithRetries = async (orderId: number, botConfig: BotConfig): Promise<FetchResult> => {
  for (let attempt = 1; attempt <= botConfig.maxRetries; attempt += 1) {
    const result = await fetchOrderOnce(orderId, botConfig);

    if (result.ok) {
      return {
        ok: true,
        statusCode: result.statusCode,
        payload: result.payload,
        rawBody: result.rawBody,
        attempts: attempt,
      };
    }

    const canRetry = result.retryable && attempt < botConfig.maxRetries;
    if (!canRetry) {
      return {
        ok: false,
        statusCode: result.statusCode,
        error: result.error,
        attempts: attempt,
      };
    }

    const waitMs = backoffMs(botConfig.retryBaseMs, botConfig.retryMaxMs, attempt) + randomBetween(150, 1200);
    console.log(`${nowTs()} retry id=${orderId} attempt=${attempt} wait_ms=${waitMs} status=${result.statusCode}`);
    await sleep(waitMs);
  }

  return {
    ok: false,
    statusCode: 0,
    error: "unexpected_retry_exhaustion",
    attempts: botConfig.maxRetries,
  };
};

const buildOrderRow = (orderId: number, payload: Record<string, unknown>, rawBody: string): OrderRow => {
  return {
    id: orderId,
    type: toText(payload.type),
    secondaryId: toInteger(payload.secondary_id),
    name: toText(payload.name),
    clientId: toText(payload.client_id),
    companyId: toText(payload.company_id),
    storeId: toText(payload.store_id),
    tableId: toInteger(payload.table_id),
    status: toText(payload.status),
    paymentType: toText(payload.payment_type),
    price: toFloat(payload.price),
    cash: toFloat(payload.cash),
    bonus: toFloat(payload.bonus),
    margin: toFloat(payload.margin),
    createdAt: toText(payload.created_at),
    finishedAt: toText(payload.finished_at),
    raw: rawBody,
  };
};

const buildProductRows = (orderId: number, payload: Record<string, unknown>): ProductRow[] => {
  const products = Array.isArray(payload.products) ? payload.products : [];
  const fallbackStore = toText(payload.store_id);
  const fallbackCompany = toText(payload.company_id);
  const byId = new Map<number, ProductRow>();

  products.forEach((item) => {
    if (!isRecord(item)) {
      return;
    }

    const id = toInteger(item.id);
    if (id === null) {
      return;
    }

    const nameObj = isRecord(item.name) ? item.name : {};
    byId.set(id, {
      id,
      orderId,
      storeId: toText(item.store_id) || fallbackStore,
      companyId: toText(item.company_id) || fallbackCompany,
      productId: toText(item.product_id),
      status: toText(item.status),
      nameRus: toText(nameObj.rus) || toText(item.name_rus),
      nameKaz: toText(nameObj.kaz) || toText(item.name_kaz),
      nameEng: toText(nameObj.eng) || toText(item.name_eng),
      price: toFloat(item.price),
      margin: toFloat(item.margin),
      comment: toText(item.comment),
      bonus: toFloat(item.bonus),
      count: toInteger(item.count),
      size: toInteger(item.size),
      terminalProductId: toText(item.terminal_product_id),
    });
  });

  return Array.from(byId.values());
};

const buildTransactionRows = (orderId: number, payload: Record<string, unknown>): TransactionRow[] => {
  const candidates: unknown[] = [];
  if (isRecord(payload.transaction)) {
    candidates.push(payload.transaction);
  }
  if (Array.isArray(payload.transactions)) {
    payload.transactions.forEach((item) => {
      candidates.push(item);
    });
  }

  const fallbackStore = toText(payload.store_id);
  const fallbackCompany = toText(payload.company_id);
  const byId = new Map<number, TransactionRow>();

  candidates.forEach((item) => {
    if (!isRecord(item)) {
      return;
    }

    const id = toInteger(item.id);
    if (id === null) {
      return;
    }

    byId.set(id, {
      id,
      orderId,
      type: toText(item.type),
      name: toText(item.name),
      clientId: toText(item.client_id),
      storeId: toText(item.store_id) || fallbackStore,
      companyId: toText(item.company_id) || fallbackCompany,
      bin: toText(item.bin),
      price: toFloat(item.price),
      cash: toFloat(item.cash),
      bonus: toFloat(item.bonus),
      status: toText(item.status),
      token: toText(item.token),
      paymentId: toInteger(item.payment_id),
      createdAt: toText(item.created_at),
      updatedAt: toText(item.updated_at),
    });
  });

  return Array.from(byId.values());
};

const upsertOrder = async (client: PoolClient, row: OrderRow): Promise<void> => {
  await client.query(
    `
    INSERT INTO orders (
      id, type, secondary_id, name, client_id, company_id, store_id, table_id,
      status, payment_type, price, cash, bonus, margin, created_at, finished_at, raw
    )
    VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8,
      $9, $10, $11, $12, $13, $14, $15, $16, $17
    )
    ON CONFLICT (id) DO UPDATE SET
      type = EXCLUDED.type,
      secondary_id = EXCLUDED.secondary_id,
      name = EXCLUDED.name,
      client_id = EXCLUDED.client_id,
      company_id = EXCLUDED.company_id,
      store_id = EXCLUDED.store_id,
      table_id = EXCLUDED.table_id,
      status = EXCLUDED.status,
      payment_type = EXCLUDED.payment_type,
      price = EXCLUDED.price,
      cash = EXCLUDED.cash,
      bonus = EXCLUDED.bonus,
      margin = EXCLUDED.margin,
      created_at = EXCLUDED.created_at,
      finished_at = EXCLUDED.finished_at,
      raw = EXCLUDED.raw
    `,
    [
      row.id,
      row.type,
      row.secondaryId,
      row.name,
      row.clientId,
      row.companyId,
      row.storeId,
      row.tableId,
      row.status,
      row.paymentType,
      row.price,
      row.cash,
      row.bonus,
      row.margin,
      row.createdAt,
      row.finishedAt,
      row.raw,
    ],
  );
};

const upsertProduct = async (client: PoolClient, row: ProductRow): Promise<void> => {
  await client.query(
    `
    INSERT INTO order_products (
      id, order_id, store_id, company_id, product_id, status,
      name_rus, name_kaz, name_eng, price, margin, comment, bonus,
      count, size, terminal_product_id
    )
    VALUES (
      $1, $2, $3, $4, $5, $6,
      $7, $8, $9, $10, $11, $12, $13,
      $14, $15, $16
    )
    ON CONFLICT (id) DO UPDATE SET
      order_id = EXCLUDED.order_id,
      store_id = EXCLUDED.store_id,
      company_id = EXCLUDED.company_id,
      product_id = EXCLUDED.product_id,
      status = EXCLUDED.status,
      name_rus = EXCLUDED.name_rus,
      name_kaz = EXCLUDED.name_kaz,
      name_eng = EXCLUDED.name_eng,
      price = EXCLUDED.price,
      margin = EXCLUDED.margin,
      comment = EXCLUDED.comment,
      bonus = EXCLUDED.bonus,
      count = EXCLUDED.count,
      size = EXCLUDED.size,
      terminal_product_id = EXCLUDED.terminal_product_id
    `,
    [
      row.id,
      row.orderId,
      row.storeId,
      row.companyId,
      row.productId,
      row.status,
      row.nameRus,
      row.nameKaz,
      row.nameEng,
      row.price,
      row.margin,
      row.comment,
      row.bonus,
      row.count,
      row.size,
      row.terminalProductId,
    ],
  );
};

const upsertTransaction = async (client: PoolClient, row: TransactionRow): Promise<void> => {
  await client.query(
    `
    INSERT INTO order_transactions (
      id, order_id, type, name, client_id, store_id, company_id,
      bin, price, cash, bonus, status, token, payment_id,
      created_at, updated_at
    )
    VALUES (
      $1, $2, $3, $4, $5, $6, $7,
      $8, $9, $10, $11, $12, $13, $14,
      $15, $16
    )
    ON CONFLICT (id) DO UPDATE SET
      order_id = EXCLUDED.order_id,
      type = EXCLUDED.type,
      name = EXCLUDED.name,
      client_id = EXCLUDED.client_id,
      store_id = EXCLUDED.store_id,
      company_id = EXCLUDED.company_id,
      bin = EXCLUDED.bin,
      price = EXCLUDED.price,
      cash = EXCLUDED.cash,
      bonus = EXCLUDED.bonus,
      status = EXCLUDED.status,
      token = EXCLUDED.token,
      payment_id = EXCLUDED.payment_id,
      created_at = EXCLUDED.created_at,
      updated_at = EXCLUDED.updated_at
    `,
    [
      row.id,
      row.orderId,
      row.type,
      row.name,
      row.clientId,
      row.storeId,
      row.companyId,
      row.bin,
      row.price,
      row.cash,
      row.bonus,
      row.status,
      row.token,
      row.paymentId,
      row.createdAt,
      row.updatedAt,
    ],
  );
};

const upsertFetchError = async (
  pool: Pool,
  orderId: number,
  statusCode: number,
  error: string,
): Promise<void> => {
  await pool.query(
    `
    INSERT INTO fetch_errors (id, status_code, error)
    VALUES ($1, $2, $3)
    ON CONFLICT (id) DO UPDATE SET
      status_code = EXCLUDED.status_code,
      error = EXCLUDED.error
    `,
    [orderId, statusCode, sanitizeError(error)],
  );
};

const clearFetchError = async (client: PoolClient, orderId: number): Promise<void> => {
  await client.query(`DELETE FROM fetch_errors WHERE id = $1`, [orderId]);
};

const persistOrderPayload = async (
  pool: Pool,
  orderId: number,
  payload: Record<string, unknown>,
  rawBody: string,
): Promise<{ status: string; products: number; transactions: number }> => {
  const normalizedRaw = JSON.stringify(payload);
  const orderRow = buildOrderRow(orderId, payload, normalizedRaw || rawBody);
  const products = buildProductRows(orderId, payload);
  const transactions = buildTransactionRows(orderId, payload);

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await upsertOrder(client, orderRow);

    for (const product of products) {
      await upsertProduct(client, product);
    }

    for (const transaction of transactions) {
      await upsertTransaction(client, transaction);
    }

    await clearFetchError(client, orderId);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }

  return {
    status: normalizeStatus(payload.status),
    products: products.length,
    transactions: transactions.length,
  };
};

const loadNonArchivedIds = async (pool: Pool, archiveStatuses: string[], limit: number): Promise<number[]> => {
  if (limit <= 0) {
    return [];
  }

  const result = await pool.query(
    `
    SELECT id
    FROM orders
    WHERE COALESCE(NULLIF(LOWER(BTRIM(status)), ''), '') <> ALL($1::text[])
    ORDER BY id DESC
    LIMIT $2
    `,
    [archiveStatuses, limit],
  );

  return result.rows
    .map((row) => toInteger(row.id))
    .filter((value): value is number => value !== null);
};

const loadOldestNonArchivedIds = async (pool: Pool, archiveStatuses: string[], limit: number): Promise<number[]> => {
  if (limit <= 0) {
    return [];
  }

  const result = await pool.query(
    `
    SELECT id
    FROM orders
    WHERE COALESCE(NULLIF(LOWER(BTRIM(status)), ''), '') <> ALL($1::text[])
    ORDER BY id ASC
    LIMIT $2
    `,
    [archiveStatuses, limit],
  );

  return result.rows
    .map((row) => toInteger(row.id))
    .filter((value): value is number => value !== null);
};

const loadRetryErrorIds = async (
  pool: Pool,
  fromIdInclusive: number,
  toIdInclusive: number,
  limit: number,
): Promise<number[]> => {
  if (limit <= 0) {
    return [];
  }

  const result = await pool.query(
    `
    SELECT id
    FROM fetch_errors
    WHERE id >= $1
      AND id <= $2
    ORDER BY id DESC
    LIMIT $3
    `,
    [fromIdInclusive, toIdInclusive, limit],
  );

  return result.rows
    .map((row) => toInteger(row.id))
    .filter((value): value is number => value !== null);
};

const loadMaxOrderId = async (pool: Pool): Promise<number> => {
  const result = await pool.query(`SELECT COALESCE(MAX(id), 0)::bigint AS max_id FROM orders`);
  const value = toInteger(result.rows[0]?.max_id);
  return value === null ? 0 : value;
};

const interleave = (left: number[], right: number[]): number[] => {
  const result: number[] = [];
  const maxLength = Math.max(left.length, right.length);
  for (let index = 0; index < maxLength; index += 1) {
    if (index < left.length) {
      result.push(left[index]);
    }
    if (index < right.length) {
      result.push(right[index]);
    }
  }
  return result;
};

const buildLiveIds = (maxId: number, tailWindow: number, lookahead: number): number[] => {
  const from = Math.max(1, maxId - tailWindow + 1);
  const to = Math.max(from, maxId + Math.max(0, lookahead));
  const result: number[] = [];

  for (let id = to; id >= from; id -= 1) {
    result.push(id);
  }

  return result;
};

const buildCycleQueue = async (pool: Pool, botConfig: BotConfig): Promise<number[]> => {
  const newestLimit = Math.max(1, Math.floor(botConfig.nonArchivedBatch / 2));
  const oldestLimit = Math.max(0, botConfig.nonArchivedBatch - newestLimit);

  const [newest, oldest, maxId] = await Promise.all([
    loadNonArchivedIds(pool, botConfig.archiveStatuses, newestLimit),
    loadOldestNonArchivedIds(pool, botConfig.archiveStatuses, oldestLimit),
    loadMaxOrderId(pool),
  ]);

  const liveIds = buildLiveIds(maxId, botConfig.liveTailWindow, botConfig.newIdsPerCycle);
  const retryFrom = Math.max(1, maxId - botConfig.liveTailWindow + 1);
  const retryTo = maxId + botConfig.newIdsPerCycle;
  const retryIds = await loadRetryErrorIds(pool, retryFrom, retryTo, botConfig.retryErrorBatch);
  const combined = interleave(newest, oldest);

  liveIds.forEach((id) => {
    combined.unshift(id);
  });

  retryIds.forEach((id) => {
    combined.push(id);
  });

  const unique: number[] = [];
  const seen = new Set<number>();

  combined.forEach((id) => {
    if (!Number.isInteger(id) || id <= 0) {
      return;
    }
    if (seen.has(id)) {
      return;
    }
    seen.add(id);
    unique.push(id);
  });

  return unique;
};

const countNonArchived = async (pool: Pool, archiveStatuses: string[]): Promise<number> => {
  const result = await pool.query(
    `
    SELECT COUNT(*)::int AS value
    FROM orders
    WHERE COALESCE(NULLIF(LOWER(BTRIM(status)), ''), '') <> ALL($1::text[])
    `,
    [archiveStatuses],
  );
  const value = toInteger(result.rows[0]?.value);
  return value === null ? 0 : value;
};

let stopRequested = false;

const requestStop = (signal: string): void => {
  if (stopRequested) {
    return;
  }
  stopRequested = true;
  console.log(`${nowTs()} received ${signal}, stopping after current request`);
};

const run = async (): Promise<void> => {
  const botConfig = buildBotConfig();

  const pool = new Pool({
    host: config.db.host,
    port: config.db.port,
    user: config.db.user,
    password: config.db.password,
    database: config.db.database,
    max: 2,
    idleTimeoutMillis: config.db.poolIdleTimeoutMs,
  });

  process.on("SIGINT", () => requestStop("SIGINT"));
  process.on("SIGTERM", () => requestStop("SIGTERM"));

  console.log(
    `${nowTs()} bot_start api=${botConfig.apiBaseUrl} dry_run=${String(botConfig.dryRun)} run_once=${String(botConfig.runOnce)}`,
  );
  console.log(
    `${nowTs()} pacing request_delay_ms=${botConfig.requestDelayMinMs}-${botConfig.requestDelayMaxMs} cycle_pause_ms=${botConfig.cyclePauseMs}`,
  );

  try {
    while (!stopRequested) {
      const cycleStartedAt = Date.now();
      const queue = await buildCycleQueue(pool, botConfig);
      const pendingBefore = await countNonArchived(pool, botConfig.archiveStatuses);

      if (queue.length === 0) {
        console.log(`${nowTs()} cycle_empty pending_non_archived=${pendingBefore}`);
      } else {
        console.log(
          `${nowTs()} cycle_start queue=${queue.length} pending_non_archived=${pendingBefore} ids=${queue.join(",")}`,
        );
      }

      let okCount = 0;
      let failCount = 0;
      let archivedCount = 0;

      for (let index = 0; index < queue.length; index += 1) {
        if (stopRequested) {
          break;
        }

        const orderId = queue[index];
        if (index > 0) {
          const delay = randomBetween(botConfig.requestDelayMinMs, botConfig.requestDelayMaxMs);
          await sleep(delay);
        }

        const fetchResult = await fetchOrderWithRetries(orderId, botConfig);

        if (!fetchResult.ok) {
          failCount += 1;
          await upsertFetchError(pool, orderId, fetchResult.statusCode, fetchResult.error);
          console.log(
            `${nowTs()} fail id=${orderId} http=${fetchResult.statusCode} attempts=${fetchResult.attempts} error="${fetchResult.error}"`,
          );
          continue;
        }

        if (botConfig.dryRun) {
          okCount += 1;
          const currentStatus = normalizeStatus(fetchResult.payload.status);
          if (botConfig.archiveStatuses.includes(currentStatus)) {
            archivedCount += 1;
          }
          console.log(
            `${nowTs()} dry_ok id=${orderId} http=${fetchResult.statusCode} attempts=${fetchResult.attempts} api_status=${currentStatus || "unknown"}`,
          );
          continue;
        }

        try {
          const persisted = await persistOrderPayload(pool, orderId, fetchResult.payload, fetchResult.rawBody);
          okCount += 1;
          if (botConfig.archiveStatuses.includes(persisted.status)) {
            archivedCount += 1;
          }
          console.log(
            `${nowTs()} ok id=${orderId} http=${fetchResult.statusCode} attempts=${fetchResult.attempts} api_status=${persisted.status || "unknown"} products=${persisted.products} transactions=${persisted.transactions}`,
          );
        } catch (error) {
          failCount += 1;
          const message = error instanceof Error ? error.message : String(error);
          await upsertFetchError(pool, orderId, fetchResult.statusCode, message);
          console.log(
            `${nowTs()} persist_fail id=${orderId} http=${fetchResult.statusCode} error="${sanitizeError(message)}"`,
          );
        }
      }

      const pendingAfter = await countNonArchived(pool, botConfig.archiveStatuses);
      const cycleMs = Date.now() - cycleStartedAt;
      console.log(
        `${nowTs()} cycle_done ok=${okCount} fail=${failCount} archived=${archivedCount} pending_non_archived=${pendingAfter} duration_ms=${cycleMs}`,
      );

      if (botConfig.runOnce || stopRequested) {
        break;
      }

      await sleep(botConfig.cyclePauseMs);
    }
  } finally {
    await pool.end();
    console.log(`${nowTs()} bot_stopped`);
  }
};

run().catch((error) => {
  const message = error instanceof Error ? error.stack || error.message : String(error);
  console.error(`${nowTs()} fatal_error ${message}`);
  process.exit(1);
});
