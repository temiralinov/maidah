import path from "node:path";
import express from "express";
import { Pool } from "pg";
import { config } from "./config";
import {
  loadDashboardStats,
  loadRestaurantsPayload,
  type DashboardStats,
  type PeriodMode,
  type RestaurantsPayload,
} from "./statsService";

const app = express();

const pool = new Pool({
  host: config.db.host,
  port: config.db.port,
  user: config.db.user,
  password: config.db.password,
  database: config.db.database,
  max: config.db.poolMax,
  idleTimeoutMillis: config.db.poolIdleTimeoutMs,
});

const publicDir = path.resolve(process.cwd(), "public");

type StatsCacheEntry = {
  timestamp: number;
  data: DashboardStats;
};

type RestaurantsCacheEntry = {
  timestamp: number;
  data: RestaurantsPayload;
};

const statsCache = new Map<string, StatsCacheEntry>();
let restaurantsCache: RestaurantsCacheEntry | null = null;

const readQueryString = (value: unknown): string => {
  if (typeof value !== "string") {
    return "";
  }
  return value.trim();
};

const normalizePeriod = (value: string): PeriodMode => {
  if (value === "month") {
    return "month";
  }
  if (value === "day") {
    return "day";
  }
  return "all";
};

const normalizeDate = (value: string): string => {
  if (/^\d{4}-\d{2}$/.test(value)) {
    return `${value}-01`;
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return value;
  }

  return "";
};

const statsCacheKey = (restaurantId: string, period: PeriodMode, anchorDate: string): string => {
  return `${restaurantId}|${period}|${anchorDate}`;
};

app.use(express.json());
app.use(express.static(publicDir));

app.get("/api/restaurants", async (_req, res) => {
  try {
    const now = Date.now();

    if (restaurantsCache && now - restaurantsCache.timestamp < config.cache.restaurantsTtlMs) {
      res.json({
        source: "cache",
        cachedAt: new Date(restaurantsCache.timestamp).toISOString(),
        data: restaurantsCache.data,
      });
      return;
    }

    const data = await loadRestaurantsPayload(
      pool,
      config.access,
      config.analytics.includedOrderStatuses,
    );
    restaurantsCache = {
      timestamp: now,
      data,
    };

    res.json({
      source: "database",
      cachedAt: new Date(now).toISOString(),
      data,
    });
  } catch (error) {
    console.error("restaurants_error", error);
    res.status(500).json({
      ok: false,
      error: "Не удалось загрузить список ресторанов",
    });
  }
});

app.get("/api/stats", async (req, res) => {
  try {
    const requestedRestaurantId = readQueryString(req.query.restaurant);
    const requestedPeriodMode = normalizePeriod(readQueryString(req.query.period));
    const requestedAnchorDate = normalizeDate(readQueryString(req.query.date));

    const key = statsCacheKey(requestedRestaurantId, requestedPeriodMode, requestedAnchorDate);
    const now = Date.now();
    const cached = statsCache.get(key);

    if (cached && now - cached.timestamp < config.cache.statsTtlMs) {
      res.json({
        source: "cache",
        cachedAt: new Date(cached.timestamp).toISOString(),
        data: cached.data,
      });
      return;
    }

    const data = await loadDashboardStats(pool, {
      requestedRestaurantId,
      requestedPeriodMode,
      requestedAnchorDate,
      accessPolicy: config.access,
      includedStatuses: config.analytics.includedOrderStatuses,
      timeZone: config.analytics.timeZone,
    });

    statsCache.set(key, {
      timestamp: now,
      data,
    });

    res.json({
      source: "database",
      cachedAt: new Date(now).toISOString(),
      data,
    });
  } catch (error) {
    console.error("stats_error", error);
    res.status(500).json({
      ok: false,
      error: "Не удалось загрузить статистику",
    });
  }
});

const sendDashboardIndex = (_req: express.Request, res: express.Response) => {
  res.sendFile(path.join(publicDir, "index.html"));
};

app.get("/", (_req, res) => {
  res.redirect(302, "/all");
});
app.get("/all", sendDashboardIndex);
app.get("/month/:month", sendDashboardIndex);
app.get("/day/:day", sendDashboardIndex);

const server = app.listen(config.server.port, () => {
  console.log(`Страница статистики: http://localhost:${config.server.port}`);
});

let isShuttingDown = false;

const shutdown = () => {
  if (isShuttingDown) {
    return;
  }
  isShuttingDown = true;

  server.close(async () => {
    try {
      await pool.end();
    } finally {
      process.exit(0);
    }
  });
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
