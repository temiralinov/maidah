import dotenv from "dotenv";

dotenv.config();

type VisibilityMode = "all" | "company" | "store";

type Config = {
  server: {
    port: number;
  };
  db: {
    host: string;
    port: number;
    user: string;
    password: string;
    database: string;
    poolMax: number;
    poolIdleTimeoutMs: number;
  };
  cache: {
    statsTtlMs: number;
    restaurantsTtlMs: number;
  };
  analytics: {
    includedOrderStatuses: string[];
    timeZone: string;
  };
  access: {
    visibilityMode: VisibilityMode;
    defaultCompanyId: string;
    defaultStoreId: string;
  };
};

const readRaw = (name: string): string => {
  const value = process.env[name];
  if (typeof value !== "string") {
    return "";
  }
  return value.trim();
};

const readString = (name: string, fallback: string): string => {
  const value = readRaw(name);
  if (value.length === 0) {
    return fallback;
  }
  return value;
};

const readInt = (name: string, fallback: number, min: number, max: number): number => {
  const raw = readRaw(name);
  const candidate = raw.length === 0 ? fallback : Number(raw);

  if (!Number.isInteger(candidate)) {
    throw new Error(`ENV ${name} must be an integer`);
  }

  if (candidate < min || candidate > max) {
    throw new Error(`ENV ${name} must be between ${min} and ${max}`);
  }

  return candidate;
};

const readCsvLower = (name: string, fallback: string[]): string[] => {
  const raw = readRaw(name);
  const source = raw.length > 0 ? raw.split(",") : fallback;

  const normalized = source
    .map((item) => item.trim().toLowerCase())
    .filter((item) => item.length > 0);

  return Array.from(new Set(normalized));
};

const readMode = (): VisibilityMode => {
  const value = readRaw("RESTAURANT_VISIBILITY").toLowerCase();

  if (value.length === 0) {
    return "company";
  }

  if (value === "all" || value === "company" || value === "store") {
    return value;
  }

  throw new Error("ENV RESTAURANT_VISIBILITY must be one of: all, company, store");
};

const buildConfig = (): Config => {
  const visibilityMode = readMode();
  const defaultCompanyId = readString("DEFAULT_COMPANY_ID", "sheker-coffee");
  const defaultStoreId = readString("DEFAULT_STORE_ID", "");

  if (visibilityMode === "store" && defaultStoreId.length === 0) {
    throw new Error("ENV DEFAULT_STORE_ID is required when RESTAURANT_VISIBILITY=store");
  }

  if (visibilityMode === "company" && defaultCompanyId.length === 0) {
    throw new Error("ENV DEFAULT_COMPANY_ID is required when RESTAURANT_VISIBILITY=company");
  }

  return {
    server: {
      port: readInt("PORT", 3000, 1, 65535),
    },
    db: {
      host: readString("PGHOST", "localhost"),
      port: readInt("PGPORT", 5432, 1, 65535),
      user: readString("PGUSER", "maidah"),
      password: readString("PGPASSWORD", "maidah"),
      database: readString("PGDATABASE", "maidah"),
      poolMax: readInt("PGPOOL_MAX", 10, 1, 100),
      poolIdleTimeoutMs: readInt("PGPOOL_IDLE_TIMEOUT_MS", 30000, 1000, 120000),
    },
    cache: {
      statsTtlMs: readInt("STATS_CACHE_TTL_MS", 30000, 0, 3600000),
      restaurantsTtlMs: readInt("RESTAURANTS_CACHE_TTL_MS", 300000, 0, 3600000),
    },
    analytics: {
      includedOrderStatuses: readCsvLower("INCLUDED_ORDER_STATUSES", ["archived"]),
      timeZone: readString("ANALYTICS_TIME_ZONE", "Asia/Almaty"),
    },
    access: {
      visibilityMode,
      defaultCompanyId,
      defaultStoreId,
    },
  };
};

export const config = buildConfig();
export type { Config, VisibilityMode };
