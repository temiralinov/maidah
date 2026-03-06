import { Pool } from "pg";

export type VisibilityMode = "all" | "company" | "store";
export type PeriodMode = "all" | "month" | "day";

export type AccessPolicy = {
  visibilityMode: VisibilityMode;
  defaultCompanyId: string;
  defaultStoreId: string;
};

export type RestaurantOption = {
  id: string;
  label: string;
  companyId: string;
  storeId: string;
  ordersCount: number;
  revenue: number;
};

export type RestaurantsPayload = {
  options: RestaurantOption[];
  canSelectRestaurant: boolean;
  defaultRestaurantId: string;
};

export type StatsRequest = {
  requestedRestaurantId: string;
  requestedPeriodMode: PeriodMode;
  requestedAnchorDate: string;
  accessPolicy: AccessPolicy;
  includedStatuses: string[];
  timeZone: string;
};

export type StatsPoint = {
  label: string;
  orders: number;
  revenue: number;
  avgCheck: number;
};

export type Bucket = {
  label: string;
  value: number;
};

export type ProductTop = {
  name: string;
  units: number;
  revenue: number;
};

export type TimelineTopProducts = {
  label: string;
  items: ProductTop[];
};

export type OrdersPerHourWindow = {
  totalOrders: number;
  hoursInWindow: number;
  avgPerHour: number;
  busiestHour: string;
  busiestHourOrders: number;
};

export type OrdersPerDayWindow = {
  totalOrders: number;
  daysInWindow: number;
  avgPerDay: number;
  busiestDay: string;
  busiestDayOrders: number;
};

export type UnitVsNetworkPoint = {
  monthDate: string;
  monthLabel: string;
  ageMonth: number;
  unitRevenue: number;
  networkAvgRevenue: number;
};

export type RecentOrder = {
  createdAt: string;
  status: string;
  paymentType: string;
  total: number;
};

export type DashboardStats = {
  generatedAt: string;
  filter: {
    restaurantId: string;
    restaurantLabel: string;
  };
  period: {
    mode: PeriodMode;
    anchorDate: string;
    fromDate: string;
    toDate: string;
    displayLabel: string;
    chartGranularity: "month" | "day" | "hour";
  };
  summary: {
    ordersCount: number;
    revenue: number;
    avgCheck: number;
    itemsSold: number;
    cash: number;
    bonus: number;
    margin: number;
    paymentsCount: number;
  };
  notes: {
    margin: string;
    timeZone: string;
    orderStatusScope: string;
    dataScope: string;
  };
  quality: {
    withoutFinishedAt: number;
    withoutClientId: number;
    withoutProducts: number;
  };
  charts: {
    timeline: StatsPoint[];
    topProductsByPoint: TimelineTopProducts[];
    statusBreakdown: Bucket[];
    paymentBreakdown: Bucket[];
    unitVsNetwork: {
      unitLabel: string;
      networkLabel: string;
      points: UnitVsNetworkPoint[];
    };
  };
  ordersPerHour: {
    allTime: OrdersPerHourWindow;
    year: OrdersPerHourWindow;
    month: OrdersPerHourWindow;
    day: OrdersPerHourWindow;
  };
  ordersPerDay: {
    allTime: OrdersPerDayWindow;
    year: OrdersPerDayWindow;
    month: OrdersPerDayWindow;
    day: OrdersPerDayWindow;
  };
  topProducts: ProductTop[];
  recentOrders: RecentOrder[];
};

type AnyRow = Record<string, unknown>;

type Scope = {
  storeFilter: string;
  companyFilter: string;
  selectedRestaurantId: string;
  selectedRestaurantLabel: string;
};

const TOP_PRODUCTS_LIMIT = 20;

const BASE_CTE = `
WITH scoped_orders AS (
  SELECT
    o.*,
    CASE
      WHEN o.created_at IS NOT NULL
        AND BTRIM(o.created_at) <> ''
        AND pg_input_is_valid(o.created_at, 'timestamptz')
      THEN (o.created_at::timestamptz AT TIME ZONE $6)
      ELSE NULL
    END AS created_ts
  FROM orders o
  WHERE ($1 = '' OR o.store_id = $1)
    AND ($2 = '' OR o.company_id = $2)
    AND COALESCE(NULLIF(LOWER(BTRIM(o.status)), ''), '') = ANY($5::text[])
),
period_orders AS (
  SELECT *
  FROM scoped_orders
  WHERE
    $3 = 'all'
    OR (
      created_ts IS NOT NULL
      AND (
        ($3 = 'month' AND created_ts::date >= date_trunc('month', $4::date)::date AND created_ts::date < (date_trunc('month', $4::date) + interval '1 month')::date)
        OR ($3 = 'day' AND created_ts::date = $4::date)
      )
    )
),
period_products AS (
  SELECT p.*
  FROM order_products p
  JOIN period_orders o ON o.id = p.order_id
),
period_transactions AS (
  SELECT t.*
  FROM order_transactions t
  JOIN period_orders o ON o.id = t.order_id
)
`;

const toNumber = (value: unknown): number => {
  if (value === null || value === undefined || value === "") {
    return 0;
  }
  return Number(value);
};

const toText = (value: unknown, fallback: string): string => {
  if (value === null || value === undefined) {
    return fallback;
  }
  const normalized = String(value).trim();
  if (normalized.length === 0) {
    return fallback;
  }
  return normalized;
};

const isValidDate = (value: string): boolean => {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return false;
  }
  const date = new Date(`${value}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) {
    return false;
  }
  return true;
};

const toIsoDate = (value: Date): string => {
  return value.toISOString().slice(0, 10);
};

const normalizeStatusList = (statuses: string[]): string[] => {
  const normalized = Array.from(
    new Set(
      statuses
        .map((item) => item.trim().toLowerCase())
        .filter((item) => item.length > 0),
    ),
  );

  if (normalized.length === 0) {
    return ["archived"];
  }

  return normalized;
};

const toTitleCase = (value: string): string => {
  return value
    .split(" ")
    .filter((part) => part.length > 0)
    .map((part) => {
      const head = part.slice(0, 1).toUpperCase();
      const tail = part.slice(1).toLowerCase();
      return `${head}${tail}`;
    })
    .join(" ");
};

const companyDisplayName = (companyId: string, index: number): string => {
  const normalized = companyId.replace(/[_-]+/g, " ").replace(/\s+/g, " ").trim();
  if (normalized.length === 0 || /^\d+$/.test(normalized)) {
    return `Сеть ${index}`;
  }
  return toTitleCase(normalized);
};

const filterRestaurantRowsByPolicy = (
  rows: AnyRow[],
  policy: AccessPolicy,
): RestaurantOption[] => {
  const allOptions: RestaurantOption[] = rows.map((row) => {
    const storeId = toText(row.store_id, "");
    const companyId = toText(row.company_id, "Ресторан");
    return {
      id: storeId,
      label: "",
      companyId,
      storeId,
      ordersCount: toNumber(row.orders_count),
      revenue: toNumber(row.revenue),
    };
  });

  let filtered = allOptions;
  if (policy.visibilityMode === "company") {
    filtered = allOptions.filter((item) => item.companyId === policy.defaultCompanyId);
  }
  if (policy.visibilityMode === "store") {
    filtered = allOptions.filter((item) => item.storeId === policy.defaultStoreId);
  }

  const companyTotals = new Map<string, number>();
  filtered.forEach((item) => {
    const current = companyTotals.get(item.companyId) || 0;
    companyTotals.set(item.companyId, current + 1);
  });

  const companyCounters = new Map<string, number>();
  const companyIndexes = new Map<string, number>();
  const companyLabels = new Map<string, string>();

  const resolveCompanyLabel = (companyId: string): string => {
    const cached = companyLabels.get(companyId);
    if (cached) {
      return cached;
    }

    const index = companyIndexes.size + 1;
    companyIndexes.set(companyId, index);
    const label = companyDisplayName(companyId, index);
    companyLabels.set(companyId, label);
    return label;
  };

  return filtered.map((item) => {
    const current = companyCounters.get(item.companyId) || 0;
    const next = current + 1;
    companyCounters.set(item.companyId, next);
    const companyLabel = resolveCompanyLabel(item.companyId);
    const totalInCompany = companyTotals.get(item.companyId) || 0;

    const label = totalInCompany <= 1
      ? companyLabel
      : `${companyLabel} — Точка ${next}`;

    return {
      ...item,
      label,
    };
  });
};

const getAllRestaurantRows = async (
  pool: Pool,
  includedStatuses: string[],
): Promise<AnyRow[]> => {
  const result = await pool.query(
    `
    SELECT
      store_id,
      COALESCE(NULLIF(company_id, ''), 'Ресторан') AS company_id,
      COUNT(*)::int AS orders_count,
      COALESCE(SUM(price), 0)::double precision AS revenue
    FROM orders
    WHERE store_id IS NOT NULL
      AND BTRIM(store_id) <> ''
      AND COALESCE(NULLIF(LOWER(BTRIM(status)), ''), '') = ANY($1::text[])
    GROUP BY store_id, COALESCE(NULLIF(company_id, ''), 'Ресторан')
    ORDER BY orders_count DESC, store_id
  `,
    [includedStatuses],
  );

  return result.rows as AnyRow[];
};

const resolveDefaultRestaurantId = (
  options: RestaurantOption[],
  policy: AccessPolicy,
): string => {
  if (policy.visibilityMode === "store") {
    return policy.defaultStoreId;
  }

  if (policy.defaultStoreId.length > 0) {
    const exists = options.some((option) => option.id === policy.defaultStoreId);
    if (exists) {
      return policy.defaultStoreId;
    }
  }

  if (options.length > 0) {
    return options[0].id;
  }

  return "";
};

const resolveScope = (
  requestedRestaurantId: string,
  options: RestaurantOption[],
  policy: AccessPolicy,
): Scope => {
  let selectedRestaurantId = "";

  if (policy.visibilityMode === "store") {
    selectedRestaurantId = policy.defaultStoreId;
  } else {
    const requestedExists = options.some((option) => option.id === requestedRestaurantId);
    if (requestedExists) {
      selectedRestaurantId = requestedRestaurantId;
    } else {
      selectedRestaurantId = resolveDefaultRestaurantId(options, policy);
    }
  }

  if (selectedRestaurantId.length > 0) {
    const found = options.find((item) => item.id === selectedRestaurantId);
    if (found) {
      return {
        storeFilter: found.storeId,
        companyFilter: found.companyId,
        selectedRestaurantId,
        selectedRestaurantLabel: found.label,
      };
    }
  }

  if (policy.visibilityMode === "store") {
    return {
      storeFilter: policy.defaultStoreId,
      companyFilter: "",
      selectedRestaurantId: policy.defaultStoreId,
      selectedRestaurantLabel: "Выбранная точка",
    };
  }

  if (policy.visibilityMode === "company") {
    const companyLabel = companyDisplayName(policy.defaultCompanyId, 1);
    return {
      storeFilter: "",
      companyFilter: policy.defaultCompanyId,
      selectedRestaurantId: "",
      selectedRestaurantLabel: `${companyLabel} — все точки`,
    };
  }

  return {
    storeFilter: "",
    companyFilter: "",
    selectedRestaurantId: "",
    selectedRestaurantLabel: "Все рестораны",
  };
};

const resolveAnchorDate = async (
  pool: Pool,
  scope: Scope,
  requestedAnchorDate: string,
  includedStatuses: string[],
  timeZone: string,
): Promise<string> => {
  if (isValidDate(requestedAnchorDate)) {
    return requestedAnchorDate;
  }

  const result = await pool.query(
    `
    WITH scoped AS (
      SELECT
        CASE
          WHEN created_at IS NOT NULL
            AND BTRIM(created_at) <> ''
            AND pg_input_is_valid(created_at, 'timestamptz')
          THEN (created_at::timestamptz AT TIME ZONE $4)
          ELSE NULL
        END AS created_ts
      FROM orders
      WHERE ($1 = '' OR store_id = $1)
        AND ($2 = '' OR company_id = $2)
        AND COALESCE(NULLIF(LOWER(BTRIM(status)), ''), '') = ANY($3::text[])
    )
    SELECT TO_CHAR(COALESCE(MAX(created_ts)::date, CURRENT_DATE), 'YYYY-MM-DD') AS anchor_date
    FROM scoped
    `,
    [scope.storeFilter, scope.companyFilter, includedStatuses, timeZone],
  );

  const row = result.rows[0] as AnyRow;
  const anchorDate = toText(row.anchor_date, "");
  if (isValidDate(anchorDate)) {
    return anchorDate;
  }
  return toIsoDate(new Date());
};

const periodDisplayLabel = (mode: PeriodMode, anchorDate: string, fromDate: string, toDate: string): string => {
  if (mode === "day") {
    return `День: ${anchorDate}`;
  }

  if (mode === "month") {
    return `Месяц: ${anchorDate.slice(0, 7)}`;
  }

  return `Весь период: ${fromDate} - ${toDate}`;
};

const toUtcDate = (value: string): Date => {
  return new Date(`${value}T00:00:00Z`);
};

const toMonthLabel = (value: Date): string => {
  return `${value.getUTCFullYear()}-${String(value.getUTCMonth() + 1).padStart(2, "0")}`;
};

const addUtcDays = (value: Date, days: number): Date => {
  return new Date(Date.UTC(value.getUTCFullYear(), value.getUTCMonth(), value.getUTCDate() + days));
};

const addUtcMonths = (value: Date, months: number): Date => {
  return new Date(Date.UTC(value.getUTCFullYear(), value.getUTCMonth() + months, 1));
};

const toIsoDateUtc = (value: Date): string => {
  return value.toISOString().slice(0, 10);
};

const mergeTimelineRows = (rows: AnyRow[]): Map<string, StatsPoint> => {
  const merged = new Map<string, StatsPoint>();

  rows.forEach((row) => {
    const label = toText(row.label, "");
    if (label.length === 0) {
      return;
    }

    const orders = toNumber(row.orders);
    const revenue = toNumber(row.revenue);
    const avgCheck = toNumber(row.avg_check);

    const existing = merged.get(label);
    if (!existing) {
      merged.set(label, {
        label,
        orders,
        revenue,
        avgCheck,
      });
      return;
    }

    const nextOrders = existing.orders + orders;
    const avgNumerator = existing.avgCheck * existing.orders + avgCheck * orders;
    merged.set(label, {
      label,
      orders: nextOrders,
      revenue: existing.revenue + revenue,
      avgCheck: nextOrders > 0 ? avgNumerator / nextOrders : 0,
    });
  });

  return merged;
};

const buildContinuousTimeline = (
  mode: PeriodMode,
  anchorDate: string,
  fromDate: string,
  toDate: string,
  rows: AnyRow[],
): StatsPoint[] => {
  const merged = mergeTimelineRows(rows);
  const resolvePoint = (label: string): StatsPoint => {
    const found = merged.get(label);
    if (found) {
      return found;
    }
    return {
      label,
      orders: 0,
      revenue: 0,
      avgCheck: 0,
    };
  };

  if (mode === "day") {
    const points: StatsPoint[] = [];
    for (let hour = 0; hour < 24; hour += 1) {
      points.push(resolvePoint(`${String(hour).padStart(2, "0")}:00`));
    }
    return points;
  }

  if (mode === "month") {
    const monthStart = toUtcDate(`${anchorDate.slice(0, 7)}-01`);
    const nextMonth = addUtcMonths(monthStart, 1);
    const points: StatsPoint[] = [];

    for (let cursor = monthStart; cursor < nextMonth; cursor = addUtcDays(cursor, 1)) {
      points.push(resolvePoint(toIsoDateUtc(cursor)));
    }

    return points;
  }

  const safeFromDate = isValidDate(fromDate) ? fromDate : anchorDate;
  const safeToDate = isValidDate(toDate) ? toDate : anchorDate;

  const fromMonthStart = toUtcDate(`${safeFromDate.slice(0, 7)}-01`);
  const toMonthStart = toUtcDate(`${safeToDate.slice(0, 7)}-01`);

  const points: StatsPoint[] = [];
  for (let cursor = fromMonthStart; cursor <= toMonthStart; cursor = addUtcMonths(cursor, 1)) {
    points.push(resolvePoint(toMonthLabel(cursor)));
  }

  return points;
};

const isLeapYear = (year: number): boolean => {
  return (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0;
};

const ORDERS_PER_HOUR_QUERY = `
WITH scoped_orders AS (
  SELECT
    CASE
      WHEN o.created_at IS NOT NULL
        AND BTRIM(o.created_at) <> ''
        AND pg_input_is_valid(o.created_at, 'timestamptz')
      THEN (o.created_at::timestamptz AT TIME ZONE $4)
      ELSE NULL
    END AS created_ts
  FROM orders o
  WHERE ($1 = '' OR o.store_id = $1)
    AND ($2 = '' OR o.company_id = $2)
    AND COALESCE(NULLIF(LOWER(BTRIM(o.status)), ''), '') = ANY($3::text[])
),
window_orders AS (
  SELECT created_ts
  FROM scoped_orders
  WHERE created_ts IS NOT NULL
    AND ($5::date IS NULL OR created_ts >= $5::date::timestamp)
    AND ($6::date IS NULL OR created_ts < $6::date::timestamp)
),
hourly AS (
  SELECT
    TO_CHAR(DATE_TRUNC('hour', created_ts), 'HH24:00') AS hour_label,
    COUNT(*)::int AS orders_count
  FROM window_orders
  GROUP BY 1
),
bounds AS (
  SELECT
    COUNT(*)::int AS total_orders,
    COALESCE(
      CEIL(EXTRACT(EPOCH FROM (MAX(created_ts) - MIN(created_ts) + INTERVAL '1 hour')) / 3600.0)::int,
      0
    ) AS active_hours
  FROM window_orders
),
busiest AS (
  SELECT hour_label, orders_count
  FROM hourly
  ORDER BY orders_count DESC, hour_label
  LIMIT 1
)
SELECT
  b.total_orders,
  CASE
    WHEN $7::int > 0 THEN $7::int
    ELSE b.active_hours
  END AS hours_in_window,
  COALESCE(bs.hour_label, '-') AS busiest_hour,
  COALESCE(bs.orders_count, 0)::int AS busiest_hour_orders
FROM bounds b
LEFT JOIN busiest bs ON TRUE
`;

const ORDERS_PER_DAY_QUERY = `
WITH scoped_orders AS (
  SELECT
    CASE
      WHEN o.created_at IS NOT NULL
        AND BTRIM(o.created_at) <> ''
        AND pg_input_is_valid(o.created_at, 'timestamptz')
      THEN (o.created_at::timestamptz AT TIME ZONE $4)
      ELSE NULL
    END AS created_ts
  FROM orders o
  WHERE ($1 = '' OR o.store_id = $1)
    AND ($2 = '' OR o.company_id = $2)
    AND COALESCE(NULLIF(LOWER(BTRIM(o.status)), ''), '') = ANY($3::text[])
),
window_orders AS (
  SELECT created_ts
  FROM scoped_orders
  WHERE created_ts IS NOT NULL
    AND ($5::date IS NULL OR created_ts >= $5::date::timestamp)
    AND ($6::date IS NULL OR created_ts < $6::date::timestamp)
),
daily AS (
  SELECT
    TO_CHAR(DATE_TRUNC('day', created_ts), 'YYYY-MM-DD') AS day_label,
    COUNT(*)::int AS orders_count
  FROM window_orders
  GROUP BY 1
),
bounds AS (
  SELECT
    COUNT(*)::int AS total_orders,
    COALESCE((MAX(created_ts)::date - MIN(created_ts)::date + 1)::int, 0) AS active_days
  FROM window_orders
),
busiest AS (
  SELECT day_label, orders_count
  FROM daily
  ORDER BY orders_count DESC, day_label
  LIMIT 1
)
SELECT
  b.total_orders,
  CASE
    WHEN $7::int > 0 THEN $7::int
    ELSE b.active_days
  END AS days_in_window,
  COALESCE(bs.day_label, '-') AS busiest_day,
  COALESCE(bs.orders_count, 0)::int AS busiest_day_orders
FROM bounds b
LEFT JOIN busiest bs ON TRUE
`;

const loadOrdersPerHourWindow = async (
  pool: Pool,
  scope: Scope,
  includedStatuses: string[],
  timeZone: string,
  startDate: string | null,
  endDateExclusive: string | null,
  fixedHours: number,
): Promise<OrdersPerHourWindow> => {
  const result = await pool.query(ORDERS_PER_HOUR_QUERY, [
    scope.storeFilter,
    scope.companyFilter,
    includedStatuses,
    timeZone,
    startDate,
    endDateExclusive,
    fixedHours,
  ]);

  const row = result.rows[0] as AnyRow;
  const totalOrders = toNumber(row.total_orders);
  const hoursInWindow = Math.max(toNumber(row.hours_in_window), 0);

  return {
    totalOrders,
    hoursInWindow,
    avgPerHour: hoursInWindow > 0 ? totalOrders / hoursInWindow : 0,
    busiestHour: toText(row.busiest_hour, "-"),
    busiestHourOrders: toNumber(row.busiest_hour_orders),
  };
};

const loadOrdersPerHourStats = async (
  pool: Pool,
  scope: Scope,
  includedStatuses: string[],
  timeZone: string,
  anchorDate: string,
): Promise<DashboardStats["ordersPerHour"]> => {
  const anchor = toUtcDate(anchorDate);
  const year = anchor.getUTCFullYear();
  const month = anchor.getUTCMonth() + 1;

  const yearStart = `${year}-01-01`;
  const yearEnd = `${year + 1}-01-01`;

  const monthStart = `${year}-${String(month).padStart(2, "0")}-01`;
  const nextMonthDate = addUtcMonths(toUtcDate(monthStart), 1);
  const monthEnd = toIsoDateUtc(nextMonthDate);

  const dayStart = anchorDate;
  const dayEnd = toIsoDateUtc(addUtcDays(anchor, 1));

  const monthDays = new Date(Date.UTC(year, month, 0)).getUTCDate();
  const yearHours = (isLeapYear(year) ? 366 : 365) * 24;
  const monthHours = monthDays * 24;

  const [allTime, yearScope, monthScope, dayScope] = await Promise.all([
    loadOrdersPerHourWindow(pool, scope, includedStatuses, timeZone, null, null, 0),
    loadOrdersPerHourWindow(pool, scope, includedStatuses, timeZone, yearStart, yearEnd, yearHours),
    loadOrdersPerHourWindow(pool, scope, includedStatuses, timeZone, monthStart, monthEnd, monthHours),
    loadOrdersPerHourWindow(pool, scope, includedStatuses, timeZone, dayStart, dayEnd, 24),
  ]);

  return {
    allTime,
    year: yearScope,
    month: monthScope,
    day: dayScope,
  };
};

const loadOrdersPerDayWindow = async (
  pool: Pool,
  scope: Scope,
  includedStatuses: string[],
  timeZone: string,
  startDate: string | null,
  endDateExclusive: string | null,
  fixedDays: number,
): Promise<OrdersPerDayWindow> => {
  const result = await pool.query(ORDERS_PER_DAY_QUERY, [
    scope.storeFilter,
    scope.companyFilter,
    includedStatuses,
    timeZone,
    startDate,
    endDateExclusive,
    fixedDays,
  ]);

  const row = result.rows[0] as AnyRow;
  const totalOrders = toNumber(row.total_orders);
  const daysInWindow = Math.max(toNumber(row.days_in_window), 0);

  return {
    totalOrders,
    daysInWindow,
    avgPerDay: daysInWindow > 0 ? totalOrders / daysInWindow : 0,
    busiestDay: toText(row.busiest_day, "-"),
    busiestDayOrders: toNumber(row.busiest_day_orders),
  };
};

const loadOrdersPerDayStats = async (
  pool: Pool,
  scope: Scope,
  includedStatuses: string[],
  timeZone: string,
  anchorDate: string,
): Promise<DashboardStats["ordersPerDay"]> => {
  const anchor = toUtcDate(anchorDate);
  const year = anchor.getUTCFullYear();
  const month = anchor.getUTCMonth() + 1;

  const yearStart = `${year}-01-01`;
  const yearEnd = `${year + 1}-01-01`;

  const monthStart = `${year}-${String(month).padStart(2, "0")}-01`;
  const nextMonthDate = addUtcMonths(toUtcDate(monthStart), 1);
  const monthEnd = toIsoDateUtc(nextMonthDate);

  const dayStart = anchorDate;
  const dayEnd = toIsoDateUtc(addUtcDays(anchor, 1));

  const monthDays = new Date(Date.UTC(year, month, 0)).getUTCDate();
  const yearDays = isLeapYear(year) ? 366 : 365;

  const [allTime, yearScope, monthScope, dayScope] = await Promise.all([
    loadOrdersPerDayWindow(pool, scope, includedStatuses, timeZone, null, null, 0),
    loadOrdersPerDayWindow(pool, scope, includedStatuses, timeZone, yearStart, yearEnd, yearDays),
    loadOrdersPerDayWindow(pool, scope, includedStatuses, timeZone, monthStart, monthEnd, monthDays),
    loadOrdersPerDayWindow(pool, scope, includedStatuses, timeZone, dayStart, dayEnd, 1),
  ]);

  return {
    allTime,
    year: yearScope,
    month: monthScope,
    day: dayScope,
  };
};

export const loadRestaurantsPayload = async (
  pool: Pool,
  policy: AccessPolicy,
  includedStatuses: string[],
): Promise<RestaurantsPayload> => {
  const rows = await getAllRestaurantRows(pool, normalizeStatusList(includedStatuses));
  const options = filterRestaurantRowsByPolicy(rows, policy);
  const defaultRestaurantId = resolveDefaultRestaurantId(options, policy);
  const canSelectRestaurant = options.length > 1 && policy.visibilityMode !== "store";

  return {
    options,
    canSelectRestaurant,
    defaultRestaurantId,
  };
};

export const loadDashboardStats = async (
  pool: Pool,
  request: StatsRequest,
): Promise<DashboardStats> => {
  const includedStatuses = normalizeStatusList(request.includedStatuses);
  const rows = await getAllRestaurantRows(pool, includedStatuses);
  const options = filterRestaurantRowsByPolicy(rows, request.accessPolicy);
  const scope = resolveScope(request.requestedRestaurantId, options, request.accessPolicy);
  const anchorDate = await resolveAnchorDate(
    pool,
    scope,
    request.requestedAnchorDate,
    includedStatuses,
    request.timeZone,
  );

  const params = [
    scope.storeFilter,
    scope.companyFilter,
    request.requestedPeriodMode,
    anchorDate,
    includedStatuses,
    request.timeZone,
  ];

  const summaryPromise = pool.query(
    `${BASE_CTE}
    SELECT
      (SELECT COUNT(*)::int FROM period_orders) AS orders_count,
      (SELECT COALESCE(SUM(price), 0)::double precision FROM period_orders) AS revenue,
      (SELECT COALESCE(AVG(price), 0)::double precision FROM period_orders) AS avg_check,
      (SELECT COALESCE(SUM(cash), 0)::double precision FROM period_orders) AS cash,
      (SELECT COALESCE(SUM(bonus), 0)::double precision FROM period_orders) AS bonus,
      (SELECT COALESCE(SUM(margin), 0)::double precision FROM period_orders) AS margin,
      (SELECT COALESCE(SUM(COALESCE(count, 1)), 0)::double precision FROM period_products) AS items_sold,
      (SELECT COUNT(*)::int FROM period_transactions) AS payments_count
    `,
    params,
  );

  const periodInfoPromise = pool.query(
    `${BASE_CTE}
    SELECT
      TO_CHAR(COALESCE(MIN(created_ts)::date, $4::date), 'YYYY-MM-DD') AS from_date,
      TO_CHAR(COALESCE(MAX(created_ts)::date, $4::date), 'YYYY-MM-DD') AS to_date
    FROM period_orders
    `,
    params,
  );

  const timelinePromise = pool.query(
    `${BASE_CTE},
    grouped AS (
      SELECT
        CASE
          WHEN $3 = 'all' THEN DATE_TRUNC('month', created_ts)
          WHEN $3 = 'month' THEN DATE_TRUNC('day', created_ts)
          ELSE DATE_TRUNC('hour', created_ts)
        END AS bucket,
        COUNT(*)::int AS orders,
        COALESCE(SUM(price), 0)::double precision AS revenue,
        COALESCE(AVG(price), 0)::double precision AS avg_check
      FROM period_orders
      WHERE created_ts IS NOT NULL
      GROUP BY 1
    )
    SELECT
      CASE
        WHEN $3 = 'all' THEN TO_CHAR(bucket, 'YYYY-MM')
        WHEN $3 = 'month' THEN TO_CHAR(bucket, 'YYYY-MM-DD')
        ELSE TO_CHAR(bucket, 'HH24:00')
      END AS label,
      orders,
      revenue,
      avg_check
    FROM grouped
    ORDER BY bucket
    `,
    params,
  );

  const topProductsPromise = pool.query(
    `${BASE_CTE}
    SELECT
      COALESCE(NULLIF(name_rus, ''), NULLIF(name_kaz, ''), NULLIF(name_eng, ''), 'Без названия') AS name,
      COALESCE(SUM(COALESCE(count, 1)), 0)::double precision AS units,
      COALESCE(SUM(price * COALESCE(count, 1)), 0)::double precision AS revenue
    FROM period_products
    GROUP BY 1
    ORDER BY revenue DESC, units DESC, name
    LIMIT ${TOP_PRODUCTS_LIMIT}
    `,
    params,
  );

  const topProductsByPointPromise = pool.query(
    `${BASE_CTE},
    product_buckets AS (
      SELECT
        CASE
          WHEN $3 = 'all' THEN TO_CHAR(DATE_TRUNC('month', o.created_ts), 'YYYY-MM')
          WHEN $3 = 'month' THEN TO_CHAR(DATE_TRUNC('day', o.created_ts), 'YYYY-MM-DD')
          ELSE TO_CHAR(DATE_TRUNC('hour', o.created_ts), 'HH24:00')
        END AS label,
        COALESCE(NULLIF(p.name_rus, ''), NULLIF(p.name_kaz, ''), NULLIF(p.name_eng, ''), 'Без названия') AS name,
        COALESCE(SUM(COALESCE(p.count, 1)), 0)::double precision AS units,
        COALESCE(SUM(p.price * COALESCE(p.count, 1)), 0)::double precision AS revenue
      FROM period_products p
      JOIN period_orders o ON o.id = p.order_id
      WHERE o.created_ts IS NOT NULL
      GROUP BY 1, 2
    ),
    ranked AS (
      SELECT
        label,
        name,
        units,
        revenue,
        ROW_NUMBER() OVER (PARTITION BY label ORDER BY units DESC, revenue DESC, name) AS rank_no
      FROM product_buckets
    )
    SELECT
      label,
      name,
      units,
      revenue
    FROM ranked
    WHERE rank_no <= 6
    ORDER BY label, rank_no
    `,
    params,
  );

  const recentOrdersPromise = pool.query(
    `${BASE_CTE}
    SELECT
      COALESCE(TO_CHAR(created_ts, 'YYYY-MM-DD"T"HH24:MI:SS'), '-') AS created_at,
      COALESCE(NULLIF(status, ''), 'Без статуса') AS status,
      COALESCE(NULLIF(payment_type, ''), 'Не указано') AS payment_type,
      COALESCE(price, 0)::double precision AS total
    FROM period_orders
    ORDER BY created_ts DESC NULLS LAST, id DESC
    LIMIT 15
    `,
    params,
  );

  const ordersPerDayPromise = loadOrdersPerDayStats(pool, scope, includedStatuses, request.timeZone, anchorDate);
  const ordersPerHourPromise = loadOrdersPerHourStats(pool, scope, includedStatuses, request.timeZone, anchorDate);

  const [
    summaryRes,
    periodInfoRes,
    timelineRes,
    topProductsRes,
    topProductsByPointRes,
    recentOrdersRes,
    ordersPerDay,
    ordersPerHour,
  ] = await Promise.all([
    summaryPromise,
    periodInfoPromise,
    timelinePromise,
    topProductsPromise,
    topProductsByPointPromise,
    recentOrdersPromise,
    ordersPerDayPromise,
    ordersPerHourPromise,
  ]);

  const summaryRow = summaryRes.rows[0] as AnyRow;
  const periodRow = periodInfoRes.rows[0] as AnyRow;

  const fromDate = toText(periodRow.from_date, anchorDate);
  const toDate = toText(periodRow.to_date, anchorDate);
  const topProductsByPointMap = new Map<string, ProductTop[]>();
  (topProductsByPointRes.rows as AnyRow[]).forEach((row) => {
    const label = toText(row.label, "");
    if (label.length === 0) {
      return;
    }
    const current = topProductsByPointMap.get(label) || [];
    current.push({
      name: toText(row.name, "Без названия"),
      units: toNumber(row.units),
      revenue: toNumber(row.revenue),
    });
    topProductsByPointMap.set(label, current);
  });

  const topProductsByPoint: TimelineTopProducts[] = Array.from(topProductsByPointMap.entries()).map(
    ([label, items]) => ({
      label,
      items,
    }),
  );

  const timeline = buildContinuousTimeline(
    request.requestedPeriodMode,
    anchorDate,
    fromDate,
    toDate,
    timelineRes.rows as AnyRow[],
  );

  let chartGranularity: "month" | "day" | "hour" = "month";
  if (request.requestedPeriodMode === "month") {
    chartGranularity = "day";
  }
  if (request.requestedPeriodMode === "day") {
    chartGranularity = "hour";
  }

  return {
    generatedAt: new Date().toISOString(),
    filter: {
      restaurantId: scope.selectedRestaurantId,
      restaurantLabel: scope.selectedRestaurantLabel,
    },
    period: {
      mode: request.requestedPeriodMode,
      anchorDate,
      fromDate,
      toDate,
      displayLabel: periodDisplayLabel(request.requestedPeriodMode, anchorDate, fromDate, toDate),
      chartGranularity,
    },
    summary: {
      ordersCount: toNumber(summaryRow.orders_count),
      revenue: toNumber(summaryRow.revenue),
      avgCheck: toNumber(summaryRow.avg_check),
      itemsSold: toNumber(summaryRow.items_sold),
      cash: toNumber(summaryRow.cash),
      bonus: toNumber(summaryRow.bonus),
      margin: toNumber(summaryRow.margin),
      paymentsCount: toNumber(summaryRow.payments_count),
    },
    notes: {
      margin: "Маржа берется из поля orders.margin (из API). Мы не рассчитываем ее из себестоимости.",
      timeZone: "UTC+5 (Asia/Almaty)",
      orderStatusScope: `Статусы в расчетах: ${includedStatuses.join(", ")}`,
      dataScope: `Скоуп данных: company_id=${scope.companyFilter || "*"}, store_id=${scope.storeFilter || "*"}`,
    },
    quality: {
      withoutFinishedAt: 0,
      withoutClientId: 0,
      withoutProducts: 0,
    },
    charts: {
      timeline,
      topProductsByPoint,
      statusBreakdown: [],
      paymentBreakdown: [],
      unitVsNetwork: {
        unitLabel: "Продажи точки",
        networkLabel: "Средние продажи по сети",
        points: [],
      },
    },
    ordersPerHour,
    ordersPerDay,
    topProducts: (topProductsRes.rows as AnyRow[]).map((row) => ({
      name: toText(row.name, "Без названия"),
      units: toNumber(row.units),
      revenue: toNumber(row.revenue),
    })),
    recentOrders: (recentOrdersRes.rows as AnyRow[]).map((row) => ({
      createdAt: toText(row.created_at, "-"),
      status: toText(row.status, "Без статуса"),
      paymentType: toText(row.payment_type, "Не указано"),
      total: toNumber(row.total),
    })),
  };
};
