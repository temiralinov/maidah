const app = document.getElementById("app");
const restaurantSelect = document.getElementById("restaurantSelect");
const periodSelect = document.getElementById("periodSelect");
const dateControl = document.getElementById("dateControl");
const dateInput = document.getElementById("dateInput");
const applyButton = document.getElementById("applyButton");
const selectedRestaurantEl = document.getElementById("selectedRestaurant");
const selectedPeriodEl = document.getElementById("selectedPeriod");
const scopeNoteEl = document.getElementById("scopeNote");
const updatedAtEl = document.getElementById("updatedAt");

const STORAGE_RESTAURANT = "stats_restaurant";
const STORAGE_PERIOD = "stats_period";
const STORAGE_MONTH = "stats_month";
const STORAGE_DAY = "stats_day";

const PERIODS = new Set(["all", "month", "day"]);

const numberFmt = new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 2 });
const moneyFmt = new Intl.NumberFormat("ru-RU", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const dateFmt = new Intl.DateTimeFormat("ru-RU", { dateStyle: "medium", timeStyle: "short" });
const monthShortFmt = new Intl.DateTimeFormat("ru-RU", { month: "short" });

const COLORS = {
  black: "#111111",
  white: "#ffffff",
  red: "#c40018",
  green: "#0f7a2f",
  grid: "rgba(17, 17, 17, 0.2)",
};

const state = {
  options: [],
  canSelectRestaurant: true,
  defaultRestaurantId: "",
  selectedRestaurantId: "",
  selectedPeriod: "all",
  selectedMonth: "",
  selectedDay: "",
  lastAnchorDate: "",
};

const chartInstances = [];

const escapeHtml = (value) =>
  String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");

const fmtNum = (value) => numberFmt.format(Number(value || 0));
const fmtMoney = (value) => moneyFmt.format(Number(value || 0));

const fmtDateTime = (value) => {
  if (!value) {
    return "-";
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return String(value);
  }

  return dateFmt.format(parsed);
};

const isMonth = (value) => /^\d{4}-\d{2}$/.test(value);
const isDate = (value) => /^\d{4}-\d{2}-\d{2}$/.test(value);

const parseDate = (value) => {
  if (isMonth(value)) {
    return new Date(`${value}-01T00:00:00Z`);
  }
  if (isDate(value)) {
    return new Date(`${value}T00:00:00Z`);
  }
  return null;
};

const toUpperMonth = (date) => {
  return monthShortFmt.format(date).replace(".", "").toUpperCase();
};

const axisLabel = (point, granularity) => {
  if (granularity === "month") {
    const parsed = parseDate(point.label);
    if (parsed) {
      const year = String(parsed.getUTCFullYear()).slice(-2);
      return `${toUpperMonth(parsed)} ${year}`;
    }
  }

  if (granularity === "day") {
    const parsed = parseDate(point.label);
    if (parsed) {
      const day = String(parsed.getUTCDate()).padStart(2, "0");
      const month = String(parsed.getUTCMonth() + 1).padStart(2, "0");
      return `${day}.${month}`;
    }
  }

  return point.label;
};

const tooltipTitle = (point, mode, anchorDate) => {
  if (mode === "all") {
    const parsed = parseDate(point.label);
    if (parsed) {
      return `${toUpperMonth(parsed)} ${parsed.getUTCFullYear()}`;
    }
  }

  if (mode === "month") {
    const parsed = parseDate(point.label);
    if (parsed) {
      const day = String(parsed.getUTCDate()).padStart(2, "0");
      const month = String(parsed.getUTCMonth() + 1).padStart(2, "0");
      return `${day}.${month}.${parsed.getUTCFullYear()}`;
    }
  }

  if (mode === "day") {
    return `${anchorDate} ${point.label}`;
  }

  return point.label;
};

const yMoneyTick = (value) => {
  const amount = Number(value || 0);
  const thousands = amount / 1000;
  if (Math.abs(thousands) >= 1) {
    return `${numberFmt.format(thousands)} тыс.`;
  }
  return fmtNum(amount);
};

const card = (title, value, tone) => {
  return `
    <article class="metric-card">
      <h3>${escapeHtml(title)}</h3>
      <div class="metric-value ${escapeHtml(tone)}">${escapeHtml(value)}</div>
    </article>
  `;
};

const panel = (title, body, wide = false) => {
  const wideClass = wide ? "panel-wide" : "";
  return `
    <section class="panel ${wideClass}">
      <h2>${escapeHtml(title)}</h2>
      ${body}
    </section>
  `;
};

const table = (headers, rows) => {
  const head = headers.map((header) => `<th>${escapeHtml(header)}</th>`).join("");
  let body = "";

  if (rows.length === 0) {
    body = `<tr><td colspan="${headers.length}">Нет данных</td></tr>`;
  } else {
    body = rows
      .map((row) => `<tr>${row.map((cell) => `<td>${escapeHtml(cell)}</td>`).join("")}</tr>`)
      .join("");
  }

  return `
    <div class="table-wrap">
      <table>
        <thead><tr>${head}</tr></thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
};

const destroyCharts = () => {
  while (chartInstances.length > 0) {
    const chart = chartInstances.pop();
    if (chart) {
      chart.destroy();
    }
  }
};

const addChart = (chart) => {
  if (chart) {
    chartInstances.push(chart);
  }
};

const parsePathState = (pathname) => {
  const normalizedPath = pathname.replace(/\/+$/, "") || "/";
  const parts = normalizedPath.split("/").filter((part) => part.length > 0);

  if (parts.length === 0) {
    return {
      period: "all",
      month: "",
      day: "",
    };
  }

  if (parts.length === 1 && parts[0] === "all") {
    return {
      period: "all",
      month: "",
      day: "",
    };
  }

  if (parts.length === 2 && parts[0] === "month" && isMonth(parts[1])) {
    return {
      period: "month",
      month: parts[1],
      day: `${parts[1]}-01`,
    };
  }

  if (parts.length === 2 && parts[0] === "day" && isDate(parts[1])) {
    return {
      period: "day",
      month: parts[1].slice(0, 7),
      day: parts[1],
    };
  }

  return {
    period: "",
    month: "",
    day: "",
  };
};

const readUrlState = () => {
  const pathState = parsePathState(window.location.pathname);
  const params = new URLSearchParams(window.location.search);
  const rawRestaurant = String(params.get("restaurant") || "").trim();
  if (PERIODS.has(pathState.period)) {
    return {
      period: pathState.period,
      restaurant: rawRestaurant,
      month: pathState.month,
      day: pathState.day,
    };
  }

  const rawPeriod = String(params.get("period") || "").trim();
  const period = PERIODS.has(rawPeriod) ? rawPeriod : "";
  const rawDate = String(params.get("date") || "").trim();

  return {
    period,
    restaurant: rawRestaurant,
    month: isMonth(rawDate) ? rawDate : isDate(rawDate) ? rawDate.slice(0, 7) : "",
    day: isDate(rawDate) ? rawDate : "",
  };
};

const persistLocalState = () => {
  localStorage.setItem(STORAGE_RESTAURANT, state.selectedRestaurantId);
  localStorage.setItem(STORAGE_PERIOD, state.selectedPeriod);
  localStorage.setItem(STORAGE_MONTH, state.selectedMonth);
  localStorage.setItem(STORAGE_DAY, state.selectedDay);
};

const syncUrlState = (pushHistory) => {
  const params = new URLSearchParams();
  let nextPath = "/all";
  if (state.selectedPeriod === "month") {
    const month = state.selectedMonth || (state.lastAnchorDate ? state.lastAnchorDate.slice(0, 7) : "");
    if (month) {
      nextPath = `/month/${month}`;
    } else {
      nextPath = "/month";
    }
  }

  if (state.selectedPeriod === "day") {
    const day = state.selectedDay || state.lastAnchorDate;
    if (day) {
      nextPath = `/day/${day}`;
    } else {
      nextPath = "/day";
    }
  }

  if (state.canSelectRestaurant && state.selectedRestaurantId) {
    params.set("restaurant", state.selectedRestaurantId);
  }

  const nextSearch = params.toString();
  const nextUrl = nextSearch.length > 0 ? `${nextPath}?${nextSearch}` : nextPath;

  if (pushHistory) {
    window.history.pushState(null, "", nextUrl);
  } else {
    window.history.replaceState(null, "", nextUrl);
  }
};

const setDateInputMode = () => {
  if (state.selectedPeriod === "all") {
    dateControl.style.display = "none";
    dateInput.disabled = true;
    return;
  }

  dateControl.style.display = "grid";

  if (state.selectedPeriod === "month") {
    dateInput.type = "month";
    dateInput.value = state.selectedMonth;
  } else {
    dateInput.type = "date";
    dateInput.value = state.selectedDay;
  }

  dateInput.disabled = applyButton.disabled;
};

const updateDateStateFromInput = () => {
  if (state.selectedPeriod === "month" && isMonth(dateInput.value)) {
    state.selectedMonth = dateInput.value;
  }

  if (state.selectedPeriod === "day" && isDate(dateInput.value)) {
    state.selectedDay = dateInput.value;
  }
};

const buildApiQuery = () => {
  const params = new URLSearchParams();

  if (state.selectedRestaurantId) {
    params.set("restaurant", state.selectedRestaurantId);
  }

  params.set("period", state.selectedPeriod);

  if (state.selectedPeriod === "month") {
    const month = state.selectedMonth || (state.lastAnchorDate ? state.lastAnchorDate.slice(0, 7) : "");
    if (month) {
      params.set("date", month);
    }
  }

  if (state.selectedPeriod === "day") {
    const day = state.selectedDay || state.lastAnchorDate;
    if (day) {
      params.set("date", day);
    }
  }

  return params.toString();
};

const syncStateFromStats = (stats) => {
  state.lastAnchorDate = stats.period.anchorDate;
  state.selectedPeriod = stats.period.mode;
  state.selectedMonth = stats.period.anchorDate.slice(0, 7);
  state.selectedDay = stats.period.anchorDate;
  state.selectedRestaurantId = stats.filter.restaurantId || "";

  periodSelect.value = state.selectedPeriod;
  restaurantSelect.value = state.selectedRestaurantId;

  setDateInputMode();
  persistLocalState();
};

const setLoadingState = (loading) => {
  applyButton.disabled = loading;
  periodSelect.disabled = loading;
  restaurantSelect.disabled = loading || !state.canSelectRestaurant;
  applyButton.textContent = loading ? "Загрузка..." : "Показать";
  setDateInputMode();
};

const showLoading = () => {
  destroyCharts();
  updatedAtEl.textContent = "Обновлено: считаем отчет...";
  app.innerHTML = panel(
    "Загрузка",
    `
      <div class="loading-state" role="status" aria-live="polite">
        <div class="loading-spinner" aria-hidden="true"></div>
        <div class="loading-copy">
          <strong>Считаем статистику по архиву...</strong>
          <p>При большом объеме данных это может занять немного времени.</p>
        </div>
      </div>
    `,
    true,
  );
};

const showError = () => {
  setLoadingState(false);
  destroyCharts();
  app.innerHTML = panel(
    "Нет данных",
    `<div class="notice">Сейчас не получилось получить отчет. Попробуйте обновить страницу позже.</div>`,
    true,
  );
};

const applyDrilldown = async (stats, point) => {
  if (stats.period.mode === "all" && isMonth(point.label)) {
    state.selectedPeriod = "month";
    state.selectedMonth = point.label;
    state.selectedDay = `${point.label}-01`;
    periodSelect.value = "month";
    setDateInputMode();
    await loadStats({ pushHistory: true });
    return;
  }

  if (stats.period.mode === "month" && isDate(point.label)) {
    state.selectedPeriod = "day";
    state.selectedDay = point.label;
    state.selectedMonth = point.label.slice(0, 7);
    periodSelect.value = "day";
    setDateInputMode();
    await loadStats({ pushHistory: true });
  }
};

const buildTimelineChart = (stats) => {
  const timeline = Array.isArray(stats.charts.timeline) ? stats.charts.timeline : [];
  if (timeline.length === 0) {
    return;
  }

  const ChartRef = window.Chart;
  if (typeof ChartRef !== "function") {
    return;
  }

  const canvas = document.getElementById("timelineChart");
  if (!canvas) {
    return;
  }

  const labels = timeline.map((point) => axisLabel(point, stats.period.chartGranularity));
  const revenue = timeline.map((point) => Number(point.revenue || 0));
  const orders = timeline.map((point) => Number(point.orders || 0));
  const avgCheck = timeline.map((point) => Number(point.avgCheck || 0));
  const topProductsByPointMap = new Map();
  const topProductsByPoint = Array.isArray(stats?.charts?.topProductsByPoint)
    ? stats.charts.topProductsByPoint
    : [];

  topProductsByPoint.forEach((entry) => {
    if (!entry || typeof entry.label !== "string") {
      return;
    }
    const items = Array.isArray(entry.items) ? entry.items : [];
    topProductsByPointMap.set(entry.label, items);
  });

  const canDrill = stats.period.mode === "all" || stats.period.mode === "month";

  const chart = new ChartRef(canvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Выручка",
          data: revenue,
          yAxisID: "yRevenue",
          borderColor: COLORS.red,
          backgroundColor: COLORS.red,
          borderWidth: 2.8,
          tension: 0.24,
          pointRadius: 3,
          pointHoverRadius: 3,
          pointBackgroundColor: COLORS.red,
          pointBorderColor: COLORS.white,
          pointBorderWidth: 1,
          spanGaps: true,
          fill: false,
        },
        {
          label: "Продажи",
          data: orders,
          yAxisID: "yOrders",
          borderColor: COLORS.green,
          backgroundColor: COLORS.green,
          borderWidth: 2.8,
          tension: 0.24,
          pointRadius: 3,
          pointHoverRadius: 3,
          pointBackgroundColor: COLORS.green,
          pointBorderColor: COLORS.white,
          pointBorderWidth: 1,
          spanGaps: true,
          fill: false,
        },
        {
          label: "Средний чек",
          data: avgCheck,
          yAxisID: "yAvg",
          borderColor: COLORS.black,
          backgroundColor: COLORS.black,
          borderWidth: 2,
          tension: 0.24,
          pointRadius: 2,
          pointHoverRadius: 2,
          pointBackgroundColor: COLORS.black,
          pointBorderColor: COLORS.white,
          pointBorderWidth: 1,
          spanGaps: true,
          borderDash: [6, 4],
          fill: false,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: {
        mode: "index",
        intersect: false,
      },
      onClick: (_event, elements) => {
        if (!canDrill || !elements.length) {
          return;
        }

        const index = elements[0].index;
        const point = timeline[index];
        window.setTimeout(() => {
          applyDrilldown(stats, point).catch(() => {
            showError();
          });
        }, 0);
      },
      onHover: (event, elements) => {
        const target = event?.native?.target;
        if (!target || typeof target.style === "undefined") {
          return;
        }
        target.style.cursor = canDrill && elements.length > 0 ? "pointer" : "default";
      },
      plugins: {
        legend: {
          display: false,
        },
        tooltip: {
          backgroundColor: COLORS.white,
          titleColor: COLORS.black,
          bodyColor: COLORS.black,
          borderColor: COLORS.black,
          borderWidth: 1,
          callbacks: {
            title: (items) => {
              if (!items || items.length === 0) {
                return "";
              }
              const point = timeline[items[0].dataIndex];
              return tooltipTitle(point, stats.period.mode, stats.period.anchorDate);
            },
            label: (ctx) => {
              if (ctx.dataset.yAxisID === "yRevenue") {
                return `Выручка: ${fmtMoney(ctx.parsed.y)}`;
              }
              if (ctx.dataset.yAxisID === "yAvg") {
                return `Средний чек: ${fmtMoney(ctx.parsed.y)}`;
              }
              return `Продажи: ${fmtNum(ctx.parsed.y)}`;
            },
            afterBody: (items) => {
              if (!items || items.length === 0) {
                return [];
              }

              const point = timeline[items[0].dataIndex];
              const topItems = topProductsByPointMap.get(point.label) || [];
              if (topItems.length === 0) {
                return [];
              }

              const lines = ["Топ 6 товаров:"];
              topItems.slice(0, 6).forEach((item, index) => {
                lines.push(`${index + 1}. ${item.name} — ${fmtNum(item.units)}`);
              });
              return lines;
            },
          },
        },
      },
      scales: {
        x: {
          ticks: {
            color: COLORS.black,
            minRotation: 45,
            maxRotation: 45,
            autoSkip: true,
            maxTicksLimit: 22,
          },
          grid: {
            color: COLORS.grid,
          },
          border: {
            color: COLORS.black,
          },
        },
        yRevenue: {
          type: "linear",
          position: "left",
          beginAtZero: true,
          ticks: {
            color: COLORS.red,
            callback: (value) => yMoneyTick(value),
          },
          grid: {
            color: COLORS.grid,
          },
          border: {
            color: COLORS.red,
          },
        },
        yOrders: {
          type: "linear",
          position: "right",
          beginAtZero: true,
          ticks: {
            color: COLORS.green,
            callback: (value) => fmtNum(value),
          },
          grid: {
            drawOnChartArea: false,
          },
          border: {
            color: COLORS.green,
          },
        },
        yAvg: {
          type: "linear",
          position: "right",
          offset: true,
          beginAtZero: true,
          ticks: {
            color: COLORS.black,
            callback: (value) => fmtMoney(value),
          },
          grid: {
            drawOnChartArea: false,
          },
          border: {
            color: COLORS.black,
          },
        },
      },
    },
  });

  addChart(chart);
};

const ordersPerHourCard = (title, data) => {
  return `
    <article class="orders-hour-card">
      <strong>${escapeHtml(title)}</strong>
      <div>Заказов: ${escapeHtml(fmtNum(data.totalOrders))}</div>
      <div>Среднее: <span class="accent">${escapeHtml(fmtNum(data.avgPerHour))}</span> / час</div>
      <div>Пиковый час: ${escapeHtml(data.busiestHour)} (${escapeHtml(fmtNum(data.busiestHourOrders))})</div>
    </article>
  `;
};

const ordersPerDayCard = (title, data) => {
  return `
    <article class="orders-hour-card">
      <strong>${escapeHtml(title)}</strong>
      <div>Заказов: ${escapeHtml(fmtNum(data.totalOrders))}</div>
      <div>Среднее: <span class="accent">${escapeHtml(fmtNum(data.avgPerDay))}</span> / день</div>
      <div>Пиковый день: ${escapeHtml(data.busiestDay)} (${escapeHtml(fmtNum(data.busiestDayOrders))})</div>
    </article>
  `;
};

const renderDashboard = (stats) => {
  syncStateFromStats(stats);

  selectedRestaurantEl.textContent = `Ресторан: ${stats.filter.restaurantLabel}`;
  selectedPeriodEl.textContent = `Период: ${stats.period.displayLabel}`;
  scopeNoteEl.textContent = stats.notes?.dataScope || "Скоуп данных: -";
  updatedAtEl.textContent = `Обновлено: ${fmtDateTime(stats.generatedAt)}`;

  const timeline = Array.isArray(stats.charts.timeline) ? stats.charts.timeline : [];

  const drillHint = stats.period.mode === "all"
    ? "Нажмите на месяц, чтобы открыть статистику по дням."
    : stats.period.mode === "month"
      ? "Нажмите на день, чтобы открыть статистику по часам."
      : "Почасовой режим выбранного дня.";

  const archiveBody = `
    <div class="metric-grid">
      ${card("Продаж", fmtNum(stats.summary.ordersCount), "green")}
      ${card("Выручка", fmtMoney(stats.summary.revenue), "red")}
      ${card("Средний чек", fmtMoney(stats.summary.avgCheck), "black")}
      ${card("Продано позиций", fmtNum(stats.summary.itemsSold), "green")}
    </div>
  `;

  const chartsBody = timeline.length === 0
    ? `<div class="notice">Нет данных для выбранного периода.</div>`
    : `
      <div class="chart-box">
        <div class="legend-row">
          <span class="legend-item"><span class="legend-dot red"></span>Выручка</span>
          <span class="legend-item"><span class="legend-dot green"></span>Продажи</span>
          <span class="legend-item"><span class="legend-dot black"></span>Средний чек</span>
        </div>
        <div class="chart-title">${escapeHtml(stats.period.displayLabel)}</div>
        <div class="chart-stage">
          <canvas class="chart-canvas" id="timelineChart"></canvas>
        </div>
        <div class="inline-note">${escapeHtml(drillHint)}</div>
      </div>
    `;

  const anchorYear = stats.period.anchorDate.slice(0, 4);
  const anchorMonth = stats.period.anchorDate.slice(0, 7);

  const ordersHour = stats.ordersPerHour || {
    allTime: { totalOrders: 0, hoursInWindow: 0, avgPerHour: 0, busiestHour: "-", busiestHourOrders: 0 },
    year: { totalOrders: 0, hoursInWindow: 0, avgPerHour: 0, busiestHour: "-", busiestHourOrders: 0 },
    month: { totalOrders: 0, hoursInWindow: 0, avgPerHour: 0, busiestHour: "-", busiestHourOrders: 0 },
    day: { totalOrders: 0, hoursInWindow: 0, avgPerHour: 0, busiestHour: "-", busiestHourOrders: 0 },
  };

  const ordersHourBody = `
    <div class="orders-hour-grid">
      ${ordersPerHourCard("Все время", ordersHour.allTime)}
      ${ordersPerHourCard(`Год ${anchorYear}`, ordersHour.year)}
      ${ordersPerHourCard(`Месяц ${anchorMonth}`, ordersHour.month)}
      ${ordersPerHourCard(`День ${stats.period.anchorDate}`, ordersHour.day)}
    </div>
  `;

  const ordersDay = stats.ordersPerDay || {
    allTime: { totalOrders: 0, daysInWindow: 0, avgPerDay: 0, busiestDay: "-", busiestDayOrders: 0 },
    year: { totalOrders: 0, daysInWindow: 0, avgPerDay: 0, busiestDay: "-", busiestDayOrders: 0 },
    month: { totalOrders: 0, daysInWindow: 0, avgPerDay: 0, busiestDay: "-", busiestDayOrders: 0 },
    day: { totalOrders: 0, daysInWindow: 0, avgPerDay: 0, busiestDay: "-", busiestDayOrders: 0 },
  };

  const ordersDayBody = `
    <div class="orders-hour-grid">
      ${ordersPerDayCard("Все время", ordersDay.allTime)}
      ${ordersPerDayCard(`Год ${anchorYear}`, ordersDay.year)}
      ${ordersPerDayCard(`Месяц ${anchorMonth}`, ordersDay.month)}
      ${ordersPerDayCard(`День ${stats.period.anchorDate}`, ordersDay.day)}
    </div>
  `;

  const recentOrdersRows = (stats.recentOrders || []).map((item) => [
    fmtDateTime(item.createdAt),
    fmtMoney(item.total),
  ]);
  const topProductsRows = (stats.topProducts || []).map((item) => [
    item.name,
    fmtNum(item.units),
    fmtMoney(item.revenue),
  ]);

  app.innerHTML = [
    panel("Архивные данные", archiveBody, true),
    panel("Динамика заказов", chartsBody, true),
    panel("Количество заказов за час", ordersHourBody, true),
    panel("Количество заказов в день", ordersDayBody, true),
    panel("Топ товаров", table(["Название", "Продано", "Выручка"], topProductsRows), true),
    panel("Последние архивные заказы", table(["Создан", "Сумма"], recentOrdersRows), true),
  ].join("");

  requestAnimationFrame(() => {
    destroyCharts();
    buildTimelineChart(stats);
  });
};

const loadRestaurants = async () => {
  const response = await fetch("/api/restaurants", { cache: "no-store" });
  if (!response.ok) {
    throw new Error("restaurants");
  }

  const payload = await response.json();
  if (!payload || !payload.data) {
    throw new Error("restaurants_payload");
  }

  const data = payload.data;
  state.options = Array.isArray(data.options) ? data.options : [];
  state.canSelectRestaurant = Boolean(data.canSelectRestaurant);
  state.defaultRestaurantId = typeof data.defaultRestaurantId === "string" ? data.defaultRestaurantId : "";

  if (!state.selectedRestaurantId) {
    const savedRestaurant = localStorage.getItem(STORAGE_RESTAURANT) || "";
    const savedExists = state.options.some((item) => item.id === savedRestaurant);

    if (savedExists) {
      state.selectedRestaurantId = savedRestaurant;
    } else {
      state.selectedRestaurantId = state.defaultRestaurantId;
    }
  }

  const optionsMarkup = [];

  if (state.canSelectRestaurant) {
    optionsMarkup.push(`<option value="">Все точки</option>`);
  }

  state.options.forEach((item) => {
    optionsMarkup.push(`<option value="${escapeHtml(item.id)}">${escapeHtml(item.label)}</option>`);
  });

  restaurantSelect.innerHTML = optionsMarkup.join("");
  restaurantSelect.disabled = !state.canSelectRestaurant;

  const selectedExists = state.options.some((item) => item.id === state.selectedRestaurantId);
  if (!selectedExists && !state.canSelectRestaurant && state.options[0]) {
    state.selectedRestaurantId = state.options[0].id;
  }

  restaurantSelect.value = state.selectedRestaurantId;
  setLoadingState(applyButton.disabled);
};

const loadPreferences = () => {
  const urlState = readUrlState();
  const savedPeriod = localStorage.getItem(STORAGE_PERIOD) || "all";
  const savedMonth = localStorage.getItem(STORAGE_MONTH) || "";
  const savedDay = localStorage.getItem(STORAGE_DAY) || "";
  const savedRestaurant = localStorage.getItem(STORAGE_RESTAURANT) || "";

  state.selectedPeriod = PERIODS.has(urlState.period) ? urlState.period : PERIODS.has(savedPeriod) ? savedPeriod : "all";
  state.selectedMonth = urlState.month || savedMonth;
  state.selectedDay = urlState.day || savedDay;
  state.selectedRestaurantId = urlState.restaurant || savedRestaurant;

  periodSelect.value = state.selectedPeriod;
  setDateInputMode();
};

const loadStats = async ({ pushHistory = false } = {}) => {
  setLoadingState(true);
  showLoading();

  try {
    const query = buildApiQuery();
    const url = query.length > 0 ? `/api/stats?${query}` : "/api/stats";

    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) {
      throw new Error("stats");
    }

    const payload = await response.json();
    if (!payload || !payload.data) {
      throw new Error("stats_payload");
    }

    renderDashboard(payload.data);
    syncUrlState(pushHistory);
  } finally {
    setLoadingState(false);
  }
};

const bindEvents = () => {
  restaurantSelect.addEventListener("change", () => {
    state.selectedRestaurantId = restaurantSelect.value;
    state.selectedMonth = "";
    state.selectedDay = "";
    state.lastAnchorDate = "";
    setDateInputMode();
  });

  periodSelect.addEventListener("change", () => {
    state.selectedPeriod = periodSelect.value;

    if (state.lastAnchorDate) {
      state.selectedMonth = state.lastAnchorDate.slice(0, 7);
      state.selectedDay = state.lastAnchorDate;
    }

    setDateInputMode();
  });

  dateInput.addEventListener("change", () => {
    updateDateStateFromInput();
  });

  applyButton.addEventListener("click", async () => {
    try {
      updateDateStateFromInput();
      await loadStats({ pushHistory: true });
    } catch {
      showError();
    }
  });

  window.addEventListener("popstate", async () => {
    try {
      const urlState = readUrlState();
      if (PERIODS.has(urlState.period)) {
        state.selectedPeriod = urlState.period;
      }
      if (urlState.month) {
        state.selectedMonth = urlState.month;
      }
      if (urlState.day) {
        state.selectedDay = urlState.day;
      }
      if (typeof urlState.restaurant === "string") {
        state.selectedRestaurantId = urlState.restaurant;
      }

      periodSelect.value = state.selectedPeriod;
      restaurantSelect.value = state.selectedRestaurantId;
      setDateInputMode();

      await loadStats({ pushHistory: false });
    } catch {
      showError();
    }
  });
};

const init = async () => {
  try {
    loadPreferences();
    await loadRestaurants();
    bindEvents();
    await loadStats({ pushHistory: false });
  } catch {
    showError();
  }
};

init();
