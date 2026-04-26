<script setup>
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import FieldSelect from "./components/FieldSelect.vue";
import IntegerField from "./components/IntegerField.vue";

const PAGE_SIZE = 5;
const MAX_LOG_LINES = 160;

const messages = {
  en: {
    appName: "Metacritic Console",
    labels: {
      language: "Language",
      health: "Health",
      writePlane: "Write plane",
      activeTasks: "Active tasks",
      failedRuns: "Failed runs",
      address: "address",
      database: "database",
      logStream: "log stream",
      mode: "mode",
      orchestrate: "Harvest Control Center",
      list: "List",
      detail: "Detail",
      review: "Review",
      category: "Category",
      metric: "Metric",
      source: "Source",
      pages: "Pages",
      workHref: "Work href",
      limit: "Limit",
      workers: "Workers",
      forceRefresh: "Force refresh",
      type: "Type",
      sentiment: "Sentiment",
      sort: "Sort",
      activeQueue: "Active queue",
      recentRuns: "Recent runs",
      tracked: "tracked",
      kind: "Kind",
      status: "Status",
      run: "Run",
      fallback: "Fallback",
      task: "Task",
      started: "Started",
      refresh: "Refresh",
      previous: "Previous",
      next: "Next",
      workbench: "Workbench",
      runViews: "Run views",
      dataProbe: "Data probe",
      latest: "Latest",
      responseSurface: "Response surface",
      liveFetch: "live fetch",
      signalStream: "Signal stream",
      liveCrawlLogs: "Live crawl logs",
      clear: "Clear",
      latestRun: "Latest run"
    },
    text: {
      mutable: "mutable",
      readOnly: "read-only",
      stable: "stable",
      degraded: "degraded",
      fullStack: "full-stack",
      backendOnly: "backend-only",
      connecting: "connecting",
      live: "live",
      reconnecting: "reconnecting",
      latestProbe: "latest probe",
      detailProbe: "detail probe",
      reviewProbe: "review probe",
      loading: "Loading...",
      dispatchListTask: "Dispatch list task",
      dispatchDetailTask: "Dispatch detail task",
      dispatchReviewTask: "Dispatch review task",
      refreshLatest: "Refresh latest",
      refreshDetail: "Refresh detail",
      refreshReviews: "Refresh reviews",
      optionalSingleWork: "optional single work",
      sortPlaceholder: "date | score | publication",
      latestRunEmpty: "No runs yet",
      writeDisabledHint:
        "Task dispatch is disabled in read-only mode. Restart serve with --enable-write to run crawls from the console.",
      idle: "idle",
      notAvailable: "--",
      fallbackUsed: "used"
    },
    options: {
      category: { game: "Game", movie: "Movie", tv: "TV" },
      metric: { metascore: "Metascore", userscore: "User score", newest: "Newest" },
      source: { api: "API", html: "HTML", auto: "Auto" },
      reviewType: { critic: "Critic", user: "User", all: "All" },
      sentiment: {
        all: "All",
        positive: "Positive",
        neutral: "Neutral",
        negative: "Negative"
      }
    }
  },
  zh: {
    appName: "\u004d\u0065\u0074\u0061\u0063\u0072\u0069\u0074\u0069\u0063\u0020\u91c7\u96c6\u4e2d\u63a7\u53f0",
    labels: {
      language: "\u8bed\u8a00",
      health: "\u5065\u5eb7\u5ea6",
      writePlane: "\u5199\u5165\u5e73\u9762",
      activeTasks: "\u8fd0\u884c\u4e2d\u4efb\u52a1",
      failedRuns: "\u5931\u8d25\u8fd0\u884c",
      address: "\u5730\u5740",
      database: "\u6570\u636e\u5e93",
      logStream: "\u65e5\u5fd7\u6d41",
      mode: "\u6a21\u5f0f",
      orchestrate: "\u91c7\u96c6\u4efb\u52a1\u8c03\u5ea6\u4e2d\u5fc3",
      list: "\u699c\u5355",
      detail: "\u8be6\u60c5",
      review: "\u8bc4\u8bba",
      category: "\u7c7b\u522b",
      metric: "\u6307\u6807",
      source: "\u6765\u6e90",
      pages: "\u9875\u6570",
      workHref: "\u4f5c\u54c1\u94fe\u63a5",
      limit: "\u9650\u5236",
      workers: "\u5e76\u53d1",
      forceRefresh: "\u5f3a\u5236\u5237\u65b0",
      type: "\u7c7b\u578b",
      sentiment: "\u60c5\u7eea",
      sort: "\u6392\u5e8f",
      activeQueue: "\u5f53\u524d\u961f\u5217",
      recentRuns: "\u6700\u8fd1\u8fd0\u884c",
      tracked: "\u6761\u8ddf\u8e2a\u4e2d",
      kind: "\u79cd\u7c7b",
      status: "\u72b6\u6001",
      run: "\u8fd0\u884c",
      fallback: "\u56de\u9000",
      task: "\u4efb\u52a1",
      started: "\u5f00\u59cb\u65f6\u95f4",
      refresh: "\u5237\u65b0",
      previous: "\u4e0a\u4e00\u9875",
      next: "\u4e0b\u4e00\u9875",
      workbench: "\u5de5\u4f5c\u53f0",
      runViews: "\u8fd0\u884c\u89c6\u56fe",
      dataProbe: "\u6570\u636e\u63a2\u9488",
      latest: "\u6700\u65b0\u699c\u5355",
      responseSurface: "\u54cd\u5e94\u9762\u677f",
      liveFetch: "\u5b9e\u65f6\u62c9\u53d6",
      signalStream: "\u4fe1\u53f7\u6d41",
      liveCrawlLogs: "\u5b9e\u65f6\u91c7\u96c6\u65e5\u5fd7",
      clear: "\u6e05\u7a7a",
      latestRun: "\u6700\u65b0\u8fd0\u884c"
    },
    text: {
      mutable: "\u53ef\u5199",
      readOnly: "\u53ea\u8bfb",
      stable: "\u7a33\u5b9a",
      degraded: "\u964d\u7ea7",
      fullStack: "\u5168\u6808",
      backendOnly: "\u4ec5\u540e\u7aef",
      connecting: "\u8fde\u63a5\u4e2d",
      live: "\u5b9e\u65f6",
      reconnecting: "\u91cd\u8fde\u4e2d",
      latestProbe: "\u6700\u65b0\u699c\u5355\u63a2\u9488",
      detailProbe: "\u8be6\u60c5\u63a2\u9488",
      reviewProbe: "\u8bc4\u8bba\u63a2\u9488",
      loading: "\u52a0\u8f7d\u4e2d...",
      dispatchListTask: "\u53d1\u8d77\u699c\u5355\u4efb\u52a1",
      dispatchDetailTask: "\u53d1\u8d77\u8be6\u60c5\u4efb\u52a1",
      dispatchReviewTask: "\u53d1\u8d77\u8bc4\u8bba\u4efb\u52a1",
      refreshLatest: "\u5237\u65b0\u6700\u65b0\u699c\u5355",
      refreshDetail: "\u5237\u65b0\u8be6\u60c5",
      refreshReviews: "\u5237\u65b0\u8bc4\u8bba",
      optionalSingleWork: "\u53ef\u9009\uff0c\u5355\u4f5c\u54c1\u94fe\u63a5",
      sortPlaceholder: "date | score | publication",
      latestRunEmpty: "\u6682\u65e0\u8fd0\u884c\u8bb0\u5f55",
      writeDisabledHint:
        "\u5f53\u524d\u670d\u52a1\u5904\u4e8e\u53ea\u8bfb\u6a21\u5f0f\uff0c\u6240\u4ee5\u4e0d\u80fd\u53d1\u8d77\u4efb\u52a1\u3002\u8bf7\u7528 --enable-write \u91cd\u65b0\u542f\u52a8 serve \u540e\u518d\u4ece\u63a7\u5236\u53f0\u53d1\u8d77\u91c7\u96c6\u3002",
      idle: "\u7a7a\u95f2",
      notAvailable: "--",
      fallbackUsed: "\u5df2\u56de\u9000"
    },
    options: {
      category: { game: "\u6e38\u620f", movie: "\u7535\u5f71", tv: "\u5267\u96c6" },
      metric: {
        metascore: "\u5a92\u4f53\u5206",
        userscore: "\u7528\u6237\u5206",
        newest: "\u6700\u65b0"
      },
      source: { api: "API", html: "HTML", auto: "\u81ea\u52a8" },
      reviewType: { critic: "\u5a92\u4f53", user: "\u7528\u6237", all: "\u5168\u90e8" },
      sentiment: {
        all: "\u5168\u90e8",
        positive: "\u6b63\u5411",
        neutral: "\u4e2d\u6027",
        negative: "\u8d1f\u5411"
      }
    }
  }
};

const tasks = ref([]);
const runs = ref([]);
const queryResult = ref("[]");
const logLines = ref([]);
const serverConfig = ref({
  addr: "127.0.0.1:8080",
  db_path: "output/metacritic.db",
  full_stack: true,
  enable_write: false
});
const health = ref({ ok: false });
const streamState = ref("connecting");
const activeQuery = ref("latest");
const activeTaskTab = ref("list");
const activeCenterTab = ref("ledger");
const locale = ref(localStorage.getItem("mh-console-locale") || "en");
const tasksPage = ref(1);
const runsPage = ref(1);

const listForm = reactive({
  category: "game",
  metric: "metascore",
  source: "api",
  pages: 1
});

const detailForm = reactive({
  category: "game",
  work_href: "",
  source: "api",
  limit: 6,
  concurrency: 1,
  force: false
});

const reviewForm = reactive({
  category: "game",
  review_type: "critic",
  sentiment: "all",
  sort: "",
  limit: 6,
  concurrency: 1
});

const latestQuery = reactive({
  category: "game",
  metric: "metascore",
  limit: 8
});

const detailQuery = reactive({
  category: "game",
  limit: 6
});

const reviewQuery = reactive({
  category: "game",
  review_type: "critic",
  limit: 8
});

let refreshHandle = null;
let eventSource = null;

function currentMessages() {
  return messages[locale.value] || messages.en;
}

function t(path) {
  return path.split(".").reduce((value, key) => value?.[key], currentMessages()) ?? path;
}

function setLocale(nextLocale) {
  locale.value = nextLocale;
  localStorage.setItem("mh-console-locale", nextLocale);
}

function labelFor(group, value) {
  return currentMessages().options[group]?.[value] || value;
}

function optionSet(group, values) {
  return values.map((value) => ({ value, label: labelFor(group, value) }));
}

function translateStreamState(value) {
  switch (value) {
    case "live":
      return t("text.live");
    case "reconnecting":
      return t("text.reconnecting");
    default:
      return t("text.connecting");
  }
}

function translateTaskStatus(value) {
  const table = {
    pending: { en: "pending", zh: "\u7b49\u5f85\u4e2d" },
    running: { en: "running", zh: "\u8fd0\u884c\u4e2d" },
    succeeded: { en: "succeeded", zh: "\u6210\u529f" },
    failed: { en: "failed", zh: "\u5931\u8d25" },
    skipped: { en: "skipped", zh: "\u5df2\u8df3\u8fc7" }
  };
  return table[value]?.[locale.value] || value || t("text.notAvailable");
}

function translateRunStatus(value) {
  const table = {
    running: { en: "running", zh: "\u8fd0\u884c\u4e2d" },
    completed: { en: "completed", zh: "\u5b8c\u6210" },
    failed: { en: "failed", zh: "\u5931\u8d25" }
  };
  return table[value]?.[locale.value] || value || t("text.notAvailable");
}

async function fetchJSON(url, options) {
  const response = await fetch(url, options);
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const op = data.op ? `${data.op}: ` : "";
    throw new Error(`${op}${data.error || `HTTP ${response.status}`}`);
  }
  return data;
}

function pretty(value) {
  return JSON.stringify(value, null, 2);
}

async function loadMeta() {
  const [healthData, configData] = await Promise.all([
    fetchJSON("/healthz"),
    fetchJSON("/api/config")
  ]);
  health.value = healthData;
  serverConfig.value = configData;
}

async function loadTasks() {
  tasks.value = await fetchJSON("/api/tasks");
}

async function loadRuns() {
  runs.value = await fetchJSON("/api/runs?limit=24");
}

async function refreshAll() {
  await Promise.all([loadMeta(), loadTasks(), loadRuns()]);
}

function normalizePayload(payload) {
  const result = {};
  for (const [key, value] of Object.entries(payload)) {
    if (value === "" || value === null || value === undefined) continue;
    result[key] = value;
  }
  return result;
}

async function submitTask(endpoint, payload) {
  try {
    await fetchJSON(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(normalizePayload(payload))
    });
    await Promise.all([loadTasks(), loadRuns()]);
  } catch (error) {
    logLines.value = [`[ui] ${String(error)}`, ...logLines.value].slice(0, MAX_LOG_LINES);
  }
}

async function runQuery(type) {
  const params = new URLSearchParams();
  let endpoint = "/api/latest";
  let payload = latestQuery;
  if (type === "detail") {
    endpoint = "/api/detail";
    payload = detailQuery;
  } else if (type === "review") {
    endpoint = "/api/review";
    payload = reviewQuery;
  }

  for (const [key, value] of Object.entries(normalizePayload(payload))) {
    params.set(key, value);
  }

  queryResult.value = t("text.loading");
  activeQuery.value = type;
  try {
    const data = await fetchJSON(`${endpoint}?${params.toString()}`);
    queryResult.value = pretty(data);
  } catch (error) {
    queryResult.value = String(error);
  }
}

function connectLogs() {
  fetchJSON("/api/logs?limit=120")
    .then((rows) => {
      logLines.value = rows
        .slice()
        .sort((a, b) => String(b.timestamp).localeCompare(String(a.timestamp)))
        .map((row) => `[${row.timestamp}] ${row.message}`)
        .slice(0, MAX_LOG_LINES);
    })
    .catch(() => {
      logLines.value = [];
    });

  if (eventSource) {
    eventSource.close();
  }
  eventSource = new EventSource("/api/logs/stream");
  eventSource.onopen = () => {
    streamState.value = "live";
  };
  eventSource.onerror = () => {
    streamState.value = "reconnecting";
  };
  eventSource.onmessage = (event) => {
    const payload = JSON.parse(event.data);
    logLines.value = [`[${payload.timestamp}] ${payload.message}`, ...logLines.value].slice(
      0,
      MAX_LOG_LINES
    );
  };
}

const runningTasks = computed(() => tasks.value.filter((task) => task.status === "running"));
const failedRuns = computed(() => runs.value.filter((run) => run.status === "failed"));
const latestRun = computed(() => runs.value[0] || null);
const pagedTasks = computed(() =>
  tasks.value.slice((tasksPage.value - 1) * PAGE_SIZE, tasksPage.value * PAGE_SIZE)
);
const pagedRuns = computed(() =>
  runs.value.slice((runsPage.value - 1) * PAGE_SIZE, runsPage.value * PAGE_SIZE)
);
const tasksPageCount = computed(() => Math.max(1, Math.ceil(tasks.value.length / PAGE_SIZE)));
const runsPageCount = computed(() => Math.max(1, Math.ceil(runs.value.length / PAGE_SIZE)));
const writeModeLabel = computed(() =>
  serverConfig.value.enable_write ? t("text.mutable") : t("text.readOnly")
);
const healthLabel = computed(() => (health.value.ok ? t("text.stable") : t("text.degraded")));
const queryLabel = computed(() => {
  switch (activeQuery.value) {
    case "detail":
      return t("text.detailProbe");
    case "review":
      return t("text.reviewProbe");
    default:
      return t("text.latestProbe");
  }
});
const categoryOptions = computed(() => optionSet("category", ["game", "movie", "tv"]));
const metricOptions = computed(() =>
  optionSet("metric", ["metascore", "userscore", "newest"])
);
const sourceOptions = computed(() => optionSet("source", ["api", "html", "auto"]));
const reviewTypeOptions = computed(() => optionSet("reviewType", ["critic", "user", "all"]));
const sentimentOptions = computed(() =>
  optionSet("sentiment", ["all", "positive", "neutral", "negative"])
);

watch(tasksPageCount, (next) => {
  if (tasksPage.value > next) tasksPage.value = next;
});

watch(runsPageCount, (next) => {
  if (runsPage.value > next) runsPage.value = next;
});

onMounted(async () => {
  await refreshAll();
  await runQuery("latest");
  connectLogs();
  refreshHandle = window.setInterval(() => {
    refreshAll().catch(() => {});
  }, 5000);
});

onBeforeUnmount(() => {
  if (refreshHandle) {
    window.clearInterval(refreshHandle);
  }
  if (eventSource) {
    eventSource.close();
  }
});
</script>

<template>
  <div class="min-h-screen text-text">
    <div class="mx-auto max-w-[1800px] px-4 py-4 md:px-6">
      <header class="panel-shell scanline relative overflow-hidden px-5 py-5 md:px-6">
        <div class="absolute inset-y-0 right-0 w-1/3 bg-[radial-gradient(circle_at_center,rgba(139,211,255,0.18),transparent_60%)]"></div>
        <div class="relative grid gap-px border border-line bg-line md:grid-cols-[292px_minmax(0,1fr)_320px]">
          <div class="flex items-center gap-3 bg-panel px-4 py-3">
            <span class="status-dot"></span>
            <div class="text-[11px] font-medium tracking-[0.28em] text-accent/80 uppercase">
              {{ t("appName") }}
            </div>
          </div>
          <div class="flex flex-wrap items-center gap-x-5 gap-y-2 bg-panel px-4 py-3 text-sm text-muted">
            <span><span class="text-white">{{ t("labels.database") }}</span> {{ serverConfig.db_path }}</span>
            <span><span class="text-white">{{ t("labels.health") }}</span> {{ healthLabel }}</span>
            <span><span class="text-white">{{ t("labels.writePlane") }}</span> {{ writeModeLabel }}</span>
            <span><span class="text-white">{{ t("labels.activeTasks") }}</span> {{ runningTasks.length }}</span>
            <span><span class="text-white">{{ t("labels.failedRuns") }}</span> {{ failedRuns.length }}</span>
          </div>
          <div class="flex items-center justify-end gap-3 bg-panel px-4 py-3">
            <span class="text-[10px] uppercase tracking-[0.2em] text-muted">{{ t("labels.language") }}</span>
            <div class="inline-grid min-w-[184px] grid-cols-2 gap-px border border-line bg-line">
              <button
                class="bg-bg px-5 py-2 text-xs font-medium transition"
                :class="locale === 'en' ? 'text-white shadow-[inset_0_0_0_1px_rgba(139,211,255,0.35)]' : 'text-muted hover:text-white'"
                @click="setLocale('en')"
              >
                EN
              </button>
              <button
                class="bg-bg px-5 py-2 text-xs font-medium transition"
                :class="locale === 'zh' ? 'text-white shadow-[inset_0_0_0_1px_rgba(139,211,255,0.35)]' : 'text-muted hover:text-white'"
                @click="setLocale('zh')"
              >
                中文
              </button>
            </div>
          </div>
        </div>
      </header>

      <main class="mt-4 grid gap-4 xl:grid-cols-[320px_minmax(0,1fr)_420px]">
        <aside class="space-y-4">
          <section class="panel-shell px-4 py-4">
            <div>
              <h2 class="text-lg font-medium text-white">{{ t("labels.orchestrate") }}</h2>
            </div>

            <div class="glass-rule mt-4"></div>

            <div
              v-if="!serverConfig.enable_write"
              class="mt-4 border border-line bg-bg px-3 py-3 text-sm leading-6 text-muted"
            >
              {{ t("text.writeDisabledHint") }}
            </div>

            <div class="mt-4 space-y-4">
              <div class="grid grid-cols-3 gap-px border border-line bg-line">
                <button
                  class="bg-panel px-3 py-2 text-sm font-medium transition"
                  :class="activeTaskTab === 'list' ? 'text-white' : 'text-muted hover:text-white'"
                  @click="activeTaskTab = 'list'"
                >
                  {{ t("labels.list") }}
                </button>
                <button
                  class="bg-panel px-3 py-2 text-sm font-medium transition"
                  :class="activeTaskTab === 'detail' ? 'text-white' : 'text-muted hover:text-white'"
                  @click="activeTaskTab = 'detail'"
                >
                  {{ t("labels.detail") }}
                </button>
                <button
                  class="bg-panel px-3 py-2 text-sm font-medium transition"
                  :class="activeTaskTab === 'review' ? 'text-white' : 'text-muted hover:text-white'"
                  @click="activeTaskTab = 'review'"
                >
                  {{ t("labels.review") }}
                </button>
              </div>

              <form
                v-if="activeTaskTab === 'list'"
                class="space-y-3"
                @submit.prevent="submitTask('/api/tasks/list', listForm)"
              >
                <div class="grid gap-3">
                  <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                    <span>{{ t("labels.category") }}</span>
                    <FieldSelect v-model="listForm.category" :options="categoryOptions" />
                  </label>
                  <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                    <span>{{ t("labels.metric") }}</span>
                    <FieldSelect v-model="listForm.metric" :options="metricOptions" />
                  </label>
                  <div class="grid grid-cols-[1fr_104px] gap-3">
                    <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                      <span>{{ t("labels.source") }}</span>
                      <FieldSelect v-model="listForm.source" :options="sourceOptions" />
                    </label>
                    <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                      <span>{{ t("labels.pages") }}</span>
                      <IntegerField
                        v-model="listForm.pages"
                        :min="1"
                        class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                      />
                    </label>
                  </div>
                </div>
                <button :disabled="!serverConfig.enable_write" class="w-full border border-accent/40 bg-accent-soft px-3 py-2 text-sm font-medium text-white transition hover:border-accent hover:bg-accent/18 disabled:opacity-40">
                  {{ t("text.dispatchListTask") }}
                </button>
              </form>

              <form
                v-else-if="activeTaskTab === 'detail'"
                class="space-y-3"
                @submit.prevent="submitTask('/api/tasks/detail', detailForm)"
              >
                <div class="grid gap-3">
                  <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                    <span>{{ t("labels.category") }}</span>
                    <FieldSelect v-model="detailForm.category" :options="categoryOptions" />
                  </label>
                  <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                    <span>{{ t("labels.workHref") }}</span>
                    <input v-model="detailForm.work_href" type="text" :placeholder="t('text.optionalSingleWork')" class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong" />
                  </label>
                  <div class="grid grid-cols-3 gap-3">
                    <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                      <span>{{ t("labels.source") }}</span>
                      <FieldSelect v-model="detailForm.source" :options="sourceOptions" />
                    </label>
                    <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                      <span>{{ t("labels.limit") }}</span>
                      <IntegerField
                        v-model="detailForm.limit"
                        :min="0"
                        class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                      />
                    </label>
                    <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                      <span>{{ t("labels.workers") }}</span>
                      <IntegerField
                        v-model="detailForm.concurrency"
                        :min="1"
                        class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                      />
                    </label>
                  </div>
                  <label class="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-muted">
                    <input v-model="detailForm.force" type="checkbox" class="h-4 w-4 rounded-none border border-line bg-bg" />
                    <span>{{ t("labels.forceRefresh") }}</span>
                  </label>
                </div>
                <button :disabled="!serverConfig.enable_write" class="w-full border border-accent/40 bg-accent-soft px-3 py-2 text-sm font-medium text-white transition hover:border-accent hover:bg-accent/18 disabled:opacity-40">
                  {{ t("text.dispatchDetailTask") }}
                </button>
              </form>

              <form
                v-else
                class="space-y-3"
                @submit.prevent="submitTask('/api/tasks/reviews', reviewForm)"
              >
                <div class="grid gap-3">
                  <div class="grid grid-cols-2 gap-3">
                    <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                      <span>{{ t("labels.category") }}</span>
                      <FieldSelect v-model="reviewForm.category" :options="categoryOptions" />
                    </label>
                    <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                      <span>{{ t("labels.type") }}</span>
                      <FieldSelect v-model="reviewForm.review_type" :options="reviewTypeOptions" />
                    </label>
                  </div>
                  <div class="grid grid-cols-2 gap-3">
                    <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                      <span>{{ t("labels.sentiment") }}</span>
                      <FieldSelect v-model="reviewForm.sentiment" :options="sentimentOptions" />
                    </label>
                    <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                      <span>{{ t("labels.sort") }}</span>
                      <input v-model="reviewForm.sort" type="text" :placeholder="t('text.sortPlaceholder')" class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong" />
                    </label>
                  </div>
                  <div class="grid grid-cols-2 gap-3">
                    <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                      <span>{{ t("labels.limit") }}</span>
                      <IntegerField
                        v-model="reviewForm.limit"
                        :min="0"
                        class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                      />
                    </label>
                    <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                      <span>{{ t("labels.workers") }}</span>
                      <IntegerField
                        v-model="reviewForm.concurrency"
                        :min="1"
                        class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                      />
                    </label>
                  </div>
                </div>
                <button :disabled="!serverConfig.enable_write" class="w-full border border-accent/40 bg-accent-soft px-3 py-2 text-sm font-medium text-white transition hover:border-accent hover:bg-accent/18 disabled:opacity-40">
                  {{ t("text.dispatchReviewTask") }}
                </button>
              </form>
            </div>
          </section>
        </aside>

        <section class="space-y-4">
          <section class="panel-shell overflow-hidden">
            <div class="flex items-center justify-between border-b border-line px-4 py-3 md:px-5">
              <div class="section-title text-[10px] text-muted">{{ t("labels.workbench") }}</div>
              <button class="border border-line bg-panel px-3 py-2 text-xs font-medium uppercase tracking-[0.18em] text-muted transition hover:border-line-strong hover:text-white" @click="refreshAll">
                {{ t("labels.refresh") }}
              </button>
            </div>
            <div class="grid grid-cols-2 gap-px border-b border-line bg-line">
              <button class="bg-panel px-4 py-3 text-sm font-medium transition" :class="activeCenterTab === 'ledger' ? 'text-white' : 'text-muted hover:text-white'" @click="activeCenterTab = 'ledger'">
                <span class="block text-center">{{ t("labels.runViews") }}</span>
              </button>
              <button class="bg-panel px-4 py-3 text-sm font-medium transition" :class="activeCenterTab === 'probe' ? 'text-white' : 'text-muted hover:text-white'" @click="activeCenterTab = 'probe'">
                <span class="block text-center">{{ t("labels.dataProbe") }}</span>
              </button>
            </div>

            <div v-if="activeCenterTab === 'ledger'" class="grid gap-px bg-line">
              <div class="bg-panel-strong px-4 py-3">
                <div class="flex items-center justify-between">
                  <div class="section-title text-[10px] text-muted">{{ t("labels.activeQueue") }}</div>
                  <div class="flex items-center gap-2 text-xs text-muted">
                    <span>{{ tasks.length }} {{ t("labels.tracked") }}</span>
                    <button class="border border-line bg-bg px-2 py-1 hover:text-white disabled:opacity-40" :disabled="tasksPage <= 1" @click="tasksPage -= 1">
                      {{ t("labels.previous") }}
                    </button>
                    <span>{{ tasksPage }}/{{ tasksPageCount }}</span>
                    <button class="border border-line bg-bg px-2 py-1 hover:text-white disabled:opacity-40" :disabled="tasksPage >= tasksPageCount" @click="tasksPage += 1">
                      {{ t("labels.next") }}
                    </button>
                  </div>
                </div>
                <div class="tech-scroll mt-3 max-h-[260px] overflow-auto">
                  <table class="min-w-full text-left text-sm">
                    <thead class="text-[10px] uppercase tracking-[0.18em] text-muted">
                      <tr>
                        <th class="pb-2 pr-3 font-normal">{{ t("labels.kind") }}</th>
                        <th class="pb-2 pr-3 font-normal">{{ t("labels.status") }}</th>
                        <th class="pb-2 pr-3 font-normal">{{ t("labels.run") }}</th>
                        <th class="pb-2 font-normal">{{ t("labels.fallback") }}</th>
                      </tr>
                    </thead>
                    <tbody class="align-top text-[#d4e1ff]">
                      <tr v-for="task in pagedTasks" :key="task.id" class="border-t border-white/4">
                        <td class="py-2 pr-3 font-medium">{{ task.kind }}</td>
                        <td class="py-2 pr-3 text-muted">{{ translateTaskStatus(task.status) }}</td>
                        <td class="py-2 pr-3 font-mono text-[12px]">{{ task.run_id || t("text.notAvailable") }}</td>
                        <td class="py-2 text-muted">{{ task.fallback_used ? task.fallback_reason || t("text.fallbackUsed") : t("text.notAvailable") }}</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>

              <div class="bg-panel px-4 py-3">
                <div class="flex items-center justify-between">
                  <div class="section-title text-[10px] text-muted">{{ t("labels.recentRuns") }}</div>
                  <div class="flex items-center gap-2 text-xs text-muted">
                    <span>{{ latestRun ? latestRun.run_id : t("text.idle") }}</span>
                    <button class="border border-line bg-bg px-2 py-1 hover:text-white disabled:opacity-40" :disabled="runsPage <= 1" @click="runsPage -= 1">
                      {{ t("labels.previous") }}
                    </button>
                    <span>{{ runsPage }}/{{ runsPageCount }}</span>
                    <button class="border border-line bg-bg px-2 py-1 hover:text-white disabled:opacity-40" :disabled="runsPage >= runsPageCount" @click="runsPage += 1">
                      {{ t("labels.next") }}
                    </button>
                  </div>
                </div>
                <div class="tech-scroll mt-3 max-h-[260px] overflow-auto">
                  <table class="min-w-full text-left text-sm">
                    <thead class="text-[10px] uppercase tracking-[0.18em] text-muted">
                      <tr>
                        <th class="pb-2 pr-3 font-normal">{{ t("labels.task") }}</th>
                        <th class="pb-2 pr-3 font-normal">{{ t("labels.status") }}</th>
                        <th class="pb-2 pr-3 font-normal">{{ t("labels.category") }}</th>
                        <th class="pb-2 font-normal">{{ t("labels.started") }}</th>
                      </tr>
                    </thead>
                    <tbody class="align-top text-[#d4e1ff]">
                      <tr v-for="run in pagedRuns" :key="run.run_id" class="border-t border-white/4">
                        <td class="py-2 pr-3">
                          <div class="font-medium">{{ run.task_name }}</div>
                          <div class="font-mono text-[11px] text-muted">{{ run.run_id }}</div>
                        </td>
                        <td class="py-2 pr-3 text-muted">{{ translateRunStatus(run.status) }}</td>
                        <td class="py-2 pr-3 text-muted">{{ run.category || labelFor('reviewType', 'all') }}</td>
                        <td class="py-2 text-muted">{{ run.started_at }}</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>

            <div v-else class="grid gap-px bg-line xl:grid-cols-[280px_minmax(0,1fr)]">
              <div class="bg-panel-strong px-4 py-4">
                <div class="mt-3 grid grid-cols-3 gap-px border border-line bg-line">
                  <button class="px-3 py-2 text-sm transition" :class="activeQuery === 'latest' ? 'border-accent/50 bg-accent-soft text-white' : 'bg-bg text-muted hover:text-white'" @click="runQuery('latest')">
                    <span class="block text-center">{{ t("labels.latest") }}</span>
                  </button>
                  <button class="px-3 py-2 text-sm transition" :class="activeQuery === 'detail' ? 'border-accent/50 bg-accent-soft text-white' : 'bg-bg text-muted hover:text-white'" @click="runQuery('detail')">
                    <span class="block text-center">{{ t("labels.detail") }}</span>
                  </button>
                  <button class="px-3 py-2 text-sm transition" :class="activeQuery === 'review' ? 'border-accent/50 bg-accent-soft text-white' : 'bg-bg text-muted hover:text-white'" @click="runQuery('review')">
                    <span class="block text-center">{{ t("labels.review") }}</span>
                  </button>
                </div>

                <div class="glass-rule my-4"></div>

                <form v-if="activeQuery === 'latest'" class="space-y-3" @submit.prevent="runQuery('latest')">
                  <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                    <span>{{ t("labels.category") }}</span>
                    <FieldSelect v-model="latestQuery.category" :options="categoryOptions" />
                  </label>
                  <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                    <span>{{ t("labels.metric") }}</span>
                    <FieldSelect v-model="latestQuery.metric" :options="metricOptions" />
                  </label>
                  <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                    <span>{{ t("labels.limit") }}</span>
                    <IntegerField
                      v-model="latestQuery.limit"
                      :min="1"
                      class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                    />
                  </label>
                  <button class="w-full border border-line bg-bg px-3 py-2 text-sm font-medium text-white hover:border-line-strong">
                    {{ t("text.refreshLatest") }}
                  </button>
                </form>

                <form v-else-if="activeQuery === 'detail'" class="space-y-3" @submit.prevent="runQuery('detail')">
                  <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                    <span>{{ t("labels.category") }}</span>
                    <FieldSelect v-model="detailQuery.category" :options="categoryOptions" />
                  </label>
                  <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                    <span>{{ t("labels.limit") }}</span>
                    <IntegerField
                      v-model="detailQuery.limit"
                      :min="1"
                      class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                    />
                  </label>
                  <button class="w-full border border-line bg-bg px-3 py-2 text-sm font-medium text-white hover:border-line-strong">
                    {{ t("text.refreshDetail") }}
                  </button>
                </form>

                <form v-else class="space-y-3" @submit.prevent="runQuery('review')">
                  <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                    <span>{{ t("labels.category") }}</span>
                    <FieldSelect v-model="reviewQuery.category" :options="categoryOptions" />
                  </label>
                  <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                    <span>{{ t("labels.type") }}</span>
                    <FieldSelect v-model="reviewQuery.review_type" :options="reviewTypeOptions" />
                  </label>
                  <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                    <span>{{ t("labels.limit") }}</span>
                    <IntegerField
                      v-model="reviewQuery.limit"
                      :min="1"
                      class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                    />
                  </label>
                  <button class="w-full border border-line bg-bg px-3 py-2 text-sm font-medium text-white hover:border-line-strong">
                    {{ t("text.refreshReviews") }}
                  </button>
                </form>
              </div>

              <div class="bg-panel px-4 py-4">
                <div class="flex items-center justify-between">
                  <div>
                    <div class="section-title text-[10px] text-muted">{{ queryLabel }}</div>
                    <h3 class="mt-1 text-base font-medium text-white">{{ t("labels.responseSurface") }}</h3>
                  </div>
                  <div class="text-xs text-muted">{{ t("labels.liveFetch") }}</div>
                </div>
                <pre class="tech-scroll mt-4 max-h-[420px] overflow-auto whitespace-pre-wrap break-words font-mono text-[12px] leading-6 text-[#bfd4ff]">{{ queryResult }}</pre>
              </div>
            </div>
          </section>
        </section>

        <aside class="space-y-4">
          <section class="panel-shell px-4 py-4">
            <div class="section-title text-[10px] text-muted">{{ t("labels.signalStream") }}</div>
            <div class="mt-2 flex items-center justify-between">
              <h2 class="text-lg font-medium text-white">{{ t("labels.liveCrawlLogs") }}</h2>
              <button class="border border-line bg-bg px-3 py-2 text-xs font-medium uppercase tracking-[0.18em] text-muted hover:border-line-strong hover:text-white" @click="logLines = []">
                {{ t("labels.clear") }}
              </button>
            </div>
            <div class="mt-3 border border-line bg-bg px-3 py-3 text-sm text-muted">
              <div class="text-[11px] uppercase tracking-[0.18em] text-muted">{{ t("labels.latestRun") }}</div>
              <div class="mt-2 text-sm text-white">{{ latestRun ? latestRun.task_name : t("text.latestRunEmpty") }}</div>
              <div class="mt-1 font-mono text-[12px]">{{ latestRun ? latestRun.run_id : t("text.notAvailable") }}</div>
            </div>
            <div class="tech-scroll mt-4 max-h-[520px] overflow-auto border border-line bg-bg">
              <div
                v-for="(line, index) in logLines"
                :key="`${index}-${line}`"
                class="border-b border-white/6 px-3 py-2 font-mono text-[12px] leading-6 text-[#b6d7ff] last:border-b-0"
              >
                {{ line }}
              </div>
            </div>
          </section>
        </aside>
      </main>
    </div>
  </div>
</template>

