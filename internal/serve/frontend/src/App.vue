<script setup>
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import FieldSelect from "./components/FieldSelect.vue";
import IntegerField from "./components/IntegerField.vue";

const PAGE_SIZE = 10;
const MAX_LOG_LINES = 160;

const messages = {
  en: {
    appName: "Metacritic Console",
    labels: {
      language: "Language",
      health: "Health",
      database: "Database",
      writePlane: "Write plane",
      activeTasks: "Active tasks",
      failedRuns: "Failed runs",
      orchestrate: "Harvest Control Center",
      list: "List",
      detail: "Detail",
      review: "Review",
      latest: "Latest",
      category: "Category",
      metric: "Metric",
      source: "Source",
      pages: "Pages",
      timeout: "Timeout",
      continueOnError: "Continue",
      rps: "RPS",
      burst: "Burst",
      workHref: "Work href",
      limit: "Limit",
      workers: "Workers",
      forceRefresh: "Force refresh",
      type: "Type",
      sentiment: "Sentiment",
      sort: "Sort",
      runs: "Runs",
      tasks: "Tasks",
      exports: "Exports",
      batchSchedule: "Guide",
      recentRuns: "Recent runs",
      taskList: "Task ledger",
      profile: "Profile",
      format: "Format",
      filterKey: "Filter key",
      reviewType: "Review type",
      platform: "Platform",
      runId: "Run ID",
      exportLatest: "Export latest",
      exportDetail: "Export detail",
      exportReview: "Export review",
      batchFiles: "Batch files",
      scheduleFiles: "Schedule files",
      exportNotes: "Export guide",
      opsGuide: "Console guide",
      liveLogs: "Live crawl logs",
      collect: "Collect",
      clear: "Clear",
      previous: "Previous",
      next: "Next",
      refresh: "Refresh",
      status: "Status",
      fallback: "Fallback",
      run: "Run",
      kind: "Kind",
      started: "Started",
      updated: "Updated",
      jobs: "Jobs",
      actions: "Actions"
    },
    text: {
      mutable: "mutable",
      readOnly: "read-only",
      stable: "stable",
      degraded: "degraded",
      live: "live",
      connecting: "connecting",
      reconnecting: "reconnecting",
      latestRunEmpty: "No runs yet",
      idle: "idle",
      notAvailable: "--",
      fallbackUsed: "used",
      loading: "Loading...",
      writeDisabledHint:
        "Write controls are disabled in read-only mode. Restart serve with --enable-write to trigger crawls from the console.",
      timeoutPlaceholder: "3h | 90m | 24h",
      optionalSingleWork: "optional single work",
      sortPlaceholder: "date | score | publication",
      runIdPlaceholder: "optional snapshot run id",
      workHrefPlaceholder: "optional normalized work href",
      filterKeyPlaceholder: "optional normalized filter key",
      platformPlaceholder: "optional platform key",
      downloadReady: "Download started",
      exportGuideLatestIntro:
        "Use this for current latest list rows or a specific list snapshot run.",
      exportGuideLatestFields:
        "Start with category and metric. Work href and filter key are optional narrow filters. Run ID switches the export from the latest view to a historical snapshot.",
      exportGuideLatestProfiles:
        "raw and flat return the same list row shape. summary groups by run and filter range with counts and rank span.",
      exportGuideDetailIntro:
        "Use this for game, movie, or TV detail records.",
      exportGuideDetailFields:
        "Category is the main scope. Work href is optional for one title. Run ID switches from the latest detail view to a historical detail snapshot.",
      exportGuideDetailProfiles:
        "raw keeps core fields plus details_json. flat expands the most common extras into analysis-friendly columns. summary returns coverage counts by category and run.",
      exportGuideReviewIntro:
        "Use this for normalized critic or user review rows.",
      exportGuideReviewFields:
        "Category and review type define the base scope. Platform is optional and mainly useful for games. Work href narrows to one title. Run ID switches to review snapshots.",
      exportGuideReviewProfiles:
        "raw keeps source_payload_json. flat removes payload noise and keeps normalized columns. summary groups by run, category, type, and platform with counts and average score.",
      consoleGuideIntro:
        "This console is the local operations surface for collection, state checks, data downloads, and live progress watching.",
      consoleGuideCenter:
        "Harvest Control Center launches list, detail, and review collection tasks. Source selects api, html, or automatic fallback. Pages and limit control breadth, and 0 means collect all candidates. Workers control concurrent requests. Force refresh reruns detail even after a prior success.",
      consoleGuideRuns:
        "Runs shows recent crawl batches written into crawl_runs. Status tells you whether the batch completed or failed. Fallback explains why auto switched away from the primary source when that happened.",
      consoleGuideTasks:
        "Tasks shows the in-process task ledger for this serve instance. Use it to see what is still running and which run ID was attached to each submission.",
      consoleGuideExports:
        "Exports downloads data directly from the backend. Format selects csv or json. Profile selects raw, flat, or summary. Run ID is optional and switches from latest views to snapshot exports.",
      consoleGuideLogs:
        "Live logs stream runtime messages from the active service process. New lines stay at the top so you can follow the newest progress without scrolling to the end."
    },
    options: {
      category: { game: "Game", movie: "Movie", tv: "TV" },
      metric: { metascore: "Metascore", userscore: "User score", newest: "Newest" },
      source: { api: "API", html: "HTML", auto: "Auto" },
      reviewType: { critic: "Critic", user: "User", all: "All" },
      sentiment: { all: "All", positive: "Positive", neutral: "Neutral", negative: "Negative" },
      format: { csv: "CSV", json: "JSON" },
      profile: { raw: "Raw", flat: "Flat", summary: "Summary" }
    }
  },
  zh: {
    appName: "Metacritic Console",
    labels: {
      language: "语言",
      health: "健康度",
      database: "数据库",
      writePlane: "写入平面",
      activeTasks: "运行中任务",
      failedRuns: "失败运行",
      orchestrate: "采集任务调度中心",
      list: "榜单",
      detail: "详情",
      review: "评论",
      latest: "榜单",
      category: "类别",
      metric: "指标",
      source: "来源",
      pages: "页数",
      workHref: "作品链接",
      limit: "限制",
      workers: "并发",
      forceRefresh: "强制刷新",
      type: "类型",
      sentiment: "情绪",
      sort: "排序",
      runs: "运行",
      tasks: "任务",
      exports: "导出",
      batchSchedule: "说明",
      recentRuns: "最近运行",
      taskList: "任务台账",
      profile: "视图",
      format: "格式",
      filterKey: "过滤键",
      reviewType: "评论类型",
      platform: "平台",
      runId: "运行 ID",
      exportLatest: "导出榜单",
      exportDetail: "导出详情",
      exportReview: "导出评论",
      batchFiles: "批量文件",
      scheduleFiles: "调度文件",
      exportNotes: "导出说明",
      opsGuide: "控制台说明",
      liveLogs: "实时采集日志",
      collect: "采集",
      clear: "清空",
      previous: "上一页",
      next: "下一页",
      refresh: "刷新",
      status: "状态",
      fallback: "回退",
      run: "运行",
      kind: "种类",
      started: "开始时间",
      updated: "更新时间",
      jobs: "作业数",
      actions: "操作"
    },
    text: {
      mutable: "可写",
      readOnly: "只读",
      stable: "稳定",
      degraded: "降级",
      live: "实时",
      connecting: "连接中",
      reconnecting: "重连中",
      latestRunEmpty: "暂无运行记录",
      idle: "空闲",
      notAvailable: "--",
      fallbackUsed: "已回退",
      loading: "加载中...",
      writeDisabledHint:
        "当前服务处于只读模式，不能发起采集操作。请使用 --enable-write 重启 serve。",
      optionalSingleWork: "可选，单作品链接",
      sortPlaceholder: "date | score | publication",
      runIdPlaceholder: "可选，快照运行 ID",
      workHrefPlaceholder: "可选，标准化作品链接",
      filterKeyPlaceholder: "可选，标准化过滤键",
      platformPlaceholder: "可选，平台键",
      downloadReady: "已开始下载",
      exportGuideLatestIntro: "用于导出当前最新榜单视图，或某次榜单抓取快照。",
      exportGuideLatestFields: "先填写类别和指标。作品链接与过滤键都是可选缩小范围条件。填写 Run ID 后，会改为导出历史快照而不是当前视图。",
      exportGuideLatestProfiles: "raw 与 flat 对榜单导出等价。summary 会按 run 与过滤范围聚合，返回数量和名次区间。",
      exportGuideDetailIntro: "用于导出游戏、电影、剧集的详情记录。",
      exportGuideDetailFields: "类别是主范围。作品链接可选，用于单作品导出。Run ID 可切换为历史详情快照导出，而不是当前最新详情视图。",
      exportGuideDetailProfiles: "raw 保留核心字段与 details_json。flat 会把常用扩展字段摊平成更适合分析的列。summary 返回按类别和 run 聚合的覆盖率摘要。",
      exportGuideReviewIntro: "用于导出标准化后的媒体或用户评论记录。",
      exportGuideReviewFields: "类别和评论类型决定主范围。平台是可选条件，多用于游戏。作品链接可缩小到单作品。Run ID 可切换为评论快照导出。",
      exportGuideReviewProfiles: "raw 保留 source_payload_json。flat 去掉原始 payload，只保留干净列。summary 会按 run、类别、评论类型、平台聚合统计数量和平均分。",
      consoleGuideIntro: "这个页面是本地运行面板，用来发起采集、查看系统状态、下载数据以及观察实时进度。",
      consoleGuideCenter: "采集控制区用于发起榜单、详情、评论任务。Source 表示 api、html 或自动回退。Pages 和 Limit 控制范围，填 0 表示抓取全部候选。Workers 控制详情或评论的并发。Force refresh 用于忽略已成功状态重新抓取详情。",
      consoleGuideRuns: "Runs 展示写入 crawl_runs 的最近批次。Status 表示批次完成情况。Fallback 列在 auto 发生回退时给出主要原因。",
      consoleGuideTasks: "Tasks 展示当前 serve 进程内的任务台账，适合查看还有哪些任务正在运行，以及每次提交关联到了哪个 run ID。",
      consoleGuideExports: "Exports 直接从后端触发下载。Format 选择 csv 或 json。Profile 选择 raw、flat 或 summary。Run ID 可选，用于从 latest 视图切换到快照导出。",
      consoleGuideLogs: "实时日志会持续展示当前服务进程里的运行消息。新日志在上，方便在采集中快速观察最新进度。"
    },
    options: {
      category: { game: "游戏", movie: "电影", tv: "剧集" },
      metric: { metascore: "媒体分", userscore: "用户分", newest: "最新" },
      source: { api: "API", html: "HTML", auto: "自动" },
      reviewType: { critic: "媒体", user: "用户", all: "全部" },
      sentiment: { all: "全部", positive: "正向", neutral: "中性", negative: "负向" },
      format: { csv: "CSV", json: "JSON" },
      profile: { raw: "原始", flat: "扁平", summary: "摘要" }
    }
  }
};

const serverConfig = ref({
  db_path: "output/metacritic.db",
  enable_write: false
});
const health = ref({ ok: false });
const overview = ref({
  runs: [],
  tasks: [],
  detail_states: { total: 0, by_status: {} },
  review_states: { total: 0, by_status: {} },
  failed_runs: 0,
  exports: []
});
const tasks = ref([]);
const logs = ref([]);
const streamState = ref("connecting");
const locale = ref(localStorage.getItem("mh-console-locale") || "en");

const workspace = ref("runs");
const launcherTab = ref("list");
const exportTab = ref("latest");
const runsPage = ref(1);
const tasksPage = ref(1);
const exportMessage = ref("");
const orchestrationMessage = ref("");

const listForm = reactive({
  category: "game",
  metric: "metascore",
  source: "api",
  pages: 0,
  timeout: "3h",
  continue_on_error: true,
  rps: 2,
  burst: 2
});
const detailForm = reactive({
  category: "game",
  work_href: "",
  source: "api",
  limit: 0,
  concurrency: 1,
  force: false,
  timeout: "3h",
  continue_on_error: true,
  rps: 2,
  burst: 2
});
const reviewForm = reactive({
  category: "game",
  review_type: "critic",
  sentiment: "all",
  sort: "",
  limit: 0,
  concurrency: 1,
  timeout: "3h",
  continue_on_error: true,
  rps: 2,
  burst: 2
});

const latestExport = reactive({
  category: "game",
  metric: "metascore",
  work_href: "",
  filter_key: "",
  run_id: "",
  format: "csv",
  profile: "raw"
});
const detailExport = reactive({
  category: "game",
  work_href: "",
  run_id: "",
  format: "json",
  profile: "raw"
});
const reviewExport = reactive({
  category: "game",
  review_type: "critic",
  platform: "",
  work_href: "",
  run_id: "",
  format: "json",
  profile: "raw"
});

let pollHandle = null;
let logStream = null;

function currentMessages() {
  return messages[locale.value] || messages.en;
}

function t(path) {
  const localized = path.split(".").reduce((value, key) => value?.[key], currentMessages());
  if (localized !== undefined && localized !== null) return localized;
  return path.split(".").reduce((value, key) => value?.[key], messages.en) ?? path;
}

function setLocale(nextLocale) {
  locale.value = nextLocale;
  localStorage.setItem("mh-console-locale", nextLocale);
}

function optionSet(group, values) {
  return values.map((value) => ({
    value,
    label: currentMessages().options[group]?.[value] || value
  }));
}

function translateTaskStatus(value) {
  const table = {
    pending: { en: "pending", zh: "等待中" },
    running: { en: "running", zh: "运行中" },
    succeeded: { en: "succeeded", zh: "成功" },
    failed: { en: "failed", zh: "失败" }
  };
  return table[value]?.[locale.value] || value || t("text.notAvailable");
}

function translateRunStatus(value) {
  const table = {
    running: { en: "running", zh: "运行中" },
    completed: { en: "completed", zh: "完成" },
    failed: { en: "failed", zh: "失败" }
  };
  return table[value]?.[locale.value] || value || t("text.notAvailable");
}

function normalizePayload(payload) {
  const result = {};
  for (const [key, value] of Object.entries(payload)) {
    if (value === "" || value === null || value === undefined) continue;
    result[key] = value;
  }
  return result;
}

async function fetchJSON(url, options) {
  const response = await fetch(url, options);
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const prefix = data.op ? `${data.op}: ` : "";
    throw new Error(`${prefix}${data.error || `HTTP ${response.status}`}`);
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

async function loadOverview() {
  overview.value = await fetchJSON("/api/overview");
  tasks.value = overview.value.tasks || [];
}

async function refreshAll() {
  await Promise.all([loadMeta(), loadOverview()]);
}

async function submitTask(endpoint, payload) {
  try {
    await fetchJSON(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(normalizePayload(payload))
    });
    orchestrationMessage.value = "";
    await refreshAll();
  } catch (error) {
    orchestrationMessage.value = String(error);
  }
}

async function downloadExport(kind, payload) {
  exportMessage.value = "";
  try {
    const params = new URLSearchParams(normalizePayload(payload));
    const response = await fetch(`/api/export/${kind}?${params.toString()}`);
    if (!response.ok) {
      const data = await response.json().catch(() => ({}));
      throw new Error(`${data.op ? `${data.op}: ` : ""}${data.error || response.statusText}`);
    }
    const blob = await response.blob();
    const fileName = extractDownloadFilename(response.headers.get("Content-Disposition")) || `${kind}.txt`;
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = fileName;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    URL.revokeObjectURL(url);
    exportMessage.value = t("text.downloadReady");
  } catch (error) {
    exportMessage.value = String(error);
  }
}

function connectLogs() {
  fetchJSON("/api/logs?limit=120")
    .then((rows) => {
      logs.value = rows
        .slice()
        .sort((a, b) => String(b.timestamp).localeCompare(String(a.timestamp)))
        .map((row) => `[${row.timestamp}] ${row.message}`)
        .slice(0, MAX_LOG_LINES);
    })
    .catch(() => {
      logs.value = [];
    });

  if (logStream) {
    logStream.close();
  }
  logStream = new EventSource("/api/logs/stream");
  logStream.onopen = () => {
    streamState.value = "live";
  };
  logStream.onerror = () => {
    streamState.value = "reconnecting";
  };
  logStream.onmessage = (event) => {
    const payload = JSON.parse(event.data);
    logs.value = [`[${payload.timestamp}] ${payload.message}`, ...logs.value].slice(0, MAX_LOG_LINES);
  };
}

function extractDownloadFilename(header) {
  if (!header) return "";
  const match = /filename=\"?([^\";]+)\"?/i.exec(header);
  return match?.[1] || "";
}

const categoryOptions = computed(() => optionSet("category", ["game", "movie", "tv"]));
const metricOptions = computed(() => optionSet("metric", ["metascore", "userscore", "newest"]));
const sourceOptions = computed(() => optionSet("source", ["api", "html", "auto"]));
const reviewTypeOptions = computed(() => optionSet("reviewType", ["critic", "user", "all"]));
const sentimentOptions = computed(() =>
  optionSet("sentiment", ["all", "positive", "neutral", "negative"])
);
const exportFormatOptions = computed(() => optionSet("format", ["csv", "json"]));
const exportProfileOptions = computed(() => optionSet("profile", ["raw", "flat", "summary"]));

const runningTasks = computed(() => tasks.value.filter((task) => task.status === "running"));
const healthLabel = computed(() => (health.value.ok ? t("text.stable") : t("text.degraded")));
const writeModeLabel = computed(() =>
  serverConfig.value.enable_write ? t("text.mutable") : t("text.readOnly")
);
const runsPageCount = computed(() =>
  Math.max(1, Math.ceil((overview.value.runs?.length || 0) / PAGE_SIZE))
);
const tasksPageCount = computed(() =>
  Math.max(1, Math.ceil((tasks.value?.length || 0) / PAGE_SIZE))
);
const pagedRuns = computed(() =>
  (overview.value.runs || []).slice((runsPage.value - 1) * PAGE_SIZE, runsPage.value * PAGE_SIZE)
);
const pagedTasks = computed(() =>
  (tasks.value || []).slice((tasksPage.value - 1) * PAGE_SIZE, tasksPage.value * PAGE_SIZE)
);

watch(runsPageCount, (next) => {
  if (runsPage.value > next) runsPage.value = next;
});
watch(tasksPageCount, (next) => {
  if (tasksPage.value > next) tasksPage.value = next;
});

onMounted(async () => {
  await refreshAll();
  connectLogs();
  pollHandle = window.setInterval(() => {
    refreshAll().catch(() => {});
  }, 5000);
});

onBeforeUnmount(() => {
  if (pollHandle) window.clearInterval(pollHandle);
  if (logStream) logStream.close();
});
</script>

<template>
  <div class="min-h-screen text-text">
    <div class="mx-auto max-w-[1900px] px-4 py-4 md:px-6">
      <header class="overflow-hidden">
        <div class="grid gap-px border border-line bg-line scanline md:grid-cols-[320px_minmax(0,1fr)_220px]">
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
            <span><span class="text-white">{{ t("labels.failedRuns") }}</span> {{ overview.failed_runs || 0 }}</span>
          </div>
          <div class="flex items-center justify-end gap-3 bg-panel px-4 py-3">
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
            <h2 class="text-lg font-medium text-white">{{ t("labels.orchestrate") }}</h2>
            <div class="glass-rule mt-4"></div>

            <div
              v-if="!serverConfig.enable_write"
              class="mt-4 border border-line bg-bg px-3 py-3 text-sm leading-6 text-muted"
            >
              {{ t("text.writeDisabledHint") }}
            </div>

            <div class="mt-4 grid grid-cols-3 gap-px border border-line bg-line">
              <button
                class="bg-panel px-3 py-2 text-sm font-medium transition"
                :class="launcherTab === 'list' ? 'text-white' : 'text-muted hover:text-white'"
                @click="launcherTab = 'list'"
              >
                {{ t("labels.list") }}
              </button>
              <button
                class="bg-panel px-3 py-2 text-sm font-medium transition"
                :class="launcherTab === 'detail' ? 'text-white' : 'text-muted hover:text-white'"
                @click="launcherTab = 'detail'"
              >
                {{ t("labels.detail") }}
              </button>
              <button
                class="bg-panel px-3 py-2 text-sm font-medium transition"
                :class="launcherTab === 'review' ? 'text-white' : 'text-muted hover:text-white'"
                @click="launcherTab = 'review'"
              >
                {{ t("labels.review") }}
              </button>
            </div>

            <form
              v-if="launcherTab === 'list'"
              class="mt-4 space-y-3"
              @submit.prevent="submitTask('/api/tasks/list', listForm)"
            >
              <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                <span>{{ t("labels.category") }}</span>
                <FieldSelect v-model="listForm.category" :options="categoryOptions" />
              </label>
              <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                <span>{{ t("labels.metric") }}</span>
                <FieldSelect v-model="listForm.metric" :options="metricOptions" />
              </label>
              <div class="grid grid-cols-2 gap-3">
                <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                  <span>{{ t("labels.source") }}</span>
                  <FieldSelect v-model="listForm.source" :options="sourceOptions" />
                </label>
                <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                  <span>{{ t("labels.pages") }}</span>
                  <IntegerField
                    v-model="listForm.pages"
                      :min="0"
                    class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                  />
                </label>
              </div>
              <div class="grid grid-cols-2 gap-3">
                <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                  <span>{{ t("labels.timeout") }}</span>
                  <input
                    v-model="listForm.timeout"
                    type="text"
                    :placeholder="t('text.timeoutPlaceholder')"
                    class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                  />
                </label>
                <label class="flex items-center gap-2 pt-6 text-[11px] uppercase tracking-[0.18em] text-muted">
                  <input
                    v-model="listForm.continue_on_error"
                    type="checkbox"
                    class="h-4 w-4 rounded-none border border-line bg-bg"
                  />
                  <span>{{ t("labels.continueOnError") }}</span>
                </label>
              </div>
              <div class="grid grid-cols-2 gap-3">
                <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                  <span>{{ t("labels.rps") }}</span>
                  <input
                    v-model.number="listForm.rps"
                    type="number"
                    min="0"
                    step="0.1"
                    class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                  />
                </label>
                <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                  <span>{{ t("labels.burst") }}</span>
                  <IntegerField
                    v-model="listForm.burst"
                    :min="1"
                    class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                  />
                </label>
              </div>
              <button
                :disabled="!serverConfig.enable_write"
                class="w-full border border-accent/40 bg-accent-soft px-3 py-2 text-sm font-medium text-white transition hover:border-accent hover:bg-accent/18 disabled:opacity-40"
              >
                {{ t("labels.collect") }}
              </button>
            </form>

            <form
              v-else-if="launcherTab === 'detail'"
              class="mt-4 space-y-3"
              @submit.prevent="submitTask('/api/tasks/detail', detailForm)"
            >
              <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                <span>{{ t("labels.category") }}</span>
                <FieldSelect v-model="detailForm.category" :options="categoryOptions" />
              </label>
              <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                <span>{{ t("labels.workHref") }}</span>
                <input
                  v-model="detailForm.work_href"
                  type="text"
                  :placeholder="t('text.optionalSingleWork')"
                  class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                />
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
              <div class="grid grid-cols-2 gap-3">
                <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                  <span>{{ t("labels.timeout") }}</span>
                  <input
                    v-model="detailForm.timeout"
                    type="text"
                    :placeholder="t('text.timeoutPlaceholder')"
                    class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                  />
                </label>
                <label class="flex items-center gap-2 pt-6 text-[11px] uppercase tracking-[0.18em] text-muted">
                  <input
                    v-model="detailForm.continue_on_error"
                    type="checkbox"
                    class="h-4 w-4 rounded-none border border-line bg-bg"
                  />
                  <span>{{ t("labels.continueOnError") }}</span>
                </label>
              </div>
              <div class="grid grid-cols-2 gap-3">
                <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                  <span>{{ t("labels.rps") }}</span>
                  <input
                    v-model.number="detailForm.rps"
                    type="number"
                    min="0"
                    step="0.1"
                    class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                  />
                </label>
                <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                  <span>{{ t("labels.burst") }}</span>
                  <IntegerField
                    v-model="detailForm.burst"
                    :min="1"
                    class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                  />
                </label>
              </div>
              <label class="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-muted">
                <input v-model="detailForm.force" type="checkbox" class="h-4 w-4 rounded-none border border-line bg-bg" />
                <span>{{ t("labels.forceRefresh") }}</span>
              </label>
              <button
                :disabled="!serverConfig.enable_write"
                class="w-full border border-accent/40 bg-accent-soft px-3 py-2 text-sm font-medium text-white transition hover:border-accent hover:bg-accent/18 disabled:opacity-40"
              >
                {{ t("labels.collect") }}
              </button>
            </form>

            <form
              v-else
              class="mt-4 space-y-3"
              @submit.prevent="submitTask('/api/tasks/reviews', reviewForm)"
            >
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
                  <input
                    v-model="reviewForm.sort"
                    type="text"
                    :placeholder="t('text.sortPlaceholder')"
                    class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                  />
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
              <div class="grid grid-cols-2 gap-3">
                <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                  <span>{{ t("labels.timeout") }}</span>
                  <input
                    v-model="reviewForm.timeout"
                    type="text"
                    :placeholder="t('text.timeoutPlaceholder')"
                    class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                  />
                </label>
                <label class="flex items-center gap-2 pt-6 text-[11px] uppercase tracking-[0.18em] text-muted">
                  <input
                    v-model="reviewForm.continue_on_error"
                    type="checkbox"
                    class="h-4 w-4 rounded-none border border-line bg-bg"
                  />
                  <span>{{ t("labels.continueOnError") }}</span>
                </label>
              </div>
              <div class="grid grid-cols-2 gap-3">
                <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                  <span>{{ t("labels.rps") }}</span>
                  <input
                    v-model.number="reviewForm.rps"
                    type="number"
                    min="0"
                    step="0.1"
                    class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                  />
                </label>
                <label class="space-y-1 text-[11px] uppercase tracking-[0.18em] text-muted">
                  <span>{{ t("labels.burst") }}</span>
                  <IntegerField
                    v-model="reviewForm.burst"
                    :min="1"
                    class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong"
                  />
                </label>
              </div>
              <button
                :disabled="!serverConfig.enable_write"
                class="w-full border border-accent/40 bg-accent-soft px-3 py-2 text-sm font-medium text-white transition hover:border-accent hover:bg-accent/18 disabled:opacity-40"
              >
                {{ t("labels.collect") }}
              </button>
            </form>

            <div v-if="orchestrationMessage" class="mt-4 border border-line bg-bg px-3 py-3 text-sm text-muted">
              {{ orchestrationMessage }}
            </div>
          </section>
        </aside>

        <section class="space-y-4">
          <section class="panel-shell overflow-hidden">
            <div class="flex items-center justify-between border-b border-line px-4 py-3 md:px-5">
              <div class="section-title text-[10px] text-muted">Workbench</div>
              <button
                class="border border-line bg-panel px-3 py-2 text-xs font-medium uppercase tracking-[0.18em] text-muted transition hover:border-line-strong hover:text-white"
                @click="refreshAll"
              >
                {{ t("labels.refresh") }}
              </button>
            </div>
            <div class="grid grid-cols-4 gap-px border-b border-line bg-line">
              <button
                v-for="tab in ['runs', 'tasks', 'exports', 'batchSchedule']"
                :key="tab"
                class="bg-panel px-4 py-3 text-sm font-medium transition"
                :class="workspace === tab ? 'text-white' : 'text-muted hover:text-white'"
                @click="workspace = tab"
              >
                <span class="block text-center">{{ t(`labels.${tab}`) }}</span>
              </button>
            </div>

            <div v-if="workspace === 'runs'" class="bg-panel px-4 py-4">
              <div class="flex items-center justify-between">
                <div></div>
                <div class="flex items-center gap-2 text-xs text-muted">
                  <button class="border border-line bg-bg px-2 py-1 hover:text-white disabled:opacity-40" :disabled="runsPage <= 1" @click="runsPage -= 1">
                    {{ t("labels.previous") }}
                  </button>
                  <span>{{ runsPage }}/{{ runsPageCount }}</span>
                  <button class="border border-line bg-bg px-2 py-1 hover:text-white disabled:opacity-40" :disabled="runsPage >= runsPageCount" @click="runsPage += 1">
                    {{ t("labels.next") }}
                  </button>
                </div>
              </div>
              <div class="tech-scroll mt-3 max-h-[520px] overflow-auto">
                <table class="min-w-full text-left text-sm">
                  <thead class="text-[10px] uppercase tracking-[0.18em] text-muted">
                    <tr>
                      <th class="pb-2 pr-3 font-normal">{{ t("labels.run") }}</th>
                      <th class="pb-2 pr-3 font-normal">{{ t("labels.status") }}</th>
                      <th class="pb-2 pr-3 font-normal">{{ t("labels.category") }}</th>
                      <th class="pb-2 font-normal">{{ t("labels.fallback") }}</th>
                    </tr>
                  </thead>
                  <tbody class="align-top text-[#d4e1ff]">
                    <tr
                      v-for="run in pagedRuns"
                      :key="run.run_id"
                      class="border-t border-white/4 transition hover:bg-white/2"
                    >
                      <td class="py-2 pr-3">
                        <div class="font-medium">{{ run.task_name }}</div>
                        <div class="font-mono text-[11px] text-muted">{{ run.run_id }}</div>
                      </td>
                      <td class="py-2 pr-3 text-muted">{{ translateRunStatus(run.status) }}</td>
                      <td class="py-2 pr-3 text-muted">{{ run.category || t("text.notAvailable") }}</td>
                      <td class="py-2 text-muted">{{ run.error || t("text.notAvailable") }}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            <div v-else-if="workspace === 'tasks'" class="bg-panel px-4 py-4">
              <div class="flex items-center justify-between">
                <div></div>
                <div class="flex items-center gap-2 text-xs text-muted">
                  <button class="border border-line bg-bg px-2 py-1 hover:text-white disabled:opacity-40" :disabled="tasksPage <= 1" @click="tasksPage -= 1">
                    {{ t("labels.previous") }}
                  </button>
                  <span>{{ tasksPage }}/{{ tasksPageCount }}</span>
                  <button class="border border-line bg-bg px-2 py-1 hover:text-white disabled:opacity-40" :disabled="tasksPage >= tasksPageCount" @click="tasksPage += 1">
                    {{ t("labels.next") }}
                  </button>
                </div>
              </div>
              <div class="tech-scroll mt-3 max-h-[520px] overflow-auto">
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
                    <tr
                      v-for="task in pagedTasks"
                      :key="task.id"
                      class="border-t border-white/4 transition hover:bg-white/2"
                    >
                      <td class="py-2 pr-3 font-medium">{{ task.kind }}</td>
                      <td class="py-2 pr-3 text-muted">{{ translateTaskStatus(task.status) }}</td>
                      <td class="py-2 pr-3 font-mono text-[12px]">{{ task.run_id || t("text.notAvailable") }}</td>
                      <td class="py-2 text-muted">{{ task.fallback_used ? task.fallback_reason || t("text.fallbackUsed") : t("text.notAvailable") }}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            <div v-else-if="workspace === 'exports'" class="grid gap-px bg-line xl:grid-cols-[320px_minmax(0,1fr)]">
              <div class="bg-panel-strong px-4 py-4">
                <div class="section-title text-[10px] text-muted">{{ t("labels.exports") }}</div>
                <div class="mt-4 grid grid-cols-3 gap-px border border-line bg-line">
                  <button
                    v-for="tab in ['latest', 'detail', 'review']"
                    :key="tab"
                    class="bg-panel px-3 py-2 text-sm font-medium transition"
                    :class="exportTab === tab ? 'text-white' : 'text-muted hover:text-white'"
                    @click="exportTab = tab"
                  >
                    {{ t(`labels.${tab}`) }}
                  </button>
                </div>

                <form
                  v-if="exportTab === 'latest'"
                  class="mt-4 space-y-3"
                  @submit.prevent="downloadExport('latest', latestExport)"
                >
                  <FieldSelect v-model="latestExport.category" :options="categoryOptions" />
                  <FieldSelect v-model="latestExport.metric" :options="metricOptions" />
                  <div class="grid grid-cols-2 gap-3">
                    <FieldSelect v-model="latestExport.format" :options="exportFormatOptions" />
                    <FieldSelect v-model="latestExport.profile" :options="exportProfileOptions" />
                  </div>
                  <input v-model="latestExport.work_href" type="text" :placeholder="t('text.workHrefPlaceholder')" class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong" />
                  <input v-model="latestExport.filter_key" type="text" :placeholder="t('text.filterKeyPlaceholder')" class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong" />
                  <input v-model="latestExport.run_id" type="text" :placeholder="t('text.runIdPlaceholder')" class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong" />
                  <button class="w-full border border-line bg-bg px-3 py-2 text-sm font-medium text-white hover:border-line-strong">{{ t("labels.exportLatest") }}</button>
                </form>

                <form
                  v-else-if="exportTab === 'detail'"
                  class="mt-4 space-y-3"
                  @submit.prevent="downloadExport('detail', detailExport)"
                >
                  <FieldSelect v-model="detailExport.category" :options="categoryOptions" />
                  <div class="grid grid-cols-2 gap-3">
                    <FieldSelect v-model="detailExport.format" :options="exportFormatOptions" />
                    <FieldSelect v-model="detailExport.profile" :options="exportProfileOptions" />
                  </div>
                  <input v-model="detailExport.work_href" type="text" :placeholder="t('text.workHrefPlaceholder')" class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong" />
                  <input v-model="detailExport.run_id" type="text" :placeholder="t('text.runIdPlaceholder')" class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong" />
                  <button class="w-full border border-line bg-bg px-3 py-2 text-sm font-medium text-white hover:border-line-strong">{{ t("labels.exportDetail") }}</button>
                </form>

                <form v-else class="mt-4 space-y-3" @submit.prevent="downloadExport('review', reviewExport)">
                  <FieldSelect v-model="reviewExport.category" :options="categoryOptions" />
                  <FieldSelect v-model="reviewExport.review_type" :options="reviewTypeOptions.filter((item) => item.value !== 'all')" />
                  <div class="grid grid-cols-2 gap-3">
                    <FieldSelect v-model="reviewExport.format" :options="exportFormatOptions" />
                    <FieldSelect v-model="reviewExport.profile" :options="exportProfileOptions" />
                  </div>
                  <input v-model="reviewExport.platform" type="text" :placeholder="t('text.platformPlaceholder')" class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong" />
                  <input v-model="reviewExport.work_href" type="text" :placeholder="t('text.workHrefPlaceholder')" class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong" />
                  <input v-model="reviewExport.run_id" type="text" :placeholder="t('text.runIdPlaceholder')" class="w-full border border-line bg-bg px-3 py-2 text-sm text-text outline-none transition hover:border-line-strong" />
                  <button class="w-full border border-line bg-bg px-3 py-2 text-sm font-medium text-white hover:border-line-strong">{{ t("labels.exportReview") }}</button>
                </form>

                <div v-if="exportMessage" class="mt-4 border border-line bg-bg px-3 py-3 text-sm text-muted">
                  {{ exportMessage }}
                </div>
              </div>

              <div class="bg-panel px-4 py-4">
                <div class="section-title text-[10px] text-muted">{{ t("labels.exportNotes") }}</div>
                <div class="mt-4 space-y-3 text-sm leading-6 text-muted">
                  <div class="border border-line bg-bg px-3 py-3">
                    <div class="text-white">{{ t(`labels.${exportTab}`) }}</div>
                    <div class="mt-2">
                      {{
                        exportTab === 'latest'
                          ? t("text.exportGuideLatestIntro")
                          : exportTab === 'detail'
                            ? t("text.exportGuideDetailIntro")
                            : t("text.exportGuideReviewIntro")
                      }}
                    </div>
                  </div>
                  <div class="border border-line bg-bg px-3 py-3">
                    <div class="text-white">{{ t("labels.filterKey") }}</div>
                    <div class="mt-2">
                      {{
                        exportTab === 'latest'
                          ? t("text.exportGuideLatestFields")
                          : exportTab === 'detail'
                            ? t("text.exportGuideDetailFields")
                            : t("text.exportGuideReviewFields")
                      }}
                    </div>
                  </div>
                  <div class="border border-line bg-bg px-3 py-3">
                    <div class="text-white">{{ t("labels.profile") }}</div>
                    <div class="mt-2">
                      {{
                        exportTab === 'latest'
                          ? t("text.exportGuideLatestProfiles")
                          : exportTab === 'detail'
                            ? t("text.exportGuideDetailProfiles")
                            : t("text.exportGuideReviewProfiles")
                      }}
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div v-else class="grid gap-px bg-line xl:grid-cols-[320px_minmax(0,1fr)]">
              <div class="bg-panel px-4 py-4 xl:col-span-2">
                <div class="section-title text-[10px] text-muted">{{ t("labels.opsGuide") }}</div>
                <div class="mt-4 grid gap-3 xl:grid-cols-2">
                  <div class="border border-line bg-bg px-3 py-3 text-sm leading-6 text-muted">
                    {{ t("text.consoleGuideIntro") }}
                  </div>
                  <div class="border border-line bg-bg px-3 py-3 text-sm leading-6 text-muted">
                    {{ t("text.consoleGuideCenter") }}
                  </div>
                  <div class="border border-line bg-bg px-3 py-3 text-sm leading-6 text-muted">
                    {{ t("text.consoleGuideRuns") }}
                  </div>
                  <div class="border border-line bg-bg px-3 py-3 text-sm leading-6 text-muted">
                    {{ t("text.consoleGuideTasks") }}
                  </div>
                  <div class="border border-line bg-bg px-3 py-3 text-sm leading-6 text-muted xl:col-span-2">
                    {{ t("text.consoleGuideExports") }}
                  </div>
                  <div class="border border-line bg-bg px-3 py-3 text-sm leading-6 text-muted xl:col-span-2">
                    {{ t("text.consoleGuideLogs") }}
                  </div>
                </div>
              </div>
            </div>
          </section>
        </section>

        <aside class="space-y-4">
          <section class="panel-shell px-4 py-4">
            <div class="mt-2 flex items-center justify-between">
              <div class="text-sm text-muted">{{ streamState === 'live' ? t('text.live') : streamState === 'reconnecting' ? t('text.reconnecting') : t('text.connecting') }}</div>
              <button class="border border-line bg-bg px-3 py-2 text-xs font-medium uppercase tracking-[0.18em] text-muted hover:border-line-strong hover:text-white" @click="logs = []">
                {{ t("labels.clear") }}
              </button>
            </div>
            <div class="mt-3 border border-line bg-bg px-3 py-3 text-sm text-muted">
              <div class="text-[11px] uppercase tracking-[0.18em] text-muted">{{ t("labels.run") }}</div>
              <div class="mt-2 text-sm text-white">{{ overview.runs?.[0] ? overview.runs[0].task_name : t("text.latestRunEmpty") }}</div>
              <div class="mt-1 font-mono text-[12px]">{{ overview.runs?.[0] ? overview.runs[0].run_id : t("text.notAvailable") }}</div>
            </div>
            <div class="tech-scroll mt-4 max-h-[420px] overflow-y-auto overflow-x-hidden border border-line bg-bg">
              <div
                v-for="(line, index) in logs"
                :key="`${index}-${line}`"
                class="whitespace-pre-wrap break-words border-b border-white/8 px-3 py-2 font-mono text-[12px] leading-6 text-[#b6d7ff] last:border-b-0"
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
