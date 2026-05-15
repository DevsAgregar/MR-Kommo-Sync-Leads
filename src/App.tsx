import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import {
  AlertTriangle,
  CheckCircle2,
  ChevronDown,
  ChevronUp,
  ClipboardList,
  Clock3,
  Database,
  History,
  Info,
  LockKeyhole,
  Loader2,
  LogOut,
  Pause,
  Play,
  RefreshCw,
  Send,
  Terminal,
  Timer,
  Users,
  XCircle
} from "lucide-react";
import React, { useCallback, useDeferredValue, useEffect, useMemo, useRef, useState } from "react";

type FieldStat = {
  candidate: number;
  safe_fill: number;
  review_fill: number;
  unmapped: number;
  fill_empty?: number;
  update_if_greater?: number;
  update_if_newer?: number;
  sync_authoritative?: number;
  merge?: number;
  skip?: number;
};

type Snapshot = {
  previewSummary?: {
    match_summary?: {
      patient_count: number;
      lead_count: number;
      exact_unique_match_count: number;
    };
    field_stats?: Record<string, FieldStat>;
    action_counts?: Record<string, number>;
    safe_lead_count?: number;
    safe_field_row_count?: number;
    review_field_row_count?: number;
  } | null;
  safePayloadCount?: number;
  safeRowsCount?: number;
  reviewRowsCount?: number;
  localFiles?: Record<string, { exists: boolean; bytes: number; modifiedUnix?: number | null }>;
};

type CommandState = {
  running: boolean;
  message: string;
  ok: boolean | null;
  task?: string;
  startedAt?: number;
  finishedAt?: number;
};

type ReviewRow = {
  patient_name: string;
  lead_name: string;
  field_label: string;
  candidate_value: string;
  mapped_value: string;
  confidence: string;
  rule: string;
};

type Page = "sync" | "review" | "history";
type SyncTask = "clinic" | "operational" | "kommo" | "preview" | "all" | "quick" | "full";
type StepStatus = "idle" | "running" | "done" | "error";
type Flow = "sync" | "apply";

type ProgressEvent = {
  flow: Flow;
  task: string;
  step: string;
  status: "started" | "completed" | "failed" | "done";
  message: string;
};

type LogEvent = {
  flow: Flow;
  step: string;
  stream: "stdout" | "stderr";
  line: string;
  tsMs: number;
};

type LogLine = { stream: "stdout" | "stderr"; line: string; tsMs: number };

type StepState = {
  label: string;
  status: StepStatus;
  message?: string;
  startedAt?: number;
  finishedAt?: number;
};

type AuthState = {
  required: boolean;
  authenticated: boolean;
  username?: string | null;
  authenticatedAt?: number | null;
  gistConfigured?: boolean;
  gistUrl?: string;
  gistFile?: string | null;
};

type SchedulerStatus = "idle" | "ok" | "error";

type SchedulerState = {
  enabled: boolean;
  intervalMinutes: number;
  nextRunUnix: number | null;
  lastRunUnix: number | null;
  lastStatus: SchedulerStatus;
  lastError: string | null;
  running: boolean;
  pausedReason: string | null;
};

const DEFAULT_SCHEDULER_STATE: SchedulerState = {
  enabled: false,
  intervalMinutes: 60,
  nextRunUnix: null,
  lastRunUnix: null,
  lastStatus: "idle",
  lastError: null,
  running: false,
  pausedReason: null
};

const LOG_BUFFER_SIZE = 400;
const PAGE_STORAGE_KEY = "mirella.lastPage";

const fallbackSnapshot: Snapshot = {
  previewSummary: {
    match_summary: {
      patient_count: 755,
      lead_count: 13618,
      exact_unique_match_count: 338
    },
    safe_lead_count: 282,
    safe_field_row_count: 964,
    review_field_row_count: 107,
    action_counts: {
      fill_empty: 739,
      update_if_greater: 170,
      update_if_newer: 16,
      merge: 133,
      skip: 1613,
      review: 13
    },
    field_stats: {
      sale_value: { candidate: 255, safe_fill: 182, review_fill: 0, unmapped: 0 },
      billed_total: { candidate: 338, safe_fill: 197, review_fill: 0, unmapped: 0 },
      visits: { candidate: 338, safe_fill: 155, review_fill: 0, unmapped: 0 },
      last_visit: { candidate: 269, safe_fill: 78, review_fill: 0, unmapped: 0 },
      appointment: { candidate: 33, safe_fill: 33, review_fill: 0, unmapped: 0 },
      next_consultation: { candidate: 33, safe_fill: 33, review_fill: 0, unmapped: 0 },
      origin: { candidate: 216, safe_fill: 22, review_fill: 0, unmapped: 0 },
      service: { candidate: 264, safe_fill: 65, review_fill: 94, unmapped: 13 }
    }
  },
  safePayloadCount: 282,
  safeRowsCount: 964,
  reviewRowsCount: 107,
  localFiles: {
    env: { exists: true, bytes: 556, modifiedUnix: null },
    patientDb: { exists: true, bytes: 5251072, modifiedUnix: null },
    kommoDb: { exists: true, bytes: 116936704, modifiedUnix: null },
    safePayloads: { exists: true, bytes: 191581, modifiedUnix: null },
    reviewRows: { exists: true, bytes: 46694, modifiedUnix: null }
  }
};

const taskSteps: Record<SyncTask | "apply", string[]> = {
  quick: ["Atualizar Clínica", "Extrair campos operacionais", "Atualizar Kommo", "Gerar prévia"],
  full: ["Atualizar Clínica", "Extrair campos operacionais", "Atualizar Kommo", "Gerar prévia"],
  clinic: ["Atualizar Clínica"],
  operational: ["Extrair campos operacionais"],
  kommo: ["Atualizar Kommo"],
  preview: ["Gerar prévia"],
  all: ["Atualizar Clínica", "Extrair campos operacionais", "Atualizar Kommo", "Gerar prévia"],
  apply: ["Aplicar no Kommo", "Atualizar espelho Kommo", "Gerar nova prévia"]
};

async function call<T>(command: string, args?: Record<string, unknown>): Promise<T> {
  return invoke<T>(command, args);
}

function cx(...classes: Array<string | false | null | undefined>) {
  return classes.filter(Boolean).join(" ");
}

function number(value: number | undefined) {
  return new Intl.NumberFormat("pt-BR").format(value ?? 0);
}

function formatDuration(ms: number) {
  if (ms < 1000) return `${ms} ms`;
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const rem = seconds % 60;
  return `${minutes}m ${rem.toString().padStart(2, "0")}s`;
}

function timeAgo(unixSeconds?: number | null) {
  if (!unixSeconds) return "ainda não atualizado";
  const minutes = Math.floor(Math.max(0, Date.now() - unixSeconds * 1000) / 60000);
  if (minutes < 1) return "agora mesmo";
  if (minutes < 60) return `há ${minutes} min`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `há ${hours} h`;
  const days = Math.floor(hours / 24);
  if (days === 1) return "ontem";
  return `há ${days} dias`;
}

function useSnapshot() {
  const [snapshot, setSnapshot] = useState<Snapshot>(fallbackSnapshot);
  const [desktop, setDesktop] = useState(false);

  const refresh = useCallback(async () => {
    try {
      const result = await call<Snapshot>("get_dashboard_snapshot");
      setSnapshot(result);
      setDesktop(true);
    } catch {
      setSnapshot(fallbackSnapshot);
      setDesktop(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { snapshot, desktop, refresh };
}

function parseCsv(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let cell = "";
  let quoted = false;

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    const next = text[index + 1];
    if (char === '"' && quoted && next === '"') {
      cell += '"';
      index += 1;
      continue;
    }
    if (char === '"') {
      quoted = !quoted;
      continue;
    }
    if (char === "," && !quoted) {
      row.push(cell);
      cell = "";
      continue;
    }
    if ((char === "\n" || char === "\r") && !quoted) {
      if (char === "\r" && next === "\n") {
        index += 1;
      }
      row.push(cell);
      if (row.some((value) => value.trim())) {
        rows.push(row);
      }
      row = [];
      cell = "";
      continue;
    }
    cell += char;
  }
  row.push(cell);
  if (row.some((value) => value.trim())) {
    rows.push(row);
  }
  return rows;
}

function useReviewRows() {
  const [rows, setRows] = useState<ReviewRow[]>([]);

  const refresh = useCallback(async () => {
    try {
      const csv = await call<string>("read_review_rows");
      const parsed = parseCsv(csv);
      const [headers, ...data] = parsed;
      const mapped = data.map((cells) => {
        const item: Record<string, string> = {};
        headers.forEach((header, index) => {
          item[header] = cells[index] ?? "";
        });
        return item as unknown as ReviewRow;
      });
      setRows(mapped);
    } catch {
      setRows([]);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { rows, refresh };
}

type ApplyResultItem = {
  id: number;
  lead_name?: string | null;
  ok: boolean;
  error?: string;
};

type ApplyResults = {
  runId: string | null;
  modifiedUnix: number | null;
  items: ApplyResultItem[];
};

type SafePayloadPreviewItem = {
  id: number;
  lead_name?: string | null;
  field_count: number;
  has_price: boolean;
};

type SafePayloadPreview = {
  items: SafePayloadPreviewItem[];
};

function SafePreviewPanel({ data }: { data: SafePayloadPreview }) {
  const [expanded, setExpanded] = useState(false);
  const items = data.items ?? [];
  if (!items.length) return null;
  const visible = expanded ? items : items.slice(0, 10);
  const fieldCount = items.reduce((total, item) => total + (item.field_count || 0), 0);
  return (
    <Card className="p-4 md:p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <Pill tone="info" icon={<ClipboardList className="h-3 w-3" />}>Prévia antes de aplicar</Pill>
          <h3 className="mt-2 text-lg font-semibold text-white">Clientes prontos para enviar ao Kommo</h3>
          <p className="mt-1 text-xs text-slate-300">
            {number(items.length)} clientes · {number(fieldCount)} campos preparados · pendências ficam fora
          </p>
        </div>
        {items.length > 10 ? (
          <button
            type="button"
            onClick={() => setExpanded((value) => !value)}
            className="btn-ghost inline-flex h-8 items-center gap-1.5 rounded-lg px-3 text-[11px] font-semibold"
          >
            {expanded ? "Mostrar menos" : `Ver todos (${number(items.length)})`}
          </button>
        ) : null}
      </div>
      <ul className="mt-3 grid gap-1.5 sm:grid-cols-2">
        {visible.map((item) => (
          <li
            key={item.id}
            className="flex items-start gap-2 rounded-lg border border-cyan-400/25 bg-cyan-500/10 px-2.5 py-1.5 text-xs text-cyan-100"
          >
            <ClipboardList className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
            <div className="min-w-0 flex-1">
              <p className="truncate font-medium text-white">
                {item.lead_name || `Lead ${item.id}`}
              </p>
              <p className="truncate text-[10px] text-slate-400">
                id {item.id} · {number(item.field_count)} campo(s){item.has_price ? " · inclui Venda" : ""}
              </p>
            </div>
          </li>
        ))}
      </ul>
    </Card>
  );
}

function useApplyResults() {
  const [data, setData] = useState<ApplyResults>({ runId: null, modifiedUnix: null, items: [] });

  const refresh = useCallback(async () => {
    try {
      const result = await call<ApplyResults>("read_apply_results");
      setData(result ?? { runId: null, modifiedUnix: null, items: [] });
    } catch {
      setData({ runId: null, modifiedUnix: null, items: [] });
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { data, refresh };
}

type ApplyHistoryItem = {
  id: number;
  leadName: string | null;
  ok: boolean;
  error: string | null;
};

type ApplyHistoryRun = {
  runId: string;
  modifiedUnix: number;
  okCount: number;
  errCount: number;
  total: number;
  items: ApplyHistoryItem[];
};

function useApplyHistory() {
  const [runs, setRuns] = useState<ApplyHistoryRun[]>([]);
  const [loaded, setLoaded] = useState(false);
  // Expor o erro permite que a Historico exiba banner em vez de "lista vazia".
  // Sem isso, falha de IPC/parse ficava invisivel: loaded=true + runs=[] parece
  // que o usuario nunca aplicou nada.
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const result = await call<{ runs: ApplyHistoryRun[] }>("read_apply_history", { limit: 50 });
      setRuns(result?.runs ?? []);
      setError(null);
    } catch (err) {
      setRuns([]);
      setError(err instanceof Error ? err.message : String(err ?? "erro ao carregar histórico"));
    } finally {
      setLoaded(true);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { runs, loaded, error, refresh };
}

function useSafePayloadPreview() {
  const [data, setData] = useState<SafePayloadPreview>({ items: [] });

  const refresh = useCallback(async () => {
    try {
      const result = await call<SafePayloadPreview>("read_safe_payload_preview");
      setData(result ?? { items: [] });
    } catch {
      setData({ items: [] });
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { data, refresh };
}

function useScheduler() {
  const [state, setState] = useState<SchedulerState>(DEFAULT_SCHEDULER_STATE);
  const [available, setAvailable] = useState(false);

  const refresh = useCallback(async () => {
    try {
      const result = await call<SchedulerState>("get_scheduler_state_cmd");
      setState(result);
      setAvailable(true);
    } catch {
      setAvailable(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  useEffect(() => {
    let disposed = false;
    let dispose: (() => void) | null = null;
    void listen<SchedulerState>("scheduler-state", (event) => {
      if (disposed) return;
      setState(event.payload);
      setAvailable(true);
    }).then((fn) => {
      if (disposed) fn();
      else dispose = fn;
    });
    return () => {
      disposed = true;
      if (dispose) dispose();
    };
  }, []);

  const setConfig = useCallback(async (enabled: boolean, intervalMinutes: number) => {
    const next = await call<SchedulerState>("set_scheduler_config", {
      enabled,
      intervalMinutes
    });
    setState(next);
    return next;
  }, []);

  const runNow = useCallback(async () => {
    const next = await call<SchedulerState>("scheduler_run_now");
    setState(next);
    return next;
  }, []);

  return { state, available, refresh, setConfig, runNow };
}

function formatClock(unixSeconds?: number | null) {
  if (!unixSeconds) return null;
  try {
    return new Date(unixSeconds * 1000).toLocaleTimeString("pt-BR", {
      hour: "2-digit",
      minute: "2-digit"
    });
  } catch {
    return null;
  }
}

// Subcomponente isolado que absorve o re-render de 1Hz do countdown para que o
// AutomationCard inteiro (que tem gradient, animate-ping etc.) nao reconstrua
// a cada segundo enquanto o usuario esta olhando a tela ociosa.
const NextRunLabel = React.memo(function NextRunLabel({
  running,
  enabled,
  nextClock,
  targetUnix
}: {
  running: boolean;
  enabled: boolean;
  nextClock: string | null;
  targetUnix?: number | null;
}) {
  const countdown = useCountdown(enabled && !running ? targetUnix ?? null : null);
  if (running) return <>—</>;
  if (enabled && nextClock) {
    return <>{`${nextClock}${countdown ? ` · ${countdown}` : ""}`}</>;
  }
  return <>sem agendamento</>;
});

function useCountdown(targetUnix?: number | null) {
  const [now, setNow] = useState(() => Math.floor(Date.now() / 1000));
  useEffect(() => {
    if (!targetUnix) return;
    const id = setInterval(() => setNow(Math.floor(Date.now() / 1000)), 1000);
    return () => clearInterval(id);
  }, [targetUnix]);
  if (!targetUnix) return null;
  const diff = targetUnix - now;
  if (diff <= 0) return "em instantes";
  const minutes = Math.floor(diff / 60);
  const seconds = diff % 60;
  if (minutes <= 0) return `em ${seconds}s`;
  if (minutes < 60) return `em ${minutes}m ${seconds.toString().padStart(2, "0")}s`;
  const hours = Math.floor(minutes / 60);
  const remMinutes = minutes % 60;
  return `em ${hours}h ${remMinutes.toString().padStart(2, "0")}m`;
}

function useElapsed(startedAt?: number, finishedAt?: number) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    if (!startedAt || finishedAt) return;
    const id = setInterval(() => setNow(Date.now()), 500);
    return () => clearInterval(id);
  }, [startedAt, finishedAt]);
  if (!startedAt) return null;
  const end = finishedAt ?? now;
  return Math.max(0, end - startedAt);
}

const Card = React.forwardRef<HTMLElement, { children: React.ReactNode; className?: string }>(
  function Card({ children, className }, ref) {
    return (
      <section
        ref={ref}
        className={cx(
          "surface rounded-lg border border-slate-200 shadow-sm shadow-slate-200/40",
          className
        )}
      >
        {children}
      </section>
    );
  }
);

function Pill({
  tone,
  children,
  icon
}: {
  tone: "ok" | "warn" | "muted" | "info";
  children: React.ReactNode;
  icon?: React.ReactNode;
}) {
  const styles = {
    ok: "border-emerald-200 bg-emerald-50 text-emerald-700",
    warn: "border-amber-200 bg-amber-50 text-amber-700",
    info: "border-blue-200 bg-blue-50 text-blue-700",
    muted: "border-slate-200 bg-slate-50 text-slate-600"
  }[tone];
  return (
    <span
      className={cx(
        "inline-flex items-center gap-1.5 rounded-md border px-2 py-0.5 text-[11px] font-medium",
        styles
      )}
    >
      {icon}
      {children}
    </span>
  );
}

export default function App() {
  const [auth, setAuth] = useState<AuthState | null>(null);
  const [loadingAuth, setLoadingAuth] = useState(true);
  const [authError, setAuthError] = useState("");

  const refreshAuth = useCallback(async () => {
    setLoadingAuth(true);
    try {
      const state = await call<AuthState>("get_auth_state");
      setAuth(state);
      setAuthError("");
    } catch (error) {
      setAuth(null);
      setAuthError(String(error));
    } finally {
      setLoadingAuth(false);
    }
  }, []);

  useEffect(() => {
    void refreshAuth();
  }, [refreshAuth]);

  async function handleLogin(username: string, password: string) {
    setAuthError("");
    try {
      const state = await call<AuthState>("login_app", { username, password });
      setAuth(state);
      setAuthError("");
    } catch (error) {
      setAuthError(String(error));
      setAuth({ required: true, authenticated: false, gistConfigured: auth?.gistConfigured ?? true });
      throw error;
    }
  }

  async function handleLogout() {
    try {
      const state = await call<AuthState>("logout_app");
      setAuth(state);
    } catch (error) {
      setAuthError(String(error));
      setAuth({ required: true, authenticated: false });
    }
  }

  if (loadingAuth || !auth || (auth.required && !auth.authenticated)) {
    return (
      <LoginScreen
        auth={auth}
        loading={loadingAuth}
        error={authError}
        onLogin={handleLogin}
      />
    );
  }

  return <AuthenticatedApp auth={auth} onLogout={handleLogout} />;
}

function LogTerminal({
  lines,
  running
}: {
  lines: LogLine[];
  running: boolean;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const stickToBottom = useRef(true);

  useEffect(() => {
    if (!containerRef.current || !stickToBottom.current) return;
    containerRef.current.scrollTop = containerRef.current.scrollHeight;
  }, [lines]);

  const handleScroll = () => {
    const el = containerRef.current;
    if (!el) return;
    const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
    stickToBottom.current = distanceFromBottom < 40;
  };

  if (!lines.length) {
    return (
      <div className="rounded-lg border border-white/10 bg-black/40 px-3 py-2 font-mono text-[11px] text-slate-500">
        {running ? "Aguardando saída do processo..." : "Sem saída registrada."}
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      onScroll={handleScroll}
      className="thin-scrollbar max-h-48 overflow-auto rounded-lg border border-white/10 bg-black/50 px-3 py-2 font-mono text-[11px] leading-[1.55] text-slate-300"
      role="log"
      aria-live="polite"
      aria-relevant="additions"
    >
      {lines.map((log, index) => (
        <div
          key={`${log.tsMs}-${index}`}
          className={cx(
            "whitespace-pre-wrap break-words",
            log.stream === "stderr" ? "text-rose-300" : "text-slate-300"
          )}
        >
          {log.line || "\u00A0"}
        </div>
      ))}
    </div>
  );
}

function ProcessTracker({
  title,
  subtitle,
  steps,
  running,
  logs
}: {
  title: string;
  subtitle?: string;
  steps: StepState[];
  running: boolean;
  logs: Record<string, LogLine[]>;
}) {
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});

  useEffect(() => {
    // Renomeado para nao sombrear a prop `running` deste componente.
    const runningStep = steps.find((s) => s.status === "running");
    if (runningStep) {
      setExpanded((prev) =>
        prev[runningStep.label] ? prev : { ...prev, [runningStep.label]: true }
      );
    }
  }, [steps]);

  if (!steps.length) return null;
  return (
    <Card className="fade-in p-4 md:p-5">
      <div className="flex items-center justify-between gap-4">
        <div>
          <div className="flex items-center gap-2">
            <h3 className="text-base font-semibold text-white md:text-lg">{title}</h3>
            {running ? (
              <span className="relative inline-block h-2 w-2 text-emerald-400">
                <span className="absolute inset-0 rounded-full bg-emerald-400" />
                <span className="absolute inset-0 animate-ping rounded-full bg-emerald-400/70" />
              </span>
            ) : null}
          </div>
          {subtitle ? <p className="mt-0.5 text-xs text-slate-400">{subtitle}</p> : null}
        </div>
        {running ? <Loader2 className="h-4 w-4 animate-spin text-emerald-300" aria-label="Em execução" /> : null}
      </div>

      <ol className="mt-4 space-y-0" role="list">
        {steps.map((step, index) => {
          const isLast = index === steps.length - 1;
          const stepLogs = logs[step.label] ?? [];
          const isExpanded = expanded[step.label] ?? false;

          return (
            <StepItem
              key={step.label}
              step={step}
              index={index}
              isLast={isLast}
              logs={stepLogs}
              expanded={isExpanded}
              onToggle={() => setExpanded((prev) => ({ ...prev, [step.label]: !isExpanded }))}
            />
          );
        })}
      </ol>
    </Card>
  );
}

function StepItem({
  step,
  index,
  isLast,
  logs,
  expanded,
  onToggle
}: {
  step: StepState;
  index: number;
  isLast: boolean;
  logs: LogLine[];
  expanded: boolean;
  onToggle: () => void;
}) {
  const elapsed = useElapsed(step.startedAt, step.finishedAt);
  const statusLabel =
    step.status === "running"
      ? "Em andamento"
      : step.status === "done"
        ? "Concluído"
        : step.status === "error"
          ? "Erro"
          : "Aguardando";

  const lastLine = logs.length ? logs[logs.length - 1].line : null;
  const canToggle = logs.length > 0 || step.status === "running" || step.status === "error";

  return (
    <li
      className="relative flex gap-3 py-2"
      aria-label={`Etapa ${index + 1}: ${step.label}, ${statusLabel.toLowerCase()}`}
    >
      <div className="relative flex flex-col items-center">
        <div
          className={cx(
            "relative z-10 grid h-8 w-8 place-items-center rounded-full border-2 transition",
            step.status === "done"
              ? "border-emerald-400 bg-emerald-400/20 text-emerald-200"
              : step.status === "running"
                ? "border-cyan-400 bg-cyan-400/20 text-cyan-100"
                : step.status === "error"
                  ? "border-rose-400 bg-rose-400/20 text-rose-200"
                  : "border-white/15 bg-white/5 text-slate-500"
          )}
          aria-hidden
        >
          {step.status === "running" ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
          ) : step.status === "done" ? (
            <CheckCircle2 className="h-4 w-4" />
          ) : step.status === "error" ? (
            <XCircle className="h-4 w-4" />
          ) : (
            <span className="text-xs font-semibold">{index + 1}</span>
          )}
        </div>
        {!isLast ? (
          <div
            className={cx(
              "w-0.5 flex-1",
              step.status === "done" ? "bg-gradient-to-b from-emerald-400 to-emerald-400/30" : "bg-white/10"
            )}
            aria-hidden
          />
        ) : null}
      </div>
      <div className="flex-1 pb-2">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold text-white">{step.label}</span>
            {elapsed !== null ? (
              <span
                className={cx(
                  "rounded-full border px-2 py-0.5 font-mono text-[10px]",
                  step.status === "running"
                    ? "border-cyan-400/40 bg-cyan-400/10 text-cyan-200"
                    : "border-white/10 bg-white/5 text-slate-400"
                )}
              >
                {formatDuration(elapsed)}
              </span>
            ) : null}
          </div>
          <div className="flex items-center gap-2">
            <span
              className={cx(
                "text-[10px] font-bold uppercase tracking-wide",
                step.status === "done"
                  ? "text-emerald-300"
                  : step.status === "running"
                    ? "text-cyan-300"
                    : step.status === "error"
                      ? "text-rose-300"
                      : "text-slate-500"
              )}
            >
              {statusLabel}
            </span>
            {canToggle ? (
              <button
                type="button"
                onClick={onToggle}
                className="inline-flex items-center gap-1 rounded-full border border-white/10 bg-white/5 px-2 py-0.5 text-[10px] font-semibold text-slate-300 transition hover:border-white/20 hover:bg-white/10"
                aria-expanded={expanded}
                aria-label={expanded ? "Ocultar detalhes" : "Ver detalhes"}
              >
                <Terminal className="h-3 w-3" />
                {expanded ? "Ocultar" : "Detalhes"}
                {expanded ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
              </button>
            ) : null}
          </div>
        </div>
        {lastLine && !expanded ? (
          <p className="mt-1 truncate font-mono text-[11px] text-slate-400" title={lastLine}>
            ↳ {lastLine}
          </p>
        ) : step.message ? (
          <p className="mt-1 text-xs text-slate-400">{step.message}</p>
        ) : null}
        {expanded ? (
          <div className="mt-2">
            <LogTerminal lines={logs} running={step.status === "running"} />
          </div>
        ) : null}
      </div>
    </li>
  );
}

function DataFreshness({ snapshot }: { snapshot: Snapshot }) {
  const files = [
    { label: "Clínica", meta: snapshot.localFiles?.patientDb, icon: <Users className="h-3.5 w-3.5" /> },
    { label: "Kommo", meta: snapshot.localFiles?.kommoDb, icon: <Database className="h-3.5 w-3.5" /> },
    { label: "Prévia", meta: snapshot.localFiles?.safePayloads, icon: <ClipboardList className="h-3.5 w-3.5" /> }
  ] as const;

  return (
    <div className="grid grid-cols-3 gap-2" aria-label="Estado dos dados locais">
      {files.map(({ label, meta, icon }) => {
        const exists = Boolean(meta?.exists);
        return (
          <div
            key={label}
            className="rounded-xl border border-white/10 bg-white/[0.035] p-3 transition hover:border-white/20 hover:bg-white/[0.06]"
          >
            <div className="flex items-center justify-between gap-2">
              <div className="flex items-center gap-1.5 text-xs font-semibold text-white">
                <span className="text-slate-300">{icon}</span>
                {label}
              </div>
              {exists ? (
                <CheckCircle2 className="h-3.5 w-3.5 text-emerald-300" aria-label="Disponível" />
              ) : (
                <XCircle className="h-3.5 w-3.5 text-rose-300" aria-label="Indisponível" />
              )}
            </div>
            <p className="mt-1 text-[11px] text-slate-500">{timeAgo(meta?.modifiedUnix)}</p>
          </div>
        );
      })}
    </div>
  );
}

function SectionTitle({
  title,
  description,
  icon
}: {
  title: string;
  description?: string;
  icon?: React.ReactNode;
}) {
  return (
    <div className="flex items-start gap-2.5">
      {icon ? (
        <div className="grid h-7 w-7 shrink-0 place-items-center rounded-lg border border-white/10 bg-white/[0.04] text-emerald-200">
          {icon}
        </div>
      ) : null}
      <div>
        <h3 className="text-base font-semibold text-white md:text-lg">{title}</h3>
        {description ? <p className="mt-0.5 text-xs text-slate-400 md:text-sm">{description}</p> : null}
      </div>
    </div>
  );
}

function AppliedPanel({ data }: { data: ApplyResults }) {
  const [expanded, setExpanded] = useState(false);
  const items = data.items ?? [];
  if (!items.length) return null;
  const okCount = items.filter((item) => item.ok).length;
  const errCount = items.length - okCount;
  const visible = expanded ? items : items.slice(0, 8);
  return (
    <Card className="p-4 md:p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <Pill tone="ok" icon={<CheckCircle2 className="h-3 w-3" />}>Última aplicação</Pill>
          <h3 className="mt-2 text-lg font-semibold text-white">Clientes atualizados no Kommo</h3>
          <p className="mt-1 text-xs text-slate-300">
            {number(okCount)} enviados com sucesso{errCount > 0 ? ` · ${number(errCount)} com erro` : ""}
            {data.modifiedUnix ? ` · ${timeAgo(data.modifiedUnix)}` : ""}
          </p>
        </div>
        {items.length > 8 ? (
          <button
            type="button"
            onClick={() => setExpanded((value) => !value)}
            className="btn-ghost inline-flex h-8 items-center gap-1.5 rounded-lg px-3 text-[11px] font-semibold"
          >
            {expanded ? "Mostrar menos" : `Ver todos (${number(items.length)})`}
          </button>
        ) : null}
      </div>
      <ul className="mt-3 grid gap-1.5 sm:grid-cols-2">
        {visible.map((item) => (
          <li
            key={item.id}
            className={cx(
              "flex items-start gap-2 rounded-lg border px-2.5 py-1.5 text-xs",
              item.ok
                ? "border-emerald-400/30 bg-emerald-500/10 text-emerald-100"
                : "border-rose-400/40 bg-rose-500/10 text-rose-100"
            )}
            title={item.error || undefined}
          >
            {item.ok ? (
              <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
            ) : (
              <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
            )}
            <div className="min-w-0 flex-1">
              <p className="truncate font-medium text-white">
                {item.lead_name || `Lead ${item.id}`}
              </p>
              <p className="truncate text-[10px] text-slate-400">
                id {item.id}
                {!item.ok && item.error ? ` · ${item.error}` : ""}
              </p>
            </div>
          </li>
        ))}
      </ul>
    </Card>
  );
}

const AutomationCard = React.memo(function AutomationCard({
  state,
  available,
  desktop,
  busy,
  onSetConfig,
  onRunNow
}: {
  state: SchedulerState;
  available: boolean;
  desktop: boolean;
  busy: boolean;
  onSetConfig: (enabled: boolean, intervalMinutes: number) => Promise<SchedulerState>;
  onRunNow: () => Promise<SchedulerState>;
}) {
  const [actionError, setActionError] = useState<string | null>(null);
  const [pending, setPending] = useState<"toggle" | "interval" | "now" | null>(null);
  // countdown agora mora dentro de <NextRunLabel/> para nao forcar rerender no
  // pai a cada segundo.
  const nextClock = formatClock(state.nextRunUnix);
  const lastClock = formatClock(state.lastRunUnix);

  const handleToggle = async () => {
    setActionError(null);
    setPending("toggle");
    try {
      await onSetConfig(!state.enabled, state.intervalMinutes);
    } catch (error) {
      setActionError(String(error));
    } finally {
      setPending(null);
    }
  };

  const handleIntervalChange = async (minutes: number) => {
    if (minutes === state.intervalMinutes) return;
    setActionError(null);
    setPending("interval");
    try {
      await onSetConfig(state.enabled, minutes);
    } catch (error) {
      setActionError(String(error));
    } finally {
      setPending(null);
    }
  };

  const handleRunNow = async () => {
    setActionError(null);
    setPending("now");
    try {
      await onRunNow();
    } catch (error) {
      setActionError(String(error));
    } finally {
      setPending(null);
    }
  };

  const disabledAll = !desktop || !available;
  const statusTone: "ok" | "warn" | "info" | "muted" =
    state.lastStatus === "ok"
      ? "ok"
      : state.lastStatus === "error"
        ? "warn"
        : state.enabled
          ? "info"
          : "muted";

  return (
    <Card className="p-3 md:p-4">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="text-sm font-semibold text-slate-950">Automação</h3>
            <Pill tone={statusTone} icon={<Timer className="h-3 w-3" />}>
              {state.running
                ? "executando"
                : state.enabled
                  ? "ativa"
                  : state.pausedReason
                    ? "pausada"
                    : "desativada"}
            </Pill>
          </div>
          <p className="mt-1 text-xs text-slate-500">
            {state.enabled ? (
              <>
                Próxima execução:{" "}
                <NextRunLabel
                  running={state.running}
                  enabled={state.enabled}
                  nextClock={nextClock}
                  targetUnix={state.nextRunUnix}
                />
              </>
            ) : state.lastRunUnix ? (
              <>Último ciclo: {state.lastStatus === "ok" ? "ok" : state.lastStatus === "error" ? "falhou" : "—"} · {lastClock ?? timeAgo(state.lastRunUnix)}</>
            ) : (
              "Configure apenas se quiser rodar sem acionar manualmente."
            )}
          </p>
        </div>
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
          <button
            type="button"
            onClick={() => void handleToggle()}
            disabled={disabledAll || pending !== null}
            className={cx(
              "inline-flex h-9 items-center justify-center gap-2 rounded-md px-3 text-xs font-medium transition disabled:cursor-not-allowed disabled:opacity-60",
              state.enabled
                ? "border border-amber-400/40 bg-amber-500/15 text-amber-100 hover:border-amber-400/60 hover:bg-amber-500/20"
                : "btn-primary"
            )}
            aria-busy={pending === "toggle"}
          >
            {pending === "toggle" ? (
              <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
            ) : state.enabled ? (
              <Pause className="h-4 w-4" aria-hidden />
            ) : (
              <Play className="h-4 w-4" aria-hidden />
            )}
            {state.enabled ? "Pausar automação" : "Ativar automação"}
          </button>
          <div className="flex items-center gap-1 rounded-md border border-slate-200 bg-slate-50 p-1">
            {[30, 60].map((minutes) => {
              const active = state.intervalMinutes === minutes;
              return (
                <button
                  key={minutes}
                  type="button"
                  onClick={() => void handleIntervalChange(minutes)}
                  disabled={disabledAll || pending !== null}
                  className={cx(
                    "inline-flex h-7 flex-1 items-center justify-center gap-1 rounded px-3 text-xs font-medium transition disabled:cursor-not-allowed disabled:opacity-60",
                    active
                      ? "bg-white text-slate-950 shadow"
                      : "text-slate-300 hover:bg-white/10 hover:text-white"
                  )}
                  aria-pressed={active}
                >
                  {minutes === 60 ? "1 h" : `${minutes} min`}
                </button>
              );
            })}
          </div>
          <button
            type="button"
            onClick={() => void handleRunNow()}
            disabled={disabledAll || busy || pending !== null}
            className="btn-ghost inline-flex h-9 items-center justify-center gap-2 rounded-md px-3 text-xs font-medium disabled:cursor-not-allowed disabled:opacity-60"
            aria-busy={pending === "now"}
          >
            {pending === "now" || state.running ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
            ) : (
              <RefreshCw className="h-3.5 w-3.5" aria-hidden />
            )}
            Executar agora
          </button>
        </div>
      </div>

      {state.pausedReason ? (
        <div className="mt-3 flex items-start gap-2 rounded-md border border-amber-400/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-100">
          <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
          <span className="leading-5">Pausada: {state.pausedReason}</span>
        </div>
      ) : null}

      {state.lastError && state.lastStatus === "error" ? (
        <div className="mt-2 flex items-start gap-2 rounded-md border border-rose-400/30 bg-rose-500/10 px-3 py-2 text-xs text-rose-100">
          <XCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
          <span className="leading-5 break-words">{state.lastError}</span>
        </div>
      ) : null}

      {actionError ? (
        <div className="mt-2 flex items-start gap-2 rounded-md border border-rose-400/30 bg-rose-500/10 px-3 py-2 text-xs text-rose-100">
          <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
          <span className="leading-5 break-words">{actionError}</span>
        </div>
      ) : null}

      {!desktop ? (
        <div className="mt-2 flex items-start gap-2 rounded-md border border-amber-400/40 bg-amber-500/10 px-3 py-2 text-xs text-amber-100">
          <Info className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
          <span>A automação só funciona com o app desktop em execução.</span>
        </div>
      ) : null}
    </Card>
  );
});

function SyncPage({
  snapshot,
  desktop,
  command,
  applyCommand,
  syncSteps,
  applySteps,
  syncLogs,
  applyLogs,
  applyResults,
  safePayloadPreview,
  scheduler,
  schedulerAvailable,
  onSchedulerSetConfig,
  onSchedulerRunNow,
  onQuickUpdate,
  onFullUpdate,
  onApply,
  onOpenReview,
  applyRef
}: {
  snapshot: Snapshot;
  desktop: boolean;
  command: CommandState;
  applyCommand: CommandState;
  syncSteps: StepState[];
  applySteps: StepState[];
  syncLogs: Record<string, LogLine[]>;
  applyLogs: Record<string, LogLine[]>;
  applyResults: ApplyResults;
  safePayloadPreview: SafePayloadPreview;
  scheduler: SchedulerState;
  schedulerAvailable: boolean;
  onSchedulerSetConfig: (enabled: boolean, intervalMinutes: number) => Promise<SchedulerState>;
  onSchedulerRunNow: () => Promise<SchedulerState>;
  onQuickUpdate: () => void;
  onFullUpdate: () => void;
  onApply: () => void;
  onOpenReview: () => void;
  applyRef: React.RefObject<HTMLDivElement | null>;
}) {
  const summary = snapshot.previewSummary;
  const actions = summary?.action_counts ?? {};
  const safeLeads = summary?.safe_lead_count ?? snapshot.safePayloadCount ?? 0;
  const safeRows = summary?.safe_field_row_count ?? snapshot.safeRowsCount ?? 0;
  const reviewRows = summary?.review_field_row_count ?? snapshot.reviewRowsCount ?? 0;
  const patientCount = summary?.match_summary?.patient_count ?? 0;
  const leadCount = summary?.match_summary?.lead_count ?? 0;

  const quickRunning = command.running && command.task === "quick";
  const fullRunning = command.running && command.task === "full";
  const pipelineBusy = command.running || applyCommand.running || scheduler.running;

  return (
    <div className="space-y-3">
      <Card className="p-4 md:p-5" ref={applyRef as unknown as React.RefObject<HTMLElement>}>
        <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <Pill tone={desktop ? "ok" : "warn"} icon={desktop ? <CheckCircle2 className="h-3 w-3" /> : <AlertTriangle className="h-3 w-3" />}>
                {desktop ? "conectado" : "modo prévia"}
              </Pill>
              {pipelineBusy ? (
                <Pill tone="info" icon={<Loader2 className="h-3 w-3 animate-spin" />}>processando</Pill>
              ) : null}
            </div>
            <h2 className="mt-3 text-2xl font-semibold tracking-tight text-slate-950">
              Sincronização
            </h2>
            <p className="mt-1 max-w-2xl text-sm leading-6 text-slate-500">
              Atualize a base, revise pendências e envie ao Kommo somente o que já foi validado.
            </p>
          </div>

          <div className="grid gap-2 sm:grid-cols-3 xl:w-[520px]">
            <button
              type="button"
              className="btn-primary inline-flex h-10 items-center justify-center gap-2 rounded-md px-4 text-sm disabled:cursor-not-allowed"
              onClick={onQuickUpdate}
              disabled={pipelineBusy}
              aria-busy={quickRunning}
            >
              {quickRunning ? <Loader2 className="h-4 w-4 animate-spin" aria-hidden /> : <RefreshCw className="h-4 w-4" aria-hidden />}
              Atualizar
            </button>
            <button
              type="button"
              className="btn-ghost inline-flex h-10 items-center justify-center gap-2 rounded-md px-4 text-sm font-medium disabled:cursor-not-allowed"
              onClick={onFullUpdate}
              disabled={pipelineBusy}
              aria-busy={fullRunning}
            >
              {fullRunning ? <Loader2 className="h-4 w-4 animate-spin" aria-hidden /> : <Database className="h-4 w-4" aria-hidden />}
              Completa
            </button>
            <button
              type="button"
              className="btn-apply inline-flex h-10 items-center justify-center gap-2 rounded-md px-4 text-sm disabled:cursor-not-allowed"
              onClick={onApply}
              disabled={pipelineBusy || !desktop || safeRows <= 0}
              aria-busy={applyCommand.running}
            >
              {applyCommand.running ? <Loader2 className="h-4 w-4 animate-spin" aria-hidden /> : <Send className="h-4 w-4" aria-hidden />}
              Enviar
            </button>
          </div>
        </div>

        <dl className="mt-5 grid divide-y divide-slate-200 border-y border-slate-200 sm:grid-cols-3 sm:divide-x sm:divide-y-0">
          <div className="py-3 sm:pr-4">
            <dt className="text-xs font-medium text-slate-500">Clientes prontos</dt>
            <dd className="mt-1 text-2xl font-semibold text-emerald-700">{number(safeLeads)}</dd>
          </div>
          <div className="py-3 sm:px-4">
            <dt className="text-xs font-medium text-slate-500">Campos para enviar</dt>
            <dd className="mt-1 text-2xl font-semibold text-blue-700">{number(safeRows)}</dd>
          </div>
          <div className="py-3 sm:pl-4">
            <dt className="text-xs font-medium text-slate-500">Pendências</dt>
            <dd className="mt-1 flex items-baseline gap-2">
              <span className="text-2xl font-semibold text-amber-700">{number(reviewRows)}</span>
              {reviewRows > 0 ? (
                <button
                  type="button"
                  onClick={onOpenReview}
                  className="text-xs font-medium text-blue-700 underline-offset-4 hover:underline"
                >
                  revisar
                </button>
              ) : null}
            </dd>
          </div>
        </dl>

        {command.message || applyCommand.message || !desktop ? (
          <div className="mt-4 space-y-2">
            {!desktop ? (
              <div className="flex items-start gap-2 rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800">
                <Info className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
                <span>Envio desabilitado fora do app desktop conectado.</span>
              </div>
            ) : null}
            {command.message ? (
              <div
                role="status"
                aria-live="polite"
                className={cx(
                  "flex items-start gap-2 rounded-md border px-3 py-2 text-xs",
                  command.ok === false
                    ? "border-rose-200 bg-rose-50 text-rose-800"
                    : command.ok === true
                      ? "border-emerald-200 bg-emerald-50 text-emerald-800"
                      : "border-blue-200 bg-blue-50 text-blue-800"
                )}
              >
                {command.ok === false ? <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" aria-hidden /> : command.ok === true ? <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0" aria-hidden /> : <Info className="mt-0.5 h-4 w-4 shrink-0" aria-hidden />}
                <span className="leading-5">{command.message}</span>
              </div>
            ) : null}
            {applyCommand.message ? (
              <div
                role="status"
                aria-live="polite"
                className={cx(
                  "flex items-start gap-2 rounded-md border px-3 py-2 text-xs",
                  applyCommand.ok === false
                    ? "border-rose-200 bg-rose-50 text-rose-800"
                    : applyCommand.ok === true
                      ? "border-emerald-200 bg-emerald-50 text-emerald-800"
                      : "border-blue-200 bg-blue-50 text-blue-800"
                )}
              >
                {applyCommand.ok === false ? <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" aria-hidden /> : applyCommand.ok === true ? <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0" aria-hidden /> : <Loader2 className="mt-0.5 h-4 w-4 shrink-0 animate-spin" aria-hidden />}
                <span className="leading-5">{applyCommand.message}</span>
              </div>
            ) : null}
          </div>
        ) : null}
      </Card>

      <AutomationCard
        state={scheduler}
        available={schedulerAvailable}
        desktop={desktop}
        busy={command.running || applyCommand.running}
        onSetConfig={onSchedulerSetConfig}
        onRunNow={onSchedulerRunNow}
      />

      {(syncSteps.length && (command.running || command.ok === false)) ? (
        <ProcessTracker
          title="Andamento"
          subtitle={command.running ? "Processando agora." : "Falhou. Abra a etapa com erro para ver o log."}
          steps={syncSteps}
          running={command.running}
          logs={syncLogs}
        />
      ) : null}

      {(applySteps.length && (applyCommand.running || applyCommand.ok === false)) ? (
        <ProcessTracker
          title="Envio ao Kommo"
          steps={applySteps}
          running={applyCommand.running}
          logs={applyLogs}
        />
      ) : null}

      <details className="minimal-details">
        <summary>Prévia, bases e último envio</summary>
        <div className="mt-3 space-y-3">
          <div className="grid grid-cols-1 gap-3 xl:grid-cols-[1fr_420px]">
            <section className="rounded-lg border border-slate-200 bg-white p-4">
              <SectionTitle
                title="O que será enviado"
                description="Contagem por tipo de atualização preparada."
                icon={<ClipboardList className="h-3.5 w-3.5" />}
              />
              <div className="mt-4 grid grid-cols-2 gap-2 md:grid-cols-5">
                {[
                  ["Vazios", "fill_empty"],
                  ["Valores", "update_if_greater"],
                  ["Datas", "update_if_newer"],
                  ["Financeiro", "sync_authoritative"],
                  ["Serviços", "merge"]
                ].map(([label, key]) => (
                  <div key={label} className="rounded-md border border-slate-200 bg-slate-50 p-3">
                    <p className="text-[11px] font-medium text-slate-500">{label}</p>
                    <p className="mt-1 text-xl font-semibold text-slate-950">
                      {number(actions[key as keyof typeof actions])}
                    </p>
                  </div>
                ))}
              </div>
            </section>

            <section className="rounded-lg border border-slate-200 bg-white p-4">
              <SectionTitle
                title="Bases"
                description={`${number(patientCount)} pacientes · ${number(leadCount)} leads`}
                icon={<Database className="h-3.5 w-3.5" />}
              />
              <div className="mt-4">
                <DataFreshness snapshot={snapshot} />
              </div>
            </section>
          </div>

          <SafePreviewPanel data={safePayloadPreview} />
          <AppliedPanel data={applyResults} />
        </div>
      </details>
    </div>
  );
}

function ReviewPage({ snapshot, rows }: { snapshot: Snapshot; rows: ReviewRow[] }) {
  const stats = snapshot.previewSummary?.field_stats ?? {};
  const service = stats.service;
  const reviewRows = snapshot.previewSummary?.review_field_row_count ?? snapshot.reviewRowsCount ?? 0;
  const [query, setQuery] = useState("");
  // Em listas grandes, filtrar a cada tecla trava a UI em maquinas fracas.
  // useDeferredValue adia o filtro para uma atualizacao de baixa prioridade,
  // mantendo o input responsivo.
  const deferredQuery = useDeferredValue(query);

  const filtered = useMemo(() => {
    const term = deferredQuery.trim().toLowerCase();
    if (!term) return rows;
    return rows.filter((row) =>
      [row.patient_name, row.lead_name, row.field_label, row.candidate_value, row.mapped_value]
        .filter(Boolean)
        .some((value) => value.toLowerCase().includes(term))
    );
  }, [rows, deferredQuery]);
  // A lista renderizada tambem e memoizada para nao recalcular slice+map no redraw.
  const visibleRows = useMemo(() => filtered.slice(0, 120), [filtered]);

  return (
    <div className="space-y-3">
      <Card className="p-4 md:p-5">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <h2 className="text-2xl font-semibold tracking-tight text-slate-950">Pendências</h2>
            <p className="mt-1 max-w-2xl text-sm leading-6 text-slate-500">
              Estes itens ficam fora do envio automático até você ajustar o mapeamento.
            </p>
          </div>
          <dl className="rounded-md border border-amber-200 bg-amber-50 px-4 py-2">
            <dt className="text-xs font-medium text-amber-700">Aguardando revisão</dt>
            <dd className="text-2xl font-semibold text-amber-800">{number(reviewRows)}</dd>
          </dl>
        </div>
      </Card>

      <section className="space-y-3">
        <Card className="p-4 md:p-5">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
            <SectionTitle
              title="Itens para revisar"
              description="Compare o valor da clínica com a sugestão de mapeamento."
              icon={<ClipboardList className="h-3.5 w-3.5" />}
            />
            <div className="relative">
              <input
                type="search"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder="Filtrar cliente ou campo..."
                className="h-9 w-full rounded-xl border border-white/10 bg-white/[0.04] px-3 text-xs text-slate-200 placeholder:text-slate-500 focus:border-emerald-400/40 focus:outline-none focus:ring-2 focus:ring-emerald-400/30 sm:w-64"
                aria-label="Filtrar pendências"
              />
            </div>
          </div>
          <div className="thin-scrollbar mt-4 max-h-[480px] overflow-auto rounded-xl border border-white/10">
            <table className="w-full min-w-[720px] border-collapse text-left text-xs">
              <thead className="sticky top-0 z-10 bg-slate-900/95 text-[10px] uppercase tracking-wide text-slate-400 backdrop-blur">
                <tr>
                  <th className="px-3 py-2 font-semibold">Cliente</th>
                  <th className="px-3 py-2 font-semibold">Campo</th>
                  <th className="px-3 py-2 font-semibold">Valor da clínica</th>
                  <th className="px-3 py-2 font-semibold">Sugestão</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-white/5">
                {visibleRows.map((row, index) => (
                  <tr
                    key={`${row.patient_name}-${row.field_label}-${index}`}
                    className={cx(
                      "transition hover:bg-white/[0.05]",
                      index % 2 === 0 ? "bg-white/[0.02]" : "bg-transparent"
                    )}
                  >
                    <td className="px-3 py-2">
                      <p className="font-semibold text-white">{row.patient_name || row.lead_name}</p>
                      {row.lead_name && row.lead_name !== row.patient_name ? (
                        <p className="mt-0.5 text-[10px] text-slate-500">lead: {row.lead_name}</p>
                      ) : null}
                    </td>
                    <td className="px-3 py-2 font-medium text-slate-200">{row.field_label}</td>
                    <td className="max-w-[240px] px-3 py-2 text-slate-300">
                      <span className="line-clamp-2">{row.candidate_value}</span>
                    </td>
                    <td className="max-w-[200px] px-3 py-2 text-slate-300">
                      {row.mapped_value ? (
                        <span className="line-clamp-2">{row.mapped_value}</span>
                      ) : (
                        <Pill tone="warn">sem regra</Pill>
                      )}
                    </td>
                  </tr>
                ))}
                {!filtered.length ? (
                  <tr>
                    <td className="px-3 py-8 text-center text-slate-400" colSpan={4}>
                      <div className="flex flex-col items-center gap-2">
                        {rows.length ? (
                          <>
                            <Info className="h-7 w-7 text-cyan-400/70" aria-hidden />
                            <p className="text-sm font-semibold text-white">Nada encontrado</p>
                            <p className="text-xs text-slate-400">Tente outro termo no filtro.</p>
                          </>
                        ) : (
                          <>
                            <CheckCircle2 className="h-7 w-7 text-emerald-400/70" aria-hidden />
                            <p className="text-sm font-semibold text-white">Nada para revisar</p>
                            <p className="text-xs text-slate-400">Toda a base está com mapeamentos automáticos.</p>
                          </>
                        )}
                      </div>
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
          <p className="mt-2 text-[11px] text-slate-500">
            Mostrando {Math.min(filtered.length, 120)} de {rows.length} itens.
          </p>

          <p className="mt-3 text-[11px] leading-5 text-slate-500">
            Ajuste o mapeamento em <code className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[10px]">mappings/</code> ou mantenha o item fora da automação.
          </p>
        </Card>

        <details className="minimal-details">
          <summary>Resumo e campos manuais</summary>
          <div className="grid gap-4 lg:grid-cols-[320px_1fr]">
            <dl className="grid grid-cols-3 gap-2 lg:grid-cols-1">
              <div className="rounded-md border border-emerald-200 bg-emerald-50 p-3">
                <dt className="text-xs font-medium text-emerald-700">Seguros</dt>
                <dd className="mt-1 text-xl font-semibold text-emerald-800">{number(service?.safe_fill)}</dd>
              </div>
              <div className="rounded-md border border-amber-200 bg-amber-50 p-3">
                <dt className="text-xs font-medium text-amber-700">Revisão</dt>
                <dd className="mt-1 text-xl font-semibold text-amber-800">{number(service?.review_fill)}</dd>
              </div>
              <div className="rounded-md border border-rose-200 bg-rose-50 p-3">
                <dt className="text-xs font-medium text-rose-700">Sem regra</dt>
                <dd className="mt-1 text-xl font-semibold text-rose-800">{number(service?.unmapped)}</dd>
              </div>
            </dl>

            <div>
              <p className="text-xs font-medium text-slate-500">Campos tratados manualmente no Kommo</p>
              <div className="mt-2 flex flex-wrap gap-2">
                {["Retorno", "Consultor", "Atendido por", "Pagamento", "Forma de resgate"].map((item) => (
                  <span
                    key={item}
                    className="rounded-md border border-slate-200 bg-slate-50 px-2.5 py-1 text-xs font-medium text-slate-600"
                  >
                    {item}
                  </span>
                ))}
              </div>
            </div>
          </div>
        </details>
      </section>
    </div>
  );
}

function parseRunId(runId: string): Date | null {
  const match = runId.match(/^(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})$/);
  if (!match) return null;
  const [, y, mo, d, h, mi, s] = match;
  return new Date(Number(y), Number(mo) - 1, Number(d), Number(h), Number(mi), Number(s));
}

function formatRunStamp(run: ApplyHistoryRun): string {
  const parsed = parseRunId(run.runId);
  const date = parsed ?? (run.modifiedUnix ? new Date(run.modifiedUnix * 1000) : null);
  if (!date) return run.runId;
  return date.toLocaleString("pt-BR", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit"
  });
}

const HistoryRunCard = React.memo(function HistoryRunCard({
  run,
  filter,
  expandedInitially
}: {
  run: ApplyHistoryRun;
  filter: string;
  expandedInitially: boolean;
}) {
  const [expanded, setExpanded] = useState(expandedInitially);
  // Memoizamos stamp e filteredItems porque, em uma lista de 50 runs,
  // cada keystroke dispararia 50 x split/parse/filter sem memoizacao.
  const stamp = useMemo(() => formatRunStamp(run), [run.runId, run.modifiedUnix]);
  const term = filter.trim().toLowerCase();
  const filteredItems = useMemo(() => {
    if (!term) return run.items;
    return run.items.filter((item) =>
      (item.leadName ?? `lead ${item.id}`).toLowerCase().includes(term)
    );
  }, [run.items, term]);
  if (term && filteredItems.length === 0) return null;
  const visible = expanded ? filteredItems : filteredItems.slice(0, 8);
  return (
    <Card className="p-4 md:p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <Pill
            tone={run.errCount > 0 ? "warn" : "ok"}
            icon={<Clock3 className="h-3 w-3" />}
          >
            {stamp}
          </Pill>
          <p className="mt-2 text-sm font-semibold text-white">
            {number(run.okCount)} cliente{run.okCount === 1 ? "" : "s"} atualizado
            {run.okCount === 1 ? "" : "s"}
            {run.errCount > 0 ? ` · ${number(run.errCount)} com erro` : ""}
          </p>
          <p className="mt-0.5 text-[11px] text-slate-400">
            {timeAgo(run.modifiedUnix)} · execução {run.runId}
          </p>
        </div>
        <button
          type="button"
          onClick={() => setExpanded((value) => !value)}
          className="btn-ghost inline-flex h-8 items-center gap-1.5 rounded-lg px-3 text-[11px] font-semibold"
          aria-expanded={expanded}
        >
          {expanded ? (
            <>
              Recolher <ChevronUp className="h-3 w-3" />
            </>
          ) : (
            <>
              Ver todos ({number(filteredItems.length)}) <ChevronDown className="h-3 w-3" />
            </>
          )}
        </button>
      </div>
      <ul className="mt-3 grid gap-1.5 sm:grid-cols-2">
        {visible.map((item) => (
          <li
            key={`${run.runId}-${item.id}`}
            className={cx(
              "flex items-start gap-2 rounded-lg border px-2.5 py-1.5 text-xs",
              item.ok
                ? "border-emerald-400/30 bg-emerald-500/10 text-emerald-100"
                : "border-rose-400/40 bg-rose-500/10 text-rose-100"
            )}
            title={item.error || undefined}
          >
            {item.ok ? (
              <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
            ) : (
              <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
            )}
            <div className="min-w-0 flex-1">
              <p className="truncate font-medium text-white">
                {item.leadName || `Lead ${item.id}`}
              </p>
              <p className="truncate text-[10px] text-slate-400">
                id {item.id}
                {!item.ok && item.error ? ` · ${item.error}` : ""}
              </p>
            </div>
          </li>
        ))}
      </ul>
    </Card>
  );
});

function HistoryPage({
  runs,
  loaded,
  error,
  onRefresh
}: {
  runs: ApplyHistoryRun[];
  loaded: boolean;
  error: string | null;
  onRefresh: () => Promise<void>;
}) {
  const [query, setQuery] = useState("");
  const [refreshing, setRefreshing] = useState(false);
  // Em historicos grandes, filtrar pacientes a cada tecla custa caro.
  // useDeferredValue mantem o input responsivo e adia o filtro.
  const deferredQuery = useDeferredValue(query);

  const handleRefresh = async () => {
    setRefreshing(true);
    try {
      await onRefresh();
    } finally {
      setRefreshing(false);
    }
  };

  const totals = useMemo(() => {
    let leads = 0;
    let errors = 0;
    for (const run of runs) {
      leads += run.okCount;
      errors += run.errCount;
    }
    return { leads, errors };
  }, [runs]);
  const totalLeads = totals.leads;
  const totalErrors = totals.errors;
  const lastStamp = useMemo(() => (runs.length ? formatRunStamp(runs[0]) : null), [runs]);

  return (
    <div className="space-y-3">
      <Card className="p-4 md:p-5">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <h2 className="text-2xl font-semibold tracking-tight text-slate-950">
              Histórico
            </h2>
            <p className="mt-1 text-sm leading-6 text-slate-500">
              Envios feitos ao Kommo e resultado de cada ciclo.
            </p>
            {lastStamp ? (
              <p className="mt-1 text-xs text-slate-500">
                Último envio: <span className="font-medium text-slate-700">{lastStamp}</span>
              </p>
            ) : null}
          </div>
          <dl className="grid grid-cols-3 divide-x divide-slate-200 rounded-md border border-slate-200 bg-slate-50">
            <div className="px-4 py-2">
              <dt className="text-xs font-medium text-slate-500">Ok</dt>
              <dd className="text-xl font-semibold text-emerald-700">{number(totalLeads)}</dd>
            </div>
            <div className="px-4 py-2">
              <dt className="text-xs font-medium text-slate-500">Ciclos</dt>
              <dd className="text-xl font-semibold text-slate-950">{number(runs.length)}</dd>
            </div>
            <div className="px-4 py-2">
              <dt className="text-xs font-medium text-slate-500">Erros</dt>
              <dd className={cx("text-xl font-semibold", totalErrors > 0 ? "text-rose-700" : "text-slate-500")}>
                {number(totalErrors)}
              </dd>
            </div>
          </dl>
        </div>

        <div className="mt-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div className="relative flex-1 sm:max-w-sm">
            <input
              type="search"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Pesquisar por nome do cliente..."
              className="h-9 w-full rounded-xl border border-white/10 bg-white/[0.04] px-3 text-xs text-slate-200 placeholder:text-slate-500 focus:border-emerald-400/40 focus:outline-none focus:ring-2 focus:ring-emerald-400/30"
              aria-label="Pesquisar histórico"
            />
          </div>
          <button
            type="button"
            onClick={() => void handleRefresh()}
            disabled={refreshing}
            className="btn-ghost inline-flex h-9 items-center justify-center gap-1.5 rounded-md px-3 text-xs font-medium disabled:cursor-not-allowed disabled:opacity-60"
          >
            {refreshing ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : (
              <RefreshCw className="h-3.5 w-3.5" />
            )}
            Atualizar
          </button>
        </div>
      </Card>

      {error ? (
        <Card className="border-rose-400/40 bg-rose-500/10 p-4 text-xs text-rose-100">
          <div className="flex items-start gap-2">
            <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-rose-300" aria-hidden />
            <div className="flex-1">
              <p className="text-sm font-semibold text-rose-100">Não foi possível carregar o histórico</p>
              <p className="mt-1 text-[11px] leading-5 text-rose-200/90">{error}</p>
            </div>
          </div>
        </Card>
      ) : null}

      {!loaded ? (
        <Card className="p-6 text-center text-xs text-slate-400">
          <Loader2 className="mx-auto h-5 w-5 animate-spin text-slate-500" />
          <p className="mt-2">Carregando histórico...</p>
        </Card>
      ) : runs.length === 0 ? (
        <Card className="p-6 text-center text-xs text-slate-400">
          <History className="mx-auto h-7 w-7 text-slate-500" aria-hidden />
          <p className="mt-2 text-sm font-semibold text-white">Nenhum envio registrado ainda</p>
          <p className="mt-1">Assim que você aplicar atualizações ao Kommo, elas aparecem aqui.</p>
        </Card>
      ) : (
        <div className="space-y-3">
          {runs.map((run, index) => (
            <HistoryRunCard
              key={run.runId}
              run={run}
              filter={deferredQuery}
              expandedInitially={index === 0 && !deferredQuery}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function LoginScreen({
  auth,
  loading,
  error,
  onLogin
}: {
  auth: AuthState | null;
  loading: boolean;
  error: string;
  onLogin: (username: string, password: string) => Promise<void>;
}) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [submitting, setSubmitting] = useState(false);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setSubmitting(true);
    try {
      await onLogin(username, password);
    } catch {
      // Error message is rendered by the parent state.
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="app-shell flex min-h-screen items-center justify-center px-4 text-slate-950">
      <section className="surface w-full max-w-md rounded-lg border border-slate-200 p-6 shadow-sm shadow-slate-200/70">
        <div className="flex items-center gap-3">
          <div className="grid h-12 w-12 shrink-0 place-items-center rounded-md border border-slate-200 bg-slate-50 text-slate-700" aria-hidden>
            <LockKeyhole className="h-6 w-6" />
          </div>
          <div>
            <p className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">
              Mirella Sync
            </p>
            <h1 className="text-2xl font-semibold tracking-tight text-slate-950">Acesso restrito</h1>
          </div>
        </div>

        <p className="mt-5 text-sm leading-6 text-slate-500">
          Entre com o usuário autorizado para acessar a rotina de sincronização do Kommo.
        </p>

        <form className="mt-6 space-y-4" onSubmit={submit}>
          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400" htmlFor="login-user">
              Usuário
            </label>
            <input
              id="login-user"
              value={username}
              onChange={(event) => setUsername(event.target.value.toUpperCase())}
              placeholder="MIRELLA RABELLO"
              autoComplete="username"
              className="mt-2 h-11 w-full rounded-md border border-slate-200 bg-white px-3 text-sm font-semibold text-slate-950 placeholder:text-slate-400 focus:border-blue-400 focus:outline-none focus:ring-2 focus:ring-blue-100"
              disabled={loading || submitting}
            />
          </div>

          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400" htmlFor="login-password">
              Senha
            </label>
            <input
              id="login-password"
              type="password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              autoComplete="current-password"
              className="mt-2 h-11 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-950 placeholder:text-slate-400 focus:border-blue-400 focus:outline-none focus:ring-2 focus:ring-blue-100"
              disabled={loading || submitting}
            />
          </div>

          {error ? (
            <div className="rounded-xl border border-rose-400/30 bg-rose-400/10 px-4 py-3 text-sm text-rose-100">
              {error}
            </div>
          ) : null}

          <button
            type="submit"
            disabled={loading || submitting || !username.trim() || !password}
            className="btn-primary inline-flex h-11 w-full items-center justify-center gap-2 rounded-md px-5 text-sm"
          >
            {loading || submitting ? <Loader2 className="h-4 w-4 animate-spin" /> : <LockKeyhole className="h-4 w-4" />}
            Entrar
          </button>
        </form>

        <div className="mt-5 rounded-md border border-slate-200 bg-slate-50 px-4 py-3 text-xs leading-5 text-slate-500">
          Configuração remota: {auth?.gistConfigured ? "Gist conectado" : "Gist não configurado"}
        </div>
      </section>
    </div>
  );
}

function loadInitialPage(): Page {
  try {
    const stored = window.localStorage.getItem(PAGE_STORAGE_KEY);
    if (stored === "sync" || stored === "review" || stored === "history") return stored;
  } catch {
    // ignore
  }
  return "sync";
}

function AuthenticatedApp({ auth, onLogout }: { auth: AuthState; onLogout: () => Promise<void> }) {
  const { snapshot, desktop, refresh } = useSnapshot();
  const review = useReviewRows();
  const applyResults = useApplyResults();
  const safePayloadPreview = useSafePayloadPreview();
  const scheduler = useScheduler();
  const applyHistory = useApplyHistory();
  const [page, setPage] = useState<Page>(loadInitialPage);
  const [command, setCommand] = useState<CommandState>({ running: false, message: "", ok: null });
  const [applyCommand, setApplyCommand] = useState<CommandState>({ running: false, message: "", ok: null });
  const [syncSteps, setSyncSteps] = useState<StepState[]>([]);
  const [applySteps, setApplySteps] = useState<StepState[]>([]);
  const [syncLogs, setSyncLogs] = useState<Record<string, LogLine[]>>({});
  const [applyLogs, setApplyLogs] = useState<Record<string, LogLine[]>>({});
  const applyCardRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    try {
      window.localStorage.setItem(PAGE_STORAGE_KEY, page);
    } catch {
      // ignore
    }
  }, [page]);

  const schedulerRunning = scheduler.state.running;
  const prevSchedulerRunning = useRef(false);
  useEffect(() => {
    if (prevSchedulerRunning.current && !schedulerRunning) {
      void refresh();
      void review.refresh();
      void applyResults.refresh();
      void safePayloadPreview.refresh();
      void applyHistory.refresh();
    }
    prevSchedulerRunning.current = schedulerRunning;
  }, [schedulerRunning, refresh, review, applyResults, safePayloadPreview, applyHistory]);

  // Em scripts Python "verbose", cada linha de stdout vira um evento process-log.
  // Antes faziamos 1 setState por linha -> centenas de re-renders/seg em
  // maquina fraca. Agora acumulamos em um ref e descarregamos uma vez por frame
  // com requestAnimationFrame: no maximo 60 setStates/seg, a UI continua fluida.
  const pendingLogsRef = useRef<{ sync: Record<string, LogLine[]>; apply: Record<string, LogLine[]> }>({
    sync: {},
    apply: {}
  });
  const flushScheduledRef = useRef(false);

  useEffect(() => {
    let disposed = false;
    const unsubs: Array<() => void> = [];

    const mergeLogs = (prev: Record<string, LogLine[]>, bucket: Record<string, LogLine[]>) => {
      const next = { ...prev };
      for (const step of Object.keys(bucket)) {
        const lines = bucket[step];
        if (!lines || lines.length === 0) continue;
        const existing = next[step] ?? [];
        const appended = existing.length === 0 ? lines.slice() : existing.concat(lines);
        next[step] = appended.length > LOG_BUFFER_SIZE ? appended.slice(-LOG_BUFFER_SIZE) : appended;
      }
      return next;
    };

    const flushLogs = () => {
      flushScheduledRef.current = false;
      if (disposed) return;
      const pending = pendingLogsRef.current;
      pendingLogsRef.current = { sync: {}, apply: {} };
      if (Object.keys(pending.sync).length > 0) {
        setSyncLogs((prev) => mergeLogs(prev, pending.sync));
      }
      if (Object.keys(pending.apply).length > 0) {
        setApplyLogs((prev) => mergeLogs(prev, pending.apply));
      }
    };

    const scheduleFlush = () => {
      if (flushScheduledRef.current) return;
      flushScheduledRef.current = true;
      if (typeof window !== "undefined" && typeof window.requestAnimationFrame === "function") {
        window.requestAnimationFrame(flushLogs);
      } else {
        setTimeout(flushLogs, 16);
      }
    };

    void listen<ProgressEvent>("process-progress", (event) => {
      if (disposed) return;
      const payload = event.payload;
      const setSteps = payload.flow === "apply" ? setApplySteps : setSyncSteps;
      setSteps((current) => {
        const next = [...current];
        const index = next.findIndex((item) => item.label === payload.step);
        const status: StepStatus =
          payload.status === "started"
            ? "running"
            : payload.status === "completed" || payload.status === "done"
              ? "done"
              : "error";
        const now = Date.now();
        if (index >= 0) {
          const base = next[index];
          next[index] = {
            ...base,
            status,
            message: payload.message,
            startedAt:
              status === "running" ? base.startedAt ?? now : base.startedAt,
            finishedAt:
              status === "done" || status === "error" ? now : base.finishedAt
          };
          return next;
        }
        return [
          ...next,
          {
            label: payload.step,
            status,
            message: payload.message,
            startedAt: status === "running" ? now : undefined,
            finishedAt: status === "done" || status === "error" ? now : undefined
          }
        ];
      });
    }).then((fn) => {
      if (disposed) fn();
      else unsubs.push(fn);
    });

    void listen<LogEvent>("process-log", (event) => {
      if (disposed) return;
      const payload = event.payload;
      const bucket = payload.flow === "apply" ? pendingLogsRef.current.apply : pendingLogsRef.current.sync;
      const arr = (bucket[payload.step] ??= []);
      arr.push({ stream: payload.stream, line: payload.line, tsMs: payload.tsMs });
      scheduleFlush();
    }).then((fn) => {
      if (disposed) fn();
      else unsubs.push(fn);
    });

    return () => {
      disposed = true;
      unsubs.forEach((fn) => fn());
    };
  }, []);

  function createStepState(task: SyncTask | "apply") {
    return taskSteps[task].map((label) => ({ label, status: "idle" as StepStatus, message: "Aguardando." }));
  }

  async function runSyncTask(task: SyncTask) {
    if (command.running) return;
    const labels: Record<SyncTask, string> = {
      quick: "Atualização rápida em andamento. Pode levar alguns minutos.",
      full: "Atualização completa em andamento. Pode levar vários minutos.",
      clinic: "Atualizando dados da clínica...",
      operational: "Atualizando agenda, serviços e origem...",
      kommo: "Atualizando dados do Kommo...",
      preview: "Gerando nova prévia...",
      all: "Atualizando tudo. Pode levar alguns minutos."
    };
    setSyncSteps(createStepState(task));
    setSyncLogs({});
    setCommand({
      running: true,
      message: labels[task],
      ok: null,
      task,
      startedAt: Date.now()
    });
    try {
      const result = await call<{ logs: Array<{ label: string }>; snapshot: Snapshot }>("run_sync_task", { task });
      const done = result.logs.map((item) => item.label).join(" → ");
      setCommand({
        running: false,
        message: `Concluído: ${done}`,
        ok: true,
        task,
        finishedAt: Date.now()
      });
      await refresh();
      await review.refresh();
      await safePayloadPreview.refresh();
    } catch (error) {
      setCommand({
        running: false,
        message: String(error),
        ok: false,
        task,
        finishedAt: Date.now()
      });
    }
  }

  async function applySafePayloads() {
    if (applyCommand.running || command.running) return;
    const confirmed = window.confirm(
      "Aplicar no Kommo somente as atualizações seguras? As pendências não serão enviadas."
    );
    if (!confirmed) return;
    setApplySteps(createStepState("apply"));
    setApplyLogs({});
    setApplyCommand({
      running: true,
      message: "Enviando atualizações seguras para o Kommo...",
      ok: null,
      startedAt: Date.now()
    });
    try {
      const result = await call<{ logs: Array<{ label: string }>; snapshot: Snapshot }>("apply_safe_payloads");
      const done = result.logs.map((item) => item.label).join(" → ");
      setApplyCommand({
        running: false,
        message: `Concluído: ${done}`,
        ok: true,
        finishedAt: Date.now()
      });
      await refresh();
      await review.refresh();
      await applyResults.refresh();
      await safePayloadPreview.refresh();
      await applyHistory.refresh();
    } catch (error) {
      setApplyCommand({
        running: false,
        message: String(error),
        ok: false,
        finishedAt: Date.now()
      });
      void applyResults.refresh();
      void applyHistory.refresh();
    }
  }

  const safeRows = snapshot.previewSummary?.safe_field_row_count ?? snapshot.safeRowsCount ?? 0;
  const reviewRows = snapshot.previewSummary?.review_field_row_count ?? snapshot.reviewRowsCount ?? 0;
  const anyRunning = command.running || applyCommand.running || scheduler.state.running;

  const subtitle = useMemo(() => {
    if (anyRunning) return "Processando em segundo plano...";
    return `${number(safeRows)} campos prontos · ${number(reviewRows)} para revisar`;
  }, [anyRunning, safeRows, reviewRows]);

  return (
    <div className="app-shell text-slate-950">
      <a className="skip-link" href="#conteudo">Pular para o conteúdo</a>
      <div className="mx-auto grid min-h-screen w-full max-w-[1440px] grid-cols-1 lg:grid-cols-[232px_minmax(0,1fr)]">
        <aside className="app-sidebar border-b border-slate-200 bg-white px-4 py-4 lg:sticky lg:top-0 lg:h-screen lg:border-b-0 lg:border-r">
          <div className="flex items-center justify-between gap-3 lg:block">
            <div className="flex items-center gap-3">
              <div className="grid h-10 w-10 shrink-0 place-items-center rounded-md border border-slate-200 bg-slate-50 text-sm font-semibold text-slate-700" aria-hidden>
                MS
              </div>
              <div>
                <p className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">
                  Mirella Sync
                </p>
                <p className="text-sm font-semibold text-slate-950">Kommo</p>
              </div>
            </div>
            <button
              type="button"
              onClick={() => void onLogout()}
              className="btn-ghost inline-flex h-9 items-center gap-1.5 rounded-md px-3 text-xs font-medium lg:hidden"
              title={auth.username ? `Sessão: ${auth.username}` : "Sair"}
            >
              <LogOut className="h-3.5 w-3.5" />
              Sair
            </button>
          </div>

          <nav className="mt-4 grid grid-cols-3 gap-1 lg:grid-cols-1" aria-label="Navegação principal">
            <button
              type="button"
              onClick={() => setPage("sync")}
              className={cx(
                "app-nav-item",
                page === "sync" ? "app-nav-item-active" : ""
              )}
              aria-current={page === "sync" ? "page" : undefined}
            >
              <RefreshCw className="h-3.5 w-3.5" aria-hidden />
              <span>Rotina</span>
              {command.running ? <Loader2 className="h-3 w-3 animate-spin" aria-hidden /> : null}
            </button>
            <button
              type="button"
              onClick={() => setPage("review")}
              className={cx(
                "app-nav-item",
                page === "review" ? "app-nav-item-active" : ""
              )}
              aria-current={page === "review" ? "page" : undefined}
            >
              <AlertTriangle className="h-3.5 w-3.5" aria-hidden />
              <span>Pendências</span>
              {reviewRows > 0 ? (
                <span className="app-nav-count" aria-label={`${reviewRows} pendências`}>
                  {reviewRows > 99 ? "99+" : reviewRows}
                </span>
              ) : null}
            </button>
            <button
              type="button"
              onClick={() => {
                setPage("history");
                void applyHistory.refresh();
              }}
              className={cx(
                "app-nav-item",
                page === "history" ? "app-nav-item-active" : ""
              )}
              aria-current={page === "history" ? "page" : undefined}
            >
              <History className="h-3.5 w-3.5" aria-hidden />
              <span>Histórico</span>
            </button>
          </nav>

          <div className="mt-4 hidden space-y-3 border-t border-slate-200 pt-4 lg:block">
            <div
              className={cx(
                "inline-flex items-center gap-2 rounded-md border px-2.5 py-1 text-xs font-medium",
                desktop
                  ? "border-emerald-200 bg-emerald-50 text-emerald-700"
                  : "border-amber-200 bg-amber-50 text-amber-700"
              )}
              aria-live="polite"
            >
              <span className={cx("h-1.5 w-1.5 rounded-full", desktop ? "bg-emerald-600" : "bg-amber-500")} />
              {desktop ? "Conectado" : "Modo prévia"}
            </div>
            {anyRunning ? (
              <div className="inline-flex items-center gap-2 rounded-md border border-blue-200 bg-blue-50 px-2.5 py-1 text-xs font-medium text-blue-700">
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
                Processando
              </div>
            ) : null}
            <button
              type="button"
              onClick={() => void onLogout()}
              className="btn-ghost inline-flex h-9 w-full items-center justify-center gap-1.5 rounded-md px-3 text-xs font-medium"
              title={auth.username ? `Sessão: ${auth.username}` : "Sair"}
            >
              <LogOut className="h-3.5 w-3.5" />
              Sair
            </button>
          </div>
        </aside>

        <div className="flex min-w-0 flex-col px-4 py-4 sm:px-6 lg:px-8 lg:py-6">
          <header className="mb-4 flex flex-col gap-2 border-b border-slate-200 pb-4 sm:flex-row sm:items-end sm:justify-between">
            <div>
              <h1 className="text-xl font-semibold tracking-tight text-slate-950 md:text-2xl">
                Atualização do Kommo
              </h1>
              <p className="mt-1 text-sm text-slate-500">{subtitle}</p>
            </div>
            {anyRunning ? (
              <div className="inline-flex items-center gap-2 rounded-md border border-blue-200 bg-blue-50 px-2.5 py-1 text-xs font-medium text-blue-700 lg:hidden">
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
                Processando
              </div>
            ) : null}
          </header>

          <main id="conteudo" className="flex-1 fade-in">
            {page === "sync" ? (
              <SyncPage
                snapshot={snapshot}
                desktop={desktop}
                command={command}
                applyCommand={applyCommand}
                syncSteps={syncSteps}
                applySteps={applySteps}
                syncLogs={syncLogs}
                applyLogs={applyLogs}
                applyResults={applyResults.data}
                safePayloadPreview={safePayloadPreview.data}
                scheduler={scheduler.state}
                schedulerAvailable={scheduler.available}
                onSchedulerSetConfig={scheduler.setConfig}
                onSchedulerRunNow={scheduler.runNow}
                onQuickUpdate={() => runSyncTask("quick")}
                onFullUpdate={() => runSyncTask("full")}
                onApply={applySafePayloads}
                onOpenReview={() => setPage("review")}
                applyRef={applyCardRef}
              />
            ) : page === "review" ? (
              <ReviewPage snapshot={snapshot} rows={review.rows} />
            ) : (
              <HistoryPage
                runs={applyHistory.runs}
                loaded={applyHistory.loaded}
                error={applyHistory.error}
                onRefresh={applyHistory.refresh}
              />
            )}
          </main>

          <footer className="mt-5 flex flex-col justify-between gap-1 border-t border-slate-200 pt-3 text-[11px] text-slate-500 sm:flex-row">
            <p>Mirella Kommo Sync · dados processados localmente no computador</p>
            <p className="flex items-center gap-1">
              <Clock3 className="h-3 w-3" aria-hidden />
              Última prévia: {timeAgo(snapshot.localFiles?.safePayloads?.modifiedUnix)}
            </p>
          </footer>
        </div>
      </div>
    </div>
  );
}
