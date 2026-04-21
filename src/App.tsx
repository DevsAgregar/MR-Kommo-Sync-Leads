import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import {
  AlertTriangle,
  ArrowRight,
  CheckCircle2,
  ChevronDown,
  ChevronUp,
  ClipboardList,
  Clock3,
  Database,
  HelpCircle,
  Info,
  LockKeyhole,
  Loader2,
  LogOut,
  RefreshCw,
  Send,
  Sparkles,
  Terminal,
  Users,
  XCircle,
  Zap
} from "lucide-react";
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";

type FieldStat = {
  candidate: number;
  safe_fill: number;
  review_fill: number;
  unmapped: number;
  fill_empty?: number;
  update_if_greater?: number;
  update_if_newer?: number;
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

type Page = "sync" | "review";
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
          "surface rounded-2xl border border-white/10 shadow-lg shadow-black/30",
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
    ok: "border-emerald-400/40 bg-emerald-400/10 text-emerald-200",
    warn: "border-amber-400/40 bg-amber-400/10 text-amber-100",
    info: "border-cyan-400/40 bg-cyan-400/10 text-cyan-100",
    muted: "border-white/10 bg-white/5 text-slate-300"
  }[tone];
  return (
    <span
      className={cx(
        "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-[11px] font-semibold uppercase tracking-wide",
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
    const state = await call<AuthState>("login_app", { username, password });
    setAuth(state);
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

function StatTile({
  label,
  value,
  help,
  tone,
  icon
}: {
  label: string;
  value: number;
  help: string;
  tone: "ok" | "warn" | "info";
  icon: React.ReactNode;
}) {
  const toneStyles = {
    ok: { accent: "text-emerald-300", chip: "bg-emerald-400/15 text-emerald-200 border-emerald-400/30", glow: "from-emerald-400/10 to-transparent" },
    info: { accent: "text-cyan-300", chip: "bg-cyan-400/15 text-cyan-200 border-cyan-400/30", glow: "from-cyan-400/10 to-transparent" },
    warn: { accent: "text-amber-300", chip: "bg-amber-400/15 text-amber-100 border-amber-400/30", glow: "from-amber-400/10 to-transparent" }
  }[tone];

  return (
    <Card className="relative overflow-hidden p-4">
      <div className={cx("absolute inset-0 bg-gradient-to-br", toneStyles.glow)} aria-hidden />
      <div className="relative">
        <div className="flex items-start justify-between gap-2">
          <p className="text-xs font-medium text-slate-400">{label}</p>
          <div className={cx("rounded-lg border p-1.5", toneStyles.chip)}>{icon}</div>
        </div>
        <p className={cx("mt-1.5 text-3xl font-bold tracking-tight", toneStyles.accent)}>
          {number(value)}
        </p>
        <p className="mt-1 text-xs leading-4 text-slate-400">{help}</p>
      </div>
    </Card>
  );
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
    const running = steps.find((s) => s.status === "running");
    if (running) {
      setExpanded((prev) => (prev[running.label] ? prev : { ...prev, [running.label]: true }));
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

function ActionHero({
  title,
  summary,
  bullets,
  primaryLabel,
  primaryIcon,
  onPrimary,
  primaryDisabled,
  primaryLoading,
  secondaryLabel,
  secondaryIcon,
  onSecondary,
  secondaryDisabled,
  secondaryLoading,
  statusMessage,
  statusOk
}: {
  title: string;
  summary: string;
  bullets: string[];
  primaryLabel: string;
  primaryIcon: React.ReactNode;
  onPrimary: () => void;
  primaryDisabled: boolean;
  primaryLoading: boolean;
  secondaryLabel?: string;
  secondaryIcon?: React.ReactNode;
  onSecondary?: () => void;
  secondaryDisabled?: boolean;
  secondaryLoading?: boolean;
  statusMessage?: string;
  statusOk?: boolean | null;
}) {
  return (
    <Card className="hero-gradient shimmer-border p-5 md:p-6">
      <div className="flex flex-col gap-5 lg:flex-row lg:items-center lg:justify-between">
        <div className="max-w-2xl">
          <Pill tone="ok" icon={<Sparkles className="h-3 w-3" />}>Principal</Pill>
          <h2 className="mt-3 text-2xl font-bold leading-tight tracking-tight text-white md:text-3xl">
            {title}
          </h2>
          <p className="mt-2 text-sm leading-6 text-slate-300">{summary}</p>
          <ul className="mt-3 grid gap-1.5 sm:grid-cols-2" aria-label="O que a atualização faz">
            {bullets.map((bullet) => (
              <li key={bullet} className="flex items-start gap-1.5 text-xs text-slate-300">
                <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 shrink-0 text-emerald-300" aria-hidden />
                <span>{bullet}</span>
              </li>
            ))}
          </ul>
        </div>
        <div className="flex w-full shrink-0 flex-col gap-2 lg:w-auto">
          <button
            type="button"
            className="btn-primary inline-flex h-12 items-center justify-center gap-2 rounded-xl px-6 text-sm disabled:cursor-not-allowed"
            onClick={onPrimary}
            disabled={primaryDisabled}
            aria-busy={primaryLoading}
          >
            {primaryLoading ? (
              <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
            ) : (
              <span aria-hidden>{primaryIcon}</span>
            )}
            <span>{primaryLabel}</span>
            {!primaryLoading ? <ArrowRight className="h-4 w-4" aria-hidden /> : null}
          </button>
          {secondaryLabel && onSecondary ? (
            <button
              type="button"
              className="btn-ghost inline-flex h-10 items-center justify-center gap-2 rounded-xl px-4 text-xs font-semibold disabled:cursor-not-allowed"
              onClick={onSecondary}
              disabled={secondaryDisabled}
              aria-busy={secondaryLoading}
            >
              {secondaryLoading ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
              ) : (
                <span aria-hidden>{secondaryIcon}</span>
              )}
              <span>{secondaryLabel}</span>
            </button>
          ) : null}
        </div>
      </div>

      {statusMessage ? (
        <div
          role="status"
          aria-live="polite"
          className={cx(
            "fade-in mt-4 flex items-start gap-2 rounded-xl border px-3 py-2 text-xs",
            statusOk === false
              ? "border-rose-400/40 bg-rose-500/10 text-rose-100"
              : statusOk === true
                ? "border-emerald-400/40 bg-emerald-500/10 text-emerald-100"
                : "border-cyan-400/40 bg-cyan-500/10 text-cyan-100"
          )}
        >
          {statusOk === false ? (
            <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" aria-hidden />
          ) : statusOk === true ? (
            <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0" aria-hidden />
          ) : (
            <Info className="mt-0.5 h-4 w-4 shrink-0" aria-hidden />
          )}
          <span className="leading-5">{statusMessage}</span>
        </div>
      ) : null}
    </Card>
  );
}

function ApplyRibbon({
  safeLeads,
  safeRows,
  onApply,
  onDetails,
  busy,
  disabled
}: {
  safeLeads: number;
  safeRows: number;
  onApply: () => void;
  onDetails: () => void;
  busy: boolean;
  disabled: boolean;
}) {
  if (safeRows <= 0) return null;
  return (
    <div className="fade-in sticky top-2 z-20 flex flex-col items-start gap-3 rounded-2xl border border-emerald-400/30 bg-gradient-to-r from-emerald-500/15 via-cyan-500/10 to-emerald-500/5 p-3 shadow-lg shadow-emerald-900/20 backdrop-blur sm:flex-row sm:items-center sm:justify-between sm:p-4">
      <div className="flex items-center gap-3">
        <div className="grid h-9 w-9 place-items-center rounded-xl bg-emerald-400/20 text-emerald-200">
          <Send className="h-4 w-4" />
        </div>
        <div>
          <p className="text-sm font-semibold text-white">
            {number(safeRows)} atualizações prontas para enviar ao Kommo
          </p>
          <p className="text-xs text-slate-300">
            {number(safeLeads)} clientes afetados · nada com pendência será enviado
          </p>
        </div>
      </div>
      <div className="flex shrink-0 gap-2">
        <button
          type="button"
          onClick={onDetails}
          className="btn-ghost inline-flex h-9 items-center gap-1.5 rounded-xl px-3 text-xs font-semibold"
        >
          <Info className="h-3.5 w-3.5" />
          Detalhes
        </button>
        <button
          type="button"
          onClick={onApply}
          disabled={busy || disabled}
          className="btn-apply inline-flex h-9 items-center gap-2 rounded-xl px-4 text-xs disabled:cursor-not-allowed"
          aria-busy={busy}
        >
          {busy ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Send className="h-3.5 w-3.5" />}
          {busy ? "Enviando..." : "Aplicar no Kommo"}
        </button>
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

  const scrollToApply = () => {
    applyRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  };

  return (
    <div className="space-y-4">
      <ApplyRibbon
        safeLeads={safeLeads}
        safeRows={safeRows}
        onApply={onApply}
        onDetails={scrollToApply}
        busy={applyCommand.running}
        disabled={command.running || !desktop}
      />

      <ActionHero
        title="Atualizar os dados do Kommo"
        summary="Atualização rápida é ideal para o dia a dia (poucos minutos). A completa reprocessa tudo desde o início."
        bullets={[
          "Busca os atendimentos recentes da clínica",
          "Cruza pacientes com leads do Kommo",
          "Calcula o que pode ir com segurança",
          "Gera prévia para você conferir"
        ]}
        primaryLabel={quickRunning ? "Atualizando..." : "Atualização rápida"}
        primaryIcon={<Zap className="h-4 w-4" />}
        onPrimary={onQuickUpdate}
        primaryDisabled={command.running}
        primaryLoading={quickRunning}
        secondaryLabel={fullRunning ? "Processando tudo..." : "Atualização completa"}
        secondaryIcon={<RefreshCw className="h-3.5 w-3.5" />}
        onSecondary={onFullUpdate}
        secondaryDisabled={command.running}
        secondaryLoading={fullRunning}
        statusMessage={command.message || undefined}
        statusOk={command.ok}
      />

      {syncSteps.length ? (
        <ProcessTracker
          title="Andamento da atualização"
          subtitle={
            command.running
              ? "Aguarde — o app mostra o que cada etapa está fazendo em tempo real."
              : command.ok === true
                ? "Atualização finalizada com sucesso."
                : command.ok === false
                  ? "Houve um problema. Expanda a etapa com erro para ver o log."
                  : "Últimas etapas executadas."
          }
          steps={syncSteps}
          running={command.running}
          logs={syncLogs}
        />
      ) : null}

      <section aria-label="Resumo da última atualização" className="grid grid-cols-1 gap-3 md:grid-cols-3">
        <StatTile
          label="Clientes prontos"
          value={safeLeads}
          help="receberão atualização com segurança"
          tone="ok"
          icon={<Users className="h-3.5 w-3.5" />}
        />
        <StatTile
          label="Campos a enviar"
          value={safeRows}
          help="atualizações preparadas para o Kommo"
          tone="info"
          icon={<Database className="h-3.5 w-3.5" />}
        />
        <StatTile
          label="Para revisar"
          value={reviewRows}
          help="precisam da sua decisão"
          tone="warn"
          icon={<AlertTriangle className="h-3.5 w-3.5" />}
        />
      </section>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-[1.35fr_1fr]">
        <Card className="p-4 md:p-5">
          <SectionTitle
            title="O que será enviado"
            description="Resumo do que está preparado para aplicar no Kommo."
            icon={<ClipboardList className="h-3.5 w-3.5" />}
          />
          <div className="mt-4 grid grid-cols-2 gap-2 md:grid-cols-4">
            {[
              ["Preencher vazios", "fill_empty"],
              ["Aumentar valores", "update_if_greater"],
              ["Datas mais novas", "update_if_newer"],
              ["Somar serviços", "merge"]
            ].map(([label, key]) => (
              <div
                key={label}
                className="rounded-xl border border-white/10 bg-white/[0.035] p-3 transition hover:border-white/20 hover:bg-white/[0.06]"
              >
                <p className="text-[11px] font-medium text-slate-400">{label}</p>
                <p className="mt-1 text-2xl font-bold text-white">
                  {number(actions[key as keyof typeof actions])}
                </p>
              </div>
            ))}
          </div>
        </Card>

        <Card className="p-4 md:p-5">
          <SectionTitle
            title="Estado dos dados"
            description="Confira se as bases estão recentes."
            icon={<Database className="h-3.5 w-3.5" />}
          />
          <div className="mt-4">
            <DataFreshness snapshot={snapshot} />
          </div>

          <div className="mt-4 rounded-xl border border-white/10 bg-black/20 p-3 text-xs text-slate-300">
            <div className="flex items-start gap-2">
              <HelpCircle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-slate-400" aria-hidden />
              <div>
                <p className="font-semibold text-white">
                  {number(patientCount)} pacientes · {number(leadCount)} leads
                </p>
                <p className="mt-0.5 text-[11px] leading-4 text-slate-400">
                  Dados da última prévia. Rode uma atualização para trazer novidades.
                </p>
              </div>
            </div>
          </div>
        </Card>
      </div>

      <Card className="p-4 md:p-5" ref={applyRef as unknown as React.RefObject<HTMLElement>}>
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div className="max-w-2xl">
            <Pill tone="warn" icon={<Send className="h-3 w-3" />}>Envio ao Kommo</Pill>
            <h3 className="mt-2 text-xl font-semibold text-white">Aplicar atualizações seguras</h3>
            <p className="mt-1 text-xs leading-5 text-slate-300 md:text-sm">
              Envia ao Kommo apenas o que foi validado como seguro pela prévia. Itens em
              <strong className="mx-1 font-semibold text-amber-200">Pendências</strong>
              nunca são enviados.
            </p>
            {reviewRows > 0 ? (
              <button
                type="button"
                onClick={onOpenReview}
                className="mt-2 inline-flex items-center gap-1.5 text-xs font-semibold text-cyan-300 underline-offset-4 hover:underline"
              >
                <AlertTriangle className="h-3.5 w-3.5" />
                Ver {number(reviewRows)} pendências antes de aplicar
              </button>
            ) : null}
          </div>
          <button
            type="button"
            className="btn-apply inline-flex h-11 shrink-0 items-center justify-center gap-2 rounded-xl px-5 text-xs disabled:cursor-not-allowed"
            onClick={onApply}
            disabled={applyCommand.running || command.running || !desktop || safeRows <= 0}
            aria-busy={applyCommand.running}
          >
            {applyCommand.running ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
            ) : (
              <Send className="h-3.5 w-3.5" aria-hidden />
            )}
            {applyCommand.running ? "Enviando..." : "Aplicar no Kommo"}
          </button>
        </div>
        {!desktop ? (
          <div className="mt-3 flex items-start gap-2 rounded-xl border border-amber-400/40 bg-amber-500/10 p-2.5 text-xs text-amber-100">
            <Info className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
            <span>O envio só é habilitado quando o app está conectado ao ambiente local.</span>
          </div>
        ) : null}
        {applyCommand.message ? (
          <div
            role="status"
            aria-live="polite"
            className={cx(
              "fade-in mt-3 flex items-start gap-2 rounded-xl border px-3 py-2 text-xs",
              applyCommand.ok === false
                ? "border-rose-400/40 bg-rose-500/10 text-rose-100"
                : applyCommand.ok === true
                  ? "border-emerald-400/40 bg-emerald-500/10 text-emerald-100"
                  : "border-cyan-400/40 bg-cyan-500/10 text-cyan-100"
            )}
          >
            {applyCommand.ok === false ? (
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" aria-hidden />
            ) : applyCommand.ok === true ? (
              <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0" aria-hidden />
            ) : (
              <Loader2 className="mt-0.5 h-4 w-4 shrink-0 animate-spin" aria-hidden />
            )}
            <span className="leading-5">{applyCommand.message}</span>
          </div>
        ) : null}

        {applySteps.length ? (
          <div className="mt-4">
            <ProcessTracker
              title="Andamento da aplicação"
              steps={applySteps}
              running={applyCommand.running}
              logs={applyLogs}
            />
          </div>
        ) : null}
      </Card>

      <AppliedPanel data={applyResults} />
    </div>
  );
}

function ReviewPage({ snapshot, rows }: { snapshot: Snapshot; rows: ReviewRow[] }) {
  const stats = snapshot.previewSummary?.field_stats ?? {};
  const service = stats.service;
  const reviewRows = snapshot.previewSummary?.review_field_row_count ?? snapshot.reviewRowsCount ?? 0;
  const [query, setQuery] = useState("");

  const filtered = useMemo(() => {
    const term = query.trim().toLowerCase();
    if (!term) return rows;
    return rows.filter((row) =>
      [row.patient_name, row.lead_name, row.field_label, row.candidate_value, row.mapped_value]
        .filter(Boolean)
        .some((value) => value.toLowerCase().includes(term))
    );
  }, [rows, query]);

  return (
    <div className="space-y-4">
      <Card className="hero-gradient p-5 md:p-6">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div className="max-w-2xl">
            <Pill tone="warn" icon={<AlertTriangle className="h-3 w-3" />}>Revisão humana</Pill>
            <h2 className="mt-3 text-2xl font-bold tracking-tight text-white md:text-3xl">Pendências</h2>
            <p className="mt-2 text-sm leading-6 text-slate-300">
              Estes itens <strong className="text-white">não vão ao Kommo automaticamente</strong>. Confira cada sugestão e decida se ajusta o mapeamento ou deixa o item fora.
            </p>
          </div>
          <div className="surface-raised rounded-2xl border border-amber-400/30 px-5 py-3 text-center">
            <p className="text-3xl font-bold text-amber-100 md:text-4xl">{number(reviewRows)}</p>
            <p className="mt-0.5 text-[11px] font-medium uppercase tracking-wide text-amber-200">aguardando revisão</p>
          </div>
        </div>
      </Card>

      <section className="grid grid-cols-1 gap-4 xl:grid-cols-[1fr_300px]">
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
                {filtered.slice(0, 120).map((row, index) => (
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

          <div className="mt-3 flex items-start gap-2 rounded-xl border border-cyan-400/30 bg-cyan-500/10 p-3 text-xs text-cyan-100">
            <Info className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
            <p className="leading-5">
              Para resolver uma pendência, ajuste o mapeamento nas planilhas da pasta
              <code className="mx-1 rounded bg-black/40 px-1.5 py-0.5 font-mono text-[10px]">mappings/</code>
              ou deixe o item fora da automação.
            </p>
          </div>
        </Card>

        <Card className="p-4 md:p-5">
          <SectionTitle
            title="Resumo de serviços"
            description="Números que compõem as pendências."
            icon={<Info className="h-3.5 w-3.5" />}
          />
          <dl className="mt-4 space-y-2.5">
            <div className="rounded-xl border border-emerald-400/20 bg-emerald-400/10 p-3">
              <dt className="text-xs font-medium text-emerald-200">Serviços seguros</dt>
              <dd className="mt-0.5 text-2xl font-bold text-white">{number(service?.safe_fill)}</dd>
            </div>
            <div className="rounded-xl border border-amber-400/30 bg-amber-400/10 p-3">
              <dt className="text-xs font-medium text-amber-100">Em revisão</dt>
              <dd className="mt-0.5 text-2xl font-bold text-amber-50">{number(service?.review_fill)}</dd>
            </div>
            <div className="rounded-xl border border-rose-400/30 bg-rose-400/10 p-3">
              <dt className="text-xs font-medium text-rose-100">Sem mapeamento</dt>
              <dd className="mt-0.5 text-2xl font-bold text-rose-50">{number(service?.unmapped)}</dd>
            </div>
          </dl>
        </Card>
      </section>

      <Card className="p-4 md:p-5">
        <SectionTitle
          title="Campos que seguem manuais"
          description="Nunca entram na automação recorrente — trate-os diretamente no Kommo."
          icon={<Info className="h-3.5 w-3.5" />}
        />
        <div className="mt-3 grid grid-cols-2 gap-2 md:grid-cols-3 lg:grid-cols-5">
          {["Retorno", "Consultor", "Atendido por", "Pagamento", "Forma de resgate"].map((item) => (
            <div
              key={item}
              className="rounded-xl border border-white/10 bg-white/[0.035] p-3 text-center transition hover:border-white/20 hover:bg-white/[0.06]"
            >
              <p className="text-xs font-semibold text-white">{item}</p>
            </div>
          ))}
        </div>
      </Card>
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
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="app-shell flex min-h-screen items-center justify-center px-4 text-slate-100">
      <section className="surface w-full max-w-md rounded-2xl border border-white/10 p-6 shadow-2xl shadow-black/40">
        <div className="flex items-center gap-3">
          <div
            className="grid h-12 w-12 shrink-0 place-items-center rounded-xl text-slate-950 shadow-lg shadow-emerald-500/30"
            style={{ background: "linear-gradient(135deg, #34d399 0%, #22d3ee 100%)" }}
            aria-hidden
          >
            <LockKeyhole className="h-6 w-6" />
          </div>
          <div>
            <p className="text-[10px] font-bold uppercase tracking-[0.2em] text-emerald-300">
              Mirella Sync
            </p>
            <h1 className="text-2xl font-bold tracking-tight text-white">Acesso restrito</h1>
          </div>
        </div>

        <p className="mt-5 text-sm leading-6 text-slate-300">
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
              className="mt-2 h-12 w-full rounded-xl border border-white/10 bg-white/[0.05] px-4 text-sm font-semibold text-white placeholder:text-slate-600 focus:border-emerald-400/50 focus:outline-none focus:ring-2 focus:ring-emerald-400/20"
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
              className="mt-2 h-12 w-full rounded-xl border border-white/10 bg-white/[0.05] px-4 text-sm text-white placeholder:text-slate-600 focus:border-emerald-400/50 focus:outline-none focus:ring-2 focus:ring-emerald-400/20"
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
            className="btn-primary inline-flex h-12 w-full items-center justify-center gap-2 rounded-xl px-5 text-sm"
          >
            {loading || submitting ? <Loader2 className="h-4 w-4 animate-spin" /> : <LockKeyhole className="h-4 w-4" />}
            Entrar
          </button>
        </form>

        <div className="mt-5 rounded-xl border border-white/10 bg-black/20 px-4 py-3 text-xs leading-5 text-slate-400">
          Configuração remota: {auth?.gistConfigured ? "Gist conectado" : "Gist não configurado"}
        </div>
      </section>
    </div>
  );
}

function loadInitialPage(): Page {
  try {
    const stored = window.localStorage.getItem(PAGE_STORAGE_KEY);
    if (stored === "sync" || stored === "review") return stored;
  } catch {
    // ignore
  }
  return "sync";
}

function AuthenticatedApp({ auth, onLogout }: { auth: AuthState; onLogout: () => Promise<void> }) {
  const { snapshot, desktop, refresh } = useSnapshot();
  const review = useReviewRows();
  const applyResults = useApplyResults();
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

  useEffect(() => {
    let disposed = false;
    const unsubs: Array<() => void> = [];

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
      const setLogs = payload.flow === "apply" ? setApplyLogs : setSyncLogs;
      setLogs((prev) => {
        const existing = prev[payload.step] ?? [];
        const appended = [...existing, { stream: payload.stream, line: payload.line, tsMs: payload.tsMs }];
        const trimmed = appended.length > LOG_BUFFER_SIZE ? appended.slice(-LOG_BUFFER_SIZE) : appended;
        return { ...prev, [payload.step]: trimmed };
      });
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
    } catch (error) {
      setApplyCommand({
        running: false,
        message: String(error),
        ok: false,
        finishedAt: Date.now()
      });
      void applyResults.refresh();
    }
  }

  const safeRows = snapshot.previewSummary?.safe_field_row_count ?? snapshot.safeRowsCount ?? 0;
  const reviewRows = snapshot.previewSummary?.review_field_row_count ?? snapshot.reviewRowsCount ?? 0;
  const anyRunning = command.running || applyCommand.running;

  const subtitle = useMemo(() => {
    if (anyRunning) return "Processando em segundo plano...";
    return `${number(safeRows)} campos prontos · ${number(reviewRows)} para revisar`;
  }, [anyRunning, safeRows, reviewRows]);

  return (
    <div className="app-shell text-slate-100">
      <a className="skip-link" href="#conteudo">Pular para o conteúdo</a>
      <div className="mx-auto flex min-h-screen w-full max-w-7xl flex-col gap-4 px-3 py-4 sm:px-5 lg:py-6">
        <header className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex items-center gap-3">
            <div
              className="relative grid h-11 w-11 shrink-0 place-items-center rounded-xl text-slate-950 shadow-lg shadow-emerald-500/30"
              style={{ background: "linear-gradient(135deg, #34d399 0%, #22d3ee 100%)" }}
              aria-hidden
            >
              <Sparkles className="h-5 w-5" />
            </div>
            <div>
              <p className="text-[10px] font-bold uppercase tracking-[0.2em] text-emerald-300">
                Mirella Sync
              </p>
              <h1 className="text-xl font-bold tracking-tight text-white md:text-2xl">
                Atualização do Kommo
              </h1>
              <p className="mt-0.5 text-xs text-slate-400">{subtitle}</p>
            </div>
          </div>

          <div className="flex items-center gap-2">
            <div
              className={cx(
                "hidden items-center gap-1.5 rounded-full border px-3 py-1 text-[10px] font-bold uppercase tracking-wide md:inline-flex",
                desktop
                  ? "border-emerald-400/30 bg-emerald-400/10 text-emerald-200"
                  : "border-amber-400/30 bg-amber-400/10 text-amber-200"
              )}
              aria-live="polite"
            >
              {desktop ? (
                <>
                  <span className="relative inline-block h-1.5 w-1.5">
                    <span className="absolute inset-0 rounded-full bg-emerald-400" />
                    <span className="absolute inset-0 animate-ping rounded-full bg-emerald-400/60" />
                  </span>
                  Conectado
                </>
              ) : (
                <>
                  <AlertTriangle className="h-3 w-3" />
                  Modo prévia
                </>
              )}
            </div>
            {anyRunning ? (
              <div className="inline-flex items-center gap-1.5 rounded-full border border-cyan-400/40 bg-cyan-400/10 px-3 py-1 text-[10px] font-bold uppercase tracking-wide text-cyan-200">
                <Loader2 className="h-3 w-3 animate-spin" />
                Processando
              </div>
            ) : null}
            <button
              type="button"
              onClick={() => void onLogout()}
              className="hidden items-center gap-1.5 rounded-full border border-white/10 bg-white/[0.04] px-3 py-1 text-[10px] font-bold uppercase tracking-wide text-slate-300 transition hover:border-white/20 hover:bg-white/[0.08] hover:text-white md:inline-flex"
              title={auth.username ? `Sessão: ${auth.username}` : "Sair"}
            >
              <LogOut className="h-3 w-3" />
              Sair
            </button>
            <nav
              className="flex items-center gap-1 rounded-xl border border-white/10 bg-white/[0.04] p-1"
              aria-label="Navegação principal"
            >
              <button
                type="button"
                onClick={() => setPage("sync")}
                className={cx(
                  "inline-flex h-9 items-center gap-1.5 rounded-lg px-3 text-xs font-semibold transition",
                  page === "sync"
                    ? "bg-white text-slate-950 shadow"
                    : "text-slate-300 hover:bg-white/10 hover:text-white"
                )}
                aria-current={page === "sync" ? "page" : undefined}
              >
                <Zap className="h-3.5 w-3.5" aria-hidden />
                Rotina
                {command.running ? (
                  <Loader2 className="h-3 w-3 animate-spin" aria-hidden />
                ) : null}
              </button>
              <button
                type="button"
                onClick={() => setPage("review")}
                className={cx(
                  "relative inline-flex h-9 items-center gap-1.5 rounded-lg px-3 text-xs font-semibold transition",
                  page === "review"
                    ? "bg-white text-slate-950 shadow"
                    : "text-slate-300 hover:bg-white/10 hover:text-white"
                )}
                aria-current={page === "review" ? "page" : undefined}
              >
                <AlertTriangle className="h-3.5 w-3.5" aria-hidden />
                Pendências
                {reviewRows > 0 ? (
                  <span
                    className={cx(
                      "ml-0.5 inline-flex min-w-[18px] items-center justify-center rounded-full px-1.5 text-[10px] font-bold",
                      page === "review" ? "bg-amber-400 text-slate-950" : "bg-amber-400/20 text-amber-200"
                    )}
                    aria-label={`${reviewRows} pendências`}
                  >
                    {reviewRows > 99 ? "99+" : reviewRows}
                  </span>
                ) : null}
              </button>
            </nav>
          </div>
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
              onQuickUpdate={() => runSyncTask("quick")}
              onFullUpdate={() => runSyncTask("full")}
              onApply={applySafePayloads}
              onOpenReview={() => setPage("review")}
              applyRef={applyCardRef}
            />
          ) : (
            <ReviewPage snapshot={snapshot} rows={review.rows} />
          )}
        </main>

        <footer className="mt-1 flex flex-col items-center justify-between gap-1 border-t border-white/5 pt-3 text-[10px] text-slate-500 sm:flex-row">
          <p>Mirella Kommo Sync · dados processados localmente no seu computador</p>
          <p className="flex items-center gap-1">
            <Clock3 className="h-2.5 w-2.5" aria-hidden />
            Última prévia: {timeAgo(snapshot.localFiles?.safePayloads?.modifiedUnix)}
          </p>
        </footer>
      </div>
    </div>
  );
}
