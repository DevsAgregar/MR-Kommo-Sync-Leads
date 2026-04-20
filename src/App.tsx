import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import {
  AlertTriangle,
  ArrowRight,
  CheckCircle2,
  ClipboardList,
  Clock3,
  Database,
  HelpCircle,
  Info,
  Loader2,
  RefreshCw,
  Send,
  Sparkles,
  Users,
  XCircle,
  Zap
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";

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

type ProgressEvent = {
  flow: "sync" | "apply";
  task: string;
  step: string;
  status: "started" | "completed" | "failed" | "done";
  message: string;
};

type StepState = {
  label: string;
  status: StepStatus;
  message?: string;
};

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

  async function refresh() {
    try {
      const result = await call<Snapshot>("get_dashboard_snapshot");
      setSnapshot(result);
      setDesktop(true);
    } catch {
      setSnapshot(fallbackSnapshot);
      setDesktop(false);
    }
  }

  useEffect(() => {
    void refresh();
  }, []);

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

  async function refresh() {
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
  }

  useEffect(() => {
    void refresh();
  }, []);

  return { rows, refresh };
}

function Card({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <section
      className={cx(
        "surface rounded-3xl border border-white/10 shadow-xl shadow-black/30",
        className
      )}
    >
      {children}
    </section>
  );
}

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
        "inline-flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-semibold uppercase tracking-wide",
        styles
      )}
    >
      {icon}
      {children}
    </span>
  );
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
    ok: {
      accent: "text-emerald-300",
      chip: "bg-emerald-400/15 text-emerald-200 border-emerald-400/30",
      glow: "from-emerald-400/10 to-transparent"
    },
    info: {
      accent: "text-cyan-300",
      chip: "bg-cyan-400/15 text-cyan-200 border-cyan-400/30",
      glow: "from-cyan-400/10 to-transparent"
    },
    warn: {
      accent: "text-amber-300",
      chip: "bg-amber-400/15 text-amber-100 border-amber-400/30",
      glow: "from-amber-400/10 to-transparent"
    }
  }[tone];

  return (
    <Card className="relative overflow-hidden p-6">
      <div className={cx("absolute inset-0 bg-gradient-to-br", toneStyles.glow)} aria-hidden />
      <div className="relative">
        <div className="flex items-start justify-between">
          <p className="text-sm font-medium text-slate-400">{label}</p>
          <div className={cx("rounded-xl border p-2", toneStyles.chip)}>{icon}</div>
        </div>
        <p className={cx("mt-3 text-5xl font-bold tracking-tight", toneStyles.accent)}>
          {number(value)}
        </p>
        <p className="mt-2 text-sm leading-5 text-slate-400">{help}</p>
      </div>
    </Card>
  );
}

function ProcessTracker({
  title,
  subtitle,
  steps,
  running
}: {
  title: string;
  subtitle?: string;
  steps: StepState[];
  running: boolean;
}) {
  if (!steps.length) return null;
  return (
    <Card className="fade-in p-6">
      <div className="flex items-center justify-between gap-4">
        <div>
          <div className="flex items-center gap-2">
            <h3 className="text-lg font-semibold text-white">{title}</h3>
            {running ? (
              <span className="relative inline-block h-2 w-2 text-emerald-400 pulse-dot">
                <span className="absolute inset-0 rounded-full bg-emerald-400" />
              </span>
            ) : null}
          </div>
          <p className="mt-1 text-sm text-slate-400">
            {subtitle ?? (running ? "Processando em segundo plano..." : "Últimas etapas executadas.")}
          </p>
        </div>
        {running ? <Loader2 className="h-5 w-5 animate-spin text-emerald-300" aria-label="Em execução" /> : null}
      </div>

      <ol className="mt-6 space-y-1" role="list">
        {steps.map((step, index) => {
          const isLast = index === steps.length - 1;
          const statusLabel =
            step.status === "running"
              ? "Em andamento"
              : step.status === "done"
                ? "Concluído"
                : step.status === "error"
                  ? "Erro"
                  : "Aguardando";

          return (
            <li
              key={step.label}
              className="relative flex gap-4 py-3"
              aria-label={`Etapa ${index + 1}: ${step.label}, ${statusLabel.toLowerCase()}`}
            >
              <div className="relative flex flex-col items-center">
                <div
                  className={cx(
                    "relative z-10 grid h-10 w-10 place-items-center rounded-full border-2 transition",
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
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : step.status === "done" ? (
                    <CheckCircle2 className="h-5 w-5" />
                  ) : step.status === "error" ? (
                    <XCircle className="h-5 w-5" />
                  ) : (
                    <span className="text-sm font-semibold">{index + 1}</span>
                  )}
                </div>
                {!isLast ? (
                  <div
                    className={cx(
                      "w-0.5 flex-1",
                      step.status === "done"
                        ? "bg-gradient-to-b from-emerald-400 to-emerald-400/30"
                        : "bg-white/10"
                    )}
                    aria-hidden
                  />
                ) : null}
              </div>
              <div className="flex-1 pb-1">
                <div className="flex items-center justify-between gap-3">
                  <span className="text-base font-semibold text-white">{step.label}</span>
                  <span
                    className={cx(
                      "text-xs font-semibold uppercase tracking-wide",
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
                </div>
                <p className="mt-1 text-sm text-slate-400">{step.message ?? "Aguardando."}</p>
              </div>
            </li>
          );
        })}
      </ol>
    </Card>
  );
}

function DataFreshness({ snapshot }: { snapshot: Snapshot }) {
  const files = [
    { label: "Base da Clínica", meta: snapshot.localFiles?.patientDb, icon: <Users className="h-4 w-4" /> },
    { label: "Base do Kommo", meta: snapshot.localFiles?.kommoDb, icon: <Database className="h-4 w-4" /> },
    { label: "Prévia pronta", meta: snapshot.localFiles?.safePayloads, icon: <ClipboardList className="h-4 w-4" /> }
  ] as const;

  return (
    <div
      className="grid grid-cols-1 gap-3 sm:grid-cols-3"
      aria-label="Estado dos dados locais"
    >
      {files.map(({ label, meta, icon }) => {
        const exists = Boolean(meta?.exists);
        return (
          <div
            key={label}
            className="rounded-2xl border border-white/10 bg-white/[0.035] p-4 transition hover:border-white/20 hover:bg-white/[0.06]"
          >
            <div className="flex items-center justify-between gap-3">
              <div className="flex items-center gap-2 text-sm font-semibold text-white">
                <span className="text-slate-300">{icon}</span>
                {label}
              </div>
              {exists ? (
                <CheckCircle2 className="h-4 w-4 text-emerald-300" aria-label="Disponível" />
              ) : (
                <XCircle className="h-4 w-4 text-rose-300" aria-label="Indisponível" />
              )}
            </div>
            <p className="mt-2 text-xs text-slate-500">{timeAgo(meta?.modifiedUnix)}</p>
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
    <div className="flex items-start gap-3">
      {icon ? (
        <div className="grid h-9 w-9 shrink-0 place-items-center rounded-xl border border-white/10 bg-white/[0.04] text-emerald-200">
          {icon}
        </div>
      ) : null}
      <div>
        <h3 className="text-xl font-semibold text-white">{title}</h3>
        {description ? <p className="mt-1 text-sm text-slate-400">{description}</p> : null}
      </div>
    </div>
  );
}

function ActionCard({
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
    <Card className="hero-gradient shimmer-border p-8">
      <div className="flex flex-col gap-7 lg:flex-row lg:items-center lg:justify-between">
        <div className="max-w-2xl">
          <Pill tone="ok" icon={<Sparkles className="h-3.5 w-3.5" />}>Principal</Pill>
          <h2 className="mt-4 text-4xl font-bold leading-tight tracking-tight text-white">
            {title}
          </h2>
          <p className="mt-3 text-base leading-7 text-slate-300">{summary}</p>
          <ul className="mt-4 space-y-2" aria-label="O que a atualização faz">
            {bullets.map((bullet) => (
              <li key={bullet} className="flex items-start gap-2 text-sm text-slate-300">
                <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-emerald-300" aria-hidden />
                <span>{bullet}</span>
              </li>
            ))}
          </ul>
        </div>
        <div className="flex w-full shrink-0 flex-col gap-3 lg:w-auto">
          <button
            type="button"
            className="btn-primary inline-flex h-16 items-center justify-center gap-3 rounded-2xl px-8 text-base disabled:cursor-not-allowed"
            onClick={onPrimary}
            disabled={primaryDisabled}
            aria-busy={primaryLoading}
          >
            {primaryLoading ? (
              <Loader2 className="h-5 w-5 animate-spin" aria-hidden />
            ) : (
              <span aria-hidden>{primaryIcon}</span>
            )}
            <span>{primaryLabel}</span>
            {!primaryLoading ? <ArrowRight className="h-5 w-5" aria-hidden /> : null}
          </button>
          {secondaryLabel && onSecondary ? (
            <button
              type="button"
              className="btn-ghost inline-flex h-12 items-center justify-center gap-2 rounded-2xl px-5 text-sm font-semibold disabled:cursor-not-allowed"
              onClick={onSecondary}
              disabled={secondaryDisabled}
              aria-busy={secondaryLoading}
            >
              {secondaryLoading ? (
                <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
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
            "fade-in mt-6 flex items-start gap-3 rounded-2xl border px-4 py-3 text-sm",
            statusOk === false
              ? "border-rose-400/40 bg-rose-500/10 text-rose-100"
              : statusOk === true
                ? "border-emerald-400/40 bg-emerald-500/10 text-emerald-100"
                : "border-cyan-400/40 bg-cyan-500/10 text-cyan-100"
          )}
        >
          {statusOk === false ? (
            <AlertTriangle className="mt-0.5 h-5 w-5 shrink-0" aria-hidden />
          ) : statusOk === true ? (
            <CheckCircle2 className="mt-0.5 h-5 w-5 shrink-0" aria-hidden />
          ) : (
            <Info className="mt-0.5 h-5 w-5 shrink-0" aria-hidden />
          )}
          <span className="leading-6">{statusMessage}</span>
        </div>
      ) : null}
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
  onQuickUpdate,
  onFullUpdate,
  onApply,
  onOpenReview
}: {
  snapshot: Snapshot;
  desktop: boolean;
  command: CommandState;
  applyCommand: CommandState;
  syncSteps: StepState[];
  applySteps: StepState[];
  onQuickUpdate: () => void;
  onFullUpdate: () => void;
  onApply: () => void;
  onOpenReview: () => void;
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

  return (
    <div className="space-y-6">
      <ActionCard
        title="Atualizar os dados do Kommo"
        summary="A atualização rápida é ideal para o dia a dia e leva poucos minutos. A completa reprocessa tudo desde o início — use apenas quando precisar revisar toda a base."
        bullets={[
          "Busca os atendimentos mais recentes da clínica",
          "Cruza pacientes com leads do Kommo",
          "Deixa pronto o que pode ser enviado com segurança"
        ]}
        primaryLabel={quickRunning ? "Atualizando..." : "Atualização rápida"}
        primaryIcon={<Zap className="h-5 w-5" />}
        onPrimary={onQuickUpdate}
        primaryDisabled={command.running}
        primaryLoading={quickRunning}
        secondaryLabel={fullRunning ? "Processando tudo..." : "Atualização completa"}
        secondaryIcon={<RefreshCw className="h-4 w-4" />}
        onSecondary={onFullUpdate}
        secondaryDisabled={command.running}
        secondaryLoading={fullRunning}
        statusMessage={command.message || undefined}
        statusOk={command.ok}
      />

      <section aria-label="Resumo da última atualização" className="grid grid-cols-1 gap-4 md:grid-cols-3">
        <StatTile
          label="Clientes prontos"
          value={safeLeads}
          help="leads do Kommo que podem receber atualização automática com segurança"
          tone="ok"
          icon={<Users className="h-4 w-4" />}
        />
        <StatTile
          label="Campos a enviar"
          value={safeRows}
          help="quantidade de campos preparados para ir ao Kommo"
          tone="info"
          icon={<Database className="h-4 w-4" />}
        />
        <StatTile
          label="Para revisar"
          value={reviewRows}
          help="itens que precisam da sua decisão antes de enviar"
          tone="warn"
          icon={<AlertTriangle className="h-4 w-4" />}
        />
      </section>

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-[1.35fr_1fr]">
        <Card className="p-6">
          <SectionTitle
            title="O que será enviado"
            description="Resumo do que está preparado para aplicar no Kommo."
            icon={<ClipboardList className="h-4 w-4" />}
          />
          <div className="mt-6 grid grid-cols-1 gap-3 sm:grid-cols-2">
            {[
              ["Preencher campos vazios", "fill_empty", "Campos sem valor no Kommo serão preenchidos"],
              ["Aumentar valores", "update_if_greater", "Quando o valor da clínica é maior que o do Kommo"],
              ["Atualizar datas", "update_if_newer", "Datas mais recentes substituem antigas"],
              ["Adicionar serviços", "merge", "Novos serviços somados à lista existente"]
            ].map(([label, key, help]) => (
              <div
                key={label}
                className="rounded-2xl border border-white/10 bg-white/[0.035] p-4 transition hover:border-white/20 hover:bg-white/[0.06]"
              >
                <p className="text-sm font-medium text-slate-400">{label}</p>
                <p className="mt-1.5 text-3xl font-bold text-white">
                  {number(actions[key as keyof typeof actions])}
                </p>
                <p className="mt-1 text-xs leading-5 text-slate-500">{help}</p>
              </div>
            ))}
          </div>
        </Card>

        <Card className="p-6">
          <SectionTitle
            title="Estado dos dados"
            description="Verifique se as bases estão recentes antes de aplicar."
            icon={<Database className="h-4 w-4" />}
          />
          <div className="mt-6">
            <DataFreshness snapshot={snapshot} />
          </div>

          <div className="mt-6 rounded-2xl border border-white/10 bg-black/20 p-4 text-sm text-slate-300">
            <div className="flex items-start gap-2">
              <HelpCircle className="mt-0.5 h-4 w-4 shrink-0 text-slate-400" aria-hidden />
              <div>
                <p className="font-semibold text-white">
                  {number(patientCount)} pacientes · {number(leadCount)} leads
                </p>
                <p className="mt-1 text-xs leading-5 text-slate-400">
                  Dados carregados na última prévia. Rode uma atualização para trazer novidades.
                </p>
              </div>
            </div>
          </div>
        </Card>
      </div>

      {syncSteps.length ? (
        <ProcessTracker
          title="Andamento da atualização"
          subtitle={
            command.running
              ? "Aguarde alguns instantes. Você pode manter o app aberto."
              : command.ok === true
                ? "Atualização finalizada com sucesso."
                : command.ok === false
                  ? "Houve um problema. Veja os detalhes abaixo."
                  : undefined
          }
          steps={syncSteps}
          running={command.running}
        />
      ) : null}

      <Card className="p-6">
        <div className="flex flex-col gap-5 lg:flex-row lg:items-start lg:justify-between">
          <div className="max-w-2xl">
            <Pill tone="warn" icon={<Send className="h-3.5 w-3.5" />}>
              Envio ao Kommo
            </Pill>
            <h3 className="mt-3 text-2xl font-semibold text-white">
              Aplicar atualizações seguras
            </h3>
            <p className="mt-2 text-sm leading-6 text-slate-300">
              Este passo envia ao Kommo apenas o que foi validado como seguro pela prévia.
              Itens em <strong className="font-semibold text-amber-200">Pendências</strong> não são enviados — eles esperam sua decisão.
            </p>
            {reviewRows > 0 ? (
              <button
                type="button"
                onClick={onOpenReview}
                className="mt-4 inline-flex items-center gap-2 text-sm font-semibold text-cyan-300 underline-offset-4 hover:underline"
              >
                <AlertTriangle className="h-4 w-4" />
                Ver {number(reviewRows)} pendências antes de aplicar
              </button>
            ) : null}
          </div>
          <button
            type="button"
            className="btn-apply inline-flex h-14 shrink-0 items-center justify-center gap-3 rounded-2xl px-6 text-sm disabled:cursor-not-allowed"
            onClick={onApply}
            disabled={applyCommand.running || command.running || !desktop}
            aria-busy={applyCommand.running}
          >
            {applyCommand.running ? (
              <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
            ) : (
              <Send className="h-4 w-4" aria-hidden />
            )}
            {applyCommand.running ? "Enviando..." : "Aplicar no Kommo"}
          </button>
        </div>
        {!desktop ? (
          <div className="mt-4 flex items-start gap-2 rounded-2xl border border-amber-400/40 bg-amber-500/10 p-3 text-sm text-amber-100">
            <Info className="mt-0.5 h-4 w-4 shrink-0" aria-hidden />
            <span>O envio só é habilitado quando o app está conectado ao ambiente local.</span>
          </div>
        ) : null}
        {applyCommand.message ? (
          <div
            role="status"
            aria-live="polite"
            className={cx(
              "fade-in mt-4 flex items-start gap-3 rounded-2xl border px-4 py-3 text-sm",
              applyCommand.ok === false
                ? "border-rose-400/40 bg-rose-500/10 text-rose-100"
                : applyCommand.ok === true
                  ? "border-emerald-400/40 bg-emerald-500/10 text-emerald-100"
                  : "border-cyan-400/40 bg-cyan-500/10 text-cyan-100"
            )}
          >
            {applyCommand.ok === false ? (
              <AlertTriangle className="mt-0.5 h-5 w-5 shrink-0" aria-hidden />
            ) : applyCommand.ok === true ? (
              <CheckCircle2 className="mt-0.5 h-5 w-5 shrink-0" aria-hidden />
            ) : (
              <Loader2 className="mt-0.5 h-5 w-5 shrink-0 animate-spin" aria-hidden />
            )}
            <span className="leading-6">{applyCommand.message}</span>
          </div>
        ) : null}

        {applySteps.length ? (
          <div className="mt-5">
            <ProcessTracker title="Andamento da aplicação" steps={applySteps} running={applyCommand.running} />
          </div>
        ) : null}
      </Card>
    </div>
  );
}

function ReviewPage({ snapshot, rows }: { snapshot: Snapshot; rows: ReviewRow[] }) {
  const stats = snapshot.previewSummary?.field_stats ?? {};
  const service = stats.service;
  const reviewRows = snapshot.previewSummary?.review_field_row_count ?? snapshot.reviewRowsCount ?? 0;

  return (
    <div className="space-y-6">
      <Card className="hero-gradient p-8">
        <div className="flex flex-col gap-6 lg:flex-row lg:items-start lg:justify-between">
          <div className="max-w-2xl">
            <Pill tone="warn" icon={<AlertTriangle className="h-3.5 w-3.5" />}>
              Revisão humana
            </Pill>
            <h2 className="mt-4 text-4xl font-bold tracking-tight text-white">Pendências</h2>
            <p className="mt-3 text-base leading-7 text-slate-300">
              Estes itens <strong className="text-white">não serão enviados automaticamente</strong> ao Kommo.
              Confira cada sugestão e decida se quer corrigir o mapeamento ou deixar o item fora da automação.
            </p>
          </div>
          <div className="surface-raised rounded-3xl border border-amber-400/30 px-7 py-5 text-center">
            <p className="text-5xl font-bold text-amber-100">{number(reviewRows)}</p>
            <p className="mt-1 text-sm font-medium text-amber-200">aguardando revisão</p>
          </div>
        </div>
      </Card>

      <section className="grid grid-cols-1 gap-6 xl:grid-cols-[1fr_360px]">
        <Card className="p-6">
          <SectionTitle
            title="Itens para revisar"
            description="Compare o valor vindo da clínica com a sugestão de mapeamento."
            icon={<ClipboardList className="h-4 w-4" />}
          />
          <div className="thin-scrollbar mt-5 max-h-[560px] overflow-auto rounded-2xl border border-white/10">
            <table className="w-full min-w-[760px] border-collapse text-left text-sm">
              <thead className="sticky top-0 bg-slate-900/95 text-xs uppercase tracking-wide text-slate-400 backdrop-blur">
                <tr>
                  <th className="px-4 py-3 font-semibold">Cliente</th>
                  <th className="px-4 py-3 font-semibold">Campo</th>
                  <th className="px-4 py-3 font-semibold">Valor da clínica</th>
                  <th className="px-4 py-3 font-semibold">Sugestão</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-white/5">
                {rows.slice(0, 80).map((row, index) => (
                  <tr
                    key={`${row.patient_name}-${row.field_label}-${index}`}
                    className={cx(
                      "transition hover:bg-white/[0.05]",
                      index % 2 === 0 ? "bg-white/[0.02]" : "bg-transparent"
                    )}
                  >
                    <td className="px-4 py-3">
                      <p className="font-semibold text-white">{row.patient_name || row.lead_name}</p>
                      {row.lead_name && row.lead_name !== row.patient_name ? (
                        <p className="mt-1 text-xs text-slate-500">lead: {row.lead_name}</p>
                      ) : null}
                    </td>
                    <td className="px-4 py-3 font-medium text-slate-200">{row.field_label}</td>
                    <td className="max-w-[260px] px-4 py-3 text-slate-300">
                      <span className="line-clamp-2">{row.candidate_value}</span>
                    </td>
                    <td className="max-w-[220px] px-4 py-3 text-slate-300">
                      {row.mapped_value ? (
                        <span className="line-clamp-2">{row.mapped_value}</span>
                      ) : (
                        <Pill tone="warn">sem regra</Pill>
                      )}
                    </td>
                  </tr>
                ))}
                {!rows.length ? (
                  <tr>
                    <td className="px-4 py-12 text-center text-slate-400" colSpan={4}>
                      <div className="flex flex-col items-center gap-3">
                        <CheckCircle2 className="h-10 w-10 text-emerald-400/70" aria-hidden />
                        <p className="text-base font-semibold text-white">Nada para revisar</p>
                        <p className="text-sm text-slate-400">Toda a base está com mapeamentos automáticos.</p>
                      </div>
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>

          <div className="mt-5 flex items-start gap-2 rounded-2xl border border-cyan-400/30 bg-cyan-500/10 p-4 text-sm text-cyan-100">
            <Info className="mt-0.5 h-4 w-4 shrink-0" aria-hidden />
            <p className="leading-6">
              Para resolver uma pendência, ajuste o mapeamento do serviço nas planilhas da pasta
              <code className="mx-1 rounded bg-black/40 px-1.5 py-0.5 font-mono text-xs">mappings/</code>
              ou deixe o item fora da automação. Nada nesta tela vai ao Kommo automaticamente.
            </p>
          </div>
        </Card>

        <Card className="p-6">
          <SectionTitle
            title="Resumo de serviços"
            description="Números que compõem as pendências."
            icon={<Info className="h-4 w-4" />}
          />
          <dl className="mt-5 space-y-4">
            <div className="rounded-2xl border border-emerald-400/20 bg-emerald-400/10 p-4">
              <dt className="text-sm font-medium text-emerald-200">Serviços seguros</dt>
              <dd className="mt-1 text-3xl font-bold text-white">{number(service?.safe_fill)}</dd>
            </div>
            <div className="rounded-2xl border border-amber-400/30 bg-amber-400/10 p-4">
              <dt className="text-sm font-medium text-amber-100">Em revisão</dt>
              <dd className="mt-1 text-3xl font-bold text-amber-50">{number(service?.review_fill)}</dd>
            </div>
            <div className="rounded-2xl border border-rose-400/30 bg-rose-400/10 p-4">
              <dt className="text-sm font-medium text-rose-100">Sem mapeamento</dt>
              <dd className="mt-1 text-3xl font-bold text-rose-50">{number(service?.unmapped)}</dd>
            </div>
          </dl>
        </Card>
      </section>

      <Card className="p-6">
        <SectionTitle
          title="Campos que seguem manuais"
          description="Estes campos nunca entram na automação recorrente. Continue tratando-os diretamente no Kommo."
          icon={<Info className="h-4 w-4" />}
        />
        <div className="mt-5 grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {["Retorno", "Consultor", "Atendido por", "Pagamento / link", "Forma de resgate"].map((item) => (
            <div
              key={item}
              className="rounded-2xl border border-white/10 bg-white/[0.035] p-4 transition hover:border-white/20 hover:bg-white/[0.06]"
            >
              <p className="font-semibold text-white">{item}</p>
              <p className="mt-2 text-sm leading-5 text-slate-400">Sem regra segura para automação recorrente.</p>
            </div>
          ))}
        </div>
      </Card>
    </div>
  );
}

export default function App() {
  const { snapshot, desktop, refresh } = useSnapshot();
  const review = useReviewRows();
  const [page, setPage] = useState<Page>("sync");
  const [command, setCommand] = useState<CommandState>({ running: false, message: "", ok: null });
  const [applyCommand, setApplyCommand] = useState<CommandState>({ running: false, message: "", ok: null });
  const [syncSteps, setSyncSteps] = useState<StepState[]>([]);
  const [applySteps, setApplySteps] = useState<StepState[]>([]);

  useEffect(() => {
    let disposed = false;
    let unlisten: (() => void) | undefined;
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
        if (index >= 0) {
          next[index] = { ...next[index], status, message: payload.message };
          return next;
        }
        return [...next, { label: payload.step, status, message: payload.message }];
      });
    }).then((fn) => {
      unlisten = fn;
    });
    return () => {
      disposed = true;
      unlisten?.();
    };
  }, []);

  function createStepState(task: SyncTask | "apply") {
    return taskSteps[task].map((label) => ({ label, status: "idle" as StepStatus, message: "Aguardando." }));
  }

  async function runSyncTask(task: SyncTask) {
    const labels: Record<SyncTask, string> = {
      quick: "Atualização rápida em andamento. Pode levar alguns minutos.",
      full: "Atualização completa em andamento. Isso pode levar vários minutos.",
      clinic: "Atualizando dados da clínica...",
      operational: "Atualizando agenda, serviços e origem...",
      kommo: "Atualizando dados do Kommo...",
      preview: "Gerando nova prévia...",
      all: "Atualizando tudo. Isso pode levar alguns minutos."
    };
    setSyncSteps(createStepState(task));
    setCommand({ running: true, message: labels[task], ok: null, task });
    try {
      const result = await call<{ logs: Array<{ label: string }>; snapshot: Snapshot }>("run_sync_task", { task });
      const done = result.logs.map((item) => item.label).join(" → ");
      setCommand({ running: false, message: `Concluído: ${done}`, ok: true, task });
      await refresh();
      await review.refresh();
    } catch (error) {
      setCommand({ running: false, message: String(error), ok: false, task });
    }
  }

  async function applySafePayloads() {
    const confirmed = window.confirm(
      "Aplicar no Kommo somente as atualizações seguras? As pendências não serão enviadas."
    );
    if (!confirmed) {
      return;
    }
    setApplySteps(createStepState("apply"));
    setApplyCommand({ running: true, message: "Enviando atualizações seguras para o Kommo...", ok: null });
    try {
      const result = await call<{ logs: Array<{ label: string }>; snapshot: Snapshot }>("apply_safe_payloads");
      const done = result.logs.map((item) => item.label).join(" → ");
      setApplyCommand({ running: false, message: `Concluído: ${done}`, ok: true });
      await refresh();
      await review.refresh();
    } catch (error) {
      setApplyCommand({ running: false, message: String(error), ok: false });
    }
  }

  const safeRows = snapshot.previewSummary?.safe_field_row_count ?? snapshot.safeRowsCount ?? 0;
  const reviewRows = snapshot.previewSummary?.review_field_row_count ?? snapshot.reviewRowsCount ?? 0;
  const subtitle = useMemo(
    () => `${number(safeRows)} campos prontos · ${number(reviewRows)} para revisar`,
    [reviewRows, safeRows]
  );

  return (
    <div className="app-shell text-slate-100">
      <a className="skip-link" href="#conteudo">Pular para o conteúdo</a>
      <div className="mx-auto flex min-h-screen w-full max-w-7xl flex-col gap-6 px-4 py-6 sm:px-6 lg:py-8">
        <header className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex items-center gap-4">
            <div
              className="relative grid h-14 w-14 shrink-0 place-items-center rounded-2xl text-slate-950 shadow-lg shadow-emerald-500/30"
              style={{
                background: "linear-gradient(135deg, #34d399 0%, #22d3ee 100%)"
              }}
              aria-hidden
            >
              <Sparkles className="h-7 w-7" />
            </div>
            <div>
              <p className="text-xs font-bold uppercase tracking-[0.2em] text-emerald-300">
                Mirella Sync
              </p>
              <h1 className="text-2xl font-bold tracking-tight text-white sm:text-3xl">
                Atualização do Kommo
              </h1>
              <p className="mt-0.5 text-sm text-slate-400">{subtitle}</p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <div
              className={cx(
                "hidden items-center gap-2 rounded-full border px-4 py-2 text-xs font-semibold uppercase tracking-wide md:inline-flex",
                desktop
                  ? "border-emerald-400/30 bg-emerald-400/10 text-emerald-200"
                  : "border-amber-400/30 bg-amber-400/10 text-amber-200"
              )}
              aria-live="polite"
            >
              {desktop ? (
                <>
                  <span className="relative inline-block h-2 w-2">
                    <span className="absolute inset-0 rounded-full bg-emerald-400" />
                    <span className="absolute inset-0 animate-ping rounded-full bg-emerald-400/60" />
                  </span>
                  Conectado
                </>
              ) : (
                <>
                  <AlertTriangle className="h-3.5 w-3.5" />
                  Modo prévia
                </>
              )}
            </div>
            <nav
              className="flex items-center gap-1 rounded-2xl border border-white/10 bg-white/[0.04] p-1"
              aria-label="Navegação principal"
            >
              <button
                type="button"
                onClick={() => setPage("sync")}
                className={cx(
                  "inline-flex h-11 items-center gap-2 rounded-xl px-4 text-sm font-semibold transition",
                  page === "sync"
                    ? "bg-white text-slate-950 shadow-md"
                    : "text-slate-300 hover:bg-white/10 hover:text-white"
                )}
                aria-current={page === "sync" ? "page" : undefined}
              >
                <Zap className="h-4 w-4" aria-hidden />
                Rotina
              </button>
              <button
                type="button"
                onClick={() => setPage("review")}
                className={cx(
                  "relative inline-flex h-11 items-center gap-2 rounded-xl px-4 text-sm font-semibold transition",
                  page === "review"
                    ? "bg-white text-slate-950 shadow-md"
                    : "text-slate-300 hover:bg-white/10 hover:text-white"
                )}
                aria-current={page === "review" ? "page" : undefined}
              >
                <AlertTriangle className="h-4 w-4" aria-hidden />
                Pendências
                {reviewRows > 0 ? (
                  <span
                    className={cx(
                      "ml-1 inline-flex min-w-[20px] items-center justify-center rounded-full px-1.5 text-xs font-bold",
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
              onQuickUpdate={() => runSyncTask("quick")}
              onFullUpdate={() => runSyncTask("full")}
              onApply={applySafePayloads}
              onOpenReview={() => setPage("review")}
            />
          ) : (
            <ReviewPage snapshot={snapshot} rows={review.rows} />
          )}
        </main>

        <footer className="mt-2 flex flex-col items-center justify-between gap-2 border-t border-white/5 pt-4 text-xs text-slate-500 sm:flex-row">
          <p>Mirella Kommo Sync · dados processados localmente no seu computador</p>
          <p className="flex items-center gap-1.5">
            <Clock3 className="h-3 w-3" aria-hidden />
            Última prévia: {timeAgo(snapshot.localFiles?.safePayloads?.modifiedUnix)}
          </p>
        </footer>
      </div>
    </div>
  );
}
