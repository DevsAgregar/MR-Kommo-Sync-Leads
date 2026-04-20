import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import {
  AlertTriangle,
  CheckCircle2,
  Clock3,
  Database,
  HeartHandshake,
  Loader2,
  RefreshCw,
  Sparkles,
  Send,
  WalletCards,
  XCircle
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

const friendlyFields = [
  ["sale_value", "Venda", "último valor gasto"],
  ["billed_total", "Faturado", "total gasto pelo cliente"],
  ["visits", "Visitas", "quantidade de compras/visitas"],
  ["birthday_month", "Aniversariantes do Mês", "mês do aniversário por extenso"],
  ["last_visit", "Última visita", "último atendimento válido"],
  ["appointment", "Agendamento", "próximo agendamento"],
  ["next_consultation", "Próxima consulta", "próximo contato"],
  ["origin", "Origem", "canal de chegada"],
  ["service", "Serviço", "serviços identificados"]
] as const;

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
  if (!unixSeconds) return "sem data";
  const minutes = Math.floor(Math.max(0, Date.now() - unixSeconds * 1000) / 60000);
  if (minutes < 1) return "agora";
  if (minutes < 60) return `há ${minutes} min`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `há ${hours} h`;
  return `há ${Math.floor(hours / 24)} d`;
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
  return <section className={cx("rounded-3xl border border-white/10 bg-slate-900/72 shadow-2xl shadow-black/20", className)}>{children}</section>;
}

function Pill({ tone, children }: { tone: "ok" | "warn" | "muted"; children: React.ReactNode }) {
  const styles = {
    ok: "border-emerald-400/30 bg-emerald-400/10 text-emerald-200",
    warn: "border-amber-400/30 bg-amber-400/10 text-amber-200",
    muted: "border-white/10 bg-white/5 text-slate-300"
  }[tone];
  return <span className={cx("rounded-full border px-3 py-1 text-xs font-semibold", styles)}>{children}</span>;
}

function BigNumber({ label, value, help, tone }: { label: string; value: number; help: string; tone: "ok" | "warn" | "blue" }) {
  const color = {
    ok: "text-emerald-200 bg-emerald-400/10",
    warn: "text-amber-200 bg-amber-400/10",
    blue: "text-cyan-200 bg-cyan-400/10"
  }[tone];
  return (
    <Card className="p-5">
      <p className="text-sm text-slate-400">{label}</p>
      <p className="mt-2 text-5xl font-semibold tracking-normal text-white">{number(value)}</p>
      <div className={cx("mt-4 rounded-2xl px-3 py-2 text-sm", color)}>{help}</div>
    </Card>
  );
}

function ProcessTracker({
  title,
  steps,
  running
}: {
  title: string;
  steps: StepState[];
  running: boolean;
}) {
  if (!steps.length) return null;
  return (
    <section className="rounded-2xl border border-white/10 bg-white/[0.03] p-6">
      <div className="flex items-center justify-between gap-4">
        <div>
          <h3 className="text-lg font-semibold text-white">{title}</h3>
          <p className="mt-1 text-sm text-slate-400">
            {running ? "Processando etapas em segundo plano." : "Último andamento do processo."}
          </p>
        </div>
        {running ? <Loader2 className="h-5 w-5 animate-spin text-emerald-300" /> : null}
      </div>
      <div className="mt-5 space-y-3">
        {steps.map((step) => (
          <div key={step.label} className="rounded-2xl border border-white/10 bg-white/[0.035] px-4 py-3">
            <div className="flex items-center justify-between gap-3">
              <span className="font-medium text-white">{step.label}</span>
              {step.status === "running" ? (
                <Loader2 className="h-4 w-4 animate-spin text-cyan-300" />
              ) : step.status === "done" ? (
                <CheckCircle2 className="h-4 w-4 text-emerald-300" />
              ) : step.status === "error" ? (
                <XCircle className="h-4 w-4 text-rose-300" />
              ) : (
                <Clock3 className="h-4 w-4 text-slate-500" />
              )}
            </div>
            <p className="mt-2 text-sm text-slate-400">{step.message ?? "Aguardando."}</p>
          </div>
        ))}
      </div>
    </section>
  );
}

function DataFreshness({ snapshot }: { snapshot: Snapshot }) {
  const files = [
    ["Clínica", snapshot.localFiles?.patientDb],
    ["Kommo", snapshot.localFiles?.kommoDb],
    ["Prévia", snapshot.localFiles?.safePayloads]
  ] as const;

  return (
    <div className="grid grid-cols-3 gap-3">
      {files.map(([label, meta]) => (
        <div key={label} className="rounded-2xl border border-white/10 bg-white/[0.035] p-4">
          <div className="flex items-center justify-between gap-3">
            <span className="text-sm font-semibold text-white">{label}</span>
            {meta?.exists ? <CheckCircle2 className="h-4 w-4 text-emerald-300" /> : <XCircle className="h-4 w-4 text-rose-300" />}
          </div>
          <p className="mt-2 text-xs text-slate-500">{timeAgo(meta?.modifiedUnix)}</p>
        </div>
      ))}
    </div>
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
  onApply
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
}) {
  const summary = snapshot.previewSummary;
  const actions = summary?.action_counts ?? {};
  const safeLeads = summary?.safe_lead_count ?? snapshot.safePayloadCount ?? 0;
  const safeRows = summary?.safe_field_row_count ?? snapshot.safeRowsCount ?? 0;
  const reviewRows = summary?.review_field_row_count ?? snapshot.reviewRowsCount ?? 0;

  return (
    <main className="space-y-6">
      <Card className="p-7">
        <div className="flex items-start justify-between gap-6">
          <div>
            <Pill tone={desktop ? "ok" : "warn"}>{desktop ? "tudo roda localmente no computador" : "modo visual"}</Pill>
            <h2 className="mt-5 max-w-3xl text-4xl font-semibold leading-tight tracking-normal text-white">
              Atualizar os dados do Kommo
            </h2>
            <p className="mt-4 max-w-2xl text-base leading-7 text-slate-300">
              Escolha o tipo de atualização. A rápida é a recomendada para o dia a dia. A completa reprocessa tudo desde o início e demora mais.
            </p>
          </div>
          <div className="flex shrink-0 flex-col gap-3">
            <button
              className="inline-flex h-14 items-center gap-3 rounded-2xl bg-emerald-400 px-6 text-base font-semibold text-slate-950 transition hover:bg-emerald-300 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400"
              onClick={onQuickUpdate}
              disabled={command.running}
            >
              {command.running && command.task === "quick" ? <Loader2 className="h-5 w-5 animate-spin" /> : <RefreshCw className="h-5 w-5" />}
              Atualização rápida
            </button>
            <button
              className="inline-flex h-12 items-center gap-3 rounded-2xl border border-white/10 bg-white/[0.05] px-5 text-sm font-semibold text-white transition hover:bg-white/[0.08] disabled:cursor-not-allowed disabled:bg-slate-800 disabled:text-slate-500"
              onClick={onFullUpdate}
              disabled={command.running}
            >
              {command.running && command.task === "full" ? <Loader2 className="h-4 w-4 animate-spin" /> : <Clock3 className="h-4 w-4" />}
              Atualização completa
            </button>
          </div>
        </div>

        {command.message ? (
          <div className={cx("mt-6 rounded-2xl border px-4 py-3 text-sm", command.ok === false ? "border-rose-400/30 bg-rose-400/10 text-rose-100" : "border-emerald-400/30 bg-emerald-400/10 text-emerald-100")}>
            {command.message}
          </div>
        ) : null}

        <div className="mt-6 rounded-2xl border border-white/10 bg-black/20 p-5">
          <h3 className="text-lg font-semibold text-white">O que cada opção faz</h3>
          <div className="mt-4 grid grid-cols-4 gap-3">
            {[
              ["Rápida", "Atualiza só o que importa no dia a dia", "usa clínica incremental, agenda só dos pacientes relevantes e Kommo incremental"],
              ["Completa", "Reprocessa tudo do começo", "serve para auditoria, correção ou revisão geral"],
              ["Resultado", "Gera a prévia no final", "o painel já reflete o estado vigente depois da execução"],
              ["Aplicação", "Não envia nada direto ao Kommo", "apenas prepara o que está seguro e separa o que precisa revisar"]
            ].map(([step, title, text]) => (
              <div key={title} className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <div className="flex items-center gap-3">
                  <div className="rounded-full bg-emerald-400 px-3 py-1 text-xs font-bold text-slate-950">{step}</div>
                  <span className="font-semibold text-white">{title}</span>
                </div>
                <p className="mt-3 text-sm leading-5 text-slate-400">{text}</p>
              </div>
            ))}
          </div>
        </div>

        <div className="mt-7">
          <DataFreshness snapshot={snapshot} />
        </div>
      </Card>

      <section className="grid grid-cols-3 gap-4">
        <BigNumber label="Leads prontos" value={safeLeads} help="podem receber atualização segura" tone="ok" />
        <BigNumber label="Atualizações" value={safeRows} help="campos preparados para o Kommo" tone="blue" />
        <BigNumber label="Para revisar" value={reviewRows} help="não serão aplicados automaticamente" tone="warn" />
      </section>

      <Card className="p-6">
        <h3 className="text-xl font-semibold text-white">O que será preparado</h3>
        <div className="mt-5 grid grid-cols-4 gap-3">
          <div className="rounded-2xl border border-white/10 bg-white/[0.035] p-4">
            <p className="text-sm text-slate-400">Preencher vazios</p>
            <p className="mt-2 text-2xl font-semibold text-white">{number(actions.fill_empty)}</p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.035] p-4">
            <p className="text-sm text-slate-400">Aumentar valores</p>
            <p className="mt-2 text-2xl font-semibold text-white">{number(actions.update_if_greater)}</p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.035] p-4">
            <p className="text-sm text-slate-400">Atualizar datas</p>
            <p className="mt-2 text-2xl font-semibold text-white">{number(actions.update_if_newer)}</p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.035] p-4">
            <p className="text-sm text-slate-400">Adicionar serviços</p>
            <p className="mt-2 text-2xl font-semibold text-white">{number(actions.merge)}</p>
          </div>
        </div>
      </Card>

      <Card className="p-6">
        <h3 className="text-xl font-semibold text-white">Quando usar cada uma</h3>
        <div className="mt-5 grid grid-cols-2 gap-4">
          <div className="rounded-2xl border border-emerald-400/20 bg-emerald-400/10 p-5">
            <p className="text-lg font-semibold text-white">Atualização rápida</p>
            <p className="mt-2 text-sm leading-6 text-slate-200">
              Use no trabalho do dia a dia. Ela é mais rápida porque foca no que já é relevante para o Kommo.
            </p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-5">
            <p className="text-lg font-semibold text-white">Atualização completa</p>
            <p className="mt-2 text-sm leading-6 text-slate-300">
              Use quando quiser revisar tudo do começo, corrigir base ou auditar o processo completo.
            </p>
          </div>
        </div>
      </Card>

      <ProcessTracker title="Andamento da atualização" steps={syncSteps} running={command.running} />

      <Card className="p-6">
        <div className="flex items-start justify-between gap-6">
          <div>
            <Pill tone="warn">etapa separada</Pill>
            <h3 className="mt-4 text-2xl font-semibold text-white">Aplicar no Kommo</h3>
            <p className="mt-3 max-w-2xl text-sm leading-6 text-slate-300">
              Esta ação envia somente as atualizações seguras para o Kommo. Pendências da aba ao lado não são enviadas.
            </p>
          </div>
          <button
            className="inline-flex h-13 shrink-0 items-center gap-3 rounded-2xl bg-white px-5 py-4 text-sm font-semibold text-slate-950 transition hover:bg-slate-200 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400"
            onClick={onApply}
            disabled={applyCommand.running || command.running}
          >
            {applyCommand.running ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
            Aplicar atualizações seguras
          </button>
        </div>
        {applyCommand.message ? (
          <div className={cx("mt-5 rounded-2xl border px-4 py-3 text-sm", applyCommand.ok === false ? "border-rose-400/30 bg-rose-400/10 text-rose-100" : "border-emerald-400/30 bg-emerald-400/10 text-emerald-100")}>
            {applyCommand.message}
          </div>
        ) : null}
        <div className="mt-5">
          <ProcessTracker title="Andamento da aplicação" steps={applySteps} running={applyCommand.running} />
        </div>
      </Card>
    </main>
  );
}

function ReviewPage({ snapshot, rows }: { snapshot: Snapshot; rows: ReviewRow[] }) {
  const stats = snapshot.previewSummary?.field_stats ?? {};
  const service = stats.service;
  const reviewRows = snapshot.previewSummary?.review_field_row_count ?? snapshot.reviewRowsCount ?? 0;

  return (
    <main className="space-y-6">
      <Card className="p-7">
        <div className="flex items-start justify-between gap-6">
          <div>
            <Pill tone="warn">revisão antes de aplicar</Pill>
            <h2 className="mt-5 text-4xl font-semibold tracking-normal text-white">Pendências</h2>
            <p className="mt-4 max-w-2xl text-base leading-7 text-slate-300">
              Estes itens precisam de decisão humana. O app não deve enviar esses dados automaticamente para o Kommo.
            </p>
          </div>
          <div className="rounded-3xl border border-amber-400/20 bg-amber-400/10 px-6 py-5 text-center">
            <p className="text-4xl font-semibold text-amber-100">{number(reviewRows)}</p>
            <p className="mt-1 text-sm text-amber-200">itens aguardando</p>
          </div>
        </div>
      </Card>

      <section className="grid grid-cols-[1fr_360px] gap-6">
        <Card className="p-6">
          <h3 className="text-xl font-semibold text-white">Itens para revisar</h3>
          <div className="thin-scrollbar mt-5 max-h-[560px] overflow-auto rounded-2xl border border-white/10">
            <table className="w-full min-w-[760px] border-collapse text-left text-sm">
              <thead className="bg-white/[0.06] text-xs uppercase tracking-wide text-slate-400">
                <tr>
                  <th className="px-4 py-3">Cliente</th>
                  <th className="px-4 py-3">Campo</th>
                  <th className="px-4 py-3">Valor da clínica</th>
                  <th className="px-4 py-3">Sugestão</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-white/10">
                {rows.slice(0, 80).map((row, index) => (
                  <tr key={`${row.patient_name}-${row.field_label}-${index}`} className="bg-white/[0.025]">
                    <td className="px-4 py-3">
                      <p className="font-semibold text-white">{row.patient_name || row.lead_name}</p>
                      <p className="mt-1 text-xs text-slate-500">{row.lead_name}</p>
                    </td>
                    <td className="px-4 py-3 text-slate-200">{row.field_label}</td>
                    <td className="max-w-[260px] px-4 py-3 text-slate-300">
                      <span className="line-clamp-2">{row.candidate_value}</span>
                    </td>
                    <td className="max-w-[220px] px-4 py-3 text-slate-300">
                      {row.mapped_value ? <span className="line-clamp-2">{row.mapped_value}</span> : <Pill tone="warn">sem regra</Pill>}
                    </td>
                  </tr>
                ))}
                {!rows.length ? (
                  <tr>
                    <td className="px-4 py-8 text-center text-slate-400" colSpan={4}>
                      Nenhuma pendência carregada.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>

          <div className="mt-6 rounded-2xl border border-white/10 bg-black/20 p-4">
            <p className="text-sm leading-6 text-slate-300">
              Para resolver uma pendência, ajuste o mapeamento do serviço ou deixe o item fora da automação. Nada nesta tela é aplicado automaticamente.
            </p>
          </div>
        </Card>

        <Card className="p-6">
          <h3 className="text-xl font-semibold text-white">Resumo</h3>
          <div className="mt-5 space-y-4">
            <div>
              <p className="text-sm text-slate-400">Serviços seguros</p>
              <p className="mt-1 text-3xl font-semibold text-white">{number(service?.safe_fill)}</p>
            </div>
            <div>
              <p className="text-sm text-slate-400">Serviços em revisão</p>
              <p className="mt-1 text-3xl font-semibold text-amber-100">{number(service?.review_fill)}</p>
            </div>
            <div>
              <p className="text-sm text-slate-400">Sem mapa</p>
              <p className="mt-1 text-3xl font-semibold text-rose-100">{number(service?.unmapped)}</p>
            </div>
          </div>
        </Card>
      </section>

      <Card className="p-6">
        <h3 className="text-xl font-semibold text-white">Campos que continuam manuais</h3>
        <div className="mt-5 grid grid-cols-3 gap-3">
          {["Retorno", "Consultor", "Atendido por", "Pagamento/link", "Forma de Resgate"].map((item) => (
            <div key={item} className="rounded-2xl border border-white/10 bg-white/[0.035] p-4">
              <p className="font-semibold text-white">{item}</p>
              <p className="mt-2 text-sm leading-5 text-slate-500">sem regra segura para automação recorrente</p>
            </div>
          ))}
        </div>
      </Card>
    </main>
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
      quick: "Atualização rápida em andamento.",
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
  const subtitle = useMemo(() => `${number(safeRows)} atualizações seguras · ${number(reviewRows)} para revisar`, [reviewRows, safeRows]);

  return (
    <div className="app-shell text-slate-100">
      <div className="mx-auto flex min-h-screen w-full max-w-7xl flex-col px-6 py-6">
        <header className="flex items-center justify-between gap-5">
          <div className="flex items-center gap-4">
            <div className="grid h-14 w-14 place-items-center rounded-3xl bg-emerald-400 text-slate-950">
              <Sparkles className="h-7 w-7" />
            </div>
            <div>
              <p className="text-sm font-semibold uppercase tracking-[0.16em] text-emerald-300">Mirella Sync</p>
              <h1 className="text-3xl font-semibold tracking-normal text-white">Atualização do Kommo</h1>
              <p className="mt-1 text-sm text-slate-400">{subtitle}</p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <div className="hidden items-center gap-2 rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-sm text-slate-300 md:flex">
              {desktop ? <CheckCircle2 className="h-4 w-4 text-emerald-300" /> : <AlertTriangle className="h-4 w-4 text-amber-300" />}
              {desktop ? "App conectado" : "Modo prévia"}
            </div>
            <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-1">
              <button
                onClick={() => setPage("sync")}
                className={cx("h-11 rounded-xl px-5 text-sm font-semibold transition", page === "sync" ? "bg-white text-slate-950" : "text-slate-400 hover:text-white")}
              >
                Rotina
              </button>
              <button
                onClick={() => setPage("review")}
                className={cx("h-11 rounded-xl px-5 text-sm font-semibold transition", page === "review" ? "bg-white text-slate-950" : "text-slate-400 hover:text-white")}
              >
                Pendências
              </button>
            </div>
          </div>
        </header>

        <div className="mt-7 flex-1">
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
            />
          ) : (
            <ReviewPage snapshot={snapshot} rows={review.rows} />
          )}
        </div>
      </div>
    </div>
  );
}
