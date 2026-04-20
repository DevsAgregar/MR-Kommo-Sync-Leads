import { invoke } from "@tauri-apps/api/core";
import {
  AlertTriangle,
  ArrowRight,
  BadgeCheck,
  CalendarClock,
  CheckCircle2,
  ChevronRight,
  ClipboardCheck,
  Database,
  FileCheck2,
  Gauge,
  HeartHandshake,
  Layers3,
  Loader2,
  LockKeyhole,
  MapPinned,
  Play,
  RefreshCw,
  ShieldCheck,
  Sparkles,
  UsersRound,
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
      overlap_name_count: number;
      ambiguous_overlap_name_count: number;
      exact_unique_match_count: number;
    };
    field_stats?: Record<string, FieldStat>;
    action_counts?: Record<string, number>;
    safe_lead_count?: number;
    safe_field_row_count?: number;
    review_field_row_count?: number;
    all_action_row_count?: number;
  } | null;
  safePayloadCount?: number;
  safeRowsCount?: number;
  reviewRowsCount?: number;
  mappings?: {
    originRows: number;
    serviceRows: number;
  };
  localFiles?: Record<string, { exists: boolean; bytes: number }>;
};

type CommandState = {
  running: boolean;
  message: string;
  ok: boolean | null;
};

type ScreenKey = "home" | "updates" | "review" | "rules" | "security";

const fallbackSnapshot: Snapshot = {
  previewSummary: {
    match_summary: {
      patient_count: 755,
      lead_count: 13618,
      overlap_name_count: 371,
      ambiguous_overlap_name_count: 33,
      exact_unique_match_count: 338
    },
    safe_lead_count: 282,
    safe_field_row_count: 964,
    review_field_row_count: 107,
    all_action_row_count: 2684,
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
      birthday: { candidate: 300, safe_fill: 89, review_fill: 0, unmapped: 0 },
      age_bucket: { candidate: 300, safe_fill: 45, review_fill: 0, unmapped: 0 },
      status: { candidate: 338, safe_fill: 65, review_fill: 0, unmapped: 0 },
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
  mappings: {
    originRows: 25,
    serviceRows: 86
  },
  localFiles: {
    env: { exists: true, bytes: 556 },
    kommoState: { exists: false, bytes: 0 },
    patientDb: { exists: true, bytes: 5251072 },
    kommoDb: { exists: true, bytes: 116936704 },
    safePayloads: { exists: true, bytes: 191581 },
    reviewRows: { exists: true, bytes: 46694 }
  }
};

const fieldCopy: Record<string, { label: string; explanation: string; behavior: string; tone: "safe" | "review" | "locked" }> = {
  sale_value: {
    label: "Venda",
    explanation: "Último valor gasto na clínica",
    behavior: "preenche vazio ou aumenta quando houver venda maior",
    tone: "safe"
  },
  billed_total: {
    label: "Faturado",
    explanation: "Total acumulado que o cliente já investiu",
    behavior: "atualiza quando o total da clínica for maior",
    tone: "safe"
  },
  visits: {
    label: "Visitas",
    explanation: "Quantidade de compras/visitas financeiras vinculadas",
    behavior: "atualiza quando aumentar",
    tone: "safe"
  },
  birthday: {
    label: "Data de aniversário",
    explanation: "Data de nascimento do cadastro do paciente",
    behavior: "preenche apenas se estiver vazio",
    tone: "safe"
  },
  age_bucket: {
    label: "Faixa Etária",
    explanation: "Faixa calculada pela data de nascimento",
    behavior: "preenche apenas se estiver vazio",
    tone: "safe"
  },
  status: {
    label: "Status do Cliente",
    explanation: "Status atual no cadastro da clínica",
    behavior: "preenche apenas se estiver vazio",
    tone: "safe"
  },
  last_visit: {
    label: "Última visita",
    explanation: "Último atendimento válido na agenda",
    behavior: "atualiza quando a data for mais nova",
    tone: "safe"
  },
  appointment: {
    label: "Agendamento",
    explanation: "Próximo agendamento válido",
    behavior: "atualiza se o agendamento mudar",
    tone: "safe"
  },
  next_consultation: {
    label: "Próxima consulta",
    explanation: "Próximo contato vindo da agenda",
    behavior: "preenche/atualiza a próxima data",
    tone: "safe"
  },
  origin: {
    label: "Origem",
    explanation: "Indicação/canal de chegada no cadastro",
    behavior: "preenche apenas se estiver vazio",
    tone: "safe"
  },
  service: {
    label: "Serviço",
    explanation: "Serviços identificados no tratamento/agendamento",
    behavior: "mescla serviços novos sem apagar os antigos",
    tone: "review"
  }
};

const manualItems = [
  ["Retorno", "depende de regra clínica por procedimento"],
  ["Consultor", "depende do processo comercial"],
  ["Atendido por", "precisa regra de prioridade entre agenda e funil"],
  ["Pagamento/link", "fora do fluxo principal do sync"],
  ["Forma de Resgate", "depende de campanha/funil"]
];

const screens: Array<{ key: ScreenKey; label: string; icon: typeof Gauge }> = [
  { key: "home", label: "Início", icon: Gauge },
  { key: "updates", label: "Atualizações", icon: ClipboardCheck },
  { key: "review", label: "Revisar", icon: AlertTriangle },
  { key: "rules", label: "Regras", icon: MapPinned },
  { key: "security", label: "Segurança", icon: ShieldCheck }
];

function cx(...classes: Array<string | false | null | undefined>) {
  return classes.filter(Boolean).join(" ");
}

function number(value: number | undefined) {
  return new Intl.NumberFormat("pt-BR").format(value ?? 0);
}

function bytes(value: number | undefined) {
  if (!value) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let amount = value;
  let index = 0;
  while (amount >= 1024 && index < units.length - 1) {
    amount /= 1024;
    index += 1;
  }
  return `${amount.toFixed(amount > 9 ? 0 : 1)} ${units[index]}`;
}

async function call<T>(command: string, args?: Record<string, unknown>): Promise<T> {
  return invoke<T>(command, args);
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

function StatusBadge({ tone, children }: { tone: "safe" | "review" | "muted" | "danger"; children: React.ReactNode }) {
  const styles = {
    safe: "border-emerald-400/30 bg-emerald-400/10 text-emerald-200",
    review: "border-amber-400/30 bg-amber-400/10 text-amber-200",
    muted: "border-white/10 bg-white/5 text-slate-300",
    danger: "border-rose-400/30 bg-rose-400/10 text-rose-200"
  }[tone];
  return <span className={cx("rounded-full border px-2.5 py-1 text-xs font-semibold", styles)}>{children}</span>;
}

function SoftPanel({ children, className }: { children: React.ReactNode; className?: string }) {
  return <section className={cx("rounded-2xl border border-white/10 bg-slate-900/72 shadow-2xl shadow-black/20", className)}>{children}</section>;
}

function Metric({
  label,
  value,
  hint,
  icon: Icon,
  tone = "emerald"
}: {
  label: string;
  value: string;
  hint: string;
  icon: typeof Gauge;
  tone?: "emerald" | "cyan" | "amber" | "violet";
}) {
  const tones = {
    emerald: "bg-emerald-400/12 text-emerald-200",
    cyan: "bg-cyan-400/12 text-cyan-200",
    amber: "bg-amber-400/12 text-amber-200",
    violet: "bg-violet-400/12 text-violet-200"
  };
  return (
    <SoftPanel className="p-5">
      <div className="flex items-start justify-between gap-4">
        <div>
          <p className="text-sm text-slate-400">{label}</p>
          <p className="mt-2 text-4xl font-semibold tracking-normal text-white">{value}</p>
        </div>
        <div className={cx("grid h-12 w-12 place-items-center rounded-2xl", tones[tone])}>
          <Icon className="h-6 w-6" />
        </div>
      </div>
      <p className="mt-4 text-sm leading-5 text-slate-400">{hint}</p>
    </SoftPanel>
  );
}

function ActionItem({ label, value, description }: { label: string; value: number; description: string }) {
  return (
    <div className="rounded-xl border border-white/10 bg-white/[0.03] p-4">
      <p className="text-sm font-medium text-slate-300">{label}</p>
      <p className="mt-2 text-2xl font-semibold tabular-nums text-white">{number(value)}</p>
      <p className="mt-2 text-xs leading-5 text-slate-500">{description}</p>
    </div>
  );
}

function FieldCard({ slug, stat }: { slug: string; stat: FieldStat | undefined }) {
  const copy = fieldCopy[slug] ?? { label: slug, explanation: "Campo do Kommo", behavior: "regra não documentada", tone: "review" as const };
  const total = stat?.candidate ?? 0;
  const safe = stat?.safe_fill ?? 0;
  const pct = total ? Math.round((safe / total) * 100) : 0;

  return (
    <div className="rounded-2xl border border-white/10 bg-white/[0.035] p-4">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h3 className="font-semibold text-white">{copy.label}</h3>
          <p className="mt-1 text-sm text-slate-400">{copy.explanation}</p>
        </div>
        <StatusBadge tone={copy.tone === "safe" ? "safe" : "review"}>{copy.tone === "safe" ? "pronto" : "atenção"}</StatusBadge>
      </div>
      <div className="mt-4 h-2 overflow-hidden rounded-full bg-slate-800">
        <div className="h-full rounded-full bg-emerald-400" style={{ width: `${pct}%` }} />
      </div>
      <div className="mt-3 flex items-center justify-between text-sm">
        <span className="text-slate-400">{copy.behavior}</span>
        <span className="font-semibold tabular-nums text-slate-200">
          {number(safe)} / {number(total)}
        </span>
      </div>
    </div>
  );
}

function Step({
  index,
  title,
  text,
  state
}: {
  index: number;
  title: string;
  text: string;
  state: "done" | "current" | "locked";
}) {
  const styles = {
    done: "border-emerald-400/25 bg-emerald-400/10 text-emerald-100",
    current: "border-cyan-400/25 bg-cyan-400/10 text-cyan-100",
    locked: "border-white/10 bg-white/[0.03] text-slate-400"
  }[state];
  return (
    <div className={cx("rounded-2xl border p-4", styles)}>
      <div className="flex items-center gap-3">
        <div className="grid h-9 w-9 place-items-center rounded-full bg-black/20 text-sm font-semibold">{index}</div>
        <h3 className="font-semibold">{title}</h3>
      </div>
      <p className="mt-3 text-sm leading-5 opacity-80">{text}</p>
    </div>
  );
}

function Home({ snapshot, onPreview }: { snapshot: Snapshot; onPreview: () => void }) {
  const summary = snapshot.previewSummary;
  const actions = summary?.action_counts ?? {};
  const safeLeads = summary?.safe_lead_count ?? snapshot.safePayloadCount ?? 0;
  const safeRows = summary?.safe_field_row_count ?? snapshot.safeRowsCount ?? 0;
  const reviewRows = summary?.review_field_row_count ?? snapshot.reviewRowsCount ?? 0;

  return (
    <main className="space-y-6">
      <section className="grid grid-cols-[1.25fr_0.75fr] gap-6">
        <SoftPanel className="overflow-hidden p-6">
          <div className="flex items-start justify-between gap-6">
            <div>
              <StatusBadge tone="safe">prévia local, sem aplicar no Kommo</StatusBadge>
              <h2 className="mt-5 max-w-2xl text-4xl font-semibold leading-tight tracking-normal text-white">
                Atualize leads com dados da clínica sem apagar trabalho manual.
              </h2>
              <p className="mt-4 max-w-2xl text-base leading-7 text-slate-300">
                O app compara Clínica Ágil e Kommo, monta uma prévia segura e separa o que precisa de decisão humana.
              </p>
            </div>
            <button
              className="inline-flex h-12 shrink-0 items-center gap-2 rounded-xl bg-emerald-400 px-5 text-sm font-semibold text-slate-950 transition hover:bg-emerald-300"
              onClick={onPreview}
            >
              <RefreshCw className="h-4 w-4" />
              Recalcular prévia
            </button>
          </div>
          <div className="mt-8 grid grid-cols-4 gap-3">
            <ActionItem label="Preencher vazios" value={actions.fill_empty ?? 0} description="campos que ainda não têm valor" />
            <ActionItem label="Aumentar valores" value={actions.update_if_greater ?? 0} description="venda, faturado e visitas" />
            <ActionItem label="Datas novas" value={actions.update_if_newer ?? 0} description="agenda e próxima consulta" />
            <ActionItem label="Mesclar serviços" value={actions.merge ?? 0} description="adiciona sem remover" />
          </div>
        </SoftPanel>

        <SoftPanel className="p-6">
          <h2 className="text-lg font-semibold text-white">Fluxo simples</h2>
          <div className="mt-5 space-y-3">
            <Step index={1} title="Preparar dados" text="clínica, Kommo e mapeamentos locais" state="done" />
            <Step index={2} title="Conferir prévia" text="ver o que será atualizado e o que exige revisão" state="current" />
            <Step index={3} title="Aplicar depois" text="envio real fica bloqueado até aprovação" state="locked" />
          </div>
        </SoftPanel>
      </section>

      <section className="grid grid-cols-4 gap-4">
        <Metric label="Leads prontos" value={number(safeLeads)} hint="leads com pelo menos uma ação segura" icon={UsersRound} tone="emerald" />
        <Metric label="Atualizações seguras" value={number(safeRows)} hint="ações que podem entrar no payload" icon={BadgeCheck} tone="cyan" />
        <Metric label="Pendências" value={number(reviewRows)} hint="itens que pedem revisão antes de aplicar" icon={AlertTriangle} tone="amber" />
        <Metric label="Matches seguros" value={number(summary?.match_summary?.exact_unique_match_count)} hint="nome único em clínica e Kommo" icon={HeartHandshake} tone="violet" />
      </section>
    </main>
  );
}

function Updates({ snapshot, state, onPreview }: { snapshot: Snapshot; state: CommandState; onPreview: () => void }) {
  const stats = snapshot.previewSummary?.field_stats ?? {};
  const order = ["sale_value", "billed_total", "visits", "last_visit", "appointment", "next_consultation", "birthday", "age_bucket", "status", "origin", "service"];

  return (
    <main className="space-y-6">
      <SoftPanel className="p-6">
        <div className="flex items-center justify-between gap-4">
          <div>
            <h2 className="text-2xl font-semibold text-white">Atualizações encontradas</h2>
            <p className="mt-2 text-sm text-slate-400">Aqui aparece o que o app consegue preparar com segurança.</p>
          </div>
          <button
            className="inline-flex h-11 items-center gap-2 rounded-xl bg-emerald-400 px-4 text-sm font-semibold text-slate-950 transition hover:bg-emerald-300 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400"
            onClick={onPreview}
            disabled={state.running}
          >
            {state.running ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
            Gerar prévia
          </button>
        </div>
        {state.message ? (
          <div className={cx("mt-5 rounded-xl border px-4 py-3 text-sm", state.ok === false ? "border-rose-400/30 bg-rose-400/10 text-rose-100" : "border-emerald-400/30 bg-emerald-400/10 text-emerald-100")}>
            {state.message}
          </div>
        ) : null}
      </SoftPanel>

      <section className="grid grid-cols-2 gap-4">
        {order.map((slug) => (
          <FieldCard key={slug} slug={slug} stat={stats[slug]} />
        ))}
      </section>
    </main>
  );
}

function Review({ snapshot }: { snapshot: Snapshot }) {
  const service = snapshot.previewSummary?.field_stats?.service;
  const reviewRows = snapshot.previewSummary?.review_field_row_count ?? snapshot.reviewRowsCount ?? 0;

  return (
    <main className="grid grid-cols-[1fr_360px] gap-6">
      <SoftPanel className="p-6">
        <div className="flex items-center gap-3">
          <AlertTriangle className="h-6 w-6 text-amber-300" />
          <div>
            <h2 className="text-2xl font-semibold text-white">O que precisa de decisão</h2>
            <p className="mt-1 text-sm text-slate-400">Principalmente serviços sem equivalente claro no Kommo.</p>
          </div>
        </div>
        <div className="mt-6 grid grid-cols-3 gap-3">
          <ActionItem label="Linhas em revisão" value={reviewRows} description="não entram no apply automático" />
          <ActionItem label="Serviços em revisão" value={service?.review_fill ?? 0} description="mapeamento com confiança média" />
          <ActionItem label="Sem mapa" value={service?.unmapped ?? 0} description="precisam regra nova ou opção no Kommo" />
        </div>

        <div className="mt-6 rounded-2xl border border-white/10 bg-white/[0.03] p-5">
          <h3 className="font-semibold text-white">Serviços que ainda travam 100%</h3>
          <div className="mt-4 grid grid-cols-2 gap-3">
            {["Avaliação Facial", "Avaliação Corporal", "Exossomos", "Manthus", "Furo em Orelha", "Vitamina D"].map((item) => (
              <div key={item} className="flex items-center justify-between rounded-xl bg-black/20 px-4 py-3">
                <span className="text-sm text-slate-200">{item}</span>
                <StatusBadge tone="review">decidir</StatusBadge>
              </div>
            ))}
          </div>
        </div>
      </SoftPanel>

      <SoftPanel className="p-6">
        <h3 className="text-lg font-semibold text-white">Regras de aplicação</h3>
        <div className="mt-5 space-y-3 text-sm leading-6 text-slate-300">
          <p><strong className="text-white">Não sobrescreve à toa.</strong> Só muda valor quando a regra permite.</p>
          <p><strong className="text-white">Serviço mescla.</strong> O app adiciona serviços novos sem apagar os antigos.</p>
          <p><strong className="text-white">Retorno fica fora.</strong> Sem regra validada, continua manual.</p>
        </div>
      </SoftPanel>
    </main>
  );
}

function Rules({ snapshot }: { snapshot: Snapshot }) {
  const [originCsv, setOriginCsv] = useState("");
  const [serviceCsv, setServiceCsv] = useState("");

  useEffect(() => {
    async function load() {
      try {
        const [origin, service] = await Promise.all([
          call<string>("read_mapping", { kind: "origin" }),
          call<string>("read_mapping", { kind: "service" })
        ]);
        setOriginCsv(origin);
        setServiceCsv(service);
      } catch {
        setOriginCsv("raw_value,mapped_value,confidence\nanuncio,Anúncio Meta,high\nsite,Site,high");
        setServiceCsv("raw_value,mapped_value,confidence\nbotox,Botox,high\nsense on sculpture,Massagem SOS,high");
      }
    }
    void load();
  }, []);

  return (
    <main className="grid grid-cols-2 gap-6">
      {[
        ["Origem", snapshot.mappings?.originRows ?? 0, originCsv, "canal de entrada do paciente"],
        ["Serviço", snapshot.mappings?.serviceRows ?? 0, serviceCsv, "família de tratamento para o Kommo"]
      ].map(([title, rows, csv, hint]) => (
        <SoftPanel key={String(title)} className="p-6">
          <div className="flex items-center justify-between gap-4">
            <div>
              <h2 className="text-xl font-semibold text-white">{title}</h2>
              <p className="mt-1 text-sm text-slate-400">{hint}</p>
            </div>
            <StatusBadge tone={title === "Origem" ? "safe" : "review"}>{number(Number(rows))} regras</StatusBadge>
          </div>
          <pre className="thin-scrollbar mt-5 h-[560px] overflow-auto rounded-2xl border border-white/10 bg-black/40 p-4 text-xs leading-5 text-slate-200">
            {String(csv)}
          </pre>
        </SoftPanel>
      ))}
    </main>
  );
}

function Security({ snapshot, state, onSecretCheck }: { snapshot: Snapshot; state: CommandState; onSecretCheck: () => void }) {
  const files = [
    ["Configuração local", "env", LockKeyhole],
    ["Sessão Kommo", "kommoState", ShieldCheck],
    ["Banco da clínica", "patientDb", Database],
    ["Banco do Kommo", "kommoDb", Database],
    ["Payloads", "safePayloads", FileCheck2],
    ["Revisões", "reviewRows", AlertTriangle]
  ] as const;

  return (
    <main className="space-y-6">
      <SoftPanel className="p-6">
        <div className="flex items-center justify-between gap-4">
          <div>
            <h2 className="text-2xl font-semibold text-white">Segurança e arquivos locais</h2>
            <p className="mt-2 text-sm text-slate-400">Credenciais e bancos ficam fora do Git.</p>
          </div>
          <button
            className="inline-flex h-11 items-center gap-2 rounded-xl bg-white px-4 text-sm font-semibold text-slate-950 transition hover:bg-slate-200 disabled:cursor-not-allowed disabled:bg-slate-700 disabled:text-slate-400"
            onClick={onSecretCheck}
            disabled={state.running}
          >
            {state.running ? <Loader2 className="h-4 w-4 animate-spin" /> : <ShieldCheck className="h-4 w-4" />}
            Verificar
          </button>
        </div>
        {state.message ? (
          <div className={cx("mt-5 rounded-xl border px-4 py-3 text-sm", state.ok === false ? "border-rose-400/30 bg-rose-400/10 text-rose-100" : "border-emerald-400/30 bg-emerald-400/10 text-emerald-100")}>
            {state.message}
          </div>
        ) : null}
      </SoftPanel>

      <section className="grid grid-cols-3 gap-4">
        {files.map(([label, key, Icon]) => {
          const meta = snapshot.localFiles?.[key];
          return (
            <SoftPanel key={key} className="p-5">
              <div className="flex items-center justify-between">
                <Icon className="h-5 w-5 text-slate-300" />
                {meta?.exists ? <CheckCircle2 className="h-5 w-5 text-emerald-300" /> : <XCircle className="h-5 w-5 text-rose-300" />}
              </div>
              <h3 className="mt-4 font-semibold text-white">{label}</h3>
              <p className="mt-2 text-sm text-slate-500">{bytes(meta?.bytes)}</p>
            </SoftPanel>
          );
        })}
      </section>
    </main>
  );
}

export default function App() {
  const { snapshot, desktop, refresh } = useSnapshot();
  const [screen, setScreen] = useState<ScreenKey>("home");
  const [previewState, setPreviewState] = useState<CommandState>({ running: false, message: "", ok: null });
  const [securityState, setSecurityState] = useState<CommandState>({ running: false, message: "", ok: null });

  const safeRows = snapshot.previewSummary?.safe_field_row_count ?? snapshot.safeRowsCount ?? 0;
  const reviewRows = snapshot.previewSummary?.review_field_row_count ?? snapshot.reviewRowsCount ?? 0;
  const headerText = useMemo(() => `${number(safeRows)} ações seguras · ${number(reviewRows)} pedem revisão`, [reviewRows, safeRows]);

  async function runPreview() {
    setPreviewState({ running: true, message: "Gerando prévia local...", ok: null });
    try {
      const result = await call<{ stdout: string; snapshot: Snapshot }>("run_preview");
      setPreviewState({ running: false, message: result.stdout.trim() || "Prévia atualizada.", ok: true });
      await refresh();
    } catch (error) {
      setPreviewState({ running: false, message: String(error), ok: false });
    }
  }

  async function runSecretCheck() {
    setSecurityState({ running: true, message: "Verificando arquivos versionáveis...", ok: null });
    try {
      const result = await call<{ stdout: string }>("run_secret_check");
      setSecurityState({ running: false, message: result.stdout.trim() || "Verificação concluída.", ok: true });
    } catch (error) {
      setSecurityState({ running: false, message: String(error), ok: false });
    }
  }

  return (
    <div className="app-shell text-slate-100">
      <div className="grid min-h-screen grid-cols-[280px_1fr]">
        <aside className="border-r border-white/10 bg-slate-950/92 px-5 py-5">
          <div className="flex items-center gap-3">
            <div className="grid h-12 w-12 place-items-center rounded-2xl bg-emerald-400 text-slate-950">
              <Sparkles className="h-6 w-6" />
            </div>
            <div>
              <h1 className="text-lg font-semibold text-white">Mirella Sync</h1>
              <p className="text-xs text-slate-500">Clínica Ágil → Kommo</p>
            </div>
          </div>

          <nav className="mt-8 space-y-2">
            {screens.map((item) => {
              const Icon = item.icon;
              const active = screen === item.key;
              return (
                <button
                  key={item.key}
                  onClick={() => setScreen(item.key)}
                  className={cx(
                    "flex h-12 w-full items-center gap-3 rounded-2xl px-4 text-left text-sm font-semibold transition",
                    active ? "bg-white text-slate-950" : "text-slate-400 hover:bg-white/8 hover:text-white"
                  )}
                >
                  <Icon className="h-5 w-5" />
                  {item.label}
                  {active ? <ChevronRight className="ml-auto h-4 w-4" /> : null}
                </button>
              );
            })}
          </nav>

          <div className="mt-8 rounded-2xl border border-white/10 bg-white/[0.04] p-4">
            <div className="flex items-center gap-2">
              {desktop ? <CheckCircle2 className="h-4 w-4 text-emerald-300" /> : <AlertTriangle className="h-4 w-4 text-amber-300" />}
              <span className="text-sm font-semibold text-slate-200">{desktop ? "Desktop conectado" : "Prévia visual"}</span>
            </div>
            <p className="mt-2 text-xs leading-5 text-slate-500">
              {desktop ? "Lendo artefatos locais do projeto." : "Abra via Tauri para executar comandos."}
            </p>
          </div>
        </aside>

        <div className="min-w-0 px-7 py-6">
          <header className="mb-7 flex items-center justify-between gap-5">
            <div>
              <p className="text-sm font-semibold uppercase tracking-[0.16em] text-emerald-300">Dashboard operacional</p>
              <h2 className="mt-2 text-3xl font-semibold tracking-normal text-white">Atualização de leads</h2>
              <p className="mt-2 text-sm text-slate-400">{headerText}</p>
            </div>
            <div className="flex items-center gap-3 rounded-2xl border border-white/10 bg-white/[0.05] px-5 py-4">
              <WalletCards className="h-5 w-5 text-emerald-300" />
              <div>
                <p className="text-xs text-slate-500">Modo atual</p>
                <p className="text-sm font-semibold text-white">prévia antes de aplicar</p>
              </div>
            </div>
          </header>

          {screen === "home" ? <Home snapshot={snapshot} onPreview={runPreview} /> : null}
          {screen === "updates" ? <Updates snapshot={snapshot} state={previewState} onPreview={runPreview} /> : null}
          {screen === "review" ? <Review snapshot={snapshot} /> : null}
          {screen === "rules" ? <Rules snapshot={snapshot} /> : null}
          {screen === "security" ? <Security snapshot={snapshot} state={securityState} onSecretCheck={runSecretCheck} /> : null}
        </div>
      </div>
    </div>
  );
}
