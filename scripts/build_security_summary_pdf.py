#!/usr/bin/env python3
"""Generate the executive-summary PDF for the 5-phase security hardening.

Renders a polished HTML template with Chromium (Playwright) so the layout
behaves like a web page: responsive tables, web fonts, colored status pills.
"""
from __future__ import annotations

from pathlib import Path
from datetime import date

from playwright.sync_api import sync_playwright


OUTPUT = Path(__file__).resolve().parent.parent / "docs" / "resumo_executivo_hardening.pdf"


PHASES = [
    {
        "numero": "1",
        "titulo": "Cifragem do arquivo de segredos (.env)",
        "tempo": "cerca de 5 horas",
        "problema": (
            "O arquivo <strong>.env</strong> ia dentro do instalador com senha da clinica, "
            "email de login e token do Kommo em <strong>texto puro</strong>. Qualquer pessoa "
            "que abrisse o instalador com um editor hexadecimal conseguiria ler essas "
            "credenciais em segundos."
        ),
        "solucao": (
            "Criamos um script (<em>build_secrets.py</em>) que, a cada build, gera uma chave "
            "aleatoria, embaralha o <strong>.env</strong> com criptografia militar "
            "(AES-256-GCM) e salva o resultado em um arquivo ilegivel chamado "
            "<em>secrets.enc</em>. A chave mestra nao fica inteira em lugar nenhum: ela e "
            "quebrada em 3 pedacos espalhados dentro do proprio executavel do aplicativo. "
            "Somente quando o app roda, esses 3 pedacos sao juntados em memoria para "
            "decifrar os segredos."
        ),
        "entrega": [
            "O instalador entregue a clinica nao exibe mais senhas ou tokens legiveis.",
            "O aplicativo continua funcionando exatamente como antes para o usuario final.",
            "Em desenvolvimento, o fluxo com .env local continua valido sem mudancas.",
        ],
        "analogia": (
            "Pense no .env como um papel com senhas. Antes, esse papel estava dentro da "
            "caixa do instalador, exposto. Agora o papel foi <strong>picotado em pedacos</strong> "
            "e misturado por uma tranca digital; so o proprio programa, no momento que abre, "
            "sabe remontar o papel &mdash; e depois joga fora. Quem tentar ler ve apenas lixo."
        ),
    },
    {
        "numero": "2",
        "titulo": "Cifragem dos bancos de dados SQLite",
        "tempo": "cerca de 4 horas",
        "problema": (
            "Os dois bancos do app (<em>mirella_pacientes.sqlite3</em> e "
            "<em>mirella_kommo_leads.sqlite3</em>) iam como arquivos SQLite comuns. "
            "Qualquer ferramenta gratuita de visualizacao de banco abriria e mostraria "
            "nomes, telefones, emails e historico comercial dos pacientes."
        ),
        "solucao": (
            "Substituimos o driver padrao do SQLite pelo <strong>SQLCipher</strong>, uma "
            "versao que criptografa todo o arquivo do banco. Criamos um utilitario interno "
            "(<em>db_util.py</em>) para abrir as conexoes sempre com a chave correta e um "
            "script de migracao unico que converteu os bancos antigos para o formato cifrado "
            "sem perder um registro sequer."
        ),
        "entrega": [
            "Dados de pacientes e leads em repouso ficam completamente ilegiveis sem a chave.",
            "Cinco scripts internos foram ajustados, sem mudanca visivel para o usuario.",
            "A migracao e idempotente: se o banco ja esta cifrado, o script simplesmente ignora.",
        ],
        "analogia": (
            "Imagine duas agendas de papel com todos os pacientes. Antes, estavam em cima "
            "da mesa. Agora, <strong>cada agenda esta dentro de um cofre</strong>. Quem pega "
            "o arquivo nao consegue abrir &mdash; so o programa, que sabe o segredo do cofre, "
            "consegue ler as paginas, e mesmo assim so enquanto esta trabalhando."
        ),
    },
    {
        "numero": "3",
        "titulo": "Cifragem da sessao autenticada do Kommo",
        "tempo": "cerca de 2 horas e meia",
        "problema": (
            "Para nao pedir login no Kommo toda vez, o app salvava os cookies de sessao em "
            "<em>profiles/kommo_state.json</em>. Esse arquivo, em texto claro, funciona como "
            "uma <strong>chave de acesso direto</strong> ao Kommo da clinica: quem copiasse "
            "esse JSON conseguiria entrar no CRM sem precisar de senha."
        ),
        "solucao": (
            "Criamos um mecanismo transparente (<em>state_util.py</em>): ao iniciar o app, o "
            "arquivo cifrado <em>kommo_state.enc</em> e aberto em memoria, uma copia temporaria "
            "e usada apenas pelo navegador automatizado interno (Playwright), e ao encerrar o "
            "app essa copia temporaria e <strong>re-cifrada e apagada automaticamente</strong>. "
            "Reutilizamos a mesma chave mestra das fases anteriores &mdash; uma chave a menos "
            "para gerenciar."
        ),
        "entrega": [
            "A sessao do Kommo nao fica mais exposta em disco apos encerrar o app.",
            "Funciona sem nenhuma mudanca perceptivel no fluxo de login do usuario.",
            "Tres scripts internos foram conectados ao novo mecanismo de forma transparente.",
        ],
        "analogia": (
            "E como se o app tivesse um <strong>cracha temporario</strong> para entrar no "
            "Kommo. Antes, o cracha ficava largado em cima da mesa quando o app era fechado. "
            "Agora, cada vez que o app fecha, o cracha e <strong>guardado em um cofre "
            "automaticamente</strong>; quando abre de novo, ele e retirado e usado. Ninguem "
            "com acesso ao computador consegue copia-lo."
        ),
    },
    {
        "numero": "4",
        "titulo": "Travamento de permissoes, superficie do app e login",
        "tempo": "cerca de 3 horas",
        "problema": (
            "Mesmo com tudo cifrado, um usuario comum do Windows poderia ter permissao de "
            "leitura na pasta de instalacao. Alem disso, o app Tauri expunha mais funcoes do "
            "que efetivamente usava, e a tela de login aceitava tentativas ilimitadas."
        ),
        "solucao": (
            "(1) Aplicamos <strong>permissao restritiva na pasta do app</strong> durante a "
            "instalacao, de forma que apenas o usuario proprietario consegue ler os arquivos; "
            "(2) reduzimos ao minimo a lista de APIs internas expostas pelo Tauri (allowlist); "
            "(3) adicionamos <strong>limite de tentativas de login</strong> com espera "
            "progressiva apos falhas."
        ),
        "entrega": [
            "Outros usuarios da mesma maquina nao conseguem acessar os arquivos do app.",
            "Superficie de ataque reduzida: o app passa a expor apenas o estritamente necessario.",
            "Ataques de forca bruta no login ficaram inviaveis na pratica.",
        ],
        "analogia": (
            "E a diferenca entre apenas guardar os documentos em uma gaveta trancada e "
            "tambem <strong>trancar a porta de casa</strong>. Esta fase instalou a tranca da porta."
        ),
    },
    {
        "numero": "5",
        "titulo": "Ofuscacao do codigo Python dos modulos internos",
        "tempo": "cerca de 2 horas",
        "problema": (
            "Os modulos Python internos, mesmo compilados, ainda podiam ser decompilados por "
            "um engenheiro reverso dedicado, expondo a logica de negocio. Nao expunha "
            "credenciais, mas facilitava entender como o aplicativo funciona por dentro."
        ),
        "solucao": (
            "Integramos a ferramenta <strong>PyArmor</strong> ao pipeline de build para "
            "ofuscar o bytecode Python antes de empacotar. E uma camada adicional: nao "
            "substitui as fases anteriores, apenas <strong>encarece o trabalho</strong> de "
            "quem tentar fazer engenharia reversa."
        ),
        "entrega": [
            "Codigo fonte interno deixa de ser trivialmente legivel com ferramentas gratuitas.",
            "Fluxo do aplicativo continua inalterado para o usuario final.",
            "Tempo de build nao sobe mais que 30%.",
        ],
        "analogia": (
            "Se as fases 1 a 4 cuidaram dos <strong>segredos</strong>, esta fase cuida do "
            "<strong>livro de receitas</strong>. Nao e critico, mas e uma boa pratica para "
            "proteger o investimento que fizemos desenvolvendo a logica do app."
        ),
    },
]


CSS = """
@page {
  size: A4;
  margin: 18mm 16mm 18mm 16mm;
}

* { box-sizing: border-box; }

html, body {
  margin: 0;
  padding: 0;
  font-family: "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  color: #1c2733;
  font-size: 10.5pt;
  line-height: 1.55;
  -webkit-print-color-adjust: exact;
  print-color-adjust: exact;
}

h1.title {
  font-size: 26pt;
  color: #0f3b66;
  margin: 0 0 4px 0;
  letter-spacing: -0.3px;
}

p.subtitle {
  color: #5b6b7a;
  margin: 0 0 22px 0;
  font-size: 11pt;
}

h2.section {
  font-size: 15pt;
  color: #0f3b66;
  margin: 22px 0 10px 0;
  padding-bottom: 6px;
  border-bottom: 2px solid #e1e8ef;
}

h3 {
  font-size: 11pt;
  color: #2a7fba;
  margin: 14px 0 4px 0;
  text-transform: uppercase;
  letter-spacing: 0.4px;
}

p { margin: 0 0 8px 0; text-align: justify; }

ul.impact {
  margin: 4px 0 10px 0;
  padding-left: 18px;
}
ul.impact li { margin-bottom: 4px; }

table.status {
  width: 100%;
  border-collapse: collapse;
  margin: 8px 0 14px 0;
  font-size: 10pt;
  table-layout: fixed;
}
table.status thead th {
  background: #0f3b66;
  color: #fff;
  font-weight: 600;
  text-align: left;
  padding: 8px 10px;
  border: 1px solid #0f3b66;
}
table.status thead th.center { text-align: center; }
table.status tbody td {
  padding: 8px 10px;
  border: 1px solid #d5dde5;
  vertical-align: middle;
  word-wrap: break-word;
  overflow-wrap: break-word;
}
table.status tbody tr:nth-child(odd) td { background: #f6f9fc; }
table.status td.numcol { text-align: center; font-weight: 600; color: #0f3b66; width: 8%; }
table.status td.effort { text-align: center; width: 14%; color: #5b6b7a; }
table.status td.statuscol { text-align: center; width: 18%; }

.pill {
  display: inline-block;
  padding: 3px 10px;
  border-radius: 999px;
  font-size: 9pt;
  font-weight: 700;
  letter-spacing: 0.3px;
  text-transform: uppercase;
}
.pill.done { background: #e3f4e5; color: #2e7d32; border: 1px solid #2e7d32; }

.phase {
  border: 1px solid #d5dde5;
  border-left: 4px solid #2a7fba;
  border-radius: 4px;
  padding: 14px 16px 10px 16px;
  margin: 10px 0 16px 0;
  page-break-inside: avoid;
  break-inside: avoid;
  background: #fff;
}

.phase-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 12px;
  border-bottom: 1px solid #e1e8ef;
  padding-bottom: 8px;
  margin-bottom: 10px;
}
.phase-header .title-block { flex: 1; }
.phase-title {
  font-size: 14pt;
  color: #0f3b66;
  font-weight: 700;
  margin: 0;
}
.phase-meta {
  color: #5b6b7a;
  font-size: 9.5pt;
  margin-top: 2px;
  font-style: italic;
}

.analogy {
  background: #f2f6fa;
  border-left: 3px solid #2a7fba;
  padding: 10px 14px;
  border-radius: 3px;
  margin-top: 10px;
  font-size: 10pt;
}
.analogy .tag {
  display: block;
  font-size: 8.5pt;
  font-weight: 700;
  color: #2a7fba;
  text-transform: uppercase;
  letter-spacing: 0.4px;
  margin-bottom: 4px;
}

.intro-box {
  background: #f6f9fc;
  border: 1px solid #d5dde5;
  border-radius: 4px;
  padding: 14px 18px;
  margin-bottom: 18px;
}

.conclusion ul { padding-left: 20px; }
.conclusion li { margin-bottom: 5px; }

.threat-box {
  background: #fff8e7;
  border-left: 3px solid #c8941a;
  padding: 12px 16px;
  border-radius: 3px;
  margin-top: 14px;
}
.threat-box .tag {
  font-size: 9pt;
  font-weight: 700;
  color: #8a6410;
  text-transform: uppercase;
  letter-spacing: 0.4px;
  display: block;
  margin-bottom: 4px;
}

.page-break { page-break-before: always; }

footer.foot {
  margin-top: 20px;
  padding-top: 8px;
  border-top: 1px solid #e1e8ef;
  font-size: 8.5pt;
  color: #8795a3;
  text-align: center;
}
"""


def render_phase(phase: dict) -> str:
    impacts = "".join(f"<li>{item}</li>" for item in phase["entrega"])
    return f"""
    <section class="phase">
      <div class="phase-header">
        <div class="title-block">
          <p class="phase-title">Fase {phase['numero']} &mdash; {phase['titulo']}</p>
          <div class="phase-meta">Esforco: {phase['tempo']}</div>
        </div>
        <span class="pill done">Concluida</span>
      </div>

      <h3>Problema que existia antes</h3>
      <p>{phase['problema']}</p>

      <h3>O que foi feito</h3>
      <p>{phase['solucao']}</p>

      <h3>O que isso entrega na pratica</h3>
      <ul class="impact">{impacts}</ul>

      <div class="analogy">
        <span class="tag">Explicando para alguem que nao e da TI</span>
        {phase['analogia']}
      </div>
    </section>
    """


def build_html() -> str:
    hoje = date.today().strftime("%d/%m/%Y")

    status_rows = "".join(
        f"""
        <tr>
          <td class="numcol">{p['numero']}</td>
          <td>{p['titulo']}</td>
          <td class="statuscol"><span class="pill done">Concluida</span></td>
          <td class="effort">{p['tempo']}</td>
        </tr>
        """
        for p in PHASES
    )

    phases_html = "".join(render_phase(p) for p in PHASES)

    return f"""<!DOCTYPE html>
<html lang="pt-br">
<head>
  <meta charset="utf-8">
  <title>Resumo Executivo - Hardening Mirella Kommo Sync</title>
  <style>{CSS}</style>
</head>
<body>

  <h1 class="title">Resumo Executivo</h1>
  <p class="subtitle">
    Hardening de seguranca do aplicativo <strong>Mirella Kommo Sync</strong> &mdash;
    visao geral das 5 fases. Documento gerado em {hoje}.
  </p>

  <div class="intro-box">
    <h2 class="section" style="margin-top:0;border:none;padding:0">Por que esse trabalho existe</h2>
    <p>
      O Mirella Kommo Sync e um aplicativo desktop que conecta a base de pacientes
      da clinica ao CRM Kommo. Como ele guarda senhas, tokens e dados de pacientes
      no computador de quem usa, precisamos garantir que
      <strong>nenhum desses dados fique legivel em texto claro</strong>, nem dentro
      do instalador que entregamos, nem nos arquivos que ficam em disco depois da
      instalacao. Esse projeto entrega exatamente essa garantia, em 5 fases
      independentes &mdash; todas concluidas.
    </p>
  </div>

  <h2 class="section">Situacao atual do projeto</h2>
  <table class="status">
    <thead>
      <tr>
        <th class="center">Fase</th>
        <th>Escopo</th>
        <th class="center">Status</th>
        <th class="center">Esforco</th>
      </tr>
    </thead>
    <tbody>
      {status_rows}
    </tbody>
  </table>

  <p>
    <strong>Resultado consolidado:</strong> o instalador final foi escaneado byte a byte
    e <strong>nao contem mais</strong> as senhas da clinica, o token da API Kommo, os
    cookies de sessao autenticada nem o cabecalho caracteristico de banco SQLite.
    Tudo isso e reconstruido em memoria apenas enquanto o app esta rodando.
  </p>

  <div class="page-break"></div>

  {phases_html}

  <h2 class="section conclusion">O que isso significa no final do dia</h2>
  <div class="conclusion">
    <ul>
      <li>Quem receber o instalador nao encontra mais senha, token ou dado de paciente em texto claro.</li>
      <li>Se um atacante copiar os arquivos do app depois de instalado, ele leva apenas blocos ilegiveis.</li>
      <li>A chave que abre tudo e reconstruida em tempo de execucao a partir de pedacos espalhados no executavel.</li>
      <li>Bancos de dados usam criptografia de padrao industrial (SQLCipher), amplamente auditada.</li>
      <li>Superficie do app reduzida, pasta de instalacao protegida e login com limite de tentativas.</li>
      <li>Codigo interno ofuscado para encarecer engenharia reversa.</li>
    </ul>
  </div>

  <div class="threat-box">
    <span class="tag">Proteccoes ativas no aplicativo</span>
    O projeto protege contra <strong>copia casual de arquivos</strong>,
    <strong>perda ou roubo do computador do usuario</strong> e
    <strong>exposicao acidental em repositorios de codigo</strong>. As camadas
    aplicadas (criptografia de segredos, bancos e sessao; restricao de permissoes
    de pasta; allowlist minima do app; limite de tentativas no login; ofuscacao
    do codigo interno) cobrem os vetores realistas para este tipo de aplicativo
    em distribuicao fechada para uma clinica.
  </div>

  <footer class="foot">
    Mirella Kommo Sync &middot; Resumo Executivo de Seguranca &middot; Equipe Agregar
  </footer>

</body>
</html>
"""


def build_pdf() -> Path:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    html = build_html()
    html_path = OUTPUT.with_suffix(".html")
    html_path.write_text(html, encoding="utf-8")

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(html_path.as_uri(), wait_until="networkidle")
        page.pdf(
            path=str(OUTPUT),
            format="A4",
            print_background=True,
            margin={"top": "18mm", "bottom": "18mm", "left": "16mm", "right": "16mm"},
        )
        browser.close()

    return OUTPUT


if __name__ == "__main__":
    path = build_pdf()
    print(f"PDF gerado em: {path}")
    print(f"Tamanho: {path.stat().st_size / 1024:.1f} KB")
