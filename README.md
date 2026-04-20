# Tatimr - Espelho do Banco Clinica Agil

Esta pasta contem um espelho isolado do subsistema de banco da Clinica Agil extraido de:

`C:\Users\User\Desktop\notafiscalmirela`

Nada foi alterado no projeto de origem. O que foi espelhado aqui:

- `login.py`: fluxo HTTP da Clinica Agil com:
  - autenticacao por HTTP
  - coleta da planilha de pacientes
  - coleta dos relatorios financeiros de vendas e recebimentos
  - extracao dos registros XLSX
  - sincronizacao incremental em SQLite
  - consolidacao financeira por paciente
  - versionamento de linhas
  - reconstrucao das tabelas curadas
- `mirella_pacientes.sqlite3`: copia local do banco incremental existente no projeto original

## Estrutura

- `login.py`
- `kommo_leads_sqlite.py`
- `mirella_pacientes.sqlite3`
- `.env.example`
- `requirements.txt`

## Dependencias

```powershell
py -3 -m pip install -r requirements.txt
```

## Variaveis de ambiente

Copie `.env.example` para `.env` e ajuste se quiser sobrescrever os padroes do script.

## Exemplos de uso

Sincronizar apenas pacientes:

```powershell
py -3 login.py --somente pacientes --sem-input --data-pacientes-de 01/01/2024 --data-pacientes-ate 31/01/2024
```

Sincronizar pacientes, vendas e recebimentos desde o inicio:

```powershell
py -3 login.py --sem-input --reprocessar-pacientes
```

Tabelas financeiras geradas em `mirella_pacientes.sqlite3`:

- `patient_financial_sales`: vendas/gastos por interessado, com match para `patients.patient_id` quando possivel
- `patient_financial_receipts`: recebimentos por interessado, com match para paciente quando possivel
- `vw_patient_financial_summary`: totais financeiros por paciente
- `vw_patients_complete_financial`: dados basicos, idade, contato, endereco e totais financeiros em uma view

Reconstruir as tabelas curadas a partir de `patients_latest`:

```powershell
py -3 login.py --rebuild-curated
```

Sincronizar leads do Kommo em SQLite e gerar dump SQL:

```powershell
$env:KOMMO_EMAIL="seu-email"
$env:KOMMO_PASSWORD="sua-senha"
py -3 kommo_leads_sqlite.py
```

Por padrao, o Kommo e sincronizado sem filtro de pipeline. Use `--pipeline-id 9715568` apenas se quiser limitar a um pipeline especifico.

Para rodar 100% via HTTP sem fallback de navegador, use um token OAuth/long-lived token do Kommo:

```powershell
$env:KOMMO_ACCESS_TOKEN="seu-token"
py -3 kommo_leads_sqlite.py
```

O script cria:

- `mirella_kommo_leads.sqlite3`
- `exports/kommo/kommo_leads_latest.sql`

Tabelas principais:

- `kommo_leads`: dados base de cada lead
- `kommo_lead_custom_fields`: cadastro dos campos do Kommo
- `kommo_lead_field_values`: matriz completa `lead + campo`, incluindo vazios
- `kommo_leads_wide`: tabela aberta com uma coluna por campo customizado

## Observacoes

- O banco SQLite e local e pode conter dados reais.
- O script cria saidas em `exports/` quando exporta arquivos.
- Se voce quiser reprocessar tudo desde o inicio, use `--reprocessar-pacientes`.
