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
- `env_config.py`
- `kommo_leads_sqlite.py`
- `clinic_kommo_field_mappings.py`
- `clinic_kommo_payload_preview.py`
- `clinic_kommo_sync_controlled_test.py`
- `clinic_operational_fields_sync.py`
- `sanity_check_secrets.py`
- `mappings/clinic_kommo_origin_mapping.csv`
- `mappings/clinic_kommo_service_mapping.csv`
- `src/`: interface React/TypeScript do app desktop
- `src-tauri/`: backend Tauri para leitura dos artefatos locais e comandos controlados
- `mirella_pacientes.sqlite3`
- `.env.example`
- `requirements.txt`

## Dependencias

```powershell
py -3 -m pip install -r requirements.txt
npm install
```

Sanity check de segredos antes de commitar:

```powershell
py -3 sanity_check_secrets.py
```

Validar interface desktop:

```powershell
npm run build
$env:CARGO_TARGET_DIR="target\tauri-check"; cargo check --manifest-path src-tauri/Cargo.toml -j 1
```

## Variaveis de ambiente

Copie `.env.example` para `.env` e ajuste se quiser sobrescrever os padroes do script.

Os scripts carregam o `.env` pelo helper centralizado [env_config.py](C:/Users/User/Desktop/tatimr/env_config.py), para evitar duplicacao de loader e manter credenciais fora do codigo versionado.

As tabelas de mapeamento de negocio entre Clinica Agil e Kommo ficam em:

- [mappings/clinic_kommo_origin_mapping.csv](C:/Users/User/Desktop/tatimr/mappings/clinic_kommo_origin_mapping.csv)
- [mappings/clinic_kommo_service_mapping.csv](C:/Users/User/Desktop/tatimr/mappings/clinic_kommo_service_mapping.csv)

## Exemplos de uso

Abrir o dashboard desktop em modo desenvolvimento:

```powershell
npm run tauri:dev
```

Gerar build do app desktop:

```powershell
npm run tauri:build
```

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
