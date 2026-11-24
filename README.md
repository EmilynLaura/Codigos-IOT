# SyncHub - HUB Integrador de Dados (Rodomax Transportes)
> **Trabalho de Data Science e Inteligência Artificial**  
> **Curso:** TADS | **Acadêmico:** Daniel Cagol (Engenheiro de Dados)

---

## 📌 1. Visão Geral e Contexto
A **Rodomax Transportes** enfrenta o desafio operacional da fragmentação de canais de recebimento de fretes e embarques. As solicitações e tabelas chegam de fontes heterogêneas (arquivos PDF, planilhas CSV de ERPs legados, mensagens estruturadas e APIs/Webhooks em formato JSON).

O **SyncHub** atua como uma camada de engenharia de dados (*middleware*) de alta disponibilidade, responsável por capturar, auditar estatisticamente, higienizar, normalizar e unificar os dados antes de persisti-los no **Data Lake** em formatos analíticos de alto desempenho (**Parquet** e **CSV**) prontos para consumo por modelos de Inteligência Artificial e bancos de dados analíticos.

---

## 🏗️ 2. Arquitetura do Pipeline

```
 ┌────────────────────────────────────────────────────────┐
 │            Fontes de Dados Heterogêneas                │
 │    • PDFs (Tabelas de Frete, Ordens de Coleta)         │
 │    • CSVs (Planilhas e ERPs - TOTVS, SAP, Senior)      │
 │    • JSONs (APIs Externas, Webhooks, JSON Lines)       │
 └───────────────────────────┬────────────────────────────┘
                             │
                             ▼
 ┌────────────────────────────────────────────────────────┐
 │           PASSO 1: Ingestão Multiformato               │
 │  • FreightPDFExtractor (pdfplumber + fallback pypdf)   │
 │  • FreightCSVExtractor (autodetect sep, encodings)     │
 │  • FreightJSONExtractor (JSON aninhado + NDJSON)       │
 └───────────────────────────┬────────────────────────────┘
                             │ DataFrame Bruto Consolidado
                             ▼
 ┌────────────────────────────────────────────────────────┐
 │           PASSO 2: Auditoria e Inconsistências         │
 │  • DataQualityAuditor: .info(), .describe(), nulos     │
 │  • Mapeamento de duplicatas e perfil estatístico       │
 └───────────────────────────┬────────────────────────────┘
                             │
                             ▼
 ┌────────────────────────────────────────────────────────┐
 │           PASSO 3: Saneamento e Limpeza                │
 │  • DataSanitizer: Descarte de registros sem dados      │
 │    vitais (Origem, Destino, Peso da Carga)             │
 │  • Expurgos de colunas obsoletas e duplicatas          │
 └───────────────────────────┬────────────────────────────┘
                             │
                             ▼
 ┌────────────────────────────────────────────────────────┐
 │           PASSO 4: Normalização Universal              │
 │  • DataNormalizer:                                     │
 │    - Datas: ISO 8601 (YYYY-MM-DD / YYYY-MM-DDTHH:MM:SS)│
 │    - Pesos: Unificados para float em quilogramas (kg)  │
 │    - Valores de frete: Convertidos para float puro     │
 │    - Localidades: CAIXA ALTA e formatação limpa        │
 └───────────────────────────┬────────────────────────────┘
                             │ Base Higienizada
                             ▼
 ┌────────────────────────────────────────────────────────┐
 │     PASSO 5: Carga e Data Lake (Pronto para IA)        │
 │  • Exportação .parquet (pyarrow + compressão Snappy)   │
 │  • Exportação .csv (padrão UTF-8 com BOM)              │
 └────────────────────────────────────────────────────────┘
```

---

## 🛠️ 3. Estrutura do Repositório

```
SyncHub---Trabalho-de-DataScience-e-IA/
│
├── pipeline_pdf_fretes.py      # Motor do Pipeline (Ingestão, Auditoria, Limpeza e Normalização)
├── channel_integrations.py    # Ingestão automática por e-mail IMAP e webhook WhatsApp
├── app_interface.py            # Interface Gráfica Desktop corporativa (Tkinter)
├── app.py                      # Ponto de entrada simplificado da aplicação
├── iniciar_interface.bat       # Launcher de duplo clique para ambiente Windows
├── requirements.txt            # Dependências Python homologadas
├── test_channel_integrations.py # Testes do parser e do webhook
├── .gitignore                  # Arquivos e diretórios ignorados pelo Git
│
├── data_raw/                   # 📂 Camada Raw (PDF, CSV, JSON de entrada)
│   ├── solicitacao_frete_001.pdf
│   ├── solicitacao_frete_002.pdf
│   ├── tabela_fretes_rotas.pdf
│   ├── pedidos_erp_totvs.csv
│   ├── solicitacoes_api_cliente.json
│   └── streaming_embarques.jsonl
│
└── data_lake/                  # 💾 Camada Processed / Data Lake
    ├── processed_freights.parquet
    └── processed_freights.csv
```

---

## 🚀 4. Como Executar

### 4.1. Instalação das Dependências
```bash
pip install -r requirements.txt
```

### 4.2. Execução via Interface Gráfica (Desktop)
- Dê duplo-clique no arquivo [`iniciar_interface.bat`](iniciar_interface.bat) ou execute:
```bash
py app.py
```

### 4.3. Execução em Lote (Batch / Engenharia de Dados)
- Coloque seus arquivos em `data_raw/` e execute:
```bash
py pipeline_pdf_fretes.py
```
O pipeline processará os arquivos, imprimirá as auditorias estatísticas (`.info()` e `.describe()`) e salvará a base final em `data_lake/`.

### 4.4. Pedidos por e-mail e WhatsApp
O módulo `channel_integrations.py` lê e-mails por IMAP e recebe mensagens pela
WhatsApp Cloud API oficial da Meta. Os pedidos importados são normalizados pelo
pipeline existente e gravados em `data_lake/channel_orders.csv`,
`data_lake/channel_orders.parquet` e `data_lake/channel_ingestion.sqlite3`.
Esses arquivos são separados do lote `processed_freights`, que não é sobrescrito.

Para evitar interpretações incorretas, envie cada pedido como texto identificado
(uma informação por linha), por exemplo:

```text
Origem: Cascavel - PR
Destino: Curitiba - PR
Peso da carga: 12 toneladas
Valor do frete: R$ 4.500,00
Data da solicitação: 12/10/2026
```

Origem, destino e peso são obrigatórios. Valor do frete e data são opcionais.
Também são aceitos aliases como `Local de coleta`, `Local de entrega`, `Peso`,
`Frete` e `Data`. A mensagem pode conter um objeto JSON com campos equivalentes.
Mensagens livres, imagens, áudio e anexos não são interpretados por este módulo.

#### E-mail (IMAP)
Configure o host IMAP e as credenciais por variáveis de ambiente e execute em
um terminal separado. O processo verifica a caixa a cada 60 segundos, por padrão:

```powershell
$env:MAIL_IMAP_HOST = "imap.exemplo.com"
$env:MAIL_USERNAME = "pedidos@exemplo.com"
$env:MAIL_PASSWORD = "senha-ou-senha-de-aplicativo"
py channel_integrations.py email
```

Opcionalmente, configure `MAIL_IMAP_PORT` (padrão `993`), `MAIL_FOLDER`
(padrão `INBOX`) e `MAIL_POLL_SECONDS` (mínimo `5`). Use uma senha de aplicativo
ou o método de autenticação permitido pelo provedor; o módulo não guarda a senha.
E-mails sem pedido válido permanecem não lidos para correção.

#### WhatsApp (Meta Cloud API)
No painel Meta Developers, configure a WhatsApp Cloud API e uma assinatura de
webhook para mensagens (`messages`). Configure a URL pública HTTPS que encaminhe
para a porta local do módulo e use o mesmo token de verificação no painel e no
ambiente:

```powershell
$env:WHATSAPP_VERIFY_TOKEN = "gere-um-token-aleatorio"
$env:WHATSAPP_APP_SECRET = "app-secret-do-painel-meta"
py channel_integrations.py whatsapp
```

O webhook escuta na porta `8080` por padrão (`WHATSAPP_WEBHOOK_PORT` pode alterá-la).
É necessário publicar o endpoint via HTTPS (por exemplo, proxy reverso com TLS);
não exponha diretamente a porta HTTP à internet. O endpoint valida a assinatura
HMAC da Meta, descarta mensagens sem texto e não envia respostas aos clientes.

Em ambos os canais, `SYNC_DATA_LAKE_DIR` pode apontar para outra pasta de destino.
Execute o processo correspondente continuamente para manter a ingestão automática.

---

## 5. Protótipo de Inteligência Artificial

### Problema e técnica

O protótipo apoia a equipe comercial na cotação inicial: estima o valor de frete
para uma demanda com base no peso da carga. Foi escolhida **regressão linear
simples**, uma técnica supervisionada adequada ao problema de estimar um valor
numérico e fácil de explicar. A variável de entrada é `peso_kg`; o alvo é
`valor_frete`. O modelo usa o método dos mínimos quadrados e converte o peso
para toneladas antes do ajuste.

O conjunto atual não contém distância da rota nem tipo de produto. Por isso,
eles não são simulados nem usados pelo modelo; são fatores importantes a incluir
quando esses dados estiverem disponíveis.

### Avaliação e linha de base

`FreightRegressionModel.evaluate()` usa uma divisão holdout determinística de
75% treino e 25% teste (semente 42). A regressão e a linha de base são ajustadas
somente no treino; a linha de base prevê sempre a média dos fretes do treino.
As métricas são MAE (erro absoluto médio) e RMSE (raiz do erro quadrático
médio), em reais.

Na amostra sintética atual, com 12 registros válidos, a divisão resulta em
9 registros de treino e 3 de teste. Na execução de referência:

- MAE da regressão: aproximadamente `R$ 104,84`
- MAE da linha de base: aproximadamente `R$ 1.300,02`
- Casos de teste: `14.000 kg`, `19.500 kg` e `32.000 kg`

Esse resultado é apenas demonstrativo: três exemplos de teste são insuficientes
para afirmar desempenho em produção. A comparação deve ser repetida com mais
dados reais e outras divisões temporais/rotas.

### Demonstração de um caso novo

Na interface, clique em **"🤖 Estimar Frete (IA)"**, informe o peso e veja a
estimativa, o valor estimado por tonelada e os erros de teste versus linha de
base. Se o peso estiver fora da faixa dos dados de treino, o protótipo avisa
que está extrapolando. O terminal também avalia o modelo e executa uma previsão
para `35000 kg` ao rodar `py pipeline_pdf_fretes.py`.

O agrupamento **K-Means** descrito no código permanece como experimento adicional
de segmentação/anomalias, mas a regressão é a técnica principal para a decisão
de cotação.

### Limitações, riscos e próximos passos

- O protótipo considera somente o peso e não incorpora rota, distância,
  produto, urgência, tipo de veículo ou sazonalidade; a estimativa não deve ser
  usada como cotação comercial definitiva.
- A base de demonstração é pequena e sintética; sua distribuição pode não
  representar a operação real.
- O holdout é aleatório e determinístico, não uma validação temporal; registros
  de uma mesma rota podem ser parecidos entre treino e teste.
- O sistema deve mostrar a estimativa como apoio, não decisão automática.
  Dados de clientes e mensagens devem ser minimizados/anonimizados e ter acesso
  controlado conforme a LGPD.
- Próximos passos: coletar histórico real de fretes cotados e realizados,
  adicionar distância/rota/produto/veículo, validar por período e região,
  monitorar erro e recalibrar o modelo.
