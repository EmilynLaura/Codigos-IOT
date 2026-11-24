"""
HUB Integrador de Dados - Rodomax Transportes
Módulo de Extração, Auditoria, Saneamento, Normalização e Ingestão de Fretes em PDF.

Autor: Daniel Cagol (Engenharia de Dados)
Contexto: Ingestão de dados heterogêneos de transporte rodoviário de cargas para
          Data Lake e treinamento de modelos de Inteligência Artificial.
"""

from __future__ import annotations

import io
import json
import logging
import math
import os
import random
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

# Assegura que o console Windows trate caracteres UTF-8 corretamente
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# Tentativa de importação das bibliotecas de PDF
try:
    import pdfplumber
except ImportError:
    pdfplumber = None

try:
    import pypdf
except ImportError:
    try:
        import PyPDF2 as pypdf
    except ImportError:
        pypdf = None


# ==============================================================================
# CONFIGURAÇÃO DE LOGGING
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("RodomaxDataHub")


# ==============================================================================
# EXCEÇÕES CUSTOMIZADAS
# ==============================================================================
class PDFExtractionError(Exception):
    """Exceção levantada para erros críticos na extração de dados do PDF."""
    pass


class DataPipelineError(Exception):
    """Exceção geral para falhas no pipeline de engenharia de dados."""
    pass


# ==============================================================================
# PASSO 1: INGESTÃO E EXTRAÇÃO DE PDF
# ==============================================================================
class FreightPDFExtractor:
    """
    Responsável pela leitura e extração de dados brutos de arquivos PDF de solicitações
    e tabelas de fretes da Rodomax Transportes, com suporte a pdfplumber e pypdf.
    """

    def __init__(self) -> None:
        if pdfplumber is None and pypdf is None:
            raise ImportError(
                "Nenhuma biblioteca de leitura de PDF instalada. "
                "Instale pdfplumber ('pip install pdfplumber') ou pypdf ('pip install pypdf')."
            )

    @staticmethod
    def _find_column_index(headers: List[str], keywords: List[str]) -> Optional[int]:
        """Localiza o índice de uma coluna a partir de uma lista de palavras-chave."""
        for idx, h in enumerate(headers):
            if not h:
                continue
            cleaned = re.sub(r"[^a-zA-Z0-9]", "", str(h).lower())
            for kw in keywords:
                kw_clean = re.sub(r"[^a-zA-Z0-9]", "", kw.lower())
                if kw_clean in cleaned:
                    return idx
        return None

    def _extract_from_tables(self, pdf_obj: Any) -> List[Dict[str, Any]]:
        """Extrai registros estruturados a partir de tabelas encontradas no PDF."""
        records: List[Dict[str, Any]] = []

        for page_idx, page in enumerate(pdf_obj.pages, start=1):
            tables = page.extract_tables()
            if not tables:
                continue

            for table in tables:
                if not table or len(table) < 2:
                    continue

                # Normaliza cabeçalhos da primeira linha
                headers = [str(col).strip() if col is not None else "" for col in table[0]]

                origem_idx = self._find_column_index(headers, ["origem", "coleta", "de", "cidadeorigem"])
                destino_idx = self._find_column_index(headers, ["destino", "entrega", "para", "cidadedestino"])
                peso_idx = self._find_column_index(headers, ["peso", "pesocarga", "pesobruto", "pesokg", "pesoda"])
                valor_idx = self._find_column_index(headers, ["valor", "frete", "preco", "tarifa", "tabela"])
                data_idx = self._find_column_index(headers, ["data", "emissao", "solicitacao", "vigencia"])

                # Se ao menos dois campos essenciais forem identificados, trata como tabela válida
                if origem_idx is not None and destino_idx is not None:
                    for row in table[1:]:
                        if not any(row):  # Linha totalmente vazia
                            continue

                        def get_val(idx: Optional[int]) -> Optional[str]:
                            if idx is not None and idx < len(row) and row[idx] is not None:
                                val = str(row[idx]).strip()
                                return val if val else None
                            return None

                        record = {
                            "origem": get_val(origem_idx),
                            "destino": get_val(destino_idx),
                            "peso": get_val(peso_idx),
                            "valor_frete": get_val(valor_idx),
                            "data": get_val(data_idx),
                            "extra_info": [str(c) for i, c in enumerate(row) if i not in (origem_idx, destino_idx, peso_idx, valor_idx, data_idx) and c],
                            "pagina": page_idx,
                            "tipo_extracao": "tabela",
                        }
                        records.append(record)

        return records

    def _extract_from_text_patterns(self, text: str, page_num: int = 1) -> List[Dict[str, Any]]:
        """Aplica padrões de Expressões Regulares (Regex) para extrair formulários textuais."""
        records: List[Dict[str, Any]] = []
        if not text or not text.strip():
            return records

        # Se houver múltiplos blocos (ex: demarcados por 'SOLICITAÇÃO' ou 'PEDIDO')
        blocks = re.split(r"(?=(?:SOLICITA[ÇC][ÃA]O|ORDEM\s+DE\s+COLETA|FRETE\s+#|PEDIDO\s+#))", text, flags=re.IGNORECASE)
        if not blocks:
            blocks = [text]

        for block in blocks:
            if not block.strip():
                continue

            origem_match = re.search(r"(?:Origem|Local de Coleta|De)\s*:\s*([^\n\r]+)", block, re.IGNORECASE)
            destino_match = re.search(r"(?:Destino|Local de Entrega|Para)\s*:\s*([^\n\r]+)", block, re.IGNORECASE)
            peso_match = re.search(r"(?:Peso(?:\s+da\s+Carga|\s+Bruto)?)\s*:\s*([^\n\r]+)", block, re.IGNORECASE)
            valor_match = re.search(r"(?:Valor(?:\s+do\s+Frete|\s+da\s+Cota[çc][ãa]o)?|Frete|Preço|Tabela)\s*:\s*([^\n\r]+)", block, re.IGNORECASE)
            data_match = re.search(r"(?:Data(?:\s+do\s+Pedido|\s+da\s+Solicita[çc][ãa]o|\s+de\s+Emiss[ãa]o)?)\s*:\s*([^\n\r]+)", block, re.IGNORECASE)

            # Só considera registro válido se encontrar campos essenciais
            if origem_match or destino_match or peso_match:
                record = {
                    "origem": origem_match.group(1).strip() if origem_match else None,
                    "destino": destino_match.group(1).strip() if destino_match else None,
                    "peso": peso_match.group(1).strip() if peso_match else None,
                    "valor_frete": valor_match.group(1).strip() if valor_match else None,
                    "data": data_match.group(1).strip() if data_match else None,
                    "pagina": page_num,
                    "tipo_extracao": "texto_regex",
                }
                records.append(record)

        return records

    def extract(self, pdf_path_or_stream: Union[str, Path, io.BytesIO, bytes]) -> List[Dict[str, Any]]:
        """
        Lê e extrai os registros do PDF utilizando pdfplumber prioritariamente
        ou pypdf/PyPDF2 como fallback resiliente.
        """
        records: List[Dict[str, Any]] = []
        file_source = getattr(pdf_path_or_stream, "name", str(pdf_path_or_stream) if isinstance(pdf_path_or_stream, (str, Path)) else "in_memory_stream")
        source_filename = Path(file_source).name if isinstance(file_source, str) else "stream.pdf"

        # Converte bytes brutos para BytesIO se necessário
        if isinstance(pdf_path_or_stream, bytes):
            stream = io.BytesIO(pdf_path_or_stream)
        else:
            stream = pdf_path_or_stream

        # ESTRATÉGIA 1: pdfplumber (leitura rica com tabelas e texto)
        if pdfplumber is not None:
            try:
                logger.info(f"Iniciando extração via pdfplumber: {source_filename}")
                with pdfplumber.open(stream) as pdf:
                    # Tenta extrair tabelas
                    table_records = self._extract_from_tables(pdf)
                    if table_records:
                        records.extend(table_records)
                        logger.info(f"Tabelas identificadas: {len(table_records)} registros encontrados em {source_filename}.")
                    else:
                        # Fallback textual com regex
                        for page_idx, page in enumerate(pdf.pages, start=1):
                            txt = page.extract_text() or ""
                            page_recs = self._extract_from_text_patterns(txt, page_num=page_idx)
                            records.extend(page_recs)
                        logger.info(f"Formulários textuais processados: {len(records)} registros extraídos em {source_filename}.")

            except Exception as e:
                logger.warning(f"Falha na extração com pdfplumber para {source_filename}: {e}. Acionando fallback...")
                records = []

        # ESTRATÉGIA 2: pypdf / PyPDF2 (Fallback resiliente)
        if not records and pypdf is not None:
            try:
                logger.info(f"Executando fallback com pypdf/PyPDF2: {source_filename}")
                # Reinicia ponteiro caso seja stream
                if hasattr(stream, "seek"):
                    stream.seek(0)
                reader = pypdf.PdfReader(stream)
                for page_idx, page in enumerate(reader.pages, start=1):
                    txt = page.extract_text() or ""
                    page_recs = self._extract_from_text_patterns(txt, page_num=page_idx)
                    records.extend(page_recs)
                logger.info(f"Fallback concluído: {len(records)} registros recuperados.")
            except Exception as e:
                logger.error(f"Erro crítico no fallback pypdf para {source_filename}: {e}")

        # Injeta metadados de rastreabilidade (Data Lineage)
        for rec in records:
            rec["arquivo_origem"] = source_filename
            rec["data_ingestao"] = datetime.now().isoformat()
            rec["formato_origem"] = "pdf"

        return records


def extract_data_from_pdf(pdf_path_or_stream: Union[str, Path, io.BytesIO, bytes]) -> List[Dict[str, Any]]:
    """
    Função principal de interface para PDFs (Passo 1 do Pipeline).
    Recebe um arquivo ou stream de PDF e extrai os campos de transporte:
    Origem, Destino, Peso, Data, Tabela/Valor de Frete.
    """
    extractor = FreightPDFExtractor()
    recs = extractor.extract(pdf_path_or_stream)
    for r in recs:
        r.setdefault("formato_origem", "pdf")
    return recs


# ==============================================================================
# INGESTÃO E EXTRAÇÃO DE CSV
# ==============================================================================
class FreightCSVExtractor:
    """
    Extrator resiliente de arquivos CSV para o HUB Integrador da Rodomax Transportes.
    Suporta múltiplos delimitadores (vírgula, ponto-e-vírgula, tabulação)
    e encodings comuns em ERPs logísticos brasileiros (utf-8, utf-8-sig, latin1, cp1252).
    """

    ENCODINGS_TO_TRY = ["utf-8", "utf-8-sig", "latin1", "cp1252"]

    @staticmethod
    def _map_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Mapeia dinamicamente os nomes das colunas heterogêneas para o padrão do HUB."""
        col_mapping = {}
        for col in df.columns:
            cleaned = re.sub(r"[^a-zA-Z0-9]", "", str(col).lower())
            if any(k in cleaned for k in ["origem", "coleta", "source", "cidadeorigem"]) and "origem" not in col_mapping.values():
                col_mapping[col] = "origem"
            elif any(k in cleaned for k in ["destino", "entrega", "destination", "cidadedestino"]) and "destino" not in col_mapping.values():
                col_mapping[col] = "destino"
            elif any(k in cleaned for k in ["peso", "cargakg", "pesobruto", "pesocarga", "weight"]) and "peso" not in col_mapping.values():
                col_mapping[col] = "peso"
            elif any(k in cleaned for k in ["valor", "frete", "preco", "price", "tarifa", "tabela"]) and "valor_frete" not in col_mapping.values():
                col_mapping[col] = "valor_frete"
            elif any(k in cleaned for k in ["data", "date", "emissao", "solicitacao", "vigencia"]) and "data" not in col_mapping.values():
                col_mapping[col] = "data"
        return df.rename(columns=col_mapping)

    def extract(self, csv_path_or_stream: Union[str, Path, io.BytesIO, bytes]) -> List[Dict[str, Any]]:
        file_source = getattr(csv_path_or_stream, "name", str(csv_path_or_stream) if isinstance(csv_path_or_stream, (str, Path)) else "stream.csv")
        source_filename = Path(file_source).name if isinstance(file_source, str) else "stream.csv"

        logger.info(f"Iniciando extração via CSV: {source_filename}")

        if isinstance(csv_path_or_stream, bytes):
            stream = io.BytesIO(csv_path_or_stream)
        elif isinstance(csv_path_or_stream, (str, Path)):
            stream = Path(csv_path_or_stream)
        else:
            stream = csv_path_or_stream

        df: Optional[pd.DataFrame] = None
        for enc in self.ENCODINGS_TO_TRY:
            try:
                if hasattr(stream, "seek"):
                    stream.seek(0)
                df = pd.read_csv(stream, sep=None, engine="python", encoding=enc, dtype=str)
                logger.info(f"CSV lido com sucesso usando encoding '{enc}' e delimitador detectado.")
                break
            except Exception:
                continue

        if df is None or df.empty:
            logger.warning(f"Não foi possível ler o arquivo CSV '{source_filename}' ou o arquivo está vazio.")
            return []

        df = self._map_columns(df)
        records = df.to_dict(orient="records")

        for rec in records:
            rec["arquivo_origem"] = source_filename
            rec["data_ingestao"] = datetime.now().isoformat()
            rec["formato_origem"] = "csv"

        logger.info(f"CSV processado com sucesso: {len(records)} registros extraídos de {source_filename}.")
        return records


def extract_data_from_csv(csv_path_or_stream: Union[str, Path, io.BytesIO, bytes]) -> List[Dict[str, Any]]:
    """Função de conveniência para extração de fretes a partir de arquivos CSV."""
    extractor = FreightCSVExtractor()
    return extractor.extract(csv_path_or_stream)


# ==============================================================================
# INGESTÃO E EXTRAÇÃO DE JSON / JSON LINES
# ==============================================================================
class FreightJSONExtractor:
    """
    Extrator flexível de arquivos JSON e JSON Lines (NDJSON) para o HUB Integrador.
    Suporta arrays de objetos, objetos aninhados de APIs/Webhooks e logs em linhas (JSONL).
    """

    @staticmethod
    def _find_field(item: Dict[str, Any], keywords: List[str]) -> Optional[Any]:
        for k, v in item.items():
            k_clean = re.sub(r"[^a-zA-Z0-9]", "", str(k).lower())
            if any(kw in k_clean for kw in keywords):
                return v

        for v in item.values():
            if isinstance(v, dict):
                for sub_k, sub_v in v.items():
                    sub_k_clean = re.sub(r"[^a-zA-Z0-9]", "", str(sub_k).lower())
                    if any(kw in sub_k_clean for kw in keywords):
                        return sub_v
        return None

    def _normalize_dict_record(self, item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "origem": self._find_field(item, ["origem", "coleta", "source", "cidadeorigem"]),
            "destino": self._find_field(item, ["destino", "entrega", "destination", "cidadedestino"]),
            "peso": self._find_field(item, ["peso", "cargakg", "pesobruto", "pesocarga", "weight"]),
            "valor_frete": self._find_field(item, ["valor", "frete", "preco", "price", "tarifa", "tabela"]),
            "data": self._find_field(item, ["data", "date", "emissao", "solicitacao", "vigencia"]),
        }

    def extract(self, json_path_or_stream: Union[str, Path, io.BytesIO, bytes]) -> List[Dict[str, Any]]:
        file_source = getattr(json_path_or_stream, "name", str(json_path_or_stream) if isinstance(json_path_or_stream, (str, Path)) else "stream.json")
        source_filename = Path(file_source).name if isinstance(file_source, str) else "stream.json"

        logger.info(f"Iniciando extração via JSON: {source_filename}")

        if isinstance(json_path_or_stream, bytes):
            content = json_path_or_stream.decode("utf-8", errors="replace")
        elif isinstance(json_path_or_stream, (str, Path)):
            with open(json_path_or_stream, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
        elif hasattr(json_path_or_stream, "read"):
            content = json_path_or_stream.read()
            if isinstance(content, bytes):
                content = content.decode("utf-8", errors="replace")
        else:
            content = str(json_path_or_stream)

        raw_items: List[Dict[str, Any]] = []

        try:
            parsed = json.loads(content)
            if isinstance(parsed, list):
                raw_items = [x for x in parsed if isinstance(x, dict)]
            elif isinstance(parsed, dict):
                inner_list = None
                for key in ["solicitacoes", "pedidos", "fretes", "cargas", "data", "items", "results"]:
                    if key in parsed and isinstance(parsed[key], list):
                        inner_list = parsed[key]
                        break
                if inner_list is not None:
                    raw_items = [x for x in inner_list if isinstance(x, dict)]
                else:
                    raw_items = [parsed]
        except json.JSONDecodeError:
            logger.info(f"Arquivo {source_filename} lido no formato JSON Lines (NDJSON)...")
            for line in content.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    line_parsed = json.loads(line)
                    if isinstance(line_parsed, dict):
                        raw_items.append(line_parsed)
                except json.JSONDecodeError:
                    continue

        if not raw_items:
            logger.warning(f"Nenhum objeto válido encontrado no arquivo JSON '{source_filename}'.")
            return []

        records = [self._normalize_dict_record(item) for item in raw_items]

        for rec in records:
            rec["arquivo_origem"] = source_filename
            rec["data_ingestao"] = datetime.now().isoformat()
            rec["formato_origem"] = "json"

        logger.info(f"JSON processado com sucesso: {len(records)} registros extraídos de {source_filename}.")
        return records


def extract_data_from_json(json_path_or_stream: Union[str, Path, io.BytesIO, bytes]) -> List[Dict[str, Any]]:
    """Função de conveniência para extração de fretes a partir de arquivos JSON/JSONL."""
    extractor = FreightJSONExtractor()
    return extractor.extract(json_path_or_stream)


# ==============================================================================
# PASSO 2: AUDITORIA E INCONSISTÊNCIAS
# ==============================================================================
class DataQualityAuditor:
    """
    Responsável por auditar os dados brutos, mapear duplicatas, valores nulos
    e emitir o sumário estatístico via .info() e .describe().
    """

    @staticmethod
    def audit(df: pd.DataFrame, stage_label: str = "Dados Brutos Iniciais") -> Dict[str, Any]:
        """
        Executa a análise de qualidade e imprime diagnósticos estatísticos completos.
        """
        logger.info(f"================ INÍCIO DA AUDITORIA: {stage_label.upper()} ================")
        total_rows = len(df)
        total_cols = len(df.columns)
        duplicates_count = int(df.duplicated(subset=["origem", "destino", "peso", "data"]).sum()) if {"origem", "destino", "peso", "data"}.issubset(df.columns) else int(df.duplicated().sum())

        missing_per_col = df.isnull().sum().to_dict()
        missing_pct_per_col = (df.isnull().sum() / total_rows * 100).round(2).to_dict() if total_rows > 0 else {}

        # 1. Emissão do .info() capturado via buffer de memória
        buffer = io.StringIO()
        df.info(buf=buffer)
        info_str = buffer.getvalue()

        # 2. Emissão do .describe()
        describe_str = df.describe(include="all").to_string() if not df.empty else "DataFrame vazio."

        # Exibição estruturada no console para auditoria do Engenheiro/Cientista de Dados
        print("\n" + "=" * 80)
        print(f"📊 RELATÓRIO DE AUDITORIA DE QUALIDADE DOS DADOS: {stage_label}")
        print("=" * 80)
        print(f"• Total de Registros: {total_rows}")
        print(f"• Total de Colunas:   {total_cols}")
        print(f"• Registros Duplicados Encontrados: {duplicates_count}")
        print("\n• Valores Ausentes por Coluna:")
        for col, count in missing_per_col.items():
            pct = missing_pct_per_col.get(col, 0.0)
            print(f"   - {col:18s}: {count:5d} nulos ({pct:6.2f}%)")

        print("\n" + "-" * 80)
        print("📋 DIAGNÓSTICO ESTRUTURAL (.info()):")
        print(info_str.strip())

        print("\n" + "-" * 80)
        print("📈 SUMÁRIO ESTATÍSTICO DAS VARIÁVEIS (.describe()):")
        print(describe_str)
        print("=" * 80 + "\n")

        summary = {
            "total_registros": total_rows,
            "total_colunas": total_cols,
            "duplicatas": duplicates_count,
            "valores_nulos": missing_per_col,
            "valores_nulos_pct": missing_pct_per_col,
        }
        return summary


# ==============================================================================
# PASSO 3: SANEAMENTO E LIMPEZA
# ==============================================================================
class DataSanitizer:
    """
    Aplica regras determinísticas de negócio:
    - Descarte de registros sem campos vitais (Origem, Destino, Peso da Carga).
    - Descarte de duplicatas.
    - Remoção de colunas irrelevantes ou 100% nulas.
    """

    VITAL_COLUMNS = ["origem", "destino", "peso"]

    @classmethod
    def sanitize(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Higieniza o DataFrame bruto removendo inconsistências e dados incompletos.
        """
        logger.info("Iniciando etapa de Saneamento e Limpeza dos dados...")
        initial_count = len(df)
        if initial_count == 0:
            logger.warning("DataFrame vazio fornecido para saneamento.")
            return df.copy()

        clean_df = df.copy()

        # 1. Remoção de duplicatas
        subset_cols = [c for c in ["origem", "destino", "peso", "data", "valor_frete"] if c in clean_df.columns]
        duplicates_removed = clean_df.duplicated(subset=subset_cols).sum()
        clean_df = clean_df.drop_duplicates(subset=subset_cols, keep="first")
        logger.info(f"Saneamento: {duplicates_removed} registros duplicados removidos.")

        # 2. Descarte de registros incompletos sem dados vitais (Origem, Destino, Peso)
        for col in cls.VITAL_COLUMNS:
            if col in clean_df.columns:
                # Trata strings vazias ou apenas espaços como nulos
                clean_df[col] = clean_df[col].replace(r"^\s*$", np.nan, regex=True)

        vital_mask = clean_df[cls.VITAL_COLUMNS].notnull().all(axis=1)
        invalid_count = (~vital_mask).sum()
        clean_df = clean_df[vital_mask].copy()
        logger.info(f"Saneamento: {invalid_count} registros descartados por falta de dados vitais ({cls.VITAL_COLUMNS}).")

        # 3. Remoção de colunas 100% nulas ou irrelevantes
        all_null_cols = [c for c in clean_df.columns if clean_df[c].isnull().all()]
        if all_null_cols:
            clean_df = clean_df.drop(columns=all_null_cols)
            logger.info(f"Saneamento: Colunas 100% nulas eliminadas: {all_null_cols}")

        irrelevant_cols = ["extra_info", "pagina", "tipo_extracao"]
        cols_to_drop = [c for c in irrelevant_cols if c in clean_df.columns]
        if cols_to_drop:
            clean_df = clean_df.drop(columns=cols_to_drop)
            logger.info(f"Saneamento: Metadados técnicos descartados da base analítica: {cols_to_drop}")

        final_count = len(clean_df)
        logger.info(f"Saneamento concluído: Base reduziu de {initial_count} para {final_count} registros úteis.")
        return clean_df


# ==============================================================================
# PASSO 4: NORMALIZAÇÃO
# ==============================================================================
class DataNormalizer:
    """
    Padronização técnica e matemática das variáveis:
    - Datas no padrão universal ISO 8601 (YYYY-MM-DD / YYYY-MM-DDTHH:MM:SS)
    - Medidas de peso unificadas para float em Quilogramas (kg)
    - Valores monetários de frete convertidos para float
    - Normalização de cidades e estados (caixa alta, sem caracteres espúrios)
    """

    @staticmethod
    def normalize_date(val: Any) -> Optional[str]:
        """Converte datas em formatos variados para ISO 8601."""
        if pd.isna(val) or val is None:
            return None
        s = str(val).strip()
        if not s:
            return None

        try:
            # Caso comece com ano (ex: 2026-10-12), evita confusão com dayfirst
            if s.startswith(("19", "20")) and "-" in s[:5]:
                dt = pd.to_datetime(s, errors="coerce")
            else:
                dt = pd.to_datetime(s, dayfirst=True, errors="coerce")

            if pd.isna(dt):
                return None

            if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
                return dt.strftime("%Y-%m-%d")
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            return None

    @staticmethod
    def normalize_weight_kg(val: Any) -> Optional[float]:
        """
        Padroniza pesos de múltiplos formatos (t, ton, toneladas, kg, g)
        para float em Quilogramas (kg).
        Exemplos: '14.500 kg' -> 14500.0, '28 ton' -> 28000.0, '14,5 t' -> 14500.0
        """
        if pd.isna(val) or val is None:
            return None

        raw = str(val).strip().lower()
        if not raw:
            return None

        # Identifica multiplicador de escala
        multiplier = 1.0
        if any(unit in raw for unit in ["ton", "tonelada", "toneladas"]) or re.search(r"\b(t)\b", raw):
            multiplier = 1000.0
        elif any(unit in raw for unit in ["kg", "quilo", "quilos"]):
            multiplier = 1.0
        elif raw.endswith("g") and not raw.endswith("kg"):
            multiplier = 0.001

        # Limpa caracteres não numéricos
        num_str = re.sub(r"[^\d.,]", "", raw)
        if not num_str:
            return None

        if "," in num_str and "." in num_str:
            # Notação brasileira: ponto milhar, vírgula decimal (ex: 14.500,50)
            num_str = num_str.replace(".", "").replace(",", ".")
        elif "," in num_str:
            # Verifica se vírgula foi usada como separador de milhar
            parts = num_str.split(",")
            if len(parts) == 2 and len(parts[1]) == 3 and int(parts[0]) >= 10:
                num_str = num_str.replace(",", "")
            else:
                num_str = num_str.replace(",", ".")
        elif "." in num_str:
            parts = num_str.split(".")
            # Formato brasileiro de milhar sem decimais (ex: 15.000 kg)
            if len(parts) == 2 and len(parts[1]) == 3 and multiplier <= 1.0:
                num_str = num_str.replace(".", "")

        try:
            return round(float(num_str) * multiplier, 2)
        except ValueError:
            return None

    @staticmethod
    def normalize_currency(val: Any) -> Optional[float]:
        """Normaliza valores monetários de frete para float puro."""
        if pd.isna(val) or val is None:
            return None

        raw = str(val).strip().lower()
        num_str = re.sub(r"[^\d.,]", "", raw)
        if not num_str:
            return None

        if "," in num_str and "." in num_str:
            num_str = num_str.replace(".", "").replace(",", ".")
        elif "," in num_str:
            num_str = num_str.replace(",", ".")
        elif "." in num_str:
            parts = num_str.split(".")
            if len(parts) == 2 and len(parts[1]) == 3:
                num_str = num_str.replace(".", "")

        try:
            return round(float(num_str), 2)
        except ValueError:
            return None

    @staticmethod
    def normalize_text_location(val: Any) -> Optional[str]:
        """Normaliza cidades/estados para caixa alta, espaços limpos e hífen uniforme."""
        if pd.isna(val) or val is None:
            return None
        s = str(val).strip()
        # Remove quebras de linha e tabulações
        s = re.sub(r"[\r\n\t]+", " ", s)
        s = re.sub(r"\s+", " ", s)
        # Padroniza separadores (ex: 'Curitiba/PR' ou 'Curitiba - PR' -> 'CURITIBA - PR')
        s = re.sub(r"\s*[/\\-]\s*", " - ", s)
        return s.strip().upper()

    @classmethod
    def normalize(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Executa a transformação e normalização completa de todas as colunas."""
        logger.info("Iniciando normalização de variáveis para padrões universais...")
        norm_df = df.copy()

        # 1. Normalização de Origem e Destino
        if "origem" in norm_df.columns:
            norm_df["origem"] = norm_df["origem"].apply(cls.normalize_text_location)
        if "destino" in norm_df.columns:
            norm_df["destino"] = norm_df["destino"].apply(cls.normalize_text_location)

        # 2. Normalização de Peso para kg (float)
        if "peso" in norm_df.columns:
            norm_df["peso_kg"] = norm_df["peso"].apply(cls.normalize_weight_kg)
            norm_df = norm_df.drop(columns=["peso"])

        # 3. Normalização de Valor do Frete (float)
        if "valor_frete" in norm_df.columns:
            norm_df["valor_frete"] = norm_df["valor_frete"].apply(cls.normalize_currency)

        # 4. Normalização de Data para ISO 8601
        if "data" in norm_df.columns:
            norm_df["data_solicitacao"] = norm_df["data"].apply(cls.normalize_date)
            norm_df = norm_df.drop(columns=["data"])

        # Reordenação e tipagem estrita para consumo analítico e IA
        desired_order = [
            "origem",
            "destino",
            "peso_kg",
            "valor_frete",
            "data_solicitacao",
            "formato_origem",
            "arquivo_origem",
            "data_ingestao",
        ]
        present_cols = [c for c in desired_order if c in norm_df.columns]
        norm_df = norm_df[present_cols]

        logger.info("Normalização concluída com sucesso.")
        return norm_df


# ==============================================================================
# PASSO 5: PREPARAÇÃO PARA IA E EXPORTAÇÃO (DATA LAKE)
# ==============================================================================
class FreightKMeansSegmenter:
    """
    Protótipo de agrupamento e detecção de anomalias para demandas de frete.

    O algoritmo implementa K-Means com três perfis usando peso e valor do frete.
    Não depende de sklearn: normaliza os atributos, realiza iterações de Lloyd,
    calcula o perfil do cluster e sinaliza casos fora do padrão do cluster.
    """

    def __init__(self, k: int = 3, max_iterations: int = 100) -> None:
        if k < 1:
            raise ValueError("O número de grupos deve ser maior que zero.")
        self.k = k
        self.max_iterations = max_iterations
        self.weight_mean = 0.0
        self.weight_std = 1.0
        self.freight_mean = 0.0
        self.freight_std = 1.0
        self.centroids: List[List[float]] = []
        self.cluster_members: Dict[int, List[List[float]]] = {}
        self.cluster_labels: Dict[int, str] = {}
        self.trained = False

    @staticmethod
    def _mean(values: List[float]) -> float:
        return sum(values) / len(values) if values else 0.0

    @classmethod
    def _std(cls, values: List[float]) -> float:
        if len(values) < 2:
            return 1.0
        mean = cls._mean(values)
        variance = sum((value - mean) ** 2 for value in values) / len(values)
        return math.sqrt(variance) or 1.0

    @staticmethod
    def _distance(first: List[float], second: List[float]) -> float:
        return math.sqrt(sum((a - b) ** 2 for a, b in zip(first, second)))

    def fit(self, df: pd.DataFrame) -> "FreightKMeansSegmenter":
        required = {"peso_kg", "valor_frete"}
        if not required.issubset(df.columns):
            raise ValueError("A base precisa conter peso_kg e valor_frete.")

        training_df = df[["peso_kg", "valor_frete"]].dropna()
        if training_df.empty:
            raise ValueError("Não há dados válidos para treinar o agrupamento.")

        weights = [float(value) for value in training_df["peso_kg"].tolist()]
        freights = [
            float(value)
            for value in training_df["valor_frete"].tolist()
            if pd.notna(value)
        ]
        if len(weights) != len(freights):
            weights = [
                float(row["peso_kg"])
                for _, row in training_df.iterrows()
                if pd.notna(row["peso_kg"]) and pd.notna(row["valor_frete"])
            ]

        self.weight_mean = self._mean(weights)
        self.weight_std = self._std(weights)
        self.freight_mean = self._mean(freights)
        self.freight_std = self._std(freights)

        points = [
            [
                (float(row["peso_kg"]) - self.weight_mean) / self.weight_std,
                (float(row["valor_frete"]) - self.freight_mean) / self.freight_std,
            ]
            for _, row in training_df.iterrows()
            if pd.notna(row["peso_kg"]) and pd.notna(row["valor_frete"])
        ]

        if not points:
            raise ValueError("Não há dados válidos para treinar o agrupamento.")

        effective_k = min(self.k, len(points))
        self.centroids = [points[0][:]]
        while len(self.centroids) < effective_k:
            farthest = max(
                points,
                key=lambda point: min(
                    self._distance(point, centroid) for centroid in self.centroids
                ),
            )
            if farthest in self.centroids:
                break
            self.centroids.append(farthest[:])

        labels: List[int] = []
        for _ in range(self.max_iterations):
            labels = [
                min(
                    range(len(self.centroids)),
                    key=lambda index: self._distance(point, self.centroids[index]),
                )
                for point in points
            ]
            new_centroids: List[List[float]] = []
            for cluster_index in range(len(self.centroids)):
                members = [
                    point
                    for point, label in zip(points, labels)
                    if label == cluster_index
                ]
                if members:
                    new_centroids.append([
                        self._mean([point[0] for point in members]),
                        self._mean([point[1] for point in members]),
                    ])
                else:
                    new_centroids.append(self.centroids[cluster_index][:])
            if all(
                self._distance(old, new) < 1e-8
                for old, new in zip(self.centroids, new_centroids)
            ):
                self.centroids = new_centroids
                break
            self.centroids = new_centroids

        self.cluster_members = {
            cluster_index: []
            for cluster_index in range(len(self.centroids))
        }
        for point, label in zip(points, labels):
            self.cluster_members[label].append(point)

        cluster_profiles: List[tuple[int, float, float]] = []
        for cluster_index, members in self.cluster_members.items():
            if members:
                weight_average = self._mean([
                    point[0] * self.weight_std + self.weight_mean
                    for point in members
                ])
                freight_average = self._mean([
                    point[1] * self.freight_std + self.freight_mean
                    for point in members
                ])
                cluster_profiles.append((cluster_index, weight_average, freight_average))

        cluster_profiles.sort(key=lambda profile: profile[1])
        profile_names = ["perfil de carga baixa", "perfil de carga media", "perfil de carga alta"]
        for position, (cluster_index, _, _) in enumerate(cluster_profiles):
            name_index = min(position, len(profile_names) - 1)
            self.cluster_labels[cluster_index] = profile_names[name_index]

        self.trained = True
        return self

    def predict(self, peso_kg: Any, valor_frete: Any) -> Dict[str, Any]:
        if not self.trained:
            raise RuntimeError("Treine o segmentador antes de prever um caso novo.")
        if pd.isna(peso_kg) or pd.isna(valor_frete):
            raise ValueError("Peso e valor do frete são obrigatórios.")

        weight = float(peso_kg)
        freight = float(valor_frete)
        if weight < 0 or freight < 0:
            raise ValueError("Peso e valor do frete não podem ser negativos.")

        point = [
            (weight - self.weight_mean) / self.weight_std,
            (freight - self.freight_mean) / self.freight_std,
        ]
        cluster_index = min(
            range(len(self.centroids)),
            key=lambda index: self._distance(point, self.centroids[index]),
        )
        distance = self._distance(point, self.centroids[cluster_index])
        members = self.cluster_members.get(cluster_index, [])
        member_distances = [
            self._distance(member, self.centroids[cluster_index])
            for member in members
        ]
        threshold = self._mean(member_distances) + 2 * self._std(member_distances)

        return {
            "cluster": cluster_index,
            "perfil": self.cluster_labels[cluster_index],
            "peso_kg": round(weight, 2),
            "valor_frete": round(freight, 2),
            "distancia_ao_centro": round(distance, 4),
            "anomalia": distance > threshold,
        }


class FreightRegressionModel:
    """Regressão linear simples para estimar frete a partir do peso da carga."""

    def __init__(self) -> None:
        self.slope = 0.0
        self.intercept = 0.0
        self.training_weight_range = (0.0, 0.0)
        self.trained = False

    @staticmethod
    def _samples(df: pd.DataFrame) -> List[tuple[float, float]]:
        required = {"peso_kg", "valor_frete"}
        if not required.issubset(df.columns):
            raise ValueError("A base precisa conter peso_kg e valor_frete.")

        samples: List[tuple[float, float]] = []
        for weight, freight in zip(df["peso_kg"], df["valor_frete"]):
            if pd.isna(weight) or pd.isna(freight):
                continue
            weight_value = float(weight)
            freight_value = float(freight)
            if (
                math.isfinite(weight_value)
                and math.isfinite(freight_value)
                and weight_value > 0
                and freight_value >= 0
            ):
                samples.append((weight_value, freight_value))
        return samples

    @staticmethod
    def _fit_coefficients(
        samples: List[tuple[float, float]],
    ) -> tuple[float, float]:
        weights_tons = [weight / 1000 for weight, _ in samples]
        freights = [freight for _, freight in samples]
        weight_mean = sum(weights_tons) / len(weights_tons)
        freight_mean = sum(freights) / len(freights)
        variance = sum((weight - weight_mean) ** 2 for weight in weights_tons)
        if variance == 0:
            raise ValueError("Os pesos precisam variar para treinar a regressão.")
        slope = sum(
            (weight - weight_mean) * (freight - freight_mean)
            for weight, freight in zip(weights_tons, freights)
        ) / variance
        return slope, freight_mean - slope * weight_mean

    def fit(self, df: pd.DataFrame) -> "FreightRegressionModel":
        samples = self._samples(df)
        if len(samples) < 2:
            raise ValueError("São necessários ao menos dois fretes válidos para treinar.")

        self.slope, self.intercept = self._fit_coefficients(samples)
        weights = [sample[0] for sample in samples]
        self.training_weight_range = (min(weights), max(weights))
        self.trained = True
        return self

    def predict(self, peso_kg: float) -> Dict[str, Any]:
        if not self.trained:
            raise RuntimeError("Treine a regressão antes de estimar um frete.")
        weight = float(peso_kg)
        if not math.isfinite(weight) or weight <= 0:
            raise ValueError("O peso precisa ser um número positivo.")

        estimate = self.intercept + self.slope * (weight / 1000)
        lower, upper = self.training_weight_range
        return {
            "peso_kg": round(weight, 2),
            "frete_estimado": round(estimate, 2),
            "frete_por_tonelada": round(estimate / (weight / 1000), 2),
            "fora_da_faixa_treinada": weight < lower or weight > upper,
        }

    @classmethod
    def evaluate(
        cls,
        df: pd.DataFrame,
        test_fraction: float = 0.25,
        random_state: int = 42,
    ) -> Dict[str, Any]:
        """Compara a regressão com a média do treino em uma divisão holdout."""
        if not 0 < test_fraction < 1:
            raise ValueError("A proporção de teste deve estar entre 0 e 1.")

        samples = cls._samples(df)
        if len(samples) < 5:
            raise ValueError("São necessários ao menos cinco fretes para avaliar o modelo.")

        indices = list(range(len(samples)))
        random.Random(random_state).shuffle(indices)
        test_count = max(1, math.ceil(len(samples) * test_fraction))
        test_indices = indices[:test_count]
        train_indices = indices[test_count:]
        train_samples = [samples[index] for index in train_indices]
        test_samples = [samples[index] for index in test_indices]

        slope, intercept = cls._fit_coefficients(train_samples)
        baseline_mean = sum(freight for _, freight in train_samples) / len(train_samples)
        cases = []
        errors: List[float] = []
        baseline_errors: List[float] = []
        squared_errors: List[float] = []
        for weight, actual in test_samples:
            predicted = intercept + slope * (weight / 1000)
            error = actual - predicted
            errors.append(abs(error))
            baseline_errors.append(abs(actual - baseline_mean))
            squared_errors.append(error ** 2)
            cases.append({
                "peso_kg": round(weight, 2),
                "frete_real": round(actual, 2),
                "frete_estimado": round(predicted, 2),
                "erro_absoluto": round(abs(actual - predicted), 2),
                "previsao_base": round(baseline_mean, 2),
            })

        mae = sum(errors) / len(errors)
        baseline_mae = sum(baseline_errors) / len(baseline_errors)
        rmse = math.sqrt(sum(squared_errors) / len(squared_errors))
        return {
            "registros_treino": len(train_samples),
            "registros_teste": len(test_samples),
            "mae_regressao": round(mae, 2),
            "mae_linha_base": round(baseline_mae, 2),
            "rmse_regressao": round(rmse, 2),
            "melhora_percentual_mae": (
                round((baseline_mae - mae) / baseline_mae * 100, 2)
                if baseline_mae > 0
                else None
            ),
            "casos_teste": cases,
            "observacao": (
                "Validacao holdout deterministica; modelo e linha de base "
                "ajustados somente no conjunto de treino."
            ),
        }


class FreightDataPipeline:
    """
    Orquestrador completo do pipeline de engenharia de dados do HUB Integrador.
    Gerencia:
    - Ingestão multiformato em lote (PDF, CSV, JSON e JSON Lines)
    - Auditoria de qualidade
    - Saneamento e filtragem
    - Normalização unificada
    - Concatenação de lotes e persistência analítica em Parquet e CSV no Data Lake
    """

    def __init__(self, data_lake_dir: Union[str, Path] = "./data_lake") -> None:
        self.data_lake_dir = Path(data_lake_dir)
        self.data_lake_dir.mkdir(parents=True, exist_ok=True)
        self.pdf_extractor = FreightPDFExtractor()
        self.csv_extractor = FreightCSVExtractor()
        self.json_extractor = FreightJSONExtractor()
        self.auditor = DataQualityAuditor()
        self.sanitizer = DataSanitizer()
        self.normalizer = DataNormalizer()

    def process_files(self, files: List[Union[str, Path]]) -> pd.DataFrame:
        """
        Processa uma lista de arquivos heterogêneos (PDF, CSV, JSON, JSONL).
        Aplica auditoria, saneamento e normalização unificada.
        """
        logger.info(f"Iniciando processamento em lote de {len(files)} arquivo(s)...")
        all_records: List[Dict[str, Any]] = []
        corrupted_files: List[str] = []

        # PASSO 1: Leitura e extração multicanal com tolerância a falhas
        for file_item in files:
            file_path = Path(file_item)
            ext = file_path.suffix.lower()
            try:
                if ext == ".pdf":
                    file_records = self.pdf_extractor.extract(file_path)
                elif ext == ".csv":
                    file_records = self.csv_extractor.extract(file_path)
                elif ext in [".json", ".jsonl", ".ndjson"]:
                    file_records = self.json_extractor.extract(file_path)
                else:
                    logger.warning(f"Formato não suportado: '{file_path.name}'. Ignorando.")
                    continue
                all_records.extend(file_records)
            except Exception as e:
                logger.error(f"Falha irrecuperável ao processar o arquivo '{file_path.name}': {e}. Registrando em log de erros.")
                corrupted_files.append(file_path.name)

        if corrupted_files:
            logger.warning(f"Total de {len(corrupted_files)} arquivo(s) com erro isolado(s): {corrupted_files}")

        if not all_records:
            logger.warning("Nenhum registro extraído dos arquivos fornecidos.")
            return pd.DataFrame()

        # Criação do DataFrame Bruto Consolidado
        raw_df = pd.DataFrame(all_records)

        # PASSO 2: Auditoria e Estatísticas Iniciais
        self.auditor.audit(raw_df, stage_label="1. Base Bruta Consolidada")

        # PASSO 3: Saneamento e Limpeza
        cleaned_df = self.sanitizer.sanitize(raw_df)

        # PASSO 4: Normalização
        normalized_df = self.normalizer.normalize(cleaned_df)

        # Auditoria Final da Base Higienizada
        self.auditor.audit(normalized_df, stage_label="2. Base Higienizada e Normalizada (Pronta para IA)")

        return normalized_df

    def process_directory(self, input_dir: Union[str, Path]) -> pd.DataFrame:
        """
        Processa todos os arquivos suportados (PDF, CSV, JSON, JSONL) no diretório de entrada.
        """
        input_path = Path(input_dir)
        if not input_path.exists():
            raise FileNotFoundError(f"Diretório de entrada não encontrado: {input_path}")

        files: List[Path] = []
        for ext_pattern in ["*.pdf", "*.csv", "*.json", "*.jsonl", "*.ndjson"]:
            files.extend(input_path.glob(ext_pattern))

        files = sorted(files, key=lambda p: p.name.lower())
        logger.info(f"Localizados {len(files)} arquivos heterogêneos (PDF, CSV, JSON) no diretório '{input_path}'.")
        return self.process_files(files)

    def export_to_data_lake(
        self,
        df: pd.DataFrame,
        base_filename: str = "processed_freights",
    ) -> Dict[str, Path]:
        """
        Exporta o DataFrame processado para o Data Lake nos formatos:
        - .parquet (otimizado via pyarrow e compressão snappy para ingestão analítica e IA)
        - .csv (formato interoperável padrão UTF-8)
        """
        if df.empty:
            logger.warning("DataFrame vazio. Nenhuma exportação realizada para o Data Lake.")
            return {}

        parquet_path = self.data_lake_dir / f"{base_filename}.parquet"
        csv_path = self.data_lake_dir / f"{base_filename}.csv"

        logger.info(f"Gravando base analítica no formato Parquet: {parquet_path}")
        df.to_parquet(
            parquet_path,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )

        logger.info(f"Gravando base no formato CSV: {csv_path}")
        df.to_csv(
            csv_path,
            sep=";",
            index=False,
            encoding="utf-8-sig",
        )

        logger.info(f"Exportação para Data Lake concluída com sucesso em: {self.data_lake_dir.resolve()}")
        return {"parquet": parquet_path, "csv": csv_path}

    def segment_new_demand(
        self,
        df: pd.DataFrame,
        peso_kg: float,
        valor_frete: float,
    ) -> Dict[str, Any]:
        """Treina o protótipo de IA e avalia uma demanda que não estava na base."""
        segmenter = FreightKMeansSegmenter(k=3)
        segmenter.fit(df)
        return segmenter.predict(peso_kg=peso_kg, valor_frete=valor_frete)

    def evaluate_freight_regression(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Avalia regressão linear versus linha-base da média em holdout."""
        return FreightRegressionModel.evaluate(df)


# ==============================================================================
# GERADOR DE DADOS DE TESTE (MOCK PARA VALIDAÇÃO)
# ==============================================================================
def generate_mock_test_environment(raw_dir: Path) -> None:
    """
    Gera arquivos PDF sintéticos representativos da rotina da Rodomax Transportes:
    1. Solicitação individual de frete (Formulário textual).
    2. Tabela consolidada de fretes (Tabela tabular com múltiplas rotas).
    3. Arquivo com registros incompletos/inválidos (para validar descarte na auditoria).
    4. Arquivo corrompido (para validar resiliência do try/except).
    """
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas

    raw_dir.mkdir(parents=True, exist_ok=True)

    # 1. Solicitação individual de frete (Texto)
    pdf_solicitation_1 = raw_dir / "solicitacao_frete_001.pdf"
    c1 = canvas.Canvas(str(pdf_solicitation_1), pagesize=letter)
    c1.setFont("Helvetica-Bold", 14)
    c1.drawString(50, 750, "RODOMAX TRANSPORTES - SOLICITAÇÃO DE FRETE E EMBARQUE")
    c1.setFont("Helvetica", 11)
    c1.drawString(50, 720, "Protocolo: RDM-2026-9812")
    c1.drawString(50, 700, "Data do Pedido: 12/10/2026")
    c1.drawString(50, 680, "Origem: Londrina - PR")
    c1.drawString(50, 660, "Destino: Campinas - SP")
    c1.drawString(50, 640, "Peso da Carga: 15.800 kg")
    c1.drawString(50, 620, "Valor do Frete: R$ 5.750,00")
    c1.drawString(50, 600, "Observação: Carga paletizada, seguro RCTR-C ativo.")
    c1.save()

    # 2. Solicitação individual com formato alternativo (t / ISO)
    pdf_solicitation_2 = raw_dir / "solicitacao_frete_002.pdf"
    c2 = canvas.Canvas(str(pdf_solicitation_2), pagesize=letter)
    c2.setFont("Helvetica-Bold", 14)
    c2.drawString(50, 750, "RODOMAX TRANSPORTES - ORDEM DE COLETA")
    c2.setFont("Helvetica", 11)
    c2.drawString(50, 720, "Data de Emissão: 2026-10-11")
    c2.drawString(50, 700, "De: Ponta Grossa / PR")
    c2.drawString(50, 680, "Para: Rio de Janeiro / RJ")
    c2.drawString(50, 660, "Peso Bruto: 24,5 t")
    c2.drawString(50, 640, "Tabela Frete: R$ 8.900,00")
    c2.save()

    # 3. Tabela de fretes em lote (com duplicatas e casos incompletos)
    pdf_table = raw_dir / "tabela_fretes_rotas.pdf"
    try:
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib import colors

        doc = SimpleDocTemplate(str(pdf_table), pagesize=letter)
        styles = getSampleStyleSheet()
        elements = [
            Paragraph("<b>RODOMAX TRANSPORTES - TABELA DE ROTAS E FRETES</b>", styles["Title"]),
            Spacer(1, 15)
        ]

        table_data = [
            ["Origem", "Destino", "Peso da Carga", "Valor Frete", "Data Solicitação"],
            ["Curitiba / PR", "Santos / SP", "14.500 kg", "R$ 4.800,00", "12/10/2026"],
            ["Maringá - PR", "Paranaguá - PR", "28 ton", "R$ 6.200,50", "11/10/2026"],
            ["Cascavel / PR", "Itajaí / SC", "18.000,00 kg", "R$ 5.100,00", "10/10/2026"],
            ["Curitiba / PR", "Santos / SP", "14.500 kg", "R$ 4.800,00", "12/10/2026"],  # Duplicata proposital
            ["", "Porto Alegre / RS", "12 ton", "R$ 4.000,00", "12/10/2026"],              # Incompleto: sem origem
            ["Foz do Iguaçu / PR", "", "15 ton", "R$ 4.500,00", "12/10/2026"],              # Incompleto: sem destino
            ["Chapecó / SC", "Curitiba / PR", "", "R$ 3.200,00", "09/10/2026"],             # Incompleto: sem peso
        ]

        t = Table(table_data)
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.navy),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        elements.append(t)
        doc.build(elements)
    except Exception as e:
        logger.warning(f"Aviso ao gerar tabela com platypus: {e}. Usando fallback canvas.")

    # 4. Arquivo PDF corrompido para testar resiliência do pipeline
    corrupt_pdf = raw_dir / "arquivo_corrompido_teste.pdf"
    with open(corrupt_pdf, "wb") as f:
        f.write(b"CONTEUDO_INVALIDO_NAO_PDF_SIMULACAO_ERRO_RODOMAX")

    # 5. Arquivo CSV estruturado (delimitador ponto-e-vírgula estilo ERP brasileiro)
    csv_file = raw_dir / "pedidos_erp_totvs.csv"
    csv_content = (
        "Origem;Destino;Peso_Carga;Valor_Frete;Data_Pedido;Observacoes\n"
        "Cascavel / PR;Curitiba / PR;16.500 kg;R$ 4.200,00;12/10/2026;Carga seca paletizada\n"
        "Maringá / PR;Santos / SP;27 t;R$ 7.100,00;11/10/2026;Granel agrícola\n"
        "Toledo / PR;Paranaguá / PR;32.000,00 kg;R$ 8.400,00;10/10/2026;Exportação soja\n"
        "Cascavel / PR;Curitiba / PR;16.500 kg;R$ 4.200,00;12/10/2026;Carga seca paletizada\n"
        ";Porto Alegre / RS;12 t;R$ 3.800,00;12/10/2026;Incompleto sem origem\n"
    )
    with open(csv_file, "w", encoding="utf-8-sig") as f:
        f.write(csv_content)

    # 6. Arquivo JSON estruturado de API externa de clientes (com objetos aninhados)
    json_file = raw_dir / "solicitacoes_api_cliente.json"
    json_payload = [
        {
            "origem": "Foz do Iguaçu - PR",
            "destino": "Itajaí - SC",
            "peso": "19.500 kg",
            "valor_frete": "R$ 5.800,00",
            "data": "2026-10-12",
            "cliente": "AgroSul Logística S/A"
        },
        {
            "rota": {
                "origem": "Ponta Grossa / PR",
                "destino": "Campinas / SP"
            },
            "carga": {
                "peso": "22 ton"
            },
            "frete": "R$ 6.950,00",
            "data_solicitacao": "11/10/2026",
            "tipo_veiculo": "Carreta LS"
        }
    ]
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(json_payload, f, indent=2, ensure_ascii=False)

    # 7. Arquivo JSON Lines (NDJSON - streaming de eventos de logística)
    jsonl_file = raw_dir / "streaming_embarques.jsonl"
    jsonl_lines = [
        json.dumps({"origem": "Guarapuava - PR", "destino": "São Paulo - SP", "peso": "14 t", "valor_frete": "R$ 4.900,00", "data": "2026-10-12"}, ensure_ascii=False),
        json.dumps({"origem": "Apucarana - PR", "destino": "Santos - SP", "peso": "26.000 kg", "valor_frete": "R$ 6.800,00", "data": "2026-10-11"}, ensure_ascii=False)
    ]
    with open(jsonl_file, "w", encoding="utf-8") as f:
        f.write("\n".join(jsonl_lines) + "\n")

    logger.info(f"Ambiente de testes multiformato gerado com sucesso em '{raw_dir}' (PDF, CSV, JSON, JSONL).")

# Diretórios padrão de dados
RAW_DATA_DIR = Path("./data_raw")
DATA_LAKE_DIR = Path("./data_lake")


# ==============================================================================
# EXECUÇÃO PRINCIPAL (EXEMPLO DE USO DO PIPELINE)
# ==============================================================================
if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("🚀 HUB INTEGRADOR DE DADOS - RODOMAX TRANSPORTES")
    print("   Módulo de Ingestão Heterogênea (PDF, CSV e JSON), Saneamento e Data Lake")
    print("=" * 80 + "\n")

    # Gera exemplos somente quando não há entradas suportadas, sem sobrescrever dados.
    supported_patterns = ["*.pdf", "*.csv", "*.json", "*.jsonl", "*.ndjson"]
    has_input_files = any(
        any(RAW_DATA_DIR.glob(pattern)) for pattern in supported_patterns
    )
    if not has_input_files:
        logger.info("Pasta de entrada vazia; criando dados sintéticos para demonstração.")
        generate_mock_test_environment(RAW_DATA_DIR)

    # 2. Inicializa o Pipeline
    pipeline = FreightDataPipeline(data_lake_dir=DATA_LAKE_DIR)

    # 3. Executa processamento em lote da pasta de PDFs
    processed_df = pipeline.process_directory(input_dir=RAW_DATA_DIR)

    # 4. Exporta para o Data Lake nos formatos Parquet e CSV
    exported_files = pipeline.export_to_data_lake(
        df=processed_df,
        base_filename="processed_freights"
    )

    # 5. Validação e Inspeção do Parquet Gerado
    if "parquet" in exported_files and exported_files["parquet"].exists():
        parquet_file = exported_files["parquet"]
        print("\n" + "=" * 80)
        print("🎯 VALIDAÇÃO DA INGESTÃO NO DATA LAKE (LEITURA DO PARQUET GERADO):")
        print("=" * 80)
        lake_df = pd.read_parquet(parquet_file)
        print(lake_df.to_string(index=False))

        print("\n📊 TIPAGEM FINAL DAS COLUNAS (SCHEMA OTIMIZADO PARA IA):")
        for col, dtype in lake_df.dtypes.items():
            print(f" - {col:20s}: {dtype}")
        print("=" * 80)

        # Caso novo usado na apresentação. Não faz parte da base histórica.
        ai_case = pipeline.segment_new_demand(
            lake_df,
            peso_kg=35000,
            valor_frete=14500,
        )
        print("\n🤖 DEMONSTRAÇÃO DE IA - K-MEANS PARA CASO NOVO:")
        print(f"   Entrada: peso=35000 kg; valor do frete=R$ 14500")
        print(f"   Resultado: {ai_case}")

        regression_metrics = pipeline.evaluate_freight_regression(lake_df)
        regression_model = FreightRegressionModel().fit(lake_df)
        freight_estimate = regression_model.predict(peso_kg=35000)
        print("\n📈 DEMONSTRAÇÃO DE IA - REGRESSÃO LINEAR:")
        print(
            f"   Holdout: treino={regression_metrics['registros_treino']}, "
            f"teste={regression_metrics['registros_teste']}"
        )
        print(
            f"   MAE do modelo=R$ {regression_metrics['mae_regressao']:.2f}; "
            f"MAE da linha-base=R$ {regression_metrics['mae_linha_base']:.2f}"
        )
        print(f"   Casos do teste: {regression_metrics['casos_teste']}")
        print(f"   Previsão para 35000 kg: {freight_estimate}")
        if freight_estimate["fora_da_faixa_treinada"]:
            print("   AVISO: peso fora da faixa observada no treino (extrapolação).")

        print(
            "\n✅ Pipeline e protótipo executados com sucesso "
            "(resultado demonstrativo; não homologado para produção)."
        )
