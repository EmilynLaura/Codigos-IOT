"""
HUB Integrador de Dados - Rodomax Transportes
Interface Gráfica Desktop (GUI) para Ingestão, Teste e Auditoria de PDFs.

Autor: Daniel Cagol (Engenharia de Dados)
Execução: python app_interface.py
"""

from __future__ import annotations

import io
import logging
import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import List, Optional

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext, simpledialog

# Importação do pipeline de dados
from pipeline_pdf_fretes import (
    FreightDataPipeline,
    FreightRegressionModel,
    generate_mock_test_environment,
    RAW_DATA_DIR,
    DATA_LAKE_DIR,
)


class TextHandler(logging.Handler):
    """Handler customizado para direcionar logs do Python diretamente para a GUI."""

    def __init__(self, text_widget: scrolledtext.ScrolledText):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)

        def append() -> None:
            self.text_widget.configure(state="normal")
            self.text_widget.insert(tk.END, msg + "\n")
            self.text_widget.see(tk.END)
            self.text_widget.configure(state="disabled")

        self.text_widget.after(0, append)


class RodomaxHubApp(tk.Tk):
    """Aplicação Desktop para operação do HUB Integrador de Dados."""

    def __init__(self) -> None:
        super().__init__()

        self.title("Rodomax Transportes | HUB Integrador de Dados")
        self.geometry("1020x680")
        self.minsize(850, 550)

        # Configuração do Pipeline
        self.raw_dir = Path("./data_raw").resolve()
        self.lake_dir = Path("./data_lake").resolve()
        self.pipeline = FreightDataPipeline(data_lake_dir=self.lake_dir)
        self.selected_files: List[Path] = []
        self.current_processed_df: Optional[pd.DataFrame] = None

        self._configure_styles()
        self._build_ui()
        self._setup_logging()

    def _configure_styles(self) -> None:
        """Configura a paleta de cores e temas visuais corporativos."""
        self.style = ttk.Style(self)
        self.style.theme_use("clam")

        # Paleta Rodomax: Azul Profundo / Cinza Claro / Branco
        self.primary_color = "#1E3A8A"
        self.accent_color = "#2563EB"
        self.bg_color = "#F8FAFC"

        self.configure(bg=self.bg_color)
        self.style.configure(".", background=self.bg_color, font=("Segoe UI", 10))
        self.style.configure("Header.TLabel", font=("Segoe UI", 15, "bold"), foreground=self.primary_color)
        self.style.configure("SubHeader.TLabel", font=("Segoe UI", 9), foreground="#64748B")
        self.style.configure("Action.TButton", font=("Segoe UI", 10, "bold"), padding=6)
        self.style.configure("Accent.TButton", background=self.accent_color, foreground="white", font=("Segoe UI", 10, "bold"), padding=6)

    def _build_ui(self) -> None:
        """Monta a estrutura visual da janela."""
        # Topo: Cabeçalho
        header_frame = ttk.Frame(self, padding="15 15 15 10")
        header_frame.pack(fill=tk.X)

        title_lbl = ttk.Label(header_frame, text="🚚 HUB Integrador de Dados - Rodomax Transportes", style="Header.TLabel")
        title_lbl.pack(anchor=tk.W)

        subtitle_lbl = ttk.Label(
            header_frame,
            text="Módulo de Ingestão Heterogênea (PDFs, Planilhas CSV e APIs JSON) com Auditoria, Saneamento e Data Lake.",
            style="SubHeader.TLabel",
        )
        subtitle_lbl.pack(anchor=tk.W)

        # Painel de Seleção de Fontes e Controles
        control_frame = ttk.LabelFrame(self, text=" Controle de Entrada de Arquivos ", padding=12)
        control_frame.pack(fill=tk.X, padx=15, pady=5)

        # Linha 1: Caminho selecionado
        path_box = ttk.Frame(control_frame)
        path_box.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(path_box, text="Origem dos PDFs:", font=("Segoe UI", 10, "bold")).pack(side=tk.LEFT, padx=(0, 5))
        self.path_var = tk.StringVar(value=str(self.raw_dir))
        self.path_entry = ttk.Entry(path_box, textvariable=self.path_var)
        self.path_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

        btn_browse_folder = ttk.Button(path_box, text="📂 Escolher Pasta", command=self._select_folder)
        btn_browse_folder.pack(side=tk.LEFT, padx=3)

        btn_browse_files = ttk.Button(path_box, text="📄 Escolher Arquivos", command=self._select_files)
        btn_browse_files.pack(side=tk.LEFT, padx=3)

        # Linha 2: Botões de Ação
        actions_box = ttk.Frame(control_frame)
        actions_box.pack(fill=tk.X)

        self.btn_run = ttk.Button(
            actions_box,
            text="▶️ Executar Pipeline Completo",
            style="Accent.TButton",
            command=self._start_pipeline_thread,
        )
        self.btn_run.pack(side=tk.LEFT, padx=(0, 8))

        btn_mock = ttk.Button(
            actions_box,
            text="🧪 Gerar PDFs de Teste Sintéticos",
            command=self._generate_mock_data,
        )
        btn_mock.pack(side=tk.LEFT, padx=5)

        btn_lake = ttk.Button(
            actions_box,
            text="💾 Abrir Pasta do Data Lake",
            command=self._open_data_lake,
        )
        btn_lake.pack(side=tk.LEFT, padx=5)

        btn_ai = ttk.Button(
            actions_box,
            text="🤖 Estimar Frete (IA)",
            command=self._test_ai_case,
        )
        btn_ai.pack(side=tk.LEFT, padx=5)

        self.progress_bar = ttk.Progressbar(actions_box, mode="indeterminate", length=180)
        self.progress_bar.pack(side=tk.RIGHT, padx=5)

        # Painel Central: Abas (Resultados, Auditoria, Logs)
        notebook_frame = ttk.Frame(self, padding="15 5 15 5")
        notebook_frame.pack(fill=tk.BOTH, expand=True)

        self.notebook = ttk.Notebook(notebook_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # Aba 1: Tabela de Dados Higienizados
        self.tab_table = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_table, text="  📋 Base Higienizada (Data Lake)  ")
        self._build_table_tab()

        # Aba 2: Relatório de Auditoria
        self.tab_audit = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_audit, text="  📊 Auditoria e Inconsistências  ")
        self._build_audit_tab()

        # Aba 3: Logs de Execução
        self.tab_logs = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_logs, text="  📝 Logs em Tempo Real  ")
        self._build_logs_tab()

        # Barra de Status
        self.status_var = tk.StringVar(value="Pronto. Adicione PDFs na pasta ou selecione arquivos para iniciar.")
        status_bar = ttk.Label(self, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W, padding=6)
        status_bar.pack(fill=tk.X, side=tk.BOTTOM)

    def _build_table_tab(self) -> None:
        """Monta o grid/tabela para visualização dos registros limpos."""
        top_bar = ttk.Frame(self.tab_table, padding=5)
        top_bar.pack(fill=tk.X)

        self.lbl_records_count = ttk.Label(top_bar, text="Nenhum dado processado ainda.", font=("Segoe UI", 10, "italic"))
        self.lbl_records_count.pack(side=tk.LEFT)

        columns = ("origem", "destino", "peso_kg", "valor_frete", "data_solicitacao", "formato_origem", "arquivo_origem")
        self.tree = ttk.Treeview(self.tab_table, columns=columns, show="headings", selectmode="browse")

        self.tree.heading("origem", text="Origem")
        self.tree.heading("destino", text="Destino")
        self.tree.heading("peso_kg", text="Peso (kg)")
        self.tree.heading("valor_frete", text="Valor Frete (R$)")
        self.tree.heading("data_solicitacao", text="Data (ISO 8601)")
        self.tree.heading("formato_origem", text="Formato")
        self.tree.heading("arquivo_origem", text="Arquivo de Origem")

        self.tree.column("origem", width=150, anchor=tk.W)
        self.tree.column("destino", width=150, anchor=tk.W)
        self.tree.column("peso_kg", width=100, anchor=tk.E)
        self.tree.column("valor_frete", width=110, anchor=tk.E)
        self.tree.column("data_solicitacao", width=110, anchor=tk.CENTER)
        self.tree.column("formato_origem", width=80, anchor=tk.CENTER)
        self.tree.column("arquivo_origem", width=220, anchor=tk.W)

        scrollbar_y = ttk.Scrollbar(self.tab_table, orient=tk.VERTICAL, command=self.tree.yview)
        scrollbar_x = ttk.Scrollbar(self.tab_table, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=scrollbar_y.set, xscrollcommand=scrollbar_x.set)

        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(5, 0), pady=5)
        scrollbar_y.pack(side=tk.RIGHT, fill=tk.Y, pady=5)
        scrollbar_x.pack(side=tk.BOTTOM, fill=tk.X, padx=5)

    def _build_audit_tab(self) -> None:
        """Monta a aba de auditoria e sumário estatístico."""
        self.txt_audit = scrolledtext.ScrolledText(self.tab_audit, wrap=tk.WORD, font=("Consolas", 10), state="disabled")
        self.txt_audit.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

    def _build_logs_tab(self) -> None:
        """Monta a aba de logs."""
        self.txt_logs = scrolledtext.ScrolledText(self.tab_logs, wrap=tk.WORD, font=("Consolas", 9), state="disabled", bg="#0F172A", fg="#38BDF8")
        self.txt_logs.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

    def _setup_logging(self) -> None:
        """Vincula o logger do Python com a aba de logs da interface."""
        logger = logging.getLogger("RodomaxDataHub")
        handler = TextHandler(self.txt_logs)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
        logger.addHandler(handler)

    def _select_folder(self) -> None:
        """Abre diálogo para seleção de pasta."""
        folder = filedialog.askdirectory(initialdir=str(self.raw_dir), title="Selecione a Pasta com Arquivos PDF")
        if folder:
            self.raw_dir = Path(folder)
            self.selected_files = []
            self.path_var.set(str(self.raw_dir))
            self.status_var.set(f"Pasta selecionada: {self.raw_dir}")

    def _select_files(self) -> None:
        """Abre diálogo para seleção de múltiplos arquivos heterogêneos (PDF, CSV, JSON)."""
        files = filedialog.askopenfilenames(
            initialdir=str(self.raw_dir),
            title="Selecione Arquivos para Ingestão (PDF, CSV, JSON)",
            filetypes=[
                ("Formatos Homologados (PDF, CSV, JSON)", "*.pdf;*.csv;*.json;*.jsonl;*.ndjson"),
                ("Documentos PDF (*.pdf)", "*.pdf"),
                ("Planilhas CSV (*.csv)", "*.csv"),
                ("Arquivos JSON/JSONL (*.json, *.jsonl)", "*.json;*.jsonl;*.ndjson"),
                ("Todos os Arquivos (*.*)", "*.*"),
            ],
        )
        if files:
            self.selected_files = [Path(f) for f in files]
            self.path_var.set(f"{len(self.selected_files)} arquivo(s) selecionado(s)")
            self.status_var.set(f"{len(self.selected_files)} arquivo(s) selecionados manualmente.")

    def _generate_mock_data(self) -> None:
        """Gera os arquivos de teste padrão na pasta raw."""
        try:
            generate_mock_test_environment(self.raw_dir)
            messagebox.showinfo("Sucesso", f"PDFs sintéticos de teste criados em:\n{self.raw_dir}")
            self.path_var.set(str(self.raw_dir))
            self.status_var.set("Ambiente de teste preenchido com arquivos sintéticos.")
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao gerar dados de teste: {e}")

    def _open_data_lake(self) -> None:
        """Abre o diretório do Data Lake no Windows Explorer."""
        if not self.lake_dir.exists():
            self.lake_dir.mkdir(parents=True, exist_ok=True)
        if sys.platform == "win32":
            os.startfile(self.lake_dir)
        else:
            subprocess.Popen(["xdg-open", str(self.lake_dir)])

    def _test_ai_case(self) -> None:
        """Avalia a regressão e estima o frete para um peso informado."""
        try:
            df = self.current_processed_df
            if df is None or df.empty:
                if self.selected_files:
                    df = self.pipeline.process_files(self.selected_files)
                else:
                    df = self.pipeline.process_directory(self.raw_dir)
                self.pipeline.export_to_data_lake(df, base_filename="processed_freights")

            if df.empty:
                messagebox.showwarning(
                    "IA indisponível",
                    "Execute o pipeline ou disponibilize dados válidos antes de testar a IA.",
                )
                return

            peso_kg = simpledialog.askfloat(
                "Nova demanda",
                "Informe o peso da carga em kg:",
                initialvalue=35000,
                minvalue=0.01,
            )
            if peso_kg is None:
                return

            metrics = self.pipeline.evaluate_freight_regression(df)
            estimate = FreightRegressionModel().fit(df).predict(peso_kg)
            range_warning = (
                "\nATENÇÃO: peso fora da faixa do histórico; previsão é extrapolação."
                if estimate["fora_da_faixa_treinada"]
                else ""
            )
            messagebox.showinfo(
                "Estimativa de frete - regressão linear",
                f"Peso informado: {estimate['peso_kg']:,.0f} kg\n"
                f"Frete estimado: R$ {estimate['frete_estimado']:,.2f}\n"
                f"Estimativa por tonelada: R$ {estimate['frete_por_tonelada']:,.2f}\n\n"
                f"Avaliação em dados não usados no treino:\n"
                f"MAE regressão: R$ {metrics['mae_regressao']:,.2f}\n"
                f"MAE linha-base (média): R$ {metrics['mae_linha_base']:,.2f}\n"
                f"Amostras de teste: {metrics['registros_teste']}"
                f"{range_warning}",
            )
            self.status_var.set(
                f"Regressão executada: frete estimado em R$ {estimate['frete_estimado']:,.2f}"
            )
        except Exception as exc:
            messagebox.showerror("Erro no protótipo de IA", str(exc))

    def _start_pipeline_thread(self) -> None:
        """Inicia a execução do pipeline em segundo plano para não travar a GUI."""
        self.btn_run.configure(state="disabled")
        self.progress_bar.start(10)
        self.status_var.set("Processando lote de dados... Por favor aguarde.")

        thread = threading.Thread(target=self._run_pipeline, daemon=True)
        thread.start()

    def _run_pipeline(self) -> None:
        """Método executado na thread de segundo plano."""
        try:
            # 1. Executa o processamento
            if self.selected_files:
                df = self.pipeline.process_files(self.selected_files)
            else:
                df = self.pipeline.process_directory(self.raw_dir)

            self.current_processed_df = df

            # 2. Exporta para o Data Lake
            exports = self.pipeline.export_to_data_lake(df, base_filename="processed_freights")

            # 3. Atualiza a interface na thread principal
            self.after(0, lambda: self._update_ui_results(df, exports))

        except Exception as e:
            self.after(0, lambda: self._handle_error(str(e)))

    def _update_ui_results(self, df: pd.DataFrame, exports: dict) -> None:
        """Atualiza a tabela e a aba de auditoria após conclusão."""
        self.progress_bar.stop()
        self.btn_run.configure(state="normal")

        # Limpa tabela atual
        for item in self.tree.get_children():
            self.tree.delete(item)

        if df.empty:
            self.lbl_records_count.configure(text="Nenhum registro válido extraído.", foreground="red")
            self.status_var.set("Aviso: Nenhum dado válido encontrado para ingestão.")
            return

        # Preenche com dados normalizados
        for _, row in df.iterrows():
            peso_str = f"{row.get('peso_kg', 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            valor_str = f"R$ {row.get('valor_frete', 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if pd.notnull(row.get('valor_frete')) else "-"

            formato_str = str(row.get("formato_origem", "-")).upper()

            self.tree.insert("", tk.END, values=(
                row.get("origem", "-"),
                row.get("destino", "-"),
                peso_str,
                valor_str,
                row.get("data_solicitacao", "-"),
                formato_str,
                row.get("arquivo_origem", "-"),
            ))

        total = len(df)
        self.lbl_records_count.configure(
            text=f"✅ {total} registro(s) higienizados e salvos no Data Lake com sucesso!",
            foreground="#15803D",
        )

        # Atualiza aba de Auditoria com .info() e .describe()
        buf = io.StringIO()
        df.info(buf=buf)
        info_text = buf.getvalue()

        audit_content = "=" * 80 + "\n"
        audit_content += "📊 RELATÓRIO ANALÍTICO DA BASE NO DATA LAKE (PRONTA PARA IA)\n"
        audit_content += "=" * 80 + "\n\n"
        audit_content += f"• Arquivos Gerados:\n"
        for k, p in exports.items():
            audit_content += f"   - {k.upper()}: {p}\n"
        audit_content += f"\n• Total de Registros: {total}\n\n"
        audit_content += "-" * 80 + "\n"
        audit_content += "📋 ESTRUTURA DOS TIPOS DE DADOS (.info()):\n"
        audit_content += "-" * 80 + "\n"
        audit_content += info_text + "\n\n"
        audit_content += "-" * 80 + "\n"
        audit_content += "📈 SUMÁRIO ESTATÍSTICO DAS VARIÁVEIS NUMÉRICAS E TEXTUAIS (.describe()):\n"
        audit_content += "-" * 80 + "\n"
        audit_content += df.describe(include="all").to_string() + "\n"

        self.txt_audit.configure(state="normal")
        self.txt_audit.delete("1.0", tk.END)
        self.txt_audit.insert(tk.END, audit_content)
        self.txt_audit.configure(state="disabled")

        self.status_var.set(f"Concluído: {total} registros persistidos em Parquet (.parquet) e CSV (.csv).")
        self.notebook.select(self.tab_table)

    def _handle_error(self, err_msg: str) -> None:
        """Trata erros disparados na thread."""
        self.progress_bar.stop()
        self.btn_run.configure(state="normal")
        self.status_var.set(f"Erro: {err_msg}")
        messagebox.showerror("Erro de Execução", f"Ocorreu um erro no pipeline:\n{err_msg}")


if __name__ == "__main__":
    app = RodomaxHubApp()
    app.mainloop()
