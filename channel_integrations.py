"""Ingestao de pedidos recebidos por e-mail (IMAP) e WhatsApp Cloud API."""

from __future__ import annotations

import argparse
import email
import email.policy
import hashlib
import hmac
import json
import logging
import os
import re
import sqlite3
import tempfile
import time
import unicodedata
from datetime import datetime
from email.message import Message
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlsplit

import pandas as pd

from pipeline_pdf_fretes import DataNormalizer


APP_DIR = Path(__file__).resolve().parent
DEFAULT_DATA_LAKE_DIR = APP_DIR / "data_lake"
MAX_WEBHOOK_BODY_BYTES = 1_048_576
logger = logging.getLogger("RodomaxChannelIntegrations")

FIELD_ALIASES = {
    "origem": "origem",
    "localdecoleta": "origem",
    "coleta": "origem",
    "destino": "destino",
    "localdeentrega": "destino",
    "entrega": "destino",
    "peso": "peso",
    "pesodacarga": "peso",
    "valorfrete": "valor_frete",
    "valordofrete": "valor_frete",
    "frete": "valor_frete",
    "data": "data",
    "datasolicitacao": "data",
    "datadasolicitacao": "data",
}


def _normalize_label(value: Any) -> str:
    without_accents = unicodedata.normalize("NFKD", str(value))
    ascii_text = "".join(char for char in without_accents if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]", "", ascii_text.lower())


def _assign_fields(source: Dict[str, Any], result: Dict[str, Any]) -> None:
    for key, value in source.items():
        canonical = FIELD_ALIASES.get(_normalize_label(key))
        if canonical and not isinstance(value, (dict, list)):
            result[canonical] = str(value).strip() if value is not None else ""


def parse_order_message(text: str) -> Dict[str, str]:
    """Le campos de um pedido em texto identificado ou em um objeto JSON."""
    if not isinstance(text, str) or not text.strip():
        raise ValueError("A mensagem do pedido esta vazia.")

    content = text.strip()
    fields: Dict[str, str] = {}
    if content.startswith("{"):
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as exc:
            raise ValueError("O JSON do pedido e invalido.") from exc
        if not isinstance(parsed, dict):
            raise ValueError("O JSON do pedido deve ser um objeto.")
        _assign_fields(parsed, fields)
        for value in parsed.values():
            if isinstance(value, dict):
                _assign_fields(value, fields)
    else:
        for line in content.splitlines():
            match = re.match(r"^\s*([^:\n]{1,80})\s*:\s*(.*?)\s*$", line)
            if match:
                canonical = FIELD_ALIASES.get(_normalize_label(match.group(1)))
                if canonical:
                    fields[canonical] = match.group(2).strip()

    missing = [field for field in ("origem", "destino", "peso") if not fields.get(field)]
    if missing:
        raise ValueError(
            "Pedido incompleto. Campos obrigatorios ausentes: " + ", ".join(missing)
        )
    if DataNormalizer.normalize_weight_kg(fields["peso"]) is None:
        raise ValueError("Peso invalido. Informe um valor numerico com unidade reconhecivel.")
    if fields.get("valor_frete") and DataNormalizer.normalize_currency(fields["valor_frete"]) is None:
        raise ValueError("Valor do frete invalido.")
    if fields.get("data") and DataNormalizer.normalize_date(fields["data"]) is None:
        raise ValueError("Data da solicitacao invalida.")
    return fields


def _export_channel_orders(records: List[Dict[str, Any]], data_lake_dir: Path) -> None:
    """Atualiza a fotografia dos pedidos de canais sem sobrescrever o lote principal."""
    raw_df = pd.DataFrame(records)
    normalized_df = DataNormalizer.normalize(raw_df)
    data_lake_dir.mkdir(parents=True, exist_ok=True)

    temporary_paths: List[Path] = []
    try:
        parquet_handle, parquet_name = tempfile.mkstemp(
            prefix=".channel_orders_", suffix=".parquet", dir=str(data_lake_dir)
        )
        os.close(parquet_handle)
        parquet_temp = Path(parquet_name)
        temporary_paths.append(parquet_temp)
        normalized_df.to_parquet(
            parquet_temp, engine="pyarrow", compression="snappy", index=False
        )

        csv_handle, csv_name = tempfile.mkstemp(
            prefix=".channel_orders_", suffix=".csv", dir=str(data_lake_dir)
        )
        os.close(csv_handle)
        csv_temp = Path(csv_name)
        temporary_paths.append(csv_temp)
        normalized_df.to_csv(csv_temp, sep=";", index=False, encoding="utf-8-sig")

        os.replace(parquet_temp, data_lake_dir / "channel_orders.parquet")
        os.replace(csv_temp, data_lake_dir / "channel_orders.csv")
    finally:
        for temporary_path in temporary_paths:
            if temporary_path.exists():
                temporary_path.unlink()


def persist_order(
    message_id: str,
    source: str,
    order: Dict[str, str],
    data_lake_dir: Path = DEFAULT_DATA_LAKE_DIR,
) -> bool:
    """Persiste o pedido de forma idempotente e regenera os arquivos do canal."""
    if source not in ("email", "whatsapp"):
        raise ValueError(f"Canal de origem nao suportado: {source}")
    if not message_id:
        raise ValueError("A mensagem precisa ter um identificador unico.")

    data_lake_dir = Path(data_lake_dir)
    data_lake_dir.mkdir(parents=True, exist_ok=True)
    database_path = data_lake_dir / "channel_ingestion.sqlite3"
    connection = sqlite3.connect(database_path, timeout=30)
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS channel_orders (
                message_id TEXT PRIMARY KEY,
                origem TEXT NOT NULL,
                destino TEXT NOT NULL,
                peso TEXT NOT NULL,
                valor_frete TEXT,
                data TEXT,
                formato_origem TEXT NOT NULL,
                arquivo_origem TEXT NOT NULL,
                data_ingestao TEXT NOT NULL
            )
            """
        )
        connection.execute("BEGIN IMMEDIATE")
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO channel_orders
                (message_id, origem, destino, peso, valor_frete, data,
                 formato_origem, arquivo_origem, data_ingestao)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message_id,
                order["origem"],
                order["destino"],
                order["peso"],
                order.get("valor_frete"),
                order.get("data"),
                source,
                source,
                datetime.now().astimezone().isoformat(),
            ),
        )
        records = [
            dict(zip(
                ("origem", "destino", "peso", "valor_frete", "data",
                 "formato_origem", "arquivo_origem", "data_ingestao"),
                row,
            ))
            for row in connection.execute(
                """
                SELECT origem, destino, peso, valor_frete, data,
                       formato_origem, arquivo_origem, data_ingestao
                FROM channel_orders ORDER BY data_ingestao, message_id
                """
            )
        ]
        _export_channel_orders(records, data_lake_dir)
        connection.commit()
        return cursor.rowcount == 1
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def extract_whatsapp_text_messages(payload: Dict[str, Any]) -> List[Dict[str, str]]:
    """Extrai mensagens de texto do formato de webhook da WhatsApp Cloud API."""
    messages: List[Dict[str, str]] = []
    entries = payload.get("entry", [])
    if not isinstance(entries, list):
        return messages
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        changes = entry.get("changes", [])
        if not isinstance(changes, list):
            continue
        for change in changes:
            if not isinstance(change, dict):
                continue
            value = change.get("value", {})
            if not isinstance(value, dict):
                continue
            incoming_messages = value.get("messages", [])
            if not isinstance(incoming_messages, list):
                continue
            for message in incoming_messages:
                if not isinstance(message, dict):
                    continue
                text = message.get("text", {})
                if not isinstance(text, dict) or not isinstance(text.get("body"), str):
                    continue
                message_id = message.get("id")
                if not message_id:
                    raise ValueError("Mensagem de texto do WhatsApp sem identificador.")
                messages.append({"id": str(message_id), "body": text["body"]})
    return messages


def make_whatsapp_handler(
    verify_token: str,
    app_secret: str,
    data_lake_dir: Path,
) -> type[BaseHTTPRequestHandler]:
    class WhatsAppWebhookHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            query = parse_qs(urlsplit(self.path).query)
            mode = query.get("hub.mode", [""])[0]
            token = query.get("hub.verify_token", [""])[0]
            challenge = query.get("hub.challenge", [""])[0]
            if mode == "subscribe" and hmac.compare_digest(token, verify_token):
                self._respond(200, challenge.encode("utf-8"), "text/plain")
            else:
                self._respond(403, b"Forbidden", "text/plain")

        def do_POST(self) -> None:
            try:
                content_length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self._respond(400, b"Invalid Content-Length", "text/plain")
                return
            if content_length <= 0 or content_length > MAX_WEBHOOK_BODY_BYTES:
                self._respond(413, b"Invalid request size", "text/plain")
                return

            body = self.rfile.read(content_length)
            signature = self.headers.get("X-Hub-Signature-256", "")
            expected = "sha256=" + hmac.new(
                app_secret.encode("utf-8"), body, hashlib.sha256
            ).hexdigest()
            if not hmac.compare_digest(signature, expected):
                self._respond(401, b"Invalid signature", "text/plain")
                return

            try:
                payload = json.loads(body)
                if not isinstance(payload, dict):
                    raise ValueError("Webhook precisa conter um objeto JSON.")
                messages = extract_whatsapp_text_messages(payload)
            except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
                logger.warning("Webhook do WhatsApp rejeitado: %s", exc)
                self._respond(400, b"Invalid payload", "text/plain")
                return

            try:
                for message in messages:
                    try:
                        order = parse_order_message(message["body"])
                    except ValueError as exc:
                        logger.warning("Pedido do WhatsApp nao importado: %s", exc)
                        continue
                    persist_order(
                        f"whatsapp:{message['id']}", "whatsapp", order, data_lake_dir
                    )
            except Exception:
                logger.exception("Falha ao persistir mensagem recebida pelo WhatsApp.")
                self._respond(500, b"Temporary processing error", "text/plain")
                return

            self._respond(200, b"OK", "text/plain")

        def _respond(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format_string: str, *args: Any) -> None:
            logger.info("Webhook HTTP %s %s", self.command, urlsplit(self.path).path)

    return WhatsAppWebhookHandler


def _plain_text_body(message: Message) -> str:
    parts: List[str] = []
    candidates = message.walk() if message.is_multipart() else [message]
    for part in candidates:
        if part.get_content_type() != "text/plain" or part.get_content_disposition() == "attachment":
            continue
        try:
            content = part.get_content()
        except (LookupError, UnicodeDecodeError):
            payload = part.get_payload(decode=True) or b""
            content = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
        if isinstance(content, str):
            parts.append(content)
    return "\n".join(parts)


def run_email_poller(data_lake_dir: Path, poll_seconds: int) -> None:
    import imaplib

    host = os.environ["MAIL_IMAP_HOST"]
    username = os.environ["MAIL_USERNAME"]
    password = os.environ["MAIL_PASSWORD"]
    port = int(os.environ.get("MAIL_IMAP_PORT", "993"))
    folder = os.environ.get("MAIL_FOLDER", "INBOX")

    logger.info("Leitura IMAP iniciada; intervalo de verificacao: %s s.", poll_seconds)
    while True:
        try:
            with imaplib.IMAP4_SSL(host, port, timeout=30) as mailbox:
                mailbox.login(username, password)
                status, _ = mailbox.select(folder)
                if status != "OK":
                    raise RuntimeError(f"Nao foi possivel abrir a pasta IMAP '{folder}'.")
                status, data = mailbox.uid("SEARCH", None, "UNSEEN")
                if status != "OK":
                    raise RuntimeError("Nao foi possivel buscar mensagens nao lidas.")
                uidvalidity_data = mailbox.response("UIDVALIDITY")[1]
                uidvalidity = (
                    uidvalidity_data[0].decode("ascii", errors="replace")
                    if uidvalidity_data and uidvalidity_data[0]
                    else "unknown"
                )
                uids = data[0].split() if data and data[0] else []

                for uid in uids:
                    status, fetched = mailbox.uid("FETCH", uid, "(RFC822)")
                    if status != "OK":
                        logger.error("Nao foi possivel ler mensagem IMAP UID %s.", uid.decode())
                        continue
                    raw_message = next(
                        (item[1] for item in fetched if isinstance(item, tuple)), None
                    )
                    if raw_message is None:
                        logger.error("Mensagem IMAP UID %s sem conteudo.", uid.decode())
                        continue

                    message = email.message_from_bytes(raw_message, policy=email.policy.default)
                    text = _plain_text_body(message)
                    try:
                        order = parse_order_message(text)
                    except ValueError as exc:
                        logger.warning(
                            "E-mail UID %s nao importado: %s", uid.decode(), exc
                        )
                        continue

                    message_id = message.get("Message-ID")
                    if message_id:
                        unique_id = f"email:{message_id.strip()}"
                    else:
                        unique_id = f"email:{folder}:{uidvalidity}:{uid.decode('ascii')}"
                    persist_order(unique_id, "email", order, data_lake_dir)
                    mailbox.uid("STORE", uid, "+FLAGS", "(\\Seen)")
        except Exception:
            logger.exception("Falha na leitura IMAP; uma nova tentativa sera feita.")
        time.sleep(poll_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="channel", required=True)
    subparsers.add_parser("email", help="Ler pedidos de uma caixa IMAP")
    subparsers.add_parser("whatsapp", help="Receber webhooks da WhatsApp Cloud API")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    data_lake_dir = Path(os.environ.get("SYNC_DATA_LAKE_DIR", str(DEFAULT_DATA_LAKE_DIR)))

    if args.channel == "email":
        required = ("MAIL_IMAP_HOST", "MAIL_USERNAME", "MAIL_PASSWORD")
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            parser.error("Configure as variaveis de ambiente: " + ", ".join(missing))
        poll_seconds = int(os.environ.get("MAIL_POLL_SECONDS", "60"))
        if poll_seconds < 5:
            parser.error("MAIL_POLL_SECONDS deve ser no minimo 5.")
        run_email_poller(data_lake_dir, poll_seconds)
    else:
        verify_token = os.environ.get("WHATSAPP_VERIFY_TOKEN", "")
        app_secret = os.environ.get("WHATSAPP_APP_SECRET", "")
        if not verify_token or not app_secret:
            parser.error("Configure WHATSAPP_VERIFY_TOKEN e WHATSAPP_APP_SECRET.")
        port = int(os.environ.get("WHATSAPP_WEBHOOK_PORT", "8080"))
        handler = make_whatsapp_handler(verify_token, app_secret, data_lake_dir)
        server = HTTPServer(("0.0.0.0", port), handler)
        server.daemon_threads = True
        logger.info("Webhook WhatsApp ouvindo na porta %s.", port)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            logger.info("Encerrando webhook WhatsApp.")
        finally:
            server.server_close()


if __name__ == "__main__":
    main()
