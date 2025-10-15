#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Chopan Email Agent
------------------
Reads an XLSX spreadsheet of leads and sends respectful, personalized emails in batches via SendGrid.
Uses OpenAI's Responses API (model: gpt-5-nano) to craft short, customized messages from lead data.
"""

import argparse
import csv
import json
import logging
import os
import re
import sqlite3
import sys, io, os 
import time
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd
from openai import OpenAI
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import logging, http.client

http.client.HTTPConnection.debuglevel = 1
logging.basicConfig(level=logging.DEBUG)



EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

DEFAULT_FROM_EMAIL = os.getenv("FROM_EMAIL", "jrapisarda1016@gmail.com")
ORG_NAME = os.getenv("ORGANIZATION_NAME", "Chopan Foundation")
ORG_URL = os.getenv("ORGANIZATION_URL", "https://chopan-foundation-web.lovable.app")
OPT_OUT_LINE = os.getenv("OPT_OUT_LINE", "If this isn’t of interest, I completely understand — reply “unsubscribe” and we won’t contact you again.")
DB_PATH = os.getenv("SENT_DB_PATH", "sent_log.db")
MODEL_NAME = os.getenv("OPENAI_MODEL", "gpt-5-nano")

SYSTEM_STYLE = f"""
You are a careful outreach writer for {ORG_NAME}, a Christian ministry in Pakistan serving vulnerable children, widows, and families.
Write a short, respectful, personalized email to a single recipient.
Tone: warm, dignified, professional, non-pushy. 140–220 words.
Always include a gentle opt-out line at the end: "{OPT_OUT_LINE}"
Use the organization URL: {ORG_URL}.
Return only valid JSON with keys: subject (string), body (string).
"""

USER_TEMPLATE = """
Craft a personalized outreach email using the following lead data.

Lead:
- Name: {name}
- Email: {email}
- Job Title: {job_title}
- Company: {company}
- Short Description (about their org): {description}

Guidelines:
1) Greet them by first name.
2) Reference their role and company explicitly.
3) Bridge to {org_name}'s mission (education, health, and family support in Pakistan) in a way that connects to their work.
4) Propose a light next step (e.g., share a one‑pager, brief call) without pressure.
5) Keep it respectful and concise; avoid generic “form letter” phrasing.
6) Close with gratitude and include the opt‑out line exactly once.

Example style (for reference only; do not copy verbatim):
Hi Kristina,
I was deeply moved to see your role as Hotline Counselor with Postpartum Support International, supporting mothers during some of life’s most delicate seasons. At {org_name} in Pakistan, we also have a heart for vulnerable families, especially mothers, children, and widows, through educational, medical, and community care programs.
I wonder if there might be a way for our organizations to connect—perhaps through resource sharing, joint awareness, or mutual encouragement. Would you be open to me sending a two‑page overview of our work for you to review?
Thank you for the care you extend to families in need. {opt_out}

Return JSON only: {{"subject": "...", "body": "..."}}.
"""

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    os.system("chcp 65001 >nul")   # switch console to UTF-8 code-page

def init_db(db_path: str = DB_PATH):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sent_emails (
            email TEXT PRIMARY KEY,
            name TEXT,
            job_title TEXT,
            company TEXT,
            subject TEXT,
            body TEXT,
            message_id TEXT,
            sent_at TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS generated_emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT,
            name TEXT,
            job_title TEXT,
            company TEXT,
            subject TEXT,
            body TEXT,
            generated_at TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS skips (
            email TEXT,
            reason TEXT,
            at TEXT
        )
    """)
    conn.commit()
    return conn

def already_sent(conn, email: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sent_emails WHERE email = ?", (email.lower().strip(),))
    return cur.fetchone() is not None

def log_skip(conn, email: str, reason: str):
    cur = conn.cursor()
    cur.execute("INSERT INTO skips (email, reason, at) VALUES (?, ?, ?)", (email, reason, datetime.utcnow().isoformat()))
    conn.commit()

def log_sent(conn, payload: Dict[str, Any]):
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO sent_emails (email, name, job_title, company, subject, body, message_id, sent_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        payload["email"].lower().strip(),
        payload.get("name",""),
        payload.get("job_title",""),
        payload.get("company",""),
        payload.get("subject",""),
        payload.get("body",""),
        payload.get("message_id",""),
        datetime.utcnow().isoformat(),
    ))
    conn.commit()

def log_generated(conn, payload: Dict[str, Any]):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO generated_emails (email, name, job_title, company, subject, body, generated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        (payload.get("email", "") or "").lower().strip(),
        payload.get("name", ""),
        payload.get("job_title", ""),
        payload.get("company", ""),
        payload.get("subject", ""),
        payload.get("body", ""),
        datetime.utcnow().isoformat(),
    ))
    conn.commit()

def normalize_email(email: Optional[str]) -> Optional[str]:
    if not email or not isinstance(email, str):
        return None
    e = email.strip().lower()
    return e if EMAIL_REGEX.match(e) else None

def valid_row(row: Dict[str, Any]) -> Tuple[bool, str]:
    name = (row.get("Name") or "").strip()
    email = normalize_email(row.get("Email"))
    job = (row.get("Job Title") or "").strip()
    company = (row.get("Company Name") or "").strip()
    if not name:
        return False, "missing name"
    if not email:
        return False, "invalid or missing email"
    if not job:
        return False, "missing job title"
    if not company:
        return False, "missing company name"
    return True, ""

def first_name(full_name: str) -> str:
    return (full_name or "").strip().split()[0] if full_name else ""

def generate_email(openai_client: OpenAI, lead: Dict[str, Any]) -> Dict[str, str]:
    prompt = USER_TEMPLATE.format(
        name=lead["Name"],
        email=lead["Email"],
        job_title=lead["Job Title"],
        company=lead["Company Name"],
        description=lead.get("Description","") or lead.get("Short Description","") or lead.get("Company Description","") or "",
        org_name=ORG_NAME,
        opt_out=OPT_OUT_LINE,
    )
    resp = openai_client.responses.create(
        model=MODEL_NAME,
        input=[
            {"role": "system", "content": SYSTEM_STYLE},
            {"role": "user", "content": prompt}
        ],
    )
    text = resp.output_text
    try:
        data = json.loads(text)
        subject = (data.get("subject") or "").strip()
        body = (data.get("body") or "").strip()
    except Exception:
        subject = f"Exploring a possible collaboration — {ORG_NAME}"
        body = text.strip()
    if not subject:
        subject = f"Exploring a possible collaboration — {ORG_NAME}"
    if not body:
        body = f"Hello {first_name(lead['Name'])},\n\nI’d like to share a brief overview of {ORG_NAME} ({ORG_URL}). {OPT_OUT_LINE}"
    return {"subject": subject, "body": body}

def send_with_sendgrid(sg: SendGridAPIClient, from_email: str, to_email: str, subject: str, body: str) -> Optional[str]:
    message = Mail(
        from_email=from_email,
        to_emails=to_email,
        subject=subject,
        plain_text_content=body
    )
    if getattr(message, "content", None):
        for part in message.content:
            try:
                part.charset = "utf-8"
            except AttributeError:
                continue
    message.add_headers = {"X-Campaign": "chopan-foundation-outreach"}
    try:
        response = sg.send(message)
        msg_id = None
        if hasattr(response, "headers"):
            # Headers may be CaseInsensitiveDict-like
            try:
                msg_id = response.headers.get("X-Message-Id") or response.headers.get("x-message-id")
            except Exception:
                msg_id = None
        return msg_id
    except Exception as e:
        logging.error("SendGrid error for %s: %s", to_email, e)
        return None

def process_batch(df: pd.DataFrame, start: int, batch_size: int, send: bool, xlsx_path: str) -> Dict[str, Any]:
    openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
    conn = init_db(DB_PATH)
    sent_records: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    batch_df = df.iloc[start:start+batch_size].copy()
    for _, row in batch_df.iterrows():
        row_dict = row.to_dict()
        is_valid, reason = valid_row(row_dict)
        email = normalize_email(row_dict.get("Email")) if row_dict.get("Email") else None
        if not is_valid:
            skipped.append({"email": row_dict.get("Email",""), "reason": reason})
            log_skip(conn, row_dict.get("Email","") or "", reason)
            continue
        assert email is not None
        if already_sent(conn, email):
            skipped.append({"email": email, "reason": "already sent in log"})
            continue
        gen = generate_email(openai_client, row_dict)
        subject = gen["subject"]
        body = gen["body"]
        message_id = ""
        payload = {
            "email": email,
            "name": row_dict.get("Name",""),
            "job_title": row_dict.get("Job Title",""),
            "company": row_dict.get("Company Name",""),
            "subject": subject,
            "body": body,
            "message_id": message_id
        }
        log_generated(conn, payload)
        if send:
            message_id = send_with_sendgrid(sg, DEFAULT_FROM_EMAIL, email, subject, body) or ""
            payload["message_id"] = message_id
            log_sent(conn, payload)
        sent_records.append(payload)
        time.sleep(0.5 if send else 0.0)
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    out_csv = f"batch_preview_{timestamp}.csv"
    out_path = os.path.join(os.path.dirname(xlsx_path) or ".", out_csv)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["email","name","job_title","company","subject","body","message_id"])
        writer.writeheader()
        for r in sent_records:
            writer.writerow(r)
    return {"sent": sent_records, "skipped": skipped, "preview_csv": out_path}

def main():
    parser = argparse.ArgumentParser(description="Chopan Email Agent")
    parser.add_argument("--xlsx", required=True, help="Path to XLSX file with leads")
    parser.add_argument("--sheet", default="Sheet1", help="Sheet name (default: Sheet1)")
    parser.add_argument("--batch-size", type=int, default=10, help="Number of emails per batch (default: 10)")
    parser.add_argument("--start", type=int, default=0, help="Start row index (default: 0)")
    parser.add_argument("--send", action="store_true", help="Actually send emails via SendGrid (default: dry-run)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    logging.info("Loading spreadsheet: %s (sheet=%s)", args.xlsx, args.sheet)
    df = pd.read_excel(args.xlsx, sheet_name=args.sheet)

    rename_map = {}
    cols = [c.strip() for c in df.columns]
    for c in cols:
        lc = c.lower()
        if lc in ("company", "company_name"):
            rename_map[c] = "Company Name"
        elif lc in ("job", "title", "job title"):
            rename_map[c] = "Job Title"
        elif lc in ("e-mail", "mail", "email address"):
            rename_map[c] = "Email"
        elif lc in ("name", "full name"):
            rename_map[c] = "Name"

    if rename_map:
        df = df.rename(columns=rename_map)

    missing_any = [col for col in ["Name","Email","Job Title","Company Name"] if col not in df.columns]
    if missing_any:
        logging.warning("Spreadsheet is missing expected columns: %s", missing_any)

    logging.info("Processing rows %d to %d", args.start, args.start + args.batch_size - 1)
    result = process_batch(df, start=args.start, batch_size=args.batch_size, send=args.send, xlsx_path=args.xlsx)

    logging.info("Done. Sent (or drafted) %d messages; skipped %d", len(result["sent"]), len(result["skipped"]))
    logging.info("Preview CSV: %s", result["preview_csv"])
    print("\n=== SUMMARY ===")
    print(f"Sent/Drafted: {len(result['sent'])}")
    print(f"Skipped: {len(result['skipped'])}")
    if result["skipped"]:
        for s in result["skipped"]:
            print(f" - {s['email']}: {s['reason']}")
    print(f"CSV: {result['preview_csv']}")

if __name__ == "__main__":
    main()
