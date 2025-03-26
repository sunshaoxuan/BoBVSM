import sys
import platform
import os
import logging
import asyncio
import time
import threading
import datetime
import uuid
import json
import sqlite3
from aiosmtpd.controller import Controller
from logging.handlers import TimedRotatingFileHandler
from dotenv import load_dotenv
from flask import Flask, render_template_string, redirect, url_for, send_from_directory

from email.parser import Parser
from email import policy

# ----------------------------------------------------------------
# 1) Windowsの場合、stdoutをUTF-8エンコーディングにリセットし、エラー時に置換
# ----------------------------------------------------------------
if platform.system() == "Windows":
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# .envファイルから環境変数を読み込む
load_dotenv()

# 設定パラメータ
SMTP_SERVER = os.getenv("SMTP_SERVER", "0.0.0.0")
SMTP_PORT = int(os.getenv("SMTP_PORT", 25))
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "noreply@example.com")
LOG_DIR = os.getenv("LOG_DIR", "logs")
DB_FILE = os.getenv("DB_FILE", "emails.db")
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", 7))

# ログディレクトリが存在しない場合は作成
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# ログシステムの設定、ログファイルは日次でローテーション、エンコーディングはutf-8で文字化けを防止
logger = logging.getLogger()
logger.setLevel(logging.INFO)

log_file = os.path.join(LOG_DIR, "smtp_server.log")
file_handler = TimedRotatingFileHandler(
    log_file, when="midnight", interval=1, backupCount=30, encoding='utf-8'
)
file_handler.suffix = "%Y-%m-%d"
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# コンソールログハンドラー
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

logger.info("SMTPサーバーを初期化中...")
logger.info("設定：SMTP_SERVER=%s, SMTP_PORT=%s, SENDER_EMAIL=%s", SMTP_SERVER, SMTP_PORT, SENDER_EMAIL)
logger.info("永続化設定：DB_FILE=%s, 保持日数=%d", DB_FILE, RETENTION_DAYS)

# ----------------------------------------------------------------
# URL转换功能
# ----------------------------------------------------------------
def convert_urls_to_links(text):
    """将文本中的URL、IP地址和带端口的地址转换为可点击的链接"""
    import re
    
    # 首先识别并保护编程语言的命名空间和特殊URL
    protected_patterns = [
        # 编程语言命名空间
        r'(?:[a-zA-Z_][a-zA-Z0-9_]*\.)+[a-zA-Z_][a-zA-Z0-9_]*\([^)]*\)',  # 函数调用
        r'(?:[a-zA-Z_][a-zA-Z0-9_]*\.)+(?:module|class|interface|enum)\b',  # 模块/类/接口定义
        r'(?:[a-zA-Z_][a-zA-Z0-9_]*\.)+[A-Z][a-zA-Z0-9_]*(?!\.[0-9])',    # 类引用
        
        # 特殊URL模式（如密码重置链接）
        r'(?:パスワード再設定|password\s+reset).*?URL[：:]\s*\n.*?(?=\n|$)',  # 密码重置URL整行
        r'target=.*?(?:\n|$)',                                              # target参数行
    ]
    
    # 保护文本，将匹配项临时替换为占位符
    protected_texts = {}
    counter = 0
    
    def protect_match(match):
        nonlocal counter
        placeholder = f"__PROTECTED_{counter}__"
        protected_texts[placeholder] = match.group(0)
        counter += 1
        return placeholder
    
    # 保护所有匹配到的文本
    result = text
    for pattern in protected_patterns:
        result = re.sub(pattern, protect_match, result, flags=re.MULTILINE)
    
    # 简化的URL匹配模式
    url_pattern = r'((?:https?|ftp)://[^\s<>"\']+|(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?(?:/[^\s<>"\']*)?|(?:www\.)?[a-zA-Z0-9][a-zA-Z0-9-]*\.[a-zA-Z0-9-]+\.[a-zA-Z]{2,}(?::\d+)?(?:/[^\s<>"\']*)?)'
    
    def replace_with_link(match):
        url = match.group(1)
        if not url.startswith(('http://', 'https://', 'ftp://')):
            if url.startswith('www.'):
                url = 'http://' + url
            elif re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', url):
                url = 'http://' + url
        return f'<a href="{url}" target="_blank">{match.group(1)}</a>'
    
    # 转换URL为链接
    result = re.sub(url_pattern, replace_with_link, result, flags=re.IGNORECASE)
    
    # 恢复被保护的文本
    for placeholder, original in protected_texts.items():
        result = result.replace(placeholder, original)
    
    return result

def clean_content(text):
    """清理文本内容，去除多余的空行和空格"""
    if not text:
        return ""
    
    # 将内容按行分割，并移除每行首尾的空白
    lines = [line.rstrip() for line in text.splitlines()]
    
    # 找到第一个非空行
    start = 0
    while start < len(lines) and not lines[start]:
        start += 1
    
    # 找到最后一个非空行
    end = len(lines) - 1
    while end >= 0 and not lines[end]:
        end -= 1
    
    # 如果全是空行，返回空字符串
    if start > end:
        return ""
    
    # 提取有效内容行
    content_lines = lines[start:end + 1]
    
    # 合并行，移除连续的空行
    result = []
    prev_empty = False
    for line in content_lines:
        if line or not prev_empty:  # 如果当前行非空，或者前一行不是空行
            result.append(line)
        prev_empty = not line
    
    # 将URL转换为链接
    return convert_urls_to_links("\n".join(result))

# ----------------------------------------------------------------
# データベース関連の操作
# ----------------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS emails (
            id TEXT PRIMARY KEY,
            time TEXT,
            subject TEXT,
            sender TEXT,
            recipients TEXT,
            client_ip TEXT,
            client_app TEXT,
            body TEXT,
            html_body TEXT,
            attachments TEXT
        )
    """)
    conn.commit()
    conn.close()

def add_email_to_db(email_data):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        INSERT INTO emails (id, time, subject, sender, recipients, client_ip, client_app, body, html_body, attachments)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        email_data["id"],
        email_data["time"],
        email_data["subject"],
        email_data["sender"],
        json.dumps(email_data["to"]),
        email_data["client_ip"],
        email_data["client_app"],
        email_data["body"],
        email_data.get("html_body", ""),
        json.dumps(email_data.get("attachments", []))
    ))
    conn.commit()
    conn.close()

def load_emails_from_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id, time, subject, sender, recipients, client_ip, client_app, body, html_body, attachments FROM emails ORDER BY time DESC")
    rows = c.fetchall()
    conn.close()
    emails = []
    for row in rows:
        attachments = []
        if row[9]:
            try:
                attachments = json.loads(row[9])
            except Exception as e:
                attachments = []
        # 在加载时应用URL转换
        body = convert_urls_to_links(row[7]) if row[7] else ""
        html_body = row[8] if row[8] else ""
        emails.append({
            "id": row[0],
            "time": row[1],
            "subject": row[2],
            "sender": row[3],
            "to": json.loads(row[4]),
            "client_ip": row[5],
            "client_app": row[6],
            "body": body,
            "html_body": html_body,
            "attachments": attachments
        })
    return emails

def delete_email_from_db(email_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM emails WHERE id=?", (email_id,))
    conn.commit()
    conn.close()

def clear_emails_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM emails")
    conn.commit()
    conn.close()

def cleanup_emails_db():
    # 閾値時間より古いメールを削除
    threshold = (datetime.datetime.now() - datetime.timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM emails WHERE time < ?", (threshold,))
    conn.commit()
    conn.close()
    # メモリデータを更新
    global received_emails
    received_emails = load_emails_from_db()
    logger.info("%s より古いメールを削除しました", threshold)

def run_cleanup():
    # 1時間ごとにクリーンアップタスクを実行
    while True:
        time.sleep(3600)
        cleanup_emails_db()

# ----------------------------------------------------------------
# グローバル変数とWebサービス
# ----------------------------------------------------------------
init_db()
received_emails = load_emails_from_db()

app = Flask(__name__)

# Bootstrap + DataTables + Google Fonts (Roboto)を使用してページを美化
HTML_TEMPLATE = """
<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <title>メールテストサービス</title>
  <!-- Google Fonts -->
  <link href="https://fonts.googleapis.com/css2?family=Roboto&display=swap" rel="stylesheet">
  <!-- Bootstrap CSS -->
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css">
  <!-- DataTables CSS -->
  <link rel="stylesheet" href="https://cdn.datatables.net/1.13.4/css/jquery.dataTables.min.css">
  <style>
    body { 
      font-family: 'Roboto', sans-serif;
      font-size: 9pt;
    }
    pre { 
      white-space: pre-wrap; 
      word-wrap: break-word;
      font-size: 9pt;
      max-width: none;
      margin: 0;
      padding: 0;
      line-height: 1.2;  /* 减小行高 */
      display: block;  /* 块级显示 */
    }
    .container { 
      margin-top: 20px;
      min-height: calc(100vh - 40px);
      display: flex;
      flex-direction: column;
      max-width: 1536px;  /* 1920px的80% */
      width: 80%;  /* 在较小屏幕上使用80%宽度 */
      padding: 0 15px;
    }
    .content-wrapper {
      flex: 1;
      display: flex;
      flex-direction: column;
      width: 100%;
    }
    .table-container {
      flex: 1;
      margin-top: 20px;
      max-width: 100%;
      overflow-x: auto;  /* 如果内容过宽，显示横向滚动条 */
    }
    .table {
      width: 100%;
      table-layout: fixed;  /* 固定表格布局 */
    }
    tfoot input { 
      width: 100%; 
      box-sizing: border-box;
      font-size: 9pt;
    }
    .attachment-links a { margin-right: 5px; }
    th { 
      white-space: nowrap;
      font-size: 9pt;
    }
    td {
      font-size: 9pt;
      max-width: none;
      white-space: pre-wrap;
      word-wrap: break-word;
      padding: 4px 8px;
      vertical-align: middle;
      line-height: 1.2;  /* 减小行高 */
    }
    td > pre {
      display: inline;  /* pre在td内使用内联显示 */
    }
    td > div {
      margin: 0;
      padding: 0;
    }
    td pre + button,
    td pre + div {
      margin-top: 8px;
    }
    .attachment-links {
      margin-top: 8px;
    }
    .attachment-links a { 
      margin-right: 5px;
      margin-bottom: 0;
    }
    h1 {
      font-size: 16pt;
      margin: 0;
      padding-top: 3px;
    }
    h5 {
      font-size: 11pt;
    }
    select {
      font-size: 9pt;
    }
    label {
      font-size: 9pt;
    }
    .dataTables_wrapper .dataTables_length select {
      font-size: 9pt;
      height: auto;
      padding: 1px;
    }
    .dataTables_wrapper .dataTables_filter input {
      font-size: 9pt;
      height: auto;
      padding: 1px;
    }
    .dataTables_info {
      font-size: 9pt;
    }
    .dataTables_paginate {
      font-size: 9pt;
    }
    .server-info {
      position: absolute;
      top: 0;
      right: 0;
      background: #f8f9fa;
      color: #212529;
      padding: 10px;
      border-radius: 5px;
      font-size: 9pt;
      z-index: 1000;
      border: 1px solid #dee2e6;
      box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .server-info p {
      margin: 0;
      padding: 2px 0;
    }
    .server-info .title {
      font-weight: bold;
      border-bottom: 1px solid #dee2e6;
      margin-bottom: 5px;
      padding-bottom: 5px;
      color: #495057;
    }
    .server-info a {
      color: #0d6efd;  /* Bootstrap 主链接颜色 */
      text-decoration: none;
    }
    .server-info a:hover {
      text-decoration: underline;
    }
    .header-container {
      position: relative;
      margin-bottom: 10px;
    }
    /* 调整各列的宽度 */
    .table th:nth-child(1) { width: 12%; }  /* 时间列 */
    .table th:nth-child(2) { width: 15%; }  /* 件名列 */
    .table th:nth-child(3) { width: 12%; }  /* 送信者列 */
    .table th:nth-child(4) { width: 12%; }  /* 受信者列 */
    .table th:nth-child(5) { width: 12%; }  /* クライアントIP列 */
    .table th:nth-child(6) { width: 12%; }  /* メールクライアント列 */
    .table th:nth-child(7) { width: 20%; }  /* 本文/添付ファイル列 */
    .table th:nth-child(8) { width: 5%; }   /* 操作列 */
  </style>
</head>
<body>
  <div class="container">
    <div class="content-wrapper">
      <div class="header-container">
        <h1>受信したメール</h1>
        <div class="server-info">
          <p class="title">サーバー情報</p>
          <p>SMTP: {{ smtp_server }}:{{ smtp_port }}</p>
          <p>Web: <a href="http://{{ web_server }}:{{ web_port }}">http://{{ web_server }}:{{ web_port }}</a></p>
        </div>
      </div>
      <div class="mb-3">
        <a href="{{ url_for('refresh_emails') }}" class="btn btn-info">手動更新</a>
        <a href="{{ url_for('clear_emails') }}" class="btn btn-warning" onclick="return confirm('すべてのメールを削除してもよろしいですか？');">すべてのメールを削除</a>
      </div>
      <div class="table-container">
        <table id="emailTable" class="table table-striped table-bordered">
          <thead class="table-dark">
            <tr>
              <th>時間</th>
              <th>件名</th>
              <th>送信者</th>
              <th>受信者</th>
              <th>クライアントIP</th>
              <th>メールクライアント</th>
              <th>本文/添付ファイル</th>
              <th>操作</th>
            </tr>
          </thead>
          <tfoot>
            <tr>
              <th>時間</th>
              <th>件名</th>
              <th>送信者</th>
              <th>受信者</th>
              <th>クライアントIP</th>
              <th>メールクライアント</th>
              <th>本文/添付ファイル</th>
              <th>操作</th>
            </tr>
          </tfoot>
          <tbody>
            {% for email in emails %}
            <tr>
              <td>{{ email.time }}</td>
              <td>{{ email.subject }}</td>
              <td>{{ email.sender }}</td>
              <td>{{ email.to|join(', ') }}</td>
              <td>{{ email.client_ip }}</td>
              <td>{{ email.client_app }}</td>
              <td>
                <div>
                  <pre>{{ email.body|safe }}</pre>
                  {% if email.html_body %}
                    <button class="btn btn-sm btn-primary" onclick="openPreview('{{ email.id }}')">HTMLプレビュー</button>
                    <div id="preview-{{ email.id }}" style="display:none;">{{ email.html_body|safe }}</div>
                  {% endif %}
                  {% if email.attachments and email.attachments|length > 0 %}
                    <div class="attachment-links mt-2">
                      {% for att in email.attachments %}
                        <a href="{{ url_for('download_attachment', filename=att['saved_name']) }}" class="btn btn-sm btn-secondary" download>{{ att['filename'] }}</a>
                      {% endfor %}
                    </div>
                  {% endif %}
                </div>
              </td>
              <td>
                <a href="{{ url_for('delete_email', email_id=email.id) }}" class="btn btn-danger btn-sm" onclick="return confirm('このメールを削除してもよろしいですか？');">削除</a>
              </td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- Modal for HTML preview -->
  <div class="modal fade" id="htmlPreviewModal" tabindex="-1" aria-labelledby="htmlPreviewModalLabel" aria-hidden="true">
    <div class="modal-dialog modal-lg">
      <div class="modal-content">
        <div class="modal-header">
          <h5 class="modal-title" id="htmlPreviewModalLabel">HTMLメールプレビュー</h5>
          <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="閉じる"></button>
        </div>
        <div class="modal-body" id="htmlPreviewContent">
        </div>
      </div>
    </div>
  </div>

  <!-- jQuery -->
  <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
  <!-- Bootstrap Bundle JS (includes Popper) -->
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
  <!-- DataTables JS -->
  <script src="https://cdn.datatables.net/1.13.4/js/jquery.dataTables.min.js"></script>
  <script>
    $(document).ready(function() {
      // DataTablesの日本語化
      var table = $('#emailTable').DataTable({
        language: {
          url: '//cdn.datatables.net/plug-ins/1.13.4/i18n/ja.json',
          search: "検索:",
          lengthMenu: "表示件数: _MENU_",
          info: "_TOTAL_件中 _START_件から_END_件を表示",
          infoEmpty: "データがありません",
          infoFiltered: "（全_MAX_件より抽出）",
          zeroRecords: "データがありません",
          paginate: {
            first: "先頭",
            previous: "前へ",
            next: "次へ",
            last: "最終"
          }
        },
        order: [[0, 'desc']],
        pageLength: 10,
        lengthMenu: [[10, 25, 50, 100], [10, 25, 50, 100]],
        dom: "<'row'<'col-sm-6'l><'col-sm-6'f>>" +
             "<'row'<'col-sm-12'tr>>" +
             "<'row'<'col-sm-5'i><'col-sm-7'p>>"
      });

      // フッターの各列にフィルター入力欄を追加
      $('#emailTable tfoot th').each(function(i) {
        if (i === 7) {
          $(this).html('');
        } else {
          var title = $(this).text();
          $(this).html('<input type="text" class="form-control form-control-sm" placeholder="' + title + 'でフィルター" />');
        }
      });

      // フィルター機能の実装
      table.columns().every(function() {
        var that = this;
        $('input', this.footer()).on('keyup change clear', function() {
          if (that.search() !== this.value) {
            that.search(this.value).draw();
          }
        });
      });
    });

    function openPreview(emailId) {
      var previewDiv = document.getElementById('preview-' + emailId);
      if (previewDiv) {
        var htmlContent = previewDiv.innerHTML;
        document.getElementById('htmlPreviewContent').innerHTML = htmlContent;
        var modal = new bootstrap.Modal(document.getElementById('htmlPreviewModal'));
        modal.show();
      }
    }
  </script>
</body>
</html>
"""

@app.route("/")
def index():
    web_host = "localhost" if SMTP_SERVER == "0.0.0.0" else SMTP_SERVER
    return render_template_string(HTML_TEMPLATE, 
        emails=received_emails,
        smtp_server=SMTP_SERVER,
        smtp_port=SMTP_PORT,
        web_server=web_host,
        web_port=5000
    )

@app.route("/delete/<email_id>")
def delete_email(email_id):
    delete_email_from_db(email_id)
    global received_emails
    received_emails = load_emails_from_db()
    return redirect(url_for('index'))

@app.route("/clear")
def clear_emails():
    clear_emails_db()
    global received_emails
    received_emails.clear()
    return redirect(url_for('index'))

# 新規：手動更新ルート
@app.route("/refresh")
def refresh_emails():
    global received_emails
    received_emails = load_emails_from_db()
    return redirect(url_for('index'))

# 新規：添付ファイルダウンロードルート
@app.route("/download/<filename>")
def download_attachment(filename):
    return send_from_directory("attachments", filename, as_attachment=True)

def run_flask():
    app.run(host="0.0.0.0", port=5000, debug=False)

# ----------------------------------------------------------------
# SMTPサーバー処理（メール解析時にテキスト、HTML、添付ファイルを同時に抽出）
# ----------------------------------------------------------------
class CustomHandler:
    async def handle_DATA(self, server, session, envelope):
        logger.info("メールを受信：")
        logger.info("  送信者: %s", envelope.mail_from)
        logger.info("  受信者: %s", envelope.rcpt_tos)

        # クライアントIPとポートを取得
        client_ip, client_port = session.peer
        logger.info("  クライアント接続 IP: %s, ポート: %s", client_ip, client_port)

        # メール内容を解析
        raw_message = envelope.content.decode('utf-8', errors='replace')
        parsed_msg = Parser(policy=policy.default).parsestr(raw_message)
        subject = parsed_msg.get('Subject', '')
        user_agent = parsed_msg.get("User-Agent", "")
        x_mailer = parsed_msg.get("X-Mailer", "")
        client_app = user_agent if user_agent else x_mailer

        plain_body = ""
        html_body = ""
        attachments = []
        attach_dir = "attachments"
        if not os.path.exists(attach_dir):
            os.makedirs(attach_dir)

        if parsed_msg.is_multipart():
            for part in parsed_msg.walk():
                if part.get_content_maintype() == "multipart":
                    continue
                content_disposition = part.get("Content-Disposition", "")
                if content_disposition and "attachment" in content_disposition.lower():
                    filename = part.get_filename()
                    if not filename:
                        filename = "attachment"
                    saved_name = str(uuid.uuid4()) + "_" + filename
                    file_path = os.path.join(attach_dir, saved_name)
                    with open(file_path, "wb") as f:
                        f.write(part.get_payload(decode=True))
                    attachments.append({"filename": filename, "saved_name": saved_name})
                elif part.get_content_type() == "text/plain" and not plain_body:
                    plain_body = clean_content(part.get_content())
                elif part.get_content_type() == "text/html" and not html_body:
                    html_body = part.get_content()
        else:
            if parsed_msg.get_content_type() == "text/html":
                html_body = parsed_msg.get_content()
            else:
                plain_body = clean_content(parsed_msg.get_content())

        logger.info("  解析後の件名: %s", subject)
        logger.info("  解析されたメールクライアント: %s", client_app if client_app else "なし")
        logger.info("  解析後の本文:\n%s", plain_body)

        # メールデータ辞書を構築（時間は比較用にISO形式で保存）
        email_data = {
            "id": str(uuid.uuid4()),
            "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "subject": subject,
            "sender": envelope.mail_from,
            "to": envelope.rcpt_tos,
            "client_ip": client_ip,
            "client_app": client_app,
            "body": plain_body,
            "html_body": html_body,
            "attachments": attachments
        }
        # データベースに永続化し、メモリにも追加
        add_email_to_db(email_data)
        received_emails.append(email_data)
        return '250 Message accepted for delivery'

if __name__ == '__main__':
    # SMTPサーバーを起動
    handler_instance = CustomHandler()
    controller = Controller(handler_instance, hostname=SMTP_SERVER, port=SMTP_PORT)
    controller.start()
    logger.info("SMTPサーバーを起動しました。待ち受けアドレス：%s:%s", SMTP_SERVER, SMTP_PORT)
    
    # Flask Webサービススレッドを起動
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Webサービスを起動しました。アクセスアドレス: http://localhost:5000")
    
    # 定時クリーンアップスレッドを起動
    cleanup_thread = threading.Thread(target=run_cleanup, daemon=True)
    cleanup_thread.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("中断を検出しました。サーバーを終了中...")
        controller.stop()
