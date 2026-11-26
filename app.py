import asyncio
from dataclasses import dataclass
import csv
import ctypes
import msvcrt
import os
import queue
import sqlite3
import threading
import time
import tkinter as tk
from contextlib import asynccontextmanager
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from python_socks import ProxyError

from telethon import TelegramClient, errors, functions, types, utils

DATA = Path(os.environ.get("LOCALAPPDATA", Path.home())) / "ClientMailTG"
DATA.mkdir(exist_ok=True)
DB = DATA / "campaigns.sqlite3"
UNKNOWN = "Исход неизвестен"
PENDING = "В очереди"
SENT = "Отправлено"
FAILED = "Ошибка"
DELETED = "Удалено у себя"
DELETE_PENDING = "Ожидает удаления"
DELETE_FAILED = "Ошибка удаления"
SEND_INTERVAL = 3.0
FLOOD_ERRORS = (errors.FloodWaitError, errors.FloodPremiumWaitError)
ACCOUNT_ERRORS = (errors.PeerFloodError, errors.AuthKeyDuplicatedError,
                  errors.UnauthorizedError)


def database():
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    db.executescript("""
        CREATE TABLE IF NOT EXISTS campaign (
            id INTEGER PRIMARY KEY, created TEXT DEFAULT CURRENT_TIMESTAMP,
            body TEXT NOT NULL, delete_copy INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS preference (
            name TEXT PRIMARY KEY, value INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS account_wait (
            account INTEGER PRIMARY KEY, until REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS session_account (
            path TEXT PRIMARY KEY, account INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS delivery (
            campaign INTEGER NOT NULL, recipient INTEGER NOT NULL,
            name TEXT NOT NULL, account INTEGER NOT NULL, sender TEXT NOT NULL,
            session TEXT NOT NULL, access_hash INTEGER NOT NULL,
            status TEXT NOT NULL, cleanup TEXT NOT NULL DEFAULT '',
            message_id INTEGER, detail TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (campaign, recipient)
        );
    """)
    return db


@dataclass(frozen=True)
class ConnectionSettings:
    api_id: int
    api_hash: str
    proxy: dict | None


def socks5_settings(host, port, username, password):
    host = host.strip()
    if (not host or any(char.isspace() for char in host)
            or any(char in host for char in "/@?#")):
        raise ValueError("Укажите IP-адрес или имя хоста SOCKS5.")
    try:
        port = int(port)
        if not 1 <= port <= 65535:
            raise ValueError
    except ValueError:
        raise ValueError("Порт SOCKS5 должен быть числом от 1 до 65535.") from None
    if password and not username:
        raise ValueError("Укажите логин для SOCKS5 с паролем.")
    return dict(proxy_type="socks5", addr=host, port=port, username=username or None,
                password=password or None, rdns=True)


class AccountWaiting(ValueError):
    def __init__(self, seconds):
        super().__init__(f"Telegram запросил паузу. Осталось {seconds} сек.")


def require_ready(db, account):
    row = db.execute("SELECT until FROM account_wait WHERE account = ?", (account,)).fetchone()
    if row:
        remaining = row["until"] - time.time()
        if remaining > 0:
            raise AccountWaiting(int(remaining) + 1)


def save_wait(db, account, exc):
    if account is None:
        return
    with db:
        db.execute("""INSERT INTO account_wait (account, until) VALUES (?, ?)
            ON CONFLICT(account) DO UPDATE SET until = max(until, excluded.until)""",
            (account, time.time() + max(1, exc.seconds)))


@asynccontextmanager
async def connect(path, credentials):
    if not Path(path).is_file():
        raise ValueError("Файл сессии отсутствует")
    path_key = os.path.normcase(str(Path(path).resolve()))
    db = database()
    try:
        saved = db.execute("SELECT account FROM session_account WHERE path = ?", (path_key,)).fetchone()
        account = saved["account"] if saved else None
        if account is not None:
            require_ready(db, account)
        client = TelegramClient(str(path), credentials.api_id, credentials.api_hash,
                                proxy=credentials.proxy, receive_updates=False,
                                connection_retries=0 if credentials.proxy else 5,
                                flood_sleep_threshold=0, request_retries=0,
                                raise_last_call_error=True)
        try:
            await asyncio.wait_for(client.connect(), 30)
            me = await asyncio.wait_for(client.get_me(), 30)
            if me is None:
                raise ValueError("Требуется повторный вход в аккаунт")
            if me.bot:
                raise ValueError("Нужна сессия пользовательского аккаунта")
            account = me.id
            with db:
                db.execute("INSERT OR REPLACE INTO session_account VALUES (?, ?)", (path_key, account))
            require_ready(db, account)
            yield client, me
        except FLOOD_ERRORS as exc:
            save_wait(db, account, exc)
            raise
        finally:
            await client.disconnect()
    finally:
        db.close()

def name(user):
    display = utils.get_display_name(user) or str(user.id)
    return f"{display} (@{user.username})" if user.username else display


def explain(exc):
    if isinstance(exc, FLOOD_ERRORS):
        return f"Telegram ограничил запросы на {exc.seconds} сек."
    if isinstance(exc, errors.PeerFloodError):
        return "Telegram ограничил отправку. Проверьте аккаунт через @SpamBot."
    if isinstance(exc, errors.AuthKeyDuplicatedError):
        return "Telegram аннулировал ключ сессии. Требуется повторный вход."
    if isinstance(exc, TimeoutError):
        return "Истекло время ответа Telegram"
    return f"{type(exc).__name__}: {exc}"


def candidate(user, me, path, incoming):
    if user.id == me.id or user.bot or user.deleted or user.support:
        return None
    try:
        peer = utils.get_input_peer(user, allow_self=False)
    except (TypeError, ValueError):
        return None
    if not isinstance(peer, types.InputPeerUser):
        return None
    return (user.id, name(user), me.id, name(me), str(path), peer.access_hash, incoming)


async def scan(folder, credentials, stopped, emit):
    found = {}
    paths = sorted(folder.glob("*.session"))
    if not paths:
        raise ValueError("В выбранной папке нет файлов .session")
    for path in paths:
        if stopped.is_set():
            break
        label = path.name
        account_name = label
        candidates = set()
        warnings = []
        contact_summary = ""
        phase = "Подключение"
        try:
            async with connect(path, credentials) as (client, me):
                account_name = name(me)
                emit("account", (label, f"{account_name} • Чтение контактов и чатов"))
                phase = "Контакты"
                try:
                    contacts = await asyncio.wait_for(
                        client(functions.contacts.GetContactsRequest(hash=0)), 30)
                except FLOOD_ERRORS + ACCOUNT_ERRORS:
                    raise
                except (OSError, errors.RPCError) as exc:
                    warnings.append(f"{phase}: {explain(exc)}")
                else:
                    if isinstance(contacts, types.contacts.Contacts):
                        entries = {contact.user_id: contact for contact in contacts.contacts}
                        mutual_ids = {contact.user_id for contact in entries.values() if contact.mutual}
                        contact_summary = f"Контактов: {len(entries)}, взаимных: {len(mutual_ids)}"
                        for user in contacts.users:
                            if isinstance(user, types.User) and user.id in mutual_ids:
                                row = candidate(user, me, path, False)
                                if row:
                                    add_candidate(found, row)
                                    candidates.add(user.id)
                    else:
                        warnings.append("Telegram не вернул список контактов")


                phase = "Личные чаты"
                history_errors = 0
                first_history_error = None
                async for dialog in client.iter_dialogs():
                    if stopped.is_set():
                        break
                    if not dialog.is_user or not isinstance(dialog.entity, types.User):
                        continue
                    user = dialog.entity
                    row = candidate(user, me, path, True)
                    if not row:
                        continue
                    incoming = bool(dialog.message and not dialog.message.out
                                    and dialog.message.sender_id == user.id)
                    if not incoming:
                        try:
                            incoming = bool(await asyncio.wait_for(
                                client.get_messages(user, limit=1, from_user=user), 30))
                        except FLOOD_ERRORS + ACCOUNT_ERRORS:
                            raise
                        except (OSError, errors.RPCError, ValueError) as exc:
                            history_errors += 1
                            if first_history_error is None:
                                first_history_error = explain(exc)
                            continue
                    if incoming:
                        add_candidate(found, row)
                        candidates.add(user.id)
                if history_errors:
                    warnings.append(f"Не прочитано чатов: {history_errors}. Первая ошибка: {first_history_error}")
        except Exception as exc:
            warnings.append(f"{phase}: {explain(exc)}")
        state = "Остановлено" if stopped.is_set() else "Найдено"
        report = f"{account_name} • {state}: {len(candidates)}"
        if contact_summary:
            report += " • " + contact_summary
        if warnings:
            report += " • " + " • ".join(warnings)
        emit("account", (label, report))
    emit("candidates", sorted(found.values(), key=lambda row: row[1].casefold()))


async def check_proxy(paths, credentials, stopped, emit):
    successes = 0
    checked = 0
    for path in paths:
        if stopped.is_set():
            break
        checked += 1
        label = path.name
        try:
            async with connect(path, credentials) as (_, me):
                successes += 1
                emit("account", (label, f"{name(me)} • Telegram доступен через SOCKS5"))
        except Exception as exc:
            emit("account", (label, f"Проверка SOCKS5: {explain(exc)}"))
    emit("note", f"Подключение через SOCKS5 подтверждено для {successes} из {checked}. Подробнее на вкладке «Аккаунты».")


def add_candidate(found, row):
    previous = found.get(row[0])
    if previous is None or (row[-1] and not previous[-1]):
        found[row[0]] = row


def update(db, row, **fields):
    assignments = ", ".join(f"{field} = ?" for field in fields)
    with db:
        db.execute(f"UPDATE delivery SET {assignments} WHERE campaign = ? AND recipient = ?",
                   (*fields.values(), row["campaign"], row["recipient"]))


async def clean_copy(db, row, client, peer, message_id):
    try:
        await asyncio.wait_for(client.delete_messages(peer, [message_id], revoke=False), 30)
        remaining = await asyncio.wait_for(client.get_messages(peer, ids=message_id), 30)
        if remaining is not None:
            raise ValueError("Сообщение ещё видно в истории отправителя.")
    except Exception as exc:
        if isinstance(exc, FLOOD_ERRORS):
            save_wait(db, row["account"], exc)
        update(db, row, cleanup=DELETE_FAILED, detail=explain(exc))
        return isinstance(exc, FLOOD_ERRORS + ACCOUNT_ERRORS)
    update(db, row, cleanup=DELETED, detail="")
    return False


async def deliver(db, row, client, campaign, retry_cleanup):
    peer = types.InputPeerUser(row["recipient"], row["access_hash"])
    if retry_cleanup:
        return await clean_copy(db, row, client, peer, row["message_id"])

    # Persist before requesting a send. An interrupted request needs manual reconciliation.
    update(db, row, status=UNKNOWN, detail="Подтверждение отправки не записано")
    try:
        message = await asyncio.wait_for(
            client.send_message(peer, campaign["body"], parse_mode=None, link_preview=False), 45)
        if not message or not isinstance(message.id, int) or message.id <= 0:
            raise ValueError("Telegram не вернул ID сообщения")
    except FLOOD_ERRORS as exc:
        with db:
            save_wait(db, row["account"], exc)
            update(db, row, status=PENDING, detail=explain(exc))
        return True
    except (errors.BadRequestError, errors.ForbiddenError) + ACCOUNT_ERRORS as exc:
        update(db, row, status=FAILED, detail=explain(exc))
        return isinstance(exc, ACCOUNT_ERRORS)
    except Exception as exc:
        update(db, row, detail=explain(exc))
        return True

    update(db, row, status=SENT, message_id=message.id, detail="",
           cleanup=DELETE_PENDING if campaign["delete_copy"] else "")
    if campaign["delete_copy"]:
        return await clean_copy(db, row, client, peer, message.id)
    return False


async def run_campaign(campaign_id, credentials, stopped, emit, retry_cleanup):
    db = database()
    try:
        campaign = db.execute("SELECT * FROM campaign WHERE id = ?", (campaign_id,)).fetchone()
        if retry_cleanup:
            rows = db.execute("""SELECT * FROM delivery
                WHERE campaign = ? AND status = ? AND cleanup IN (?, ?)
                ORDER BY session, recipient""",
                (campaign_id, SENT, DELETE_PENDING, DELETE_FAILED)).fetchall()
        else:
            rows = db.execute("""SELECT * FROM delivery WHERE campaign = ? AND status = ?
                ORDER BY session, recipient""", (campaign_id, PENDING)).fetchall()
        accounts = {}
        for row in rows:
            accounts.setdefault(row["session"], []).append(row)
        next_send = 0.0
        for path, batch in accounts.items():
            if stopped.is_set():
                break
            try:
                require_ready(db, batch[0]["account"])
                async with connect(path, credentials) as (client, me):
                    if me.id != batch[0]["account"]:
                        raise ValueError("В файле сессии изменился аккаунт")
                    if credentials.proxy:
                        emit("account", (Path(path).name, f"{name(me)} • Telegram доступен через SOCKS5"))
                    for row in batch:
                        if stopped.is_set():
                            break
                        if not retry_cleanup:
                            remaining = next_send - time.monotonic()
                            if remaining > 0 and await asyncio.to_thread(stopped.wait, remaining):
                                break
                        halt = await deliver(db, row, client, campaign, retry_cleanup)
                        next_send = time.monotonic() + SEND_INTERVAL
                        emit("refresh", campaign_id)
                        if halt:
                            emit("note", "Обработка приостановлена. Подробности в отчёте.")
                            return
            except (OSError, ValueError, ProxyError, errors.RPCError) as exc:
                if isinstance(exc, FLOOD_ERRORS):
                    save_wait(db, batch[0]["account"], exc)
                reason = explain(exc)
                transport = " через SOCKS5" if credentials.proxy else ""
                emit("account", (Path(path).name, reason))
                emit("note", f"Проверка аккаунта {batch[0]['sender']}{transport}: {reason}. Очередь приостановлена.")
                return
    finally:
        db.close()


def configure_theme(root):
    root.configure(background="#0f1722")
    style = ttk.Style(root)
    style.theme_use("clam")
    ink, muted, white = "#e5edf6", "#9eb2c4", "#17232f"
    blue, line, tint = "#76c3f5", "#34495d", "#22364a"
    style.configure(".", font=("Segoe UI", 10), foreground=ink, background=white,
                    bordercolor=line, lightcolor=line, darkcolor=line,
                    focuscolor="#6eb9ee")
    style.configure("TLabel", background=white, foreground=ink)
    style.configure("Hint.TLabel", foreground=muted)
    style.configure("Section.TLabel", font=("Segoe UI Semibold", 11))
    style.configure("Count.TLabel", background=tint, foreground=blue,
                    font=("Segoe UI Semibold", 10), padding=(10, 5))
    style.configure("Status.TLabel", background="#0f1722", foreground=muted)
    style.configure("TButton", background=tint, foreground=blue, padding=(10, 5),
                    font=("Segoe UI Semibold", 10))
    style.map("TButton", background=[("disabled", "#202c39"), ("pressed", "#34536e"),
                                     ("active", "#2a445c")],
              foreground=[("disabled", "#96a6b7")])
    primary = "#20577e"
    style.configure("Primary.TButton", background=primary, foreground="#f0f8ff",
                    bordercolor="#437da6", lightcolor="#437da6", darkcolor="#437da6")
    style.map("Primary.TButton", background=[("disabled", "#202c39"),
                ("pressed", "#1b5077"), ("active", "#246493")])
    style.configure("Stop.TButton", foreground="#f2aab2", background="#402a34")
    style.map("Stop.TButton", background=[("disabled", "#202c39"), ("active", "#53323d")])
    root.option_add("*TCombobox*Listbox.background", "#17232f")
    root.option_add("*TCombobox*Listbox.foreground", ink)
    root.option_add("*TCombobox*Listbox.selectBackground", "#2b4d69")
    root.option_add("*TCombobox*Listbox.selectForeground", "#f0f8ff")
    style.configure("TCheckbutton", indicatorbackground="#101b27", indicatorforeground=blue)
    style.map("TCheckbutton", indicatorbackground=[("selected", "#253d53")])
    style.configure("TEntry", fieldbackground="#101b27", padding=4, insertcolor=ink)
    style.configure("TCombobox", fieldbackground="#101b27", padding=4, arrowsize=13)
    style.map("TCombobox", fieldbackground=[("readonly", "#101b27")])
    style.configure("TLabelframe.Label", foreground=muted, font=("Segoe UI Semibold", 9))
    style.layout("Pages.TNotebook.Tab", [])
    style.configure("Pages.TNotebook", background="#0f1722")
    style.configure("Nav.TButton", background="#202e3d", foreground=muted)
    style.configure("Active.Nav.TButton", background="#2b4d69", foreground="#f0f8ff")
    style.map("Active.Nav.TButton", background=[("active", "#34536e")])
    style.configure("Treeview", rowheight=30, fieldbackground=white, background=white)
    style.configure("Treeview.Heading", background="#233343", foreground=ink,
                    font=("Segoe UI Semibold", 10), padding=(7, 6))
    style.map("Treeview.Heading", background=[("active", "#233343")])
    style.map("Treeview", background=[("selected", "#2b4d69")],
              foreground=[("selected", "#f0f8ff")])
    for orientation in ("Horizontal", "Vertical"):
        scrollbar = f"{orientation}.TScrollbar"
        style.configure(scrollbar, background="#4b6175", troughcolor="#15212d",
                        arrowcolor="#a7bdd0", bordercolor="#15212d",
                        lightcolor="#4b6175", darkcolor="#4b6175", arrowsize=11)
        style.map(scrollbar, background=[("active", "#5d7890")])




def dark_title_bar(root):
    hwnd = int(root.wm_frame(), 16)
    color = ctypes.c_int(1)
    set_attribute = ctypes.windll.dwmapi.DwmSetWindowAttribute
    set_attribute.argtypes = (ctypes.c_void_p, ctypes.c_uint, ctypes.c_void_p, ctypes.c_uint)
    for attribute in (20, 19):
        if set_attribute(hwnd, attribute, ctypes.byref(color), ctypes.sizeof(color)) == 0:
            break



def paste_shortcut(event):
    # Windows reports the same virtual key for V under Cyrillic and Latin layouts.
    if event.keycode == 0x56 or event.keysym.lower() == "v":
        event.widget.event_generate("<<Paste>>")
        return "break"


def bind_paste_shortcut(parent):
    for widget in parent.winfo_children():
        if isinstance(widget, (ttk.Entry, tk.Text)):
            widget.bind("<Control-KeyPress>", paste_shortcut, add="+")
        bind_paste_shortcut(widget)




class App:
    def __init__(self, root):
        self.root = root
        self.db = database()
        self.events = queue.Queue()
        self.stopped = threading.Event()
        self.worker = None
        self.rows = {}
        self.account_rows = {}
        self.campaign_id = None
        self.closed = False

        root.title("TgMailing")
        root.geometry("1120x790")
        root.minsize(900, 700)
        configure_theme(root)
        root.bind("<Map>", lambda event: dark_title_bar(root) if event.widget is root else None, add="+")

        top = ttk.Frame(root, padding=(14, 9))
        top.pack(fill="x", padx=16, pady=(12, 10))
        top.columnconfigure(1, weight=1)
        top.columnconfigure((2, 3), uniform="connection_actions")
        self.folder = tk.StringVar()
        ttk.Label(top, text="Папка с .session").grid(row=0, column=0, sticky="w", padx=(0, 8))
        ttk.Entry(top, textvariable=self.folder).grid(row=0, column=1, sticky="ew")
        ttk.Button(top, text="Выбрать папку", command=self.choose_folder, width=17).grid(row=0, column=2, padx=6, sticky="ew")

        self.api_id = tk.StringVar()
        self.api_hash = tk.StringVar()
        keys = ttk.Frame(top)
        keys.grid(row=1, column=1, sticky="w", pady=6)
        ttk.Label(top, text="Приложение Telegram").grid(row=1, column=0, sticky="w", padx=(0, 8))
        ttk.Label(keys, text="API ID").pack(side="left")
        ttk.Entry(keys, textvariable=self.api_id, width=12).pack(side="left", padx=6)
        ttk.Label(keys, text="API Hash").pack(side="left")
        ttk.Entry(keys, textvariable=self.api_hash, width=36, show="•").pack(side="left", padx=6)
        self.actions = []
        self.button(top, "Сканировать", self.start_scan, style="Primary.TButton", width=17).grid(row=1, column=2, padx=6, sticky="ew")
        self.stop_button = ttk.Button(top, text="Остановить", command=self.stopped.set, state="disabled", style="Stop.TButton", width=17)
        self.stop_button.grid(row=1, column=3, padx=6, sticky="ew")

        proxy_box = ttk.LabelFrame(top, text="Подключение через прокси", padding=6)
        proxy_box.grid(row=2, column=0, columnspan=4, sticky="ew", pady=(4, 0))
        self.proxy_enabled = tk.BooleanVar()
        ttk.Checkbutton(proxy_box, text="SOCKS5", variable=self.proxy_enabled).pack(side="left", padx=(0, 8))
        self.proxy_host = tk.StringVar()
        self.proxy_port = tk.StringVar()
        self.proxy_login = tk.StringVar()
        self.proxy_password = tk.StringVar()
        for label, variable, width, mask in (
            ("Хост", self.proxy_host, 19, ""),
            ("Порт", self.proxy_port, 6, ""),
            ("Логин", self.proxy_login, 13, ""),
            ("Пароль", self.proxy_password, 13, "•"),
        ):
            ttk.Label(proxy_box, text=label).pack(side="left")
            ttk.Entry(proxy_box, textvariable=variable, width=width, show=mask).pack(side="left", padx=(4, 9))
        self.button(proxy_box, "Проверить прокси", self.start_proxy_check, width=17).pack(side="right")

        navigation = tk.Frame(root, bg="#0f1722")
        navigation.pack(fill="x", padx=16, pady=(0, 4))
        self.tabs = ttk.Notebook(root, style="Pages.TNotebook")
        self.tabs.pack(fill="both", expand=True, padx=16)
        recipients = ttk.Frame(self.tabs, padding=12)
        report = ttk.Frame(self.tabs, padding=12)
        accounts = ttk.Frame(self.tabs, padding=12)
        self.navigation = {}
        for page, title in ((recipients, "Рассылка"), (report, "Отчёт"), (accounts, "Аккаунты")):
            self.tabs.add(page, text=title)
            button = ttk.Button(navigation, text=title, width=14, style="Nav.TButton",
                                command=lambda selected=page: self.tabs.select(selected))
            button.pack(side="left", padx=(0, 5))
            self.navigation[str(page)] = button
        self.tabs.bind("<<NotebookTabChanged>>", self.highlight_navigation)
        self.highlight_navigation()
        self.report_tab = report
        self.accounts_tab = accounts

        recipient_header = ttk.Frame(recipients)
        recipient_header.pack(fill="x")
        ttk.Label(recipient_header, text="Получатели", style="Section.TLabel").pack(side="left")
        self.selection_count = tk.StringVar(value="Выбрано 0")
        ttk.Label(recipient_header, textvariable=self.selection_count, style="Count.TLabel").pack(side="right")
        ttk.Label(recipients, text="Выберите клиентов, которые согласились получать сообщения.",
                  style="Hint.TLabel").pack(anchor="w", pady=(0, 3))
        composer = ttk.Frame(recipients)
        composer.pack(side="bottom", fill="x")
        choices = ttk.Frame(recipients)
        choices.pack(side="bottom", fill="x", pady=(5, 0))
        self.recipients = self.table(recipients, ("Клиент", "Рабочий аккаунт", "Источник"), (330, 310, 180), 11, True)
        ttk.Button(choices, text="Выбрать всех", command=self.select_all).pack(side="left")
        ttk.Button(choices, text="Снять выбор", command=lambda: self.recipients.selection_set([])).pack(side="left", padx=6)
        self.recipients.bind("<<TreeviewSelect>>", self.show_selection)

        composer_header = ttk.Frame(composer)
        composer_header.pack(fill="x", pady=(8, 3))
        ttk.Label(composer_header, text="Текст сообщения", style="Section.TLabel").pack(side="left")
        ttk.Button(composer_header, text="Вставить", command=self.paste_body).pack(side="right")
        self.body = tk.Text(composer, height=4, wrap="word", font=("Segoe UI", 11),
                            bg="#101b27", fg="#e5edf6", insertbackground="#e5edf6",
                            selectbackground="#2b4d69", selectforeground="#f0f8ff",
                            padx=10, pady=7, bd=0, highlightthickness=1,
                            highlightbackground="#34495d", highlightcolor="#6eb9ee")
        self.body.pack(fill="x")
        body_menu = tk.Menu(self.body, tearoff=False, bg="#17232f", fg="#e5edf6",
                            activebackground="#2b4d69", activeforeground="#f0f8ff")
        body_menu.add_command(label="Вставить", command=self.paste_body)
        self.body.bind("<Button-3>", lambda event: body_menu.post(event.x_root, event.y_root))
        bottom = ttk.Frame(composer)
        bottom.pack(fill="x", pady=(8, 0))
        preference = self.db.execute("SELECT value FROM preference WHERE name = 'delete_copy'").fetchone()
        self.delete_copy = tk.BooleanVar(value=bool(preference["value"]) if preference else True)
        ttk.Checkbutton(bottom, text="Удалять копии у отправителей после отправки",
                        variable=self.delete_copy, command=self.save_delete_preference).pack(side="left")
        self.button(bottom, "Запустить рассылку", self.start_campaign, style="Primary.TButton").pack(side="right")

        report_top = ttk.Frame(report)
        report_top.pack(fill="x", pady=(0, 6))
        self.history = ttk.Combobox(report_top, state="readonly", width=27)
        self.history.pack(side="left")
        self.history.bind("<<ComboboxSelected>>", self.select_campaign)
        self.button(report_top, "Продолжить очередь", lambda: self.resume(False)).pack(side="left", padx=5)
        self.button(report_top, "Повторить удаление", lambda: self.resume(True)).pack(side="left", padx=5)
        ttk.Button(report_top, text="Экспорт CSV", command=self.export).pack(side="right")
        ttk.Label(report, text="Сводка по аккаунтам", style="Section.TLabel").pack(anchor="w")
        self.summary = self.table(report, ("Аккаунт", "Отправлено", "Ошибки", "В очереди",
                                          "Неизвестно", "Удалено", "Ош. удаления"),
                                  (235, 100, 75, 85, 95, 75, 105), 5, False)
        ttk.Label(report, text="Результаты по получателям", style="Section.TLabel").pack(anchor="w", pady=(5, 0))
        self.detail = self.table(report, ("Клиент", "Аккаунт", "Отправка", "Удаление", "Подробности"),
                                 (170, 170, 150, 150, 360), 10, True)
        self.accounts = self.table(accounts, ("Аккаунт / сессия", "Состояние"), (390, 520), 16, True)
        self.status = tk.StringVar(value="Выберите папку и укажите API ID и API Hash.")
        ttk.Label(root, textvariable=self.status, padding=(17, 8), style="Status.TLabel", wraplength=870).pack(side="bottom", fill="x", before=self.tabs)
        root.protocol("WM_DELETE_WINDOW", self.close)
        root.after(120, self.poll)
        self.load_history()
        bind_paste_shortcut(root)

    def paste_body(self):
        self.body.focus_set()
        self.body.event_generate("<<Paste>>")

    def save_delete_preference(self):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO preference VALUES ('delete_copy', ?)",
                            (self.delete_copy.get(),))

    def highlight_navigation(self, event=None):
        active = self.tabs.select()
        for page, button in self.navigation.items():
            button.configure(style="Active.Nav.TButton" if page == active else "Nav.TButton")

    def button(self, parent, label, command, style="TButton", width=None):
        widget = ttk.Button(parent, text=label, command=command, style=style, width=width)
        self.actions.append(widget)
        return widget

    @staticmethod
    def table(parent, columns, widths, height, expand):
        frame = ttk.Frame(parent)
        frame.pack(fill="both", expand=expand, pady=4)
        tree = ttk.Treeview(frame, columns=columns, show="headings", height=height)
        for column, width in zip(columns, widths):
            tree.heading(column, text=column, anchor="w")
            tree.column(column, width=width, minwidth=55)
        scroll = ttk.Scrollbar(frame, command=tree.yview)
        horizontal = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=scroll.set, xscrollcommand=horizontal.set)
        horizontal.pack(side="bottom", fill="x")
        scroll.pack(side="right", fill="y")
        tree.pack(side="left", fill="both", expand=True)
        return tree

    def credentials(self):
        try:
            api_id = int(self.api_id.get())
            api_hash = self.api_hash.get().strip()
            if not 0 < api_id < 2**31 or len(api_hash) != 32:
                raise ValueError
            int(api_hash, 16)
        except ValueError:
            messagebox.showerror("Данные Telegram API", "Укажите API ID и API Hash с my.telegram.org.")
            return None
        proxy = None
        if self.proxy_enabled.get():
            try:
                proxy = socks5_settings(self.proxy_host.get(), self.proxy_port.get(),
                                        self.proxy_login.get(), self.proxy_password.get())
            except ValueError as exc:
                messagebox.showerror("SOCKS5", str(exc))
                return None
        return ConnectionSettings(api_id, api_hash, proxy)

    def start_proxy_check(self):
        if not self.proxy_enabled.get():
            messagebox.showerror("SOCKS5", "Включите SOCKS5 и заполните параметры прокси.")
            return
        credentials = self.credentials()
        if not credentials:
            return
        folder = self.folder.get().strip()
        paths = sorted(Path(folder).resolve().glob("*.session")) if folder else []
        if not paths:
            messagebox.showerror("Проверка прокси", "Выберите папку с файлами .session.")
            return
        self.tabs.select(self.accounts_tab)
        self.start(check_proxy(paths, credentials, self.stopped, self.emit), "Проверка подключения к Telegram через SOCKS5")

    def choose_folder(self):
        folder = filedialog.askdirectory()
        if folder:
            self.folder.set(folder)

    def start_scan(self):
        credentials = self.credentials()
        raw_folder = self.folder.get().strip()
        if not credentials:
            return
        if not raw_folder or not Path(raw_folder).is_dir():
            messagebox.showerror("Папка сессий", "Выберите существующую папку с .session.")
            return
        self.rows.clear()
        self.account_rows.clear()
        self.recipients.delete(*self.recipients.get_children())
        self.accounts.delete(*self.accounts.get_children())
        self.start(scan(Path(raw_folder).resolve(), credentials, self.stopped, self.emit), "Сканирование аккаунтов")

    def select_all(self):
        self.recipients.selection_set(self.recipients.get_children())

    def show_selection(self, event):
        self.selection_count.set(f"Выбрано {len(self.recipients.selection())}")

    def start_campaign(self):
        credentials = self.credentials()
        if not credentials:
            return
        body = self.body.get("1.0", "end-1c")
        selected = [self.rows[key] for key in self.recipients.selection()]
        if not selected or not body.strip() or len(body.encode("utf-16-le")) // 2 > 4096:
            messagebox.showerror("Рассылка", "Выберите клиентов и введите текст до 4096 единиц UTF-16.")
            return
        cleanup = self.delete_copy.get()
        action = "Сообщения будут удалены только у отправителей." if cleanup else "Копии будут сохранены в чатах."
        preview = body if len(body) <= 320 else body[:320] + "…"
        accounts = len({row[2] for row in selected})
        if not messagebox.askyesno("Подтверждение отправки",
                                   f"Отправить клиентам ({len(selected)}) с аккаунтов ({accounts})?\n"
                                   f"{action}\n\n{preview}"):
            return
        with self.db:
            cursor = self.db.execute("INSERT INTO campaign (body, delete_copy) VALUES (?, ?)", (body, cleanup))
            campaign_id = cursor.lastrowid
            self.db.executemany("""INSERT INTO delivery
                (campaign, recipient, name, account, sender, session, access_hash, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [(campaign_id, *row[:-1], PENDING) for row in selected])
        self.load_history(campaign_id)
        self.tabs.select(self.report_tab)
        self.start(run_campaign(campaign_id, credentials, self.stopped, self.emit, False), "Отправка")

    def resume(self, cleanup):
        if self.campaign_id is None:
            return
        credentials = self.credentials()
        if credentials:
            action = "Повторить удаление копий у отправителей?" if cleanup else "Продолжить отправку сообщений из очереди?"
            if messagebox.askyesno("Подтверждение", action):
                self.start(run_campaign(self.campaign_id, credentials, self.stopped, self.emit, cleanup), "Обработка отчёта")

    def start(self, coroutine, status):
        self.stopped.clear()
        self.status.set(status)
        self.stop_button.configure(state="normal")
        for button in self.actions:
            button.configure(state="disabled")

        def work():
            try:
                asyncio.run(coroutine)
            except Exception as exc:
                self.emit("note", explain(exc))
            finally:
                self.emit("done", None)

        self.worker = threading.Thread(target=work)
        self.worker.start()

    def emit(self, kind, data):
        self.events.put((kind, data))

    def poll(self):
        refreshed = False
        while True:
            try:
                kind, data = self.events.get_nowait()
            except queue.Empty:
                break
            if kind == "candidates":
                for row in data:
                    key = str(row[0])
                    self.rows[key] = row
                    source = "Входящее сообщение" if row[-1] else "Взаимный контакт"
                    self.recipients.insert("", "end", iid=key, values=(row[1], row[3], source))
                if data:
                    self.status.set(f"Найдено кандидатов: {len(data)}. Результаты по сессиям на вкладке «Аккаунты».")
                else:
                    self.tabs.select(self.accounts_tab)
                    self.status.set("Кандидаты не найдены. Проверьте результаты по сессиям на вкладке «Аккаунты».")
            elif kind == "account":
                key = self.account_rows.get(data[0])
                if key is None:
                    self.account_rows[data[0]] = self.accounts.insert("", "end", values=data)
                else:
                    self.accounts.item(key, values=data)
            elif kind == "refresh":
                refreshed |= data == self.campaign_id
            elif kind == "note":
                self.status.set(data)
            elif kind == "done":
                if self.closed:
                    self.db.close()
                    self.root.destroy()
                    return
                if self.stopped.is_set():
                    self.status.set("Остановлено. Очередь можно продолжить из отчёта.")
                elif self.status.get() in ("Отправка", "Обработка отчёта"):
                    self.status.set("Обработка завершена. Итоги в отчёте.")
                for button in self.actions:
                    button.configure(state="normal")
                self.stop_button.configure(state="disabled")
        if refreshed:
            self.refresh()
        self.root.after(120, self.poll)

    def load_history(self, selected=None):
        rows = self.db.execute("""SELECT id, datetime(created, 'localtime') AS local_time
                                  FROM campaign ORDER BY id DESC""").fetchall()
        self.campaign_ids = [row["id"] for row in rows]
        self.history["values"] = [f"№ {row['id']} • {row['local_time']}" for row in rows]
        if rows:
            self.history.current(self.campaign_ids.index(selected) if selected else 0)
            self.select_campaign()

    def select_campaign(self, event=None):
        index = self.history.current()
        if index >= 0:
            self.campaign_id = self.campaign_ids[index]
            self.refresh()

    def deliveries(self):
        return self.db.execute("""SELECT * FROM delivery WHERE campaign = ?
                                  ORDER BY sender, name, recipient""", (self.campaign_id,)).fetchall()

    def refresh(self):
        self.summary.delete(*self.summary.get_children())
        self.detail.delete(*self.detail.get_children())
        totals = {}
        for row in self.deliveries():
            total = totals.setdefault((row["account"], row["sender"]), [0] * 6)
            for index, status in enumerate((SENT, FAILED, PENDING, UNKNOWN)):
                total[index] += row["status"] == status
            total[4] += row["cleanup"] == DELETED
            total[5] += row["cleanup"] == DELETE_FAILED
            self.detail.insert("", "end", values=(row["name"], row["sender"], row["status"],
                                                  row["cleanup"] or "—", row["detail"]))
        for (_, sender), total in totals.items():
            self.summary.insert("", "end", values=(sender, *total))

    def export(self):
        if self.campaign_id is None:
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".csv", initialfile=f"campaign-{self.campaign_id}.csv",
            filetypes=[("CSV", "*.csv")])
        if not path:
            return
        try:
            with open(path, "w", newline="", encoding="utf-8-sig") as stream:
                writer = csv.writer(stream, delimiter=";")
                writer.writerow(("ID аккаунта", "Аккаунт", "ID клиента", "Клиент",
                                 "Отправка", "Удаление", "ID сообщения", "Подробности"))
                for row in self.deliveries():
                    fields = [row[key] for key in ("account", "sender", "recipient", "name",
                                                   "status", "cleanup", "message_id", "detail")]
                    writer.writerow([csv_value(value) for value in fields])
        except OSError as exc:
            messagebox.showerror("Экспорт", str(exc))

    def close(self):
        if self.worker and self.worker.is_alive() and self.stop_button["state"] == "normal":
            self.closed = True
            self.stopped.set()
            self.status.set("Завершаем текущий запрос перед закрытием…")
            return
        self.db.close()
        self.root.destroy()


def csv_value(value):
    if isinstance(value, str) and value.lstrip().startswith(("=", "+", "-", "@")):
        return "'" + value
    return value


if __name__ == "__main__":
    root = tk.Tk()
    with (DATA / "app.lock").open("a+b") as lock:
        lock.seek(0)
        try:
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            root.withdraw()
            messagebox.showerror("TgMailing", "Программа уже запущена.")
            root.destroy()
        else:
            App(root)
            root.mainloop()
