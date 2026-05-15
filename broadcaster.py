"""
Telegram Group Broadcaster
- Читает группы + шаблоны из Google Sheets (листы "Группы" и "Шаблоны")
- Отправляет случайный шаблон в каждую группу
- Записывает статус, ссылку на пост, считает разницу постов
- Anti-flood: случайные задержки, обработка FloodWait
"""

import asyncio
import random
import logging
import os
import json
import base64
import re
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError, ChatWriteForbiddenError, UserBannedInChannelError,
    ChannelPrivateError, PeerFloodError, SlowModeWaitError
)
from telethon.tl.functions.messages import ImportChatInviteRequest

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("broadcaster.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ── Config from environment ───────────────────────────────────────────────────
API_ID            = int(os.environ["TG_API_ID"])
API_HASH          = os.environ["TG_API_HASH"]
SESSION_STRING    = os.environ["TG_SESSION_STRING"]
SHEET_ID          = os.environ["GOOGLE_SHEET_ID"]

DELAY_MIN         = int(os.environ.get("DELAY_MIN", "45"))
DELAY_MAX         = int(os.environ.get("DELAY_MAX", "120"))
BATCH_SIZE        = int(os.environ.get("BATCH_SIZE", "10"))
BATCH_PAUSE_MIN   = int(os.environ.get("BATCH_PAUSE_MIN", "300"))
BATCH_PAUSE_MAX   = int(os.environ.get("BATCH_PAUSE_MAX", "600"))

# ── Колонки листа "Группы" (1-based для gspread) ─────────────────────────────
# A=1  Ссылка на группу
# B=2  Ключ шаблона
# C=3  Время последней публикации
# D=4  Активность (TRUE/FALSE)
# E=5  Статус отправки
# F=6  Ссылка на пост
# G=7  Ссылка на новый пост
# H=8  Постов между

COL_URL           = 1
COL_TEMPLATE_KEY  = 2
COL_LAST_SENT     = 3
COL_ACTIVE        = 4
COL_STATUS        = 5
COL_POST_LINK     = 6
COL_POST_NEW      = 7
COL_POSTS_BETWEEN = 8

# ── Helpers ───────────────────────────────────────────────────────────────────

def extract_post_id(url: str) -> int | None:
    """Извлекает числовой ID поста из ссылки вида https://t.me/channel/12345"""
    if not url:
        return None
    m = re.search(r"/(\d+)\s*$", url.strip())
    return int(m.group(1)) if m else None


def calc_posts_between(old_url: str, new_url: str) -> str:
    """Считает разницу между ID постов. Возвращает строку или ''."""
    old_id = extract_post_id(old_url)
    new_id = extract_post_id(new_url)
    if old_id is not None and new_id is not None:
        return str(abs(new_id - old_id))
    return ""

# ── Google Sheets ─────────────────────────────────────────────────────────────

def get_sheet_data():
    """
    Возвращает (groups, templates, groups_ws).

    groups — список dict с полями:
        row, username, template_key, last_sent, enabled,
        status, post_link, post_new, posts_between

    templates — dict {key: text}
    """
    creds_info = json.loads(
        base64.b64decode(os.environ["GOOGLE_CREDS_B64"]).decode("utf-8")
    )
    creds = Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SHEET_ID)

    # ── Лист "Группы" ──
    groups_ws = spreadsheet.worksheet("Группы")
    rows = groups_ws.get_all_values()[1:]   # пропускаем заголовок

    groups = []
    for i, row in enumerate(rows, start=2):
        # Дополняем строку до нужной длины пустыми значениями
        row = row + [""] * (COL_POSTS_BETWEEN - len(row))

        url = row[COL_URL - 1].strip()
        if not url:
            continue

        active_raw = row[COL_ACTIVE - 1].strip().upper()
        groups.append({
            "row":           i,
            "username":      url,
            "template_key":  row[COL_TEMPLATE_KEY - 1].strip(),
            "last_sent":     row[COL_LAST_SENT - 1].strip(),
            "enabled":       active_raw != "FALSE",
            "status":        row[COL_STATUS - 1].strip(),
            "post_link":     row[COL_POST_LINK - 1].strip(),
            "post_new":      row[COL_POST_NEW - 1].strip(),
            "posts_between": row[COL_POSTS_BETWEEN - 1].strip(),
        })

    # ── Лист "Шаблоны" ──
    templates_ws = spreadsheet.worksheet("Шаблоны")
    templates = {}
    for row in templates_ws.get_all_values()[1:]:
        if len(row) >= 2 and row[0].strip():
            templates[row[0].strip()] = row[1].strip()

    log.info(f"Загружено групп: {len(groups)}, шаблонов: {len(templates)}")
    return groups, templates, groups_ws


def pick_template(group: dict, templates: dict) -> str | None:
    key = group["template_key"]
    if key and key in templates:
        return templates[key]
    if templates:
        return random.choice(list(templates.values()))
    return None


def update_group_row(ws, group: dict, status: str, new_post_link: str = ""):
    """
    Обновляет строку группы после попытки отправки:
      - Время последней публикации (только при успехе)
      - Статус отправки
      - Ссылки на посты + разница

    Логика ссылок:
      1й прогон  (post_link пуст)            → post_link = new_post_link
      2й прогон  (post_link есть, post_new пуст) → post_new = new_post_link, считаем разницу
      3й прогон+ (оба заполнены)             → post_link = старый post_new, post_new = new_post_link, пересчитываем
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    post_link     = group["post_link"]
    post_new      = group["post_new"]
    posts_between = group["posts_between"]

    if new_post_link:
        if not post_link:
            # первый пост
            post_link = new_post_link
            post_new  = ""
            posts_between = ""
        elif not post_new:
            # второй пост
            post_new = new_post_link
            posts_between = calc_posts_between(post_link, post_new)
        else:
            # третий и далее: сдвигаем
            post_link = post_new
            post_new  = new_post_link
            posts_between = calc_posts_between(post_link, post_new)

    success = status == "Отправлено"

    updates = [
        (group["row"], COL_STATUS,        status),
        (group["row"], COL_POST_LINK,     post_link),
        (group["row"], COL_POST_NEW,      post_new),
        (group["row"], COL_POSTS_BETWEEN, posts_between),
    ]
    if success:
        updates.append((group["row"], COL_LAST_SENT, now))

    for row, col, val in updates:
        ws.update_cell(row, col, val)

# ── Telegram ──────────────────────────────────────────────────────────────────

FLOOD_WAIT_SKIP_THRESHOLD = 180  # секунд — если больше, пропускаем группу


async def send_to_group(
    client: TelegramClient, group: dict, text: str
) -> tuple[bool, str, str]:
    """
    Отправляет сообщение в группу.
    Возвращает (success, status_text, post_link).

    FloodWait:
      ≤ 180с → ждём и делаем одну повторную попытку
      > 180с → пропускаем группу, пишем статус
    """
    target = group["username"]

    for attempt in range(2):  # 0 = первая попытка, 1 = повтор после FloodWait
        try:
            if target.startswith("https://t.me/+") or target.startswith("t.me/+"):
                hash_ = target.split("+")[-1]
                try:
                    await client(ImportChatInviteRequest(hash_))
                except Exception:
                    pass
                entity = await client.get_entity(target)
            else:
                entity = await client.get_entity(target)

            msg = await client.send_message(entity, text)

            # Формируем ссылку на пост
            post_link = ""
            try:
                chat_username = getattr(entity, "username", None)
                if chat_username and msg.id:
                    post_link = f"https://t.me/{chat_username}/{msg.id}"
            except Exception:
                pass

            log.info(f"✅ Отправлено в {target}" + (f" → {post_link}" if post_link else ""))
            return True, "Отправлено", post_link

        except FloodWaitError as e:
            if e.seconds > FLOOD_WAIT_SKIP_THRESHOLD:
                log.warning(
                    f"⏭️ FloodWait {e.seconds}с (>{FLOOD_WAIT_SKIP_THRESHOLD}с) "
                    f"на {target} — пропускаем"
                )
                return False, f"Пропущено (FloodWait {e.seconds}с)", ""
            else:
                wait = e.seconds + 5
                log.warning(
                    f"⏳ FloodWait {e.seconds}с на {target} — "
                    f"ждём {wait}с и повторяем..."
                )
                await asyncio.sleep(wait)
                # переходим к следующей итерации цикла (повтор)
                continue

        except PeerFloodError:
            log.error(f"🚫 PeerFlood на {target} — слишком много сообщений, останавливаемся")
            raise

        except SlowModeWaitError as e:
            log.warning(f"🐢 SlowMode {e.seconds}с на {target} — пропускаем")
            return False, f"SlowMode {e.seconds}с", ""

        except ChatWriteForbiddenError:
            log.warning(f"⚠️ Нет прав писать в {target}")
            return False, "Нет прав на запись", ""

        except UserBannedInChannelError:
            log.warning(f"⚠️ Аккаунт забанен в {target}")
            return False, "Аккаунт забанен", ""

        except ChannelPrivateError:
            log.warning(f"⚠️ Приватный канал/группа: {target}")
            return False, "Приватный канал", ""

        except Exception as e:
            log.error(f"❌ Ошибка для {target}: {e}")
            return False, f"Ошибка: {e}", ""

    # Если оба attempt исчерпаны (повтор тоже дал FloodWait)
    return False, "FloodWait — не удалось после повтора", ""

# ── Main ──────────────────────────────────────────────────────────────────────

async def run_broadcast():
    groups, templates, groups_ws = get_sheet_data()

    if not templates:
        log.error("Шаблоны не найдены в таблице!")
        return

    active_groups = [g for g in groups if g["enabled"]]
    log.info(f"Начинаем рассылку: {len(active_groups)} активных групп")

    random.shuffle(active_groups)

    async with TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH) as client:
        sent_count = 0

        for i, group in enumerate(active_groups):
            text = pick_template(group, templates)
            if not text:
                continue

            try:
                success, status, post_link = await send_to_group(client, group, text)
            except PeerFloodError:
                log.error("PeerFlood: досрочно останавливаем рассылку для защиты аккаунта")
                update_group_row(groups_ws, group, "PeerFlood — остановлено")
                break

            update_group_row(groups_ws, group, status, post_link if success else "")

            if success:
                sent_count += 1

            delay = random.randint(DELAY_MIN, DELAY_MAX)
            log.info(f"Ждём {delay}с перед следующим сообщением...")
            await asyncio.sleep(delay)

            if (i + 1) % BATCH_SIZE == 0:
                pause = random.randint(BATCH_PAUSE_MIN, BATCH_PAUSE_MAX)
                log.info(f"📦 Пауза между батчами: {pause}с")
                await asyncio.sleep(pause)

        log.info(f"✅ Рассылка завершена. Отправлено: {sent_count}/{len(active_groups)}")


if __name__ == "__main__":
    asyncio.run(run_broadcast())
