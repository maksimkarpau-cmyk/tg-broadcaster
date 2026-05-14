"""
Telegram Group Broadcaster
- Reads groups + templates from Google Sheets
- Sends random template per group (or group-specific template)
- Anti-flood: random delays, session reuse, flood wait handling
"""

import asyncio
import random
import logging
import os
import json
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError, ChatWriteForbiddenError, UserBannedInChannelError,
    ChannelPrivateError, PeerFloodError, SlowModeWaitError
)
from telethon.tl.functions.messages import ImportChatInviteRequest

# ── Logging ──────────────────────────────────────────────────────────────────
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
SESSION_NAME      = os.environ.get("TG_SESSION_NAME", "broadcaster")
SHEET_ID          = os.environ["GOOGLE_SHEET_ID"]
GOOGLE_CREDS_JSON = os.environ["GOOGLE_CREDS_JSON"]   # full JSON string of service account

# Anti-flood settings (seconds)
DELAY_MIN         = int(os.environ.get("DELAY_MIN", "45"))
DELAY_MAX         = int(os.environ.get("DELAY_MAX", "120"))
BATCH_SIZE        = int(os.environ.get("BATCH_SIZE", "10"))   # pause after N groups
BATCH_PAUSE_MIN   = int(os.environ.get("BATCH_PAUSE_MIN", "300"))
BATCH_PAUSE_MAX   = int(os.environ.get("BATCH_PAUSE_MAX", "600"))

# ── Google Sheets ─────────────────────────────────────────────────────────────

def get_sheet_data() -> tuple[list[dict], list[str]]:
    """
    Returns:
        groups    — list of dicts: {username, template_key, last_sent, enabled}
        templates — dict: {key -> text}

    Sheet 1 "Groups":
        A: group username or invite link  (e.g. @mygroup or https://t.me/+abc)
        B: template_key  (leave empty = random)
        C: last_sent     (filled by script)
        D: enabled       (TRUE/FALSE)

    Sheet 2 "Templates":
        A: key
        B: text
    """
    creds_info = json.loads(base64.b64decode(os.environ["GOOGLE_CREDS_B64"]).decode("utf-8"))
    creds = Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SHEET_ID)

    # Groups sheet
    groups_ws = spreadsheet.worksheet("Groups")
    groups_rows = groups_ws.get_all_values()[1:]  # skip header
    groups = []
    for i, row in enumerate(groups_rows, start=2):  # row index for update
        if len(row) < 1 or not row[0].strip():
            continue
        enabled = row[3].strip().upper() if len(row) > 3 else "TRUE"
        groups.append({
            "row":          i,
            "username":     row[0].strip(),
            "template_key": row[1].strip() if len(row) > 1 else "",
            "last_sent":    row[2].strip() if len(row) > 2 else "",
            "enabled":      enabled != "FALSE",
        })

    # Templates sheet
    templates_ws = spreadsheet.worksheet("Templates")
    templates_rows = templates_ws.get_all_values()[1:]
    templates = {}
    for row in templates_rows:
        if len(row) >= 2 and row[0].strip():
            templates[row[0].strip()] = row[1].strip()

    log.info(f"Loaded {len(groups)} groups, {len(templates)} templates")
    return groups, templates, groups_ws


def pick_template(group: dict, templates: dict) -> str | None:
    """Pick template for a group: specific key > random."""
    key = group["template_key"]
    if key and key in templates:
        return templates[key]
    if templates:
        return random.choice(list(templates.values()))
    return None


def mark_sent(ws, row_index: int):
    """Write current timestamp to column C."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    ws.update_cell(row_index, 3, ts)


# ── Telegram ──────────────────────────────────────────────────────────────────

async def send_to_group(client: TelegramClient, group: dict, text: str) -> bool:
    """Send message to one group. Returns True on success."""
    target = group["username"]
    try:
        # Support both @username and invite links
        if target.startswith("https://t.me/+") or target.startswith("t.me/+"):
            hash_ = target.split("+")[-1]
            try:
                await client(ImportChatInviteRequest(hash_))
            except Exception:
                pass  # already a member
            entity = await client.get_entity(target)
        else:
            entity = await client.get_entity(target)

        await client.send_message(entity, text)
        log.info(f"✅ Sent to {target}")
        return True

    except FloodWaitError as e:
        log.warning(f"⏳ FloodWait {e.seconds}s — pausing...")
        await asyncio.sleep(e.seconds + 10)
        return False

    except PeerFloodError:
        log.error(f"🚫 PeerFlood on {target} — too many messages, stopping batch")
        raise  # bubble up to stop the run

    except SlowModeWaitError as e:
        log.warning(f"🐢 SlowMode {e.seconds}s on {target} — skipping")
        return False

    except (ChatWriteForbiddenError, UserBannedInChannelError, ChannelPrivateError) as e:
        log.warning(f"⚠️  Cannot write to {target}: {type(e).__name__}")
        return False

    except Exception as e:
        log.error(f"❌ Error on {target}: {e}")
        return False


async def run_broadcast():
    groups, templates, groups_ws = get_sheet_data()

    if not templates:
        log.error("No templates found in sheet!")
        return

    active_groups = [g for g in groups if g["enabled"]]
    log.info(f"Starting broadcast to {len(active_groups)} active groups")

    # Shuffle to vary order between runs
    random.shuffle(active_groups)

    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        sent_count = 0
        for i, group in enumerate(active_groups):
            text = pick_template(group, templates)
            if not text:
                continue

            try:
                success = await send_to_group(client, group, text)
            except PeerFloodError:
                log.error("PeerFlood: stopping broadcast early to protect account")
                break

            if success:
                mark_sent(groups_ws, group["row"])
                sent_count += 1

            # Delay between messages
            delay = random.randint(DELAY_MIN, DELAY_MAX)
            log.info(f"Waiting {delay}s before next message...")
            await asyncio.sleep(delay)

            # Longer pause after each batch
            if (i + 1) % BATCH_SIZE == 0:
                pause = random.randint(BATCH_PAUSE_MIN, BATCH_PAUSE_MAX)
                log.info(f"📦 Batch pause: {pause}s")
                await asyncio.sleep(pause)

    log.info(f"✅ Broadcast complete. Sent: {sent_count}/{len(active_groups)}")


if __name__ == "__main__":
    asyncio.run(run_broadcast())
