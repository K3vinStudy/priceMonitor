# 功能：将HTML清洗并整理为json
# 输入：由HTML文本组成的list（每个页面一个元素）
# 输出：整理完的json文本

import re
import json
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

from zoneinfo import ZoneInfo

# Global debug switch for comment parsing/cleaning.
DEBUG_CLEAN_HTML = False
# DEBUG_CLEAN_HTML = True

def set_debug_clean_html(enabled: bool = True) -> None:
    """Enable/disable debug prints inside clean_html parsing functions."""
    global DEBUG_CLEAN_HTML
    DEBUG_CLEAN_HTML = bool(enabled)

# 1) 去掉懂车帝页面里那堆“图标字体字符”（通常落在私用区）
ICON_PUA = re.compile(r"[\uE000-\uF8FF]")

# 2) 抓 thread_title / motor_title（raw html 里就有） [oai_citation:5‡1845775220762699.html](sediment://file_00000000cf1872099085a22b320258d9)
THREAD_TITLE_RE = re.compile(r'"thread_title"\s*:\s*"([^"]+)"')
MOTOR_TITLE_RE  = re.compile(r'"motor_title"\s*:\s*"([^"]*)"')
UNAME_RE = re.compile(r'"uname"\s*:\s*"([^"]+)"')

NICKNAME_RE = re.compile(r'"nickname"\s*:\s*"([^"\\]+)"')
USERNAME_RE = re.compile(r'"user_name"\s*:\s*"([^"\\]+)"')

# Try to capture OP uname near the thread title block (avoid picking random commenters)
THREAD_UNAME_RE = re.compile(r'"thread_title"\s*:\s*"[^\"]+".{0,800}?"uname"\s*:\s*"([^"\\]+)"', re.S)

# UI noise tokens commonly present in comment cards
_NOISE_TOKENS = {
    "回复", "点赞", "收起回复", "我也说两句", "全部", "表情", "图片", "发布评论",
    "关注", "关注TA", "获赞", "精华", "动态", "更多徽章",
}

def _clean_comment_text(raw: str, author: str = "") -> str:
    """Clean comment/reply text extracted from a community-card."""
    raw = ICON_PUA.sub("", raw)
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    out = []
    for ln in lines:
        # Drop pure UI tokens or counters like "6点赞" / "1点赞"
        if ln in _NOISE_TOKENS:
            continue
        if re.fullmatch(r"\d+点赞", ln):
            continue
        if re.fullmatch(r"全部\d+条回复", ln):
            continue
        if ln.startswith("@") and "：" in ln:
            # quoted mention line
            continue
        if author and ln == author:
            # avoid duplicating author line as content
            continue
        # Drop common badge/meta lines like "xxx · 奔驰GLB车主·车龄1年"
        if "·" in ln and ("车主" in ln or "车龄" in ln):
            continue
        # Drop navigation-like tokens
        if any(k in ln for k in ("关注TA", "获赞", "精华", "动态")):
            continue
        out.append(ln)
    return _clean_line("\n".join(out)).strip()

def _normalize_author(name: str) -> str:
    """Normalize author display name.

    - Remove private-use icon chars
    - If the name contains badge meta like "xxx · 奔驰GLB车主·车龄1年", keep only the part before the first "·".
    """
    name = _clean_line(name or "")
    if "·" in name:
        name = name.split("·", 1)[0].strip()
    return name

def _unescape_json_string(s: str) -> str:
    """Safely unescape a JSON string fragment captured without surrounding quotes.

    NOTE: Do NOT use unicode_escape here; it will corrupt already-decoded UTF-8 text.
    """
    try:
        # s is the raw JSON string content without the surrounding quotes.
        # json.loads will correctly handle \n, \t, and \uXXXX sequences.
        return json.loads(f'"{s}"')
    except Exception:
        return s

def _clean_line(s: str) -> str:
    s = ICON_PUA.sub("", s)
    s = s.replace("\xa0", " ").strip()
    return s

def extract_post_strict(raw_html: str):
    """
    优先从 embedded json 抓 thread_title/motor_title；
    抓不到再从 DOM 的 h1/title + content span 抓。 [oai_citation:6‡1845775220762699.html](sediment://file_00000000cf1872099085a22b320258d9)  [oai_citation:7‡1845775220762699.html](sediment://file_00000000cf1872099085a22b320258d9)
    """
    # A) embedded JSON
    m1 = THREAD_TITLE_RE.search(raw_html)
    m2 = MOTOR_TITLE_RE.search(raw_html)
    title = _unescape_json_string(m1.group(1)) if m1 else ""
    motor = _unescape_json_string(m2.group(1)) if m2 else ""

    if motor:
        lines = [_clean_line(x) for x in motor.splitlines() if _clean_line(x)]
        # If thread_title missing, fall back to first non-empty line as a weak title (rare).
        if not title and lines:
            title = lines[0]
        return title.strip(), lines

    # B) DOM fallback
    soup = BeautifulSoup(raw_html, "lxml")
    h1 = soup.select_one("h1.title")
    title2 = h1.get_text(strip=True) if h1 else ""

    # content span 在同一个 block 里（见 raw html） [oai_citation:8‡1845775220762699.html](sediment://file_00000000cf1872099085a22b320258d9)
    content_span = soup.select_one("div.content span")
    text = content_span.get_text("\n", strip=True) if content_span else ""
    lines = [_clean_line(x) for x in text.splitlines() if _clean_line(x)]

    return title2.strip(), lines

def extract_published_at(raw_html: str) -> str:
    """Return the raw published time text for the *post* (NOT comments).

    DCD pages have multiple variants, for example:
      - "2025-10-12发布于" / "2025-10-12 发布于"
      - "前天 09:34发布于" / "6天前发布于"
      - "03-06发布" / "03-06 发布" (often without the character "于")

    We return only the time token part (e.g. "2025-10-12", "前天 09:34", "03-06").
    """
    # 1) Most common: token + '发布于'
    m = re.search(
        r"(\d{4}-\d{2}-\d{2}|今天\s*\d{2}:\d{2}|昨天\s*\d{2}:\d{2}|前天\s*\d{2}:\d{2}|\d+天前(?:\s*\d{2}:\d{2})?|\d{2}-\d{2}(?:\s*\d{2}:\d{2})?)\s*发布于",
        raw_html,
    )
    if m:
        return m.group(1).strip()

    # 2) Another common: token + '发布' (NO '于'), usually for the main post header
    #    e.g. '<span>03-06发布</span>' or '2025-10-12发布' or '4天前发布'
    m2 = re.search(
        r"(\d{4}-\d{2}-\d{2}|\d{2}-\d{2}|今天\s*\d{2}:\d{2}|昨天\s*\d{2}:\d{2}|前天\s*\d{2}:\d{2}|\d+天前(?:\s*\d{2}:\d{2})?)\s*发布(?!于)",
        raw_html,
    )
    if m2:
        # Re-capture with a wider group to include optional HH:MM when present.
        m2b = re.search(
            r"((?:\d{4}-\d{2}-\d{2}|\d{2}-\d{2}|今天|昨天|前天|\d+天前)(?:\s*\d{2}:\d{2})?)\s*发布(?!于)",
            raw_html,
        )
        return (m2b.group(1) if m2b else m2.group(1)).strip()

    # 3) Last resort: look for '发布时间' style labels in embedded JSON / text
    #    Keep this permissive but still anchored to '发布'
    m3 = re.search(
        r"发布\s*时间\s*[:：]\s*(\d{4}-\d{2}-\d{2}(?:\s*\d{2}:\d{2})?|\d{2}-\d{2}(?:\s*\d{2}:\d{2})?|今天\s*\d{2}:\d{2}|昨天\s*\d{2}:\d{2}|前天\s*\d{2}:\d{2}|\d+天前(?:\s*\d{2}:\d{2})?)",
        raw_html,
    )
    if m3:
        return m3.group(1).strip()

    return ""

def extract_series(raw_html: str) -> str:
    soup = BeautifulSoup(raw_html, "lxml")
    a = soup.select_one("a.series-card_series-name__3SvjA")
    return a.get_text(strip=True) if a else ""

def extract_op_author(raw_html: str) -> str:
    # 0) Try thread-scoped uname fallback
    m0 = THREAD_UNAME_RE.search(raw_html)
    if m0:
        name0 = _normalize_author(_unescape_json_string(m0.group(1)))
        if name0:
            return name0

    # 1) Prefer embedded JSON fields when present (most stable).
    for rx in (UNAME_RE, NICKNAME_RE, USERNAME_RE):
        m = rx.search(raw_html)
        if m:
            name = _clean_line(_unescape_json_string(m.group(1)))
            if name:
                return name

    # 2) Fallback: from DOM near the post title
    soup = BeautifulSoup(raw_html, "lxml")

    # Try to locate OP near the "楼主" label
    op_label = soup.find(string=lambda x: isinstance(x, str) and x.strip() == "楼主")
    if op_label:
        container = op_label.find_parent()
        if container:
            a = container.select_one('a[title*="个人主页"]') or container.select_one('a[href*="/user/"]')
            if a:
                t = _clean_line(a.get_text(" ", strip=True))
                if t:
                    return t

    # Try to locate OP near the "发布于" meta line (often in the post header)
    pub_node = soup.find(string=lambda x: isinstance(x, str) and "发布于" in x)
    if pub_node:
        header = pub_node.find_parent()
        if header:
            # look for a profile link inside the same header container
            a = header.select_one('a[title*="个人主页"]') or header.select_one('a[href*="/user/"]')
            if a:
                t = _normalize_author(a.get_text(" ", strip=True))
                if t:
                    return t
            # or scan nearby ancestors up to a few levels
            cur = header
            for _ in range(4):
                a2 = cur.select_one('a[title*="个人主页"]') or cur.select_one('a[href*="/user/"]')
                if a2:
                    t2 = _normalize_author(a2.get_text(" ", strip=True))
                    if t2:
                        return t2
                cur = cur.parent if cur and cur.parent else None
                if cur is None:
                    break

    # raw 页面里楼主名是“多放奶盖1”，现在能抓到  [oai_citation:10‡1845775220762699.txt](sediment://file_00000000d23c7206af593e1b9d4d06f3)
    # 从主贴容器附近找第一个 /user/ 链接的可见文本

    # 3) Fallback: pick the first profile link that is NOT inside any comment/reply card.
    # This avoids grabbing commenters when OP extraction above fails.
    for a in soup.select('a[title*="个人主页"], a[href*="/user/"]'):
        # Skip links inside comment/reply cards
        if a.find_parent('section', class_=re.compile(r"community-card")) is not None:
            continue
        t = _normalize_author(a.get_text(" ", strip=True))
        if t and t not in ("关注", "关注TA", "私信", "主页"):
            return t

    h1 = soup.select_one("h1.title")
    if not h1:
        return ""
    root = h1.find_parent("div")  # 往上找一层容器
    if not root:
        root = soup
    for a in root.select('a[href*="/user/"]'):
        t = _normalize_author(a.get_text(" ", strip=True))
        if t and t not in ("关注", "关注TA", "私信", "主页"):
            return t
    return ""

def extract_comments_strict(
    raw_html: str,
    op_name: str,
    max_depth: int = 3,
    debug: bool | None = None,
    fetched_at: str = "",
    timezone: str = "Asia/Shanghai",
):
    """Extract comments in a 2-level structure.

    - Floors: top-level comments ("评论发表于")
    - Replies: all reply cards under that floor ("回复发表于")
      * Replies to replies are flattened into the same `replies` list.
      * Such replies carry `reply_to` referencing the quoted target (author + content) when present.

    Note: max_depth is kept for safety but we only output 2 levels.
    """
    soup = BeautifulSoup(raw_html, "lxml")

    # Remove noise nodes that can pollute text
    for t in soup(["script", "style", "noscript"]):
        t.decompose()
    for img in soup.find_all("img"):
        img.decompose()
    for emo in soup.select("i.emoji, span.emoji"):
        emo.decompose()

    # Stable document order for cards (more reliable than Tag.sourceline)
    _all_cards_in_doc = list(soup.select("[class*='community-card']"))
    _card_order = {id(c): i for i, c in enumerate(_all_cards_in_doc)}

    if debug is None:
        debug = DEBUG_CLEAN_HTML

    if debug:
        total_li = len(soup.find_all("li"))
        total_cards = len(soup.select("[class*='community-card']"))
        print(f"[debug] total li={total_li}, total community-card={total_cards}")
        # How many li have a card
        li_with_card = sum(1 for li0 in soup.find_all("li") if li0.select_one("[class*='community-card']") is not None)
        print(f"[debug] li_with_card={li_with_card}")
        # Counters to understand why floors are missed
        li_with_pinglun = 0
        li_with_huifu = 0
        dom_floor_candidates = 0
        for li0 in soup.find_all("li"):
            if li0.select_one("[class*='community-card']") is None:
                continue
            t0 = li0.get_text(" ", strip=True)
            if "评论发表于" in t0:
                li_with_pinglun += 1
            if "回复发表于" in t0:
                li_with_huifu += 1
            anc0 = li0.find_parent("li")
            is_floor0 = True
            while anc0 is not None:
                if anc0.select_one("[class*='community-card']") is not None:
                    is_floor0 = False
                    break
                anc0 = anc0.find_parent("li")
            if is_floor0:
                dom_floor_candidates += 1
        print(f"[debug] li_with_评论发表于={li_with_pinglun}, li_with_回复发表于={li_with_huifu}, dom_floor_candidates={dom_floor_candidates}")

    def _extract_date(li_text: str, kind: str) -> str:
        # kind in ("评论发表于", "回复发表于")
        pat = kind + r"\s*([0-9]{4}-[0-9]{2}-[0-9]{2}(?:\s*[0-9]{2}:[0-9]{2})?|今天\s*[0-9]{2}:[0-9]{2}|昨天\s*[0-9]{2}:[0-9]{2}|前天\s*[0-9]{2}:[0-9]{2}|[0-9]+天前(?:\s*[0-9]{2}:[0-9]{2})?|[0-9]{2}-[0-9]{2}(?:\s*[0-9]{2}:[0-9]{2})?)"
        m = re.search(pat, li_text)
        if m:
            return m.group(1)
        # Fallback: first date-like token anywhere in the text
        m2 = re.search(r"([0-9]{4}-[0-9]{2}-[0-9]{2}(?:\s*[0-9]{2}:[0-9]{2})?|今天\s*[0-9]{2}:[0-9]{2}|昨天\s*[0-9]{2}:[0-9]{2}|前天\s*[0-9]{2}:[0-9]{2}|[0-9]+天前(?:\s*[0-9]{2}:[0-9]{2})?|[0-9]{2}-[0-9]{2}(?:\s*[0-9]{2}:[0-9]{2})?)", li_text)
        return m2.group(1) if m2 else ""
    
    def _parse_fetched_at() -> datetime | None:
        if not fetched_at:
            return None
        try:
            return datetime.fromisoformat(fetched_at)
        except Exception:
            return None

    def _norm_datetime_token(raw_token: str) -> str:
        """Normalize to absolute 'YYYY-MM-DD HH:MM' (HH:MM optional) for sorting only.
        If cannot normalize, return a high sentinel so it sorts last.
        """
        tok = (raw_token or "").strip()
        if not tok:
            return "9999-12-31 23:59"

        base_dt = _parse_fetched_at()
        if base_dt is None:
            return "9999-12-31 23:59"

        # Optional HH:MM anywhere in token
        tm = None
        m_time = re.search(r"(\d{2}:\d{2})", tok)
        if m_time:
            tm = m_time.group(1)

        def with_time(d: datetime) -> str:
            return d.strftime("%Y-%m-%d") + ((" " + tm) if tm else "")

        # Full absolute date (YYYY-MM-DD or YYYY-MM-DD HH:MM)
        m_full = re.fullmatch(r"(\d{4}-\d{2}-\d{2})(?:\s*(\d{2}:\d{2}))?", tok)
        if m_full:
            return m_full.group(1) + ((" " + m_full.group(2)) if m_full.group(2) else "")

        # Month-day without year: infer year relative to fetched_at
        m_md = re.fullmatch(r"(\d{2})-(\d{2})(?:\s*(\d{2}:\d{2}))?", tok)
        if m_md:
            mm = int(m_md.group(1))
            dd = int(m_md.group(2))
            # If tok contains its own time, prefer it
            if m_md.group(3):
                tm = m_md.group(3)
            y = base_dt.year
            # If interpreting as fetched_at year would put the date in the future, assume it's last year.
            try:
                cand_date = datetime(y, mm, dd)
                if cand_date.date() > base_dt.date():
                    y -= 1
            except Exception:
                pass
            return f"{y:04d}-{mm:02d}-{dd:02d}" + ((" " + tm) if tm else "")

        # 今天/昨天/前天（支持“前天21:33”这种不带空格的）
        if tok.startswith("今天"):
            return with_time(base_dt)
        if tok.startswith("昨天"):
            return with_time(base_dt - timedelta(days=1))
        if tok.startswith("前天"):
            return with_time(base_dt - timedelta(days=2))

        # n天前
        m_days = re.search(r"(\d+)天前", tok)
        if m_days:
            n = int(m_days.group(1))
            return with_time(base_dt - timedelta(days=n))

        return "9999-12-31 23:59"

    def _extract_author_from_card(card, fallback_li=None) -> str:
        """Extract author name from the header area of one community-card."""
        if card is None:
            return "unknown"
        # Prefer profile link inside this card
        a = card.select_one('a[title*="个人主页"]') or card.select_one('a[href*="/user/"]')
        if a:
            # Many pages put the visible name in a nested span
            name_span = (
                a.select_one('span.tw-text-black')
                or a.select_one('span.tw-text-common-black')
                or a.select_one('span.tw-text-video-shallow-gray')
            )
            if name_span:
                t = _normalize_author(name_span.get_text(" ", strip=True))
                if t:
                    return t
            t = _normalize_author(a.get_text(" ", strip=True))
            if t:
                return t
        # Fallback: on some pages the author link is outside the community-card section but inside the same floor <li>.
        if fallback_li is not None:
            for a2 in fallback_li.select('a[title*="个人主页"], a[href*="/user/"]'):
                # The closest li ancestor of this link must be the floor li itself; otherwise it's from a nested reply.
                if a2.find_parent('li') is not fallback_li:
                    continue
                t2 = _normalize_author(a2.get_text(" ", strip=True))
                if t2 and t2 not in ("关注", "关注TA", "私信", "主页"):
                    return t2
        # Do NOT infer author from card text lines; it's easy to confuse with content.
        # Only fall back to explicit patterns like 用户123... from the surrounding li.
        # Last fallback: pattern like 用户123...
        if fallback_li is not None:
            m = re.search(r"(用户\d{6,})", fallback_li.get_text(" ", strip=True))
            if m:
                return m.group(1)
        return "unknown"

    def _extract_content(card, author: str):
        """Return content_lines for a floor/reply card.

        Prefer the actual content paragraph(s) and exclude quoted blocks and action toolbars.
        """
        if card is None:
            return []

        # Detect quoted block (reply_to preview) text so we can remove it from content WITHOUT mutating the DOM
        quoted_full_text = ""
        quote_span = card.select_one('span.tw-font-medium')
        if quote_span:
            qc = quote_span.find_parent('p') or quote_span.find_parent('div')
            if qc:
                quoted_full_text = qc.get_text("\n", strip=True)

        # Prefer paragraph(s) that contain the main text
        paras = card.select('p.tw-whitespace-pre-wrap')
        lines = []
        if paras:
            for p in paras:
                txt = p.get_text("\n", strip=True)
                if txt:
                    lines.extend([t.strip() for t in txt.splitlines() if t.strip()])
        else:
            # fallback to whole card text
            txt = card.get_text("\n", strip=True)
            if txt:
                lines = [t.strip() for t in txt.splitlines() if t.strip()]

        raw_text = "\n".join(lines)

        # Remove the quoted block's text if present
        if quoted_full_text:
            raw_text = raw_text.replace(quoted_full_text, "").strip()

        raw_text = re.sub(
            r"(评论发表于|回复发表于)\s*(?:\d{4}-\d{2}-\d{2}|今天\s*\d{2}:\d{2}|昨天\s*\d{2}:\d{2}|前天\s*\d{2}:\d{2}|\d+天前\s*\d{2}:\d{2}|\d{2}-\d{2})",
            "",
            raw_text,
        )

        cleaned = _clean_comment_text(raw_text, author=author)
        return [ln for ln in cleaned.splitlines() if ln.strip()]

    def _extract_reply_to(card):
        """If the card contains a quoted block like '@用户xxx：...'
        return {'author': '用户xxx', 'content': '...'} else 'none'.
        """
        # The quoted block is usually a white-background container (e.g. div.tw-bg-common-white)
        quote_container = card.select_one('div.tw-bg-common-white')
        if not quote_container:
            # Fallback: look for any container that has the '@用户...：' marker span
            quote_span = card.select_one('span.tw-font-medium')
            if not quote_span:
                return "none"
            quote_container = quote_span.find_parent('p') or quote_span.find_parent('div')
        # Now find the marker span inside the quote container
        quote_span = quote_container.select_one('span.tw-font-medium')
        if not quote_span:
            return "none"
        q_author = quote_span.get_text(" ", strip=True)
        q_author = q_author.lstrip("@").rstrip("：").strip()
        q_author = _normalize_author(q_author)

        # The quoted content is typically the remaining text in the quoted container
        q_text = ""
        q_text = quote_container.get_text(" ", strip=True)
        # remove the author prefix part
        q_text = q_text.replace(quote_span.get_text(" ", strip=True), "", 1).strip()
        q_text = _clean_line(q_text)
        if not q_author and not q_text:
            return "none"
        return {"author_id": q_author or "unknown", "content": q_text}

    def _comment_id_from_card(card) -> str:
        if card is None:
            return ""
        dlv = card.get("data-log-view") or ""
        # data-log-view is HTML-escaped JSON; comment_id appears as a string of digits
        m = re.search(r'"comment_id"\s*:\s*"(\d+)"', dlv)
        return m.group(1) if m else ""

    def _primary_card_in_li(li_tag):
        """Return the community-card that belongs to this li itself (not nested in child li)."""
        if li_tag is None:
            return None
        for c in li_tag.select("[class*='community-card']"):
            # The closest li ancestor of the card must be this li (otherwise it's a nested reply card)
            if c.find_parent("li") is li_tag:
                return c
        return li_tag.select_one("[class*='community-card']")

    floors = []
    seen_floor_ids = set()

    # Floor = a <li> that contains a community-card and has "评论发表于" (and not "回复发表于")
    for li in soup.find_all("li"):
        card = _primary_card_in_li(li)
        if not card:
            continue

        li_text = li.get_text(" ", strip=True)
        card_text = card.get_text(" ", strip=True)
        # Determine whether THIS card is a floor card or a reply card.
        # IMPORTANT: li_text may include nested replies' "回复发表于"; do not use li_text for has_huifu.
        has_pinglun = ("评论发表于" in li_text) or ("评论发表于" in card_text)
        has_huifu_self = ("回复发表于" in card_text)

        # Fallback floor heuristic: if we can't see '评论发表于', treat it as floor when it has no ancestor li with a comment card.
        anc = li.find_parent("li")
        no_card_ancestor = True
        while anc is not None:
            if anc.select_one("[class*='community-card']") is not None:
                no_card_ancestor = False
                break
            anc = anc.find_parent("li")

        # Skip reply-cards masquerading as floors, but KEEP floors that merely CONTAIN replies.
        if has_huifu_self:
            continue
        if not has_pinglun and not no_card_ancestor:
            continue

        if debug:
            why = "pinglun" if has_pinglun else ("dom_root" if no_card_ancestor else "unknown")
            print(f"[debug] floor candidate matched_by={why}")

        cid = _comment_id_from_card(card)
        # Keep a numeric comment id for stable ordering (higher id is typically newer)
        try:
            _cid_num = int(cid) if cid else 0
        except Exception:
            _cid_num = 0
        # Deduplicate by comment_id when available; otherwise fallback to li identity
        key = cid if cid else str(id(li))
        if key in seen_floor_ids:
            continue
        seen_floor_ids.add(key)

        floor_author = _extract_author_from_card(card, fallback_li=li)
        floor_date = _extract_date(li_text, "评论发表于")
        _floor_date_norm = _norm_datetime_token(floor_date)
        if _floor_date_norm.startswith("9999-"):
            _floor_date_norm = floor_date
        floor_content = _extract_content(card, floor_author)
        if not floor_content:
            if debug:
                li_text_short = li_text[:120]
                print(f"[debug] skip floor: empty content; li_text={li_text_short}")
            continue

        if debug:
            preview = " ".join(floor_content[:2])
            print(f"[debug] floor ok author={floor_author} date={floor_date} preview={preview[:80]}")

        has_louzhu = li.find(string=lambda x: isinstance(x, str) and x.strip() == "楼主") is not None
        floor_is_op = 1 if ((op_name and floor_author == op_name) or has_louzhu) else 0
        if floor_is_op == 1 and (not floor_author or floor_author == "unknown") and op_name:
            floor_author = op_name

        floor = {
            "published_at": _floor_date_norm,
            "author": floor_author,
            "is_op": floor_is_op,
            "content_lines": floor_content,
            "replies": [],
        }
        # Order: by published date (asc), then by comment_id (desc) to fix within-day reversal, then by doc order
        floor["_order"] = (_floor_date_norm or "9999-12-31 23:59", -_cid_num, _card_order.get(id(card), 10**9))

        # Replies under this floor: descendant li with "回复发表于" and a community-card
        for rli in li.find_all("li"):
            if rli is li:
                continue
            rcard = rli.select_one("[class*='community-card']")
            if not rcard:
                continue
            rli_text = rli.get_text(" ", strip=True)
            rcard_text = rcard.get_text(" ", strip=True)
            if ("回复发表于" not in rli_text) and ("回复发表于" not in rcard_text):
                continue

            rauthor = _extract_author_from_card(rcard, fallback_li=rli)
            rdate = _extract_date(rli_text, "回复发表于")
            _rdate_norm = _norm_datetime_token(rdate)
            if _rdate_norm.startswith("9999-"):
                _rdate_norm = rdate
            rcontent = _extract_content(rcard, rauthor)
            if not rcontent:
                continue

            has_author_badge = rcard.find(string=lambda x: isinstance(x, str) and x.strip() == "作者") is not None
            ris_op = 1 if ((op_name and rauthor == op_name) or has_author_badge) else 0
            if ris_op == 1 and (not rauthor or rauthor == "unknown") and op_name:
                rauthor = op_name

            reply_to = _extract_reply_to(rcard)
            rcid = _comment_id_from_card(rcard)
            try:
                _rcid_num = int(rcid) if rcid else 0
            except Exception:
                _rcid_num = 0
            reply = {
                "published_at": _rdate_norm,
                "author": rauthor,
                "is_op": ris_op,
                "content_lines": rcontent,
                "reply_to": reply_to,
            }
            # Order replies by date (asc), then comment_id (asc) to preserve within-day on-page order, then doc order
            reply["_order"] = (_rdate_norm or "9999-12-31 23:59", _rcid_num, _card_order.get(id(rcard), 10**9))
            floor["replies"].append(reply)

        # Sort replies by _order and remove the key
        if floor["replies"]:
            floor["replies"].sort(key=lambda x: x.get("_order", 0))
            for r in floor["replies"]:
                r.pop("_order", None)

        floors.append(floor)

    # Safety fallback: if nothing detected (DOM variance), try card-based detection
    if not floors:
        for card in soup.select("[class*='community-card']"):
            if card.find(string=lambda x: isinstance(x, str) and "评论发表于" in x) is None:
                continue
            li = card.find_parent("li")
            if not li:
                continue
            li_text = li.get_text(" ", strip=True)
            floor_author = _extract_author_from_card(card, fallback_li=li)
            floor_date = _extract_date(li_text, "评论发表于")
            _floor_date_norm = _norm_datetime_token(floor_date)
            if _floor_date_norm.startswith("9999-"):
                _floor_date_norm = floor_date
            floor_content = _extract_content(card, floor_author)
            if not floor_content:
                if debug:
                    li_text_short = li_text[:120]
                    print(f"[debug] skip floor: empty content; li_text={li_text_short}")
                continue
            if debug:
                preview = " ".join(floor_content[:2]) if isinstance(floor_content, list) else str(floor_content)
                print(f"[debug] floor ok author={floor_author} date={floor_date} preview={preview[:80]}")
            has_louzhu = li.find(string=lambda x: isinstance(x, str) and x.strip() == "楼主") is not None
            floor_is_op = 1 if ((op_name and floor_author == op_name) or has_louzhu) else 0
            if floor_is_op == 1 and (not floor_author or floor_author == "unknown") and op_name:
                floor_author = op_name
            floors.append({
                "published_at": _floor_date_norm,
                "author": floor_author,
                "is_op": floor_is_op,
                "content_lines": floor_content,
                "replies": [],
                "_order": _card_order.get(id(card), 10**9),
            })
        # Sort floors and remove _order in fallback
        floors.sort(key=lambda x: x.get("_order", 0))
        for f in floors:
            f.pop("_order", None)
        return floors

    # Sort floors by _order and remove the key
    if floors:
        floors.sort(key=lambda x: x.get("_order", 0))
        for f in floors:
            f.pop("_order", None)
    return floors

def extract_source_url(raw_html: str) -> str:
    """Extract canonical/source URL from the HTML head.

    The user observed it exists at /html/head/link[12]. We try that first, then fall back
    to common canonical/og:url patterns.
    """
    soup = BeautifulSoup(raw_html, "lxml")
    head = soup.head
    if head:
        links = head.find_all("link")
        if len(links) >= 12:
            href = links[11].get("href")
            if href:
                return href.strip()
        # canonical link
        canon = head.find("link", rel=lambda v: isinstance(v, str) and "canonical" in v.lower())
        if canon and canon.get("href"):
            return canon["href"].strip()
        # og:url
        og = head.find("meta", attrs={"property": "og:url"})
        if og and og.get("content"):
            return og["content"].strip()
    return ""

def html_to_json(raw_html: str, source_url: str = "", fetched_at: str = "", timezone: str = "Asia/Shanghai") -> dict:
    """Parse a DCD post HTML into structured JSON.

    This is intended to replace the stage-1 LLM step.

    Output fields:
    - source_url: page url (optional)
    - series
    - post: {title, published_at, author, is_op, content_lines}
    - comments: list of floors; each floor has replies (one-level list). Replies may include reply_to.

    Notes:
    - is_op is kept as 0/1 (int) to match your current cleaning workflow.
    """
    series = extract_series(raw_html)
    title, content_lines = extract_post_strict(raw_html)
    published_at = extract_published_at(raw_html)
    op = extract_op_author(raw_html)

    if not fetched_at:
        try:
            fetched_at = datetime.now(ZoneInfo(timezone)).isoformat(timespec="seconds")
        except Exception:
            fetched_at = datetime.now().isoformat(timespec="seconds")

    # Normalize post published_at as well (fill year / resolve relative time) for consistency.
    def _norm_post_time(tok: str) -> str:
        t = (tok or "").strip()
        if not t:
            return t
        try:
            base_dt = datetime.fromisoformat(fetched_at)
        except Exception:
            return t

        tm = None
        m_time = re.search(r"(\d{2}:\d{2})", t)
        if m_time:
            tm = m_time.group(1)

        def with_time(d: datetime) -> str:
            return d.strftime("%Y-%m-%d") + ((" " + tm) if tm else "")

        if re.fullmatch(r"\d{4}-\d{2}-\d{2}(?:\s*\d{2}:\d{2})?", t):
            return t

        m_md = re.fullmatch(r"(\d{2})-(\d{2})(?:\s*(\d{2}:\d{2}))?", t)
        if m_md:
            mm = int(m_md.group(1)); dd = int(m_md.group(2))
            if m_md.group(3):
                tm_local = m_md.group(3)
            else:
                tm_local = tm
            y = base_dt.year
            try:
                cand = datetime(y, mm, dd)
                if cand.date() > base_dt.date():
                    y -= 1
            except Exception:
                pass
            return f"{y:04d}-{mm:02d}-{dd:02d}" + ((" " + tm_local) if tm_local else "")

        if t.startswith("今天"):
            return with_time(base_dt)
        if t.startswith("昨天"):
            return with_time(base_dt - timedelta(days=1))
        if t.startswith("前天"):
            return with_time(base_dt - timedelta(days=2))
        m_days = re.search(r"(\d+)天前", t)
        if m_days:
            n = int(m_days.group(1))
            return with_time(base_dt - timedelta(days=n))

        return t

    published_at = _norm_post_time(published_at)

    comments = extract_comments_strict(raw_html, op, debug=None, fetched_at=fetched_at, timezone=timezone)

    return {
        "source_url": source_url or extract_source_url(raw_html),
        "meta": {"fetched_at": fetched_at, "timezone": timezone},
        "series": series,
        "post": {
            "title": title,
            "published_at": published_at,
            "author": op,
            "is_op": 1,
            "content_lines": content_lines,
        },
        "comments": comments,
    }



def _dedup_floor_key(floor: dict) -> tuple:
    """Best-effort dedup key for a floor comment across pages."""
    author = floor.get("author") or ""
    date = floor.get("published_at") or ""
    first_line = ""
    cl = floor.get("content_lines")
    if isinstance(cl, list) and cl:
        first_line = cl[0]
    return (author, date, first_line)


def html_list_to_json(html_pages: list[str], source_url: str = "", fetched_at: str = "", timezone: str = "Asia/Shanghai") -> dict:
    """Parse multiple HTML pages of the same thread into one merged JSON dict.

    This is for paginated articles where page2 is `url-2`, page3 is `url-3`, etc.

    Behavior:
    - Page 1 provides series/post/op and meta.
    - Comments from all pages are concatenated and de-duplicated.
    - Comments are sorted by the same time-ordering rule already implemented in `extract_comments_strict`.

    Args:
      html_pages: list of rendered HTML strings (page1..N)
    """
    if not html_pages:
        # Keep schema consistent
        if not fetched_at:
            try:
                fetched_at = datetime.now(ZoneInfo(timezone)).isoformat(timespec="seconds")
            except Exception:
                fetched_at = datetime.now().isoformat(timespec="seconds")
        return {
            "source_url": source_url,
            "meta": {"fetched_at": fetched_at, "timezone": timezone},
            "series": "",
            "post": {"title": "", "published_at": "", "author": "", "is_op": 1, "content_lines": []},
            "comments": [],
        }

    # Parse page 1 as base
    base = html_to_json(html_pages[0], source_url=source_url, fetched_at=fetched_at, timezone=timezone)
    base.setdefault("meta", {})

    # Ensure fetched_at exists
    if not base["meta"].get("fetched_at"):
        if not fetched_at:
            try:
                fetched_at = datetime.now(ZoneInfo(timezone)).isoformat(timespec="seconds")
            except Exception:
                fetched_at = datetime.now().isoformat(timespec="seconds")
        base["meta"]["fetched_at"] = fetched_at

    # Merge pages
    merged_comments: list[dict] = []
    seen = set()

    for idx, html in enumerate(html_pages, start=1):
        try:
            obj = html_to_json(html, timezone=timezone)
        except Exception as e:
            continue

        for floor in (obj.get("comments") or []):
            k = _dedup_floor_key(floor)
            if k in seen:
                continue
            seen.add(k)
            merged_comments.append(floor)

    # Do NOT re-sort by raw string tokens; each page is already time-sorted using fetched_at normalization.
    # Keep the merged order (page1..N) stable to avoid breaking the corrected ordering.

    base["comments"] = merged_comments

    # If base source_url is empty, fill from first html
    if not base.get("source_url"):
        base["source_url"] = extract_source_url(html_pages[0])

    return base


def html_list_to_json_str(html_pages: list[str], source_url: str = "", fetched_at: str = "", timezone: str = "Asia/Shanghai") -> str:
    """JSON string wrapper for `html_list_to_json`."""
    return json.dumps(html_list_to_json(html_pages, source_url=source_url, fetched_at=fetched_at, timezone=timezone), ensure_ascii=False, indent=2)

def html_to_json_str(raw_html: str, source_url: str = "", fetched_at: str = "", timezone: str = "Asia/Shanghai") -> str:
    return json.dumps(html_to_json(raw_html, source_url=source_url, fetched_at=fetched_at, timezone=timezone), ensure_ascii=False, indent=2)

def html_to_clean_text(raw_html: str, max_depth: int = 3, debug: bool | None = None) -> str:
    series = extract_series(raw_html)
    title, content_lines = extract_post_strict(raw_html)
    published_at = extract_published_at(raw_html)
    op = extract_op_author(raw_html)

    comments = extract_comments_strict(raw_html, op, max_depth=max_depth, debug=debug)

    # 输出成你“写法A + is_op=0/1”的清洗格式
    out = []
    out += ["[series]", series, "", "[post]"]
    out += [f"title: {title}", f"published_at: {published_at}", f"author: {op}", "is_op: 1", "content:"]
    for ln in content_lines:
        out.append(f"- {ln}")
    out.append("")
    out.append("[comments]")
    for i, c in enumerate(comments, 1):
        out.append(f"#{i}")
        out.append(f"published_at: {c['published_at']}")
        out.append(f"author: {c['author']}")
        out.append(f"is_op: {c['is_op']}")
        out.append("content:")
        for ln in c.get('content_lines', []):
            out.append(f"- {ln}")

        if not c.get("replies"):
            out.append("replies: none")
            continue

        out.append("replies:")
        for r in c["replies"]:
            out.append("  - published_at: " + (r.get("published_at") or ""))
            out.append("    author: " + (r.get("author") or ""))
            out.append("    is_op: " + str(r.get("is_op", 0)))
            out.append("    content:")
            for ln in r.get('content_lines', []):
                out.append("    - " + ln)
            rt = r.get("reply_to", "none")
            if rt == "none" or rt is None:
                out.append("    reply_to: none")
            else:
                out.append(f"    reply_to: {rt.get('author_id','')} | {rt.get('content','')}")
    return "\n".join(out)

if __name__ == "__main__":
    import argparse
    from pathlib import Path
    from datetime import datetime
    from zoneinfo import ZoneInfo

    parser = argparse.ArgumentParser(description="Parse DCD HTML into cleaned text or JSON")
    parser.add_argument("html", help="Path to saved HTML file")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    parser.add_argument("--url", default="", help="Source URL to include in JSON")
    parser.add_argument("--timezone", default="Asia/Shanghai")
    parser.add_argument("--debug", action="store_true", help="Enable debug prints for comment parsing")

    args = parser.parse_args()
    raw = Path(args.html).read_text(encoding="utf-8", errors="ignore")
    fetched_at = datetime.now(ZoneInfo(args.timezone)).isoformat(timespec="seconds")

    if args.format == "json":
        # Support multiple html files: pass as "a.html,b.html,c.html"
        if "," in args.html:
            paths = [p.strip() for p in args.html.split(",") if p.strip()]
            html_pages = [Path(p).read_text(encoding="utf-8", errors="ignore") for p in paths]
            print(html_list_to_json_str(html_pages, source_url=args.url, fetched_at=fetched_at, timezone=args.timezone))
        else:
            op = extract_op_author(raw)
            comments = extract_comments_strict(raw, op, debug=args.debug)
            if not fetched_at:
                fetched_at = datetime.now(ZoneInfo(args.timezone)).isoformat(timespec="seconds")
            data = {
                "source_url": args.url or extract_source_url(raw),
                "meta": {"fetched_at": fetched_at, "timezone": args.timezone},
                "series": extract_series(raw),
                "post": {
                    "title": extract_post_strict(raw)[0],
                    "published_at": extract_published_at(raw),
                    "author": op,
                    "is_op": 1,
                    "content_lines": extract_post_strict(raw)[1],
                },
                "comments": comments,
            }
            print(json.dumps(data, ensure_ascii=False, indent=2))
    else:
        print(html_to_clean_text(raw, debug=args.debug))