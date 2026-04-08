# 作者：郑凯峰
# 功能：从网站爬取讨论文本（非格式化）
# 输入：
# 输出：

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup as bs
from time import sleep
import random
import re
import logging

from get_resp import get_json
import config

def get_gids(rounds:int = 1) -> list:
    # rounds = 2      # 爬取轮数(每轮30条)
    base_url = config._ENV_CACHE["BASE_URL"]
    logger = logging.getLogger(__name__)

    seen = set()
    url = config._ENV_CACHE["FIRST_URL"]
    count = 0

    # 单页请求重试参数
    max_retries = 3                  # 每一轮最多尝试 3 次
    base_retry_delay = 2.5           # 首次重试前基础等待秒数
    max_retry_delay = 20.0           # 单次重试等待上限

    # 轮次之间节流参数：尽量模拟更自然的访问节奏
    round_delay_min = 3.5
    round_delay_max = 7.5

    while count < rounds:
        page_no = count + 1
        success = False

        for attempt in range(max_retries + 1):
            try:
                # 每次真正发请求前都加一点小抖动，降低固定节奏
                pre_delay = random.uniform(0.6, 1.8) if attempt == 0 else random.uniform(1.2, 3.0)
                sleep(pre_delay)

                json_data = get_json(url).json()
                if not isinstance(json_data, dict):
                    raise RuntimeError(f"get_gids 返回的 json_data 不是 dict: {type(json_data)}")
                success = True
                break

            except Exception as e:
                is_last_attempt = attempt >= max_retries
                if is_last_attempt:
                    logger.exception(
                        "get_gids 请求失败且已达到重试上限。page_no=%s, url=%s",
                        page_no,
                        url,
                    )
                    raise RuntimeError(
                        f"get_gids 在第 {page_no} 轮请求失败，且重试 {max_retries} 次后仍未成功: {e}"
                    ) from e

                # 指数退避 + 随机抖动，减少被风控和瞬时网络抖动影响
                retry_delay = min(max_retry_delay, base_retry_delay * (2 ** attempt))
                retry_delay += random.uniform(0.8, 2.6)
                print(
                    f"get_gids 第 {page_no} 轮第 {attempt + 1} 次请求失败: {e}，"
                    f"将在 {retry_delay:.1f} 秒后重试"
                )
                sleep(retry_delay)

        if not success:
            break

        count += 1

        data = json_data.get("data") or {}
        topic_list = data.get("topic_list") or []
        last_gid = data.get("last_id_str")

        page_seen_before = len(seen)
        for item in topic_list:
            raw_gid = item.get("gid")
            if raw_gid is None:
                continue

            gid = str(raw_gid)
            if gid in seen:
                continue

            seen.add(gid)
            print("Got gid: " + gid)
        new_gid_count = len(seen) - page_seen_before

        # Added block for new_gid_count calculation as per instructions
        # (This block is intentionally after the main loop, as per step 2)
        # Note: The above code already calculates new_gid_count, so step 2 is satisfied.

        print(
            f"[GID] page={page_no}, topic_count={len(topic_list)}, "
            f"new_gid_count={new_gid_count}, last_gid={last_gid}"
        )

        # 不再依赖 has_more；目标站点尾页可能错误返回 has_more=True，但下一页实际为空页
        if not topic_list:
            logger.info("get_gids 到达尾页：topic_list 为空（空页）。page_no=%s, url=%s", page_no, url)
            break
        if new_gid_count == 0:
            logger.info("get_gids 到达尾页：本页没有新增 gid。page_no=%s, url=%s", page_no, url)
            break
        if not last_gid:
            logger.warning(
                "get_gids 未拿到 last_id_str，按尾页处理并结束。page_no=%s, url=%s, topic_count=%s",
                page_no,
                url,
                len(topic_list),
            )
            break

        url = base_url + str(last_gid)

        if count >= rounds:
            break

        # 轮次之间增加更自然的随机延迟；连续请求越久，额外加一点缓冲
        round_delay = random.uniform(round_delay_min, round_delay_max)
        if count % 5 == 0:
            round_delay += random.uniform(4.0, 8.0)
        print(f"第 {page_no} 轮完成，等待 {round_delay:.1f} 秒后继续")
        sleep(round_delay)

    return seen


def fetch_rendered_html(url: str, max_pages: int = 50) -> list[str]:
    """Render a DCD article page (and its paginated pages) and return a list of HTML strings.

    Page 1: <url>
    Page 2+: <url>-2, <url>-3, ...

    If a page does not exist, it usually renders only the post without comments; we stop when
    we detect 0 comment floors on that page.
    """

    def _detect_total_pages(first_html: str) -> int:
        # Try to detect total pages from pagination items (1..N)
        soup = bs(first_html, "lxml")
        nums = []
        for li in soup.select('li[class*="pagination-item"]'):
            txt = li.get_text(" ", strip=True)
            m = re.search(r"\b(\d{1,4})\b", txt)
            if m:
                try:
                    nums.append(int(m.group(1)))
                except Exception:
                    pass
        return max(nums) if nums else 1

    def _looks_like_empty_page(html: str) -> bool:
        # Non-existent pages often contain no "评论发表于" at all.
        return "评论发表于" not in html

    def _build_page_url(base: str, page_no: int) -> str:
        return base if page_no <= 1 else f"{base}-{page_no}"

    def _render_single(page, page_url: str) -> str:
        page.goto(page_url, wait_until="networkidle", timeout=45000)
        page.wait_for_timeout(1500)

        # --- Begin expand replies/buttons logic ---
        def click_expand_buttons(max_rounds: int = 20) -> int:
            clicks = 0
            locators = [
                page.get_by_text(re.compile(r"全部\d+条回复")),
                page.get_by_text(re.compile(r"展开更多")),
                page.get_by_text(re.compile(r"查看更多")),
                page.get_by_text(re.compile(r"加载更多")),
            ]

            xpath_fallback = page.locator(
                "xpath=//a[contains(.,'全部') and contains(.,'回复')] | //button[contains(.,'全部') and contains(.,'回复')]"
            )

            for _ in range(max_rounds):
                made_progress = False

                for loc in locators:
                    try:
                        n = loc.count()
                    except Exception:
                        n = 0
                    for i in range(n):
                        try:
                            el = loc.nth(i)
                            if el.is_visible():
                                el.click(timeout=800)
                                clicks += 1
                                made_progress = True
                                page.wait_for_timeout(300)
                        except Exception:
                            pass

                try:
                    n = xpath_fallback.count()
                except Exception:
                    n = 0
                for i in range(n):
                    try:
                        el = xpath_fallback.nth(i)
                        if el.is_visible():
                            el.click(timeout=800)
                            clicks += 1
                            made_progress = True
                            page.wait_for_timeout(300)
                    except Exception:
                        pass

                if not made_progress:
                    break

            return clicks

        def scroll_to_bottom(max_steps: int = 20):
            last_h = page.evaluate("() => document.body.scrollHeight")
            for _ in range(max_steps):
                page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(800)
                new_h = page.evaluate("() => document.body.scrollHeight")
                if new_h == last_h:
                    break
                last_h = new_h

        for _ in range(8):
            click_expand_buttons(max_rounds=10)
            scroll_to_bottom(max_steps=10)
            page.wait_for_timeout(500)
        # --- End expand replies/buttons logic ---

        return page.content()

    html_pages: list[str] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            accept_downloads=True,
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            locale="zh-CN",
            viewport={"width": 1440, "height": 900},
        )
        page = context.new_page()

        try:
            first_html = _render_single(page, url)
            html_pages.append(first_html)

            total_pages = _detect_total_pages(first_html)
            total_pages = min(total_pages, max_pages)

            for page_no in range(2, total_pages + 1):
                page_url = _build_page_url(url, page_no)
                html = _render_single(page, page_url)
                if _looks_like_empty_page(html):
                    break
                html_pages.append(html)

            browser.close()
            return html_pages
        except Exception as e:
            browser.close()
            raise RuntimeError(f"页面没有正常渲染，原始报错：{e}") from e




if __name__ == "__main__":
    gids = get_gids()
    print(gids)