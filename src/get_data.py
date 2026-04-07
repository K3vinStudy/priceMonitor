# 作者：郑凯峰
# 功能：从网站爬取讨论文本（非格式化）
# 输入：
# 输出：

from time import sleep
from bs4 import BeautifulSoup as bs
from playwright.sync_api import sync_playwright
import re

from get_resp import get_json
import config

def get_gids(rounds = 1) -> list:
    # rounds = 2      # 爬取轮数(每轮30条)
    base_url = config._ENV_CACHE["BASE_URL"]
    
    # headers = {
    #     "User-Agent": "Mozilla/5.0"
    # }
    # resp = requests.get(url, timeout=10).json()
    # print("status:", resp.status_code)
    # print("headers:", resp.headers)
    # print("text:", resp.text)        # 返回的原始文本内容
    # print(resp["data"]["topic_list"][0]["gid"])
    
    seen = set()
    url = config._ENV_CACHE["FIRST_URL"]
    count = 0
    while count < rounds:
        json = get_json(url).json()
        count += 1
        
        topic_list = json["data"]["topic_list"]
        has_more = json["data"]["has_more"]
        last_gid = json["data"]["last_id_str"]
        
        for item in topic_list:
            raw_gid = str(item.get("gid"))
            if raw_gid is None:
                continue
            
            gid = str(raw_gid)
            if gid in seen:
                continue
            
            seen.add(gid)
            print("Got gid: " + gid)
            # gids.append(gid)
        
        url = base_url + last_gid
        
        if not has_more:
            break
        if not topic_list:
            break
        
        sleep(2)
        
    # print(seen)
    # save_html_with_gid(seen)
    
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
    gids = get_gids
    print(gids)
    