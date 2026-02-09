import asyncio, json, os, time, random, re
from datetime import datetime
from zoneinfo import ZoneInfo

import aiohttp
from bs4 import BeautifulSoup
from tqdm import tqdm
from playwright.async_api import async_playwright
from playwright._impl._errors import TargetClosedError
from temporalio import activity

BASE_URL = "http://www.law.go.kr"

# ───────────── 공통 상수 (네 코드 유지) ─────────────
START_PAGE = 50
END_PAGE = 66
WORKERS = 14
BROWSER_COUNT = 2
WORKERS_PER_BROWSER = WORKERS // BROWSER_COUNT

HEADERS = {
    "User-Agent": random.choice([
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0)"
    ]),
    "Accept": "text/html,application/json,*/*",
    "Referer": "https://www.law.go.kr/",
    "Connection": "keep-alive",
    "Accept-Language": "ko-KR,ko;q=0.9"
}

link_cache = {}

# ───────────── 유틸 (네 코드 거의 그대로) ─────────────

def clean_html(text: str):
    comments = re.findall(r"<!--.*?-->", text, flags=re.DOTALL)
    placeholders = {}

    for i, c in enumerate(comments):
        key = f"__COMMENT_{i}__"
        placeholders[key] = c
        text = text.replace(c, key)

    soup = BeautifulSoup(text, "html.parser")
    for tag in soup(["script", "style", "noscript", ".cont_icon"]):
        tag.decompose()

    r = soup.get_text("\n").strip()

    for key, comment in placeholders.items():
        r = r.replace(key, comment)

    return r


def build_detail_url(onclick, params):
    if "fncLsPttnLinkPop" in onclick:
        return f"https://www.law.go.kr/LSW/lsLinkCommonInfo.do?lspttninfSeq={params[0]}"
    if "fncLsLawPop" in onclick and not any(x in onclick for x in ["XX","BG","BF","BE"]):
        return f"https://www.law.go.kr/LSW/lsLinkCommonInfo.do?lsJoLnkSeq={params[0]}"
    if "fncArLawPop" in onclick:
        return (
            "https://www.law.go.kr/LSW/lsSideInfoP.do"
            f"?lsNm={params[0]}&ancYd={params[1]}"
            f"&urlMode=lsRvsDocInfoR&ancNo={params[2]}"
        )
    return None


async def safe_goto(page, url):
    for _ in range(3):
        try:
            return await page.goto(url, timeout=90000, wait_until="networkidle")
        except Exception:
            await asyncio.sleep(2)


async def fetch_list(session, page_no):
    params = {
        "OC": "admin",
        "target": "eflaw",
        "type": "JSON",
        "sort": "ddsc",
        "nw": "2,3",
        "display": 100,
        "page": page_no
    }
    url = f"{BASE_URL}/DRF/lawSearch.do"

    for retry in range(5):
        try:
            async with session.get(url, params=params, headers=HEADERS, timeout=30) as r:
                data = await r.json()
                return data["LawSearch"].get("law", [])
        except Exception as e:
            tqdm.write(f"[fetch_list] page {page_no} retry {retry+1}/5 | {e}")
            await asyncio.sleep(2)

    return []


async def fetch_detail(page, url):
    if url in link_cache:
        return link_cache[url]

    await safe_goto(page, url)
    await page.wait_for_timeout(random.uniform(150, 400))

    txt = ""
    try:
        t = await page.locator("div#rvsConTop, div#conTop").text_content()
        txt = f"({t})"
    except:
        pass

    try:
        subs = await page.locator(
            "div#viewwrapCenter, div.lawcon"
        ).all_text_contents()
        txt += "".join(subs)
    except:
        pass

    link_cache[url] = txt
    return txt


# ───────────── 핵심: ACTIVITY ─────────────

@activity.defn
async def crawl_law_page(page_no: int) -> str:
    """
    한 페이지 크롤링 전담 Activity
    → 결과 JSON 파일 경로 반환
    """

    start = time.time()

    async with aiohttp.ClientSession() as session:
        laws = await fetch_list(session, page_no)

    async with async_playwright() as p:
        browser_a = await p.chromium.launch(headless=True)
        browser_b = await p.chromium.launch(headless=True)

        async def process_with_browser(browser, items):
            contexts = [await browser.new_context() for _ in range(WORKERS_PER_BROWSER)]
            queue = asyncio.Queue()
            results = []
            lock = asyncio.Lock()

            for i, item in enumerate(items, start=1):
                await queue.put((i, item))

            async def worker(context):
                while True:
                    try:
                        idx, item = await queue.get()
                    except asyncio.CancelledError:
                        break

                    page = await context.new_page()
                    try:
                        url = BASE_URL + item["법령상세링크"]
                        await safe_goto(page, url)

                        frame = page.frame_locator("iframe")
                        try:
                            await frame.locator("a#closeModalBtn").click(timeout=500)
                        except:
                            pass

                        raw_html = await frame.locator("#conScroll").inner_html()
                        anchors = await frame.locator("a").element_handles()

                        parsed = {}
                        last_key = ""

                        for a in anchors:
                            outer = await (await a.get_property("outerHTML")).json_value() or ""
                            onclick = await a.get_attribute("onclick")
                            if not onclick:
                                continue

                            params = re.findall(r"'([^']+)'", onclick)
                            if "ALLJO" in onclick:
                                continue

                            if any(cls in outer for cls in ["sfon3","sfon4","sfon5"]):
                                txt = await a.text_content()
                                if last_key in parsed:
                                    prev_outer, prev_txt = last_key.split("_",1)
                                    url_val = parsed[last_key]
                                    parsed.pop(last_key)
                                    m = f"{prev_outer}{outer}_{prev_txt}{txt}"
                                    parsed[m] = url_val
                                    last_key = m
                                continue

                            durl = build_detail_url(onclick, params)
                            if durl:
                                txt = await a.text_content()
                                key = f"{outer}_{txt}"
                                parsed[key] = durl
                                last_key = key

                        for key, durl in parsed.items():
                            identifier = key.split("_")[0]
                            txt = await fetch_detail(page, durl)
                            raw_html = raw_html.replace(
                                identifier,
                                f"{key.split('_')[1]}\n[[{txt}]]\n"
                            )

                        item["법령"] = clean_html(raw_html)

                        async with lock:
                            results.append(item)

                    except TargetClosedError:
                        await context.close()
                        context = await browser.new_context()
                    finally:
                        await page.close()
                        queue.task_done()

            tasks = [asyncio.create_task(worker(ctx)) for ctx in contexts]
            await queue.join()

            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

            for ctx in contexts:
                await ctx.close()

            return results

        mid = len(laws) // 2
        r1 = asyncio.create_task(process_with_browser(browser_a, laws[:mid]))
        r2 = asyncio.create_task(process_with_browser(browser_b, laws[mid:]))

        out1, out2 = await asyncio.gather(r1, r2)
        results = out1 + out2

        await browser_a.close()
        await browser_b.close()

    today = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%y%m%d")
    path = (
        "/mnt/e/workspace/pytem/datacollection/downloads/law/"
        f"law_{page_no}_{today}.json"
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        f.write(json.dumps(results, ensure_ascii=False, indent=2)
                .replace("\\n", "\n"))

    elapsed = time.time() - start
    tqdm.write(f"[Activity] page {page_no} 완료 | {elapsed:.1f}초")

    return path
