from temporalio import activity
import aiohttp, os, json, asyncio, re, time, random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from playwright.async_api import async_playwright
from playwright._impl._errors import TargetClosedError
from bs4 import BeautifulSoup

BASE_URL = "http://www.law.go.kr"
DISPLAY = 100
TARGET_DAYS = 1

HEADERS = {
    "User-Agent": random.choice([
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0)"
    ])
}

link_cache = {}

# ---------- (A) 기존 상세 크롤링 유틸 거의 그대로 유지 ----------
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
        return f"https://www.law.go.kr/LSW/lsSideInfoP.do?lsNm={params[0]}&ancYd={params[1]}&urlMode=lsRvsDocInfoR&ancNo={params[2]}"
    return None

async def safe_goto(page, url):
    for _ in range(3):
        try:
            return await page.goto(url, timeout=90000, wait_until="networkidle")
        except:
            await asyncio.sleep(2)

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
        subs = await page.locator("div#viewwrapCenter, div.lawcon").all_text_contents()
        txt += "".join(subs)
    except:
        pass

    link_cache[url] = txt
    return txt

# ---------- (B) 목록 API ----------
async def fetch_list(session, page_no):
    url = f"{BASE_URL}/DRF/lawSearch.do"
    params = {
        "OC": "admin",
        "target": "eflaw",
        "type": "JSON",
        "sort": "ddes",
        "display": DISPLAY,
        "page": page_no
    }

    async with session.get(url, params=params, headers=HEADERS) as r:
        data = await r.json()
        return data["LawSearch"].get("law", [])

# ---------- (C) 핵심 Activity ----------
@activity.defn
async def law_activity():
    now = datetime.now(ZoneInfo("Asia/Seoul")).date()
    yesterday = now - timedelta(days=TARGET_DAYS)
    fmt = "%Y%m%d"

    collected = []
    page_no = 1

    async with aiohttp.ClientSession() as session:

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)

            while True:
                items = await fetch_list(session, page_no)

                # 종료조건 ①: display 미만
                if len(items) < DISPLAY:
                    print(f"[STOP] page={page_no} : {len(items)} < {DISPLAY}")
                    break

                # 종료조건 ②: 첫 아이템이 기준일 이전
                first_day = datetime.strptime(items[0]["공포일자"], fmt).date()
                if yesterday > first_day:
                    print(f"[STOP] 신규 법령 없음 (page={page_no})")
                    break

                for item in items:
                    cont_day = datetime.strptime(item["공포일자"], fmt).date()
                    if yesterday > cont_day:
                        print(f"[STOP] {item['공포일자']} 이전 → 이후 중단")
                        break

                    # ====== (중요) Playwright 상세 크롤링 시작 ======
                    page = await browser.new_page()
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
                        collected.append(item)

                    except TargetClosedError:
                        pass
                    finally:
                        await page.close()

                page_no += 1

            await browser.close()

    # ====== 저장 ======
    save_time = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%y%m%d_%H%M%S")
    out_dir = "/mnt/e/workspace/pytem/datacollection/crawl_results"
    os.makedirs(out_dir, exist_ok=True)

    file_path = f"{out_dir}/law_{save_time}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(collected, f, ensure_ascii=False, indent=2)

    print(f"[ACTIVITY] 저장 완료 → {file_path}")

    return {
        "file_path": file_path,
        "total_items": len(collected),
        "last_page": page_no
    }
