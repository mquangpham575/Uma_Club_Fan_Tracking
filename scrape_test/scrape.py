import asyncio
import re
import zendriver as zd
import os

SEARCH_TERM = "yurimusume"
CLUB_ID_STARTING = "4155"

RESPONSES = []

REGEX = re.compile(
    rf".*/api/club_profile\?circle_id={CLUB_ID_STARTING}.*", re.IGNORECASE
)


async def resp_handler(e: zd.cdp.network.ResponseReceived):
    if REGEX.match(e.response.url):
        RESPONSES.append(e.request_id)


async def main():
    print("Đang lụm dữ liệu... ", end="", flush=True)

    browser = await zd.start(
        browser="edge",
        browser_executable_path="C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe",
    )

    page = await browser.get("https://chronogenesis.net/")

    club_profile = await page.select_all(".home-menu-item")
    await club_profile[1].click()
    await asyncio.sleep(1)

    page.add_handler(zd.cdp.network.ResponseReceived, resp_handler)

    search_box = await page.select(".club-id-input", timeout=20)
    await search_box.send_keys(SEARCH_TERM)
    await search_box.send_keys(zd.SpecialKeys.ENTER)
    await asyncio.sleep(1)

    try:
        results = await page.select_all(".club-results-row", timeout=3)

        for result in results:
            if SEARCH_TERM in str(result):
                await result.click()
                break
    except:
        pass

    await asyncio.sleep(3)

    largest_response = None
    largest_size = 0

    for request_id in RESPONSES:
        response_body, _ = await page.send(
            zd.cdp.network.get_response_body(request_id=request_id)
        )
        size = len(response_body)
        if size > largest_size:
            largest_size = size
            largest_response = response_body

    await browser.stop()

    output_path = os.path.join(os.path.dirname(__file__), "response.json")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(largest_response)
        
    print(f"✅ Saved to: {output_path}", end="", flush=True)

    print("✅ Xong!", end="", flush=True)


if __name__ == "__main__":
    asyncio.run(main())