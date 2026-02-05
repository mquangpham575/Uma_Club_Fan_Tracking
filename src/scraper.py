import asyncio
import json
import os
import re

import zendriver as zd


async def fetch_club_data_browser(club_cfg: dict):
    SEARCH_TERM = club_cfg["SEARCH_TERM"]
    CLUB_ID_STARTING = str(club_cfg["CLUB_ID_STARTING"])
    
    REGEX = re.compile(
        rf".*/api/club_profile\?circle_id={CLUB_ID_STARTING}.*", re.IGNORECASE
    )

    RESPONSES = [] 

    async def resp_handler(e: zd.cdp.network.ResponseReceived):
        if REGEX.match(e.response.url):
            RESPONSES.append(e.request_id)

    # Browser Path Setup
    brave_path = "C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe"
    if not os.path.exists(brave_path):
        brave_path = "C:/Program Files (x86)/BraveSoftware/Brave-Browser/Application/brave.exe"
    
    # OPTIMIZATION
    browser_args = [
        "--mute-audio",
        "--disable-extensions",
        "--window-position=-3000,0",             
        "--disable-background-timer-throttling", 
        "--disable-backgrounding-occluded-windows",
        "--disable-renderer-backgrounding",
        "--no-first-run",
        "--no-default-browser-check"
    ]

    # Use headless=False so the site doesn't block us, but args hide it
    browser = await zd.start(
        browser="edge", 
        browser_executable_path=brave_path,
        headless=False, 
        arguments=browser_args
    )

    try:
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
        except Exception:
            pass

        # Silent wait
        await asyncio.sleep(3)

        largest_response = None
        largest_size = 0

        if not RESPONSES:
            raise Exception("No API request matched.")

        for request_id in RESPONSES:
            try:
                response_body, _ = await page.send(
                    zd.cdp.network.get_response_body(request_id=request_id)
                )
                
                if isinstance(response_body, bytes) or isinstance(response_body, bytearray):
                      content = response_body.decode('utf-8', errors='replace')
                else:
                      content = str(response_body)

                size = len(content)
                if size > largest_size:
                    largest_size = size
                    largest_response = content
            except Exception:
                continue
        
        await browser.stop()
        
        if largest_response:
            return json.loads(largest_response)
        else:
            raise Exception("Empty response body.")

    except Exception as e:
        try:
            await browser.stop()
        except Exception:
            pass
        raise e
