import asyncio

async def scrape_club_data(cfg: dict, zd):
    """
    Automates the browser to search for a club by ID on ChronoGenesis
    and captures the club_profile API response.
    """
    search_id = cfg.get('club_id')

    import sys
    is_linux = sys.platform != "win32"
    if not is_linux:
        executable = "C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe"
        browser_type = "edge"
    else:
        executable = "/usr/bin/google-chrome"
        browser_type = "chrome"

    # Optimization: Use headless=False even on Linux to leverage xvfb-run
    # This is less likely to be detected as a bot by services like Cloudflare.
    browser = await zd.start(
        browser=browser_type,
        browser_executable_path=executable,
        headless=False, 
        sandbox=False,
        browser_args=[
            "--disable-gpu",
            "--disable-dev-shm-usage",
            "--no-first-run",
            "--no-default-browser-check",
            "--window-size=1280,720",
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
        ],
        browser_connection_timeout=30.0,
        browser_connection_max_tries=60,
    )

    best_response = None
    try:
        captured_responses = {} # map request_id -> (url, body)

        async def resp_handler(*args, **kwargs):
            if args and hasattr(args[0], 'response'):
                 url = args[0].response.url
                 if "api.chronogenesis.net/club_profile" in url:
                     try:
                        captured_responses[args[0].request_id] = url
                     except Exception:
                        pass

        page = await browser.get("https://chronogenesis.net/club_profile")
        await page.send(zd.cdp.network.enable())
        page.add_handler(zd.cdp.network.ResponseReceived, resp_handler)

        # Give the page extra time to render slow UI components on limited-resource CI
        await asyncio.sleep(10)
        
        try:
            search_box = await page.select(".club-id-input", timeout=60)
        except asyncio.TimeoutError:
            try:
                title = await page.evaluate("document.title")
            except:
                title = "Unknown"
            url = page.url
            print(f"  [Scraper Error] search_box timeout at {url} (Title: {title})", flush=True)
            raise


        await search_box.send_keys(search_id)
        await search_box.send_keys(zd.SpecialKeys.ENTER)
        await asyncio.sleep(3)

        # Click the result to ensure full club data is loaded
        try:
            results = await page.select_all(".club-results-row", timeout=20)
            for result in results:
                content = result.text_all.lower()
                if search_id in content:
                    await result.click()
                    break
        except Exception:
            pass

        # Wait for background requests to complete
        await asyncio.sleep(8)

        target_url_prefix = f"https://api.chronogenesis.net/club_profile?circle_id={search_id}"

        print(f"  [Scraper] Captured {len(captured_responses)} club_profile requests.", flush=True)

        for req_id, url in captured_responses.items():
            try:
                 response_body, _ = await page.send(
                    zd.cdp.network.get_response_body(request_id=req_id)
                )
                 if url.startswith(target_url_prefix) or "club_friend_history" in response_body:
                     if best_response is None or "club_friend_history" in response_body:
                         best_response = response_body
                         print(f"  [Scraper] Selecting response: {url}", flush=True)
            except Exception:
                pass
    finally:
        await browser.stop()

    return best_response


