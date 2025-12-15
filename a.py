import asyncio
import zendriver as zd
import os

async def main():
    print("Đang lụm dữ liệu... ", end="", flush=True)

    # Đường dẫn mặc định thường thấy của Brave trên Windows
    # Nếu không chạy, hãy kiểm tra lại xem Brave của bạn nằm ở 'Program Files' hay 'Program Files (x86)'
    brave_path = "C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe"
    
    # Kiểm tra file có tồn tại không để tránh lỗi
    if not os.path.exists(brave_path):
        brave_path = "C:/Program Files (x86)/BraveSoftware/Brave-Browser/Application/brave.exe"

    browser = await zd.start(
        browser="chrome",  # Brave dùng nhân Chromium nên cấu hình là "chrome"
        browser_executable_path=brave_path,
        headless=False # Thường cần để false để tránh bị chặn
    )
    
    try:
        page = await browser.get("https://google.com")

        # Regex để bắt request API
        async with page.expect_request(r".*\/api\/club_profile.*") as request_info:
            await page.get("https://chronogenesis.net/club_profile?circle_id=145606097")

            await request_info.value
            response_body, _ = await request_info.response_body

        # Có response rồi, làm gì thì làm
        with open("response.json", "w", encoding="utf-8") as f:
            f.write(response_body)
            
        print("✅ Xong!", end="", flush=True)

    finally:
        # Đảm bảo tắt trình duyệt dù có lỗi hay không
        await browser.stop()

if __name__ == "__main__":
    asyncio.run(main())