import aiohttp
import asyncio
from bs4 import BeautifulSoup
import re
import time
import sys
import logging

logging.basicConfig(level=logging.INFO)

BASE_URL = "http://tv.roarzone.info/"
PLAYER_URL_TEMPLATE = "http://tv.roarzone.info/player.php?stream={}"


async def fetch_main_page(session):
    """Fetches the main page content."""
    print("Fetching main channel list...")
    try:
        async with session.get(BASE_URL) as response:
            if response.status == 200:
                return await response.text()
            else:
                print(f"Error fetching main page: {response.status}")
                return None
    except Exception as e:
        logging.exception(f"Exception fetching main page: {e}")
        print(f"Exception fetching main page: {e}")
        return None


async def process_channel(session, channel, semaphore):
    """Process a single channel to find its auth token."""
    async with semaphore:
        stream_path = channel.get("stream_path")
        name = channel.get("name")
        if not stream_path:
            return None
        player_url = PLAYER_URL_TEMPLATE.format(stream_path)
        try:
            async with session.get(player_url) as response:
                if response.status == 200:
                    text = await response.text()
                    m3u8_matches = re.findall(
                        "https?://[^\\s\\\"'<>]+\\.m3u8[^\\s\\\"'<>]*", text
                    )
                    if m3u8_matches:
                        channel["m3u8_url"] = m3u8_matches[0]
                        print(f"[OK] Found token for {name}")
                        return channel
                    else:
                        print(f"[ERR] No m3u8 found for {name}")
                else:
                    print(f"[ERR] HTTP {response.status} for {name}")
        except Exception as e:
            logging.exception(f"[EXC] Error processing {name}: {e}")
            print(f"[EXC] Error processing {name}: {e}")
        return None


async def main():
    start_time = time.time()
    print("Starting scraper job...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": BASE_URL,
    }
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        html = await fetch_main_page(session)
        if not html:
            print("Failed to retrieve channel list. Exiting.")
            return
        soup = BeautifulSoup(html, "html.parser")
        channel_cards = soup.find_all("div", class_="channel-card")
        extracted_channels = []
        for idx, card in enumerate(channel_cards):
            stream_path = card.get("data-stream", "")
            title = card.get("data-title", "")
            tags = card.get("data-tags", "")
            img = card.find("img")
            logo = img.get("src", "") if img else ""
            if logo and (not logo.startswith("http")):
                logo = f"{BASE_URL.rstrip('/')}/{logo.lstrip('/')}"
            if not title and img:
                title = img.get("alt", f"Channel {idx + 1}")
            if stream_path:
                extracted_channels.append(
                    {
                        "name": title,
                        "logo": logo,
                        "stream_path": stream_path,
                        "tags": tags,
                    }
                )
        total_channels = len(extracted_channels)
        print(
            f"Found {total_channels} channels. Starting concurrent token extraction (75 workers)..."
        )
        semaphore = asyncio.Semaphore(75)
        tasks = [process_channel(session, c, semaphore) for c in extracted_channels]
        results = await asyncio.gather(*tasks)
        valid_channels = [r for r in results if r is not None]
        print(
            f"\nScraping complete. Found {len(valid_channels)} valid streams out of {total_channels}."
        )
        filename = "playlist.m3u"
        print(f"Generating M3U playlist: {filename}")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            f.write("# Made with love by Mahamudun Nabi Siam\n")
            f.write(f"# Generated at {time.ctime()}\n\n")
            for channel in valid_channels:
                name = channel["name"].replace(",", " ")
                logo = channel["logo"]
                group = channel["tags"] or "Uncategorized"
                url = channel["m3u8_url"]
                f.write(
                    f'#EXTINF:-1 tvg-id="{name}" tvg-name="{name}" tvg-logo="{logo}" group-title="{group}",{name}\n'
                )
                f.write(f"{url}\n")
        print("Playlist file created successfully.")
        print(f"File saved locally as: {filename}")
    print(f"Total execution time: {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError as e:
        logging.exception(f"Error getting running loop: {e}")
        loop = None
    if loop and loop.is_running():
        print("Event loop already running. Scheduling main task...")
        loop.create_task(main())
    else:
        asyncio.run(main())
