import json
import threading
import asyncio
import os
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
from django.shortcuts import render
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup
from pydantic import BaseModel, HttpUrl, Field
from typing import Optional

# Store status for each user (this could be moved to a database if persistence is required)
scraping_status = {}

# Define the ScrapedData model using Pydantic for structured data
class ScrapedData(BaseModel):
    name: Optional[str] = Field(None, title="H1 Tag Content")
    meta_title: Optional[str] = Field(None, title="Meta Title")
    meta_description: Optional[str] = Field(None, title="Meta Description")
    meta_keywords: Optional[str] = Field(None, title="Meta Keywords")
    url: HttpUrl
    content: str
    Learn_More: str

    class Config:
        json_encoders = {HttpUrl: str}

# Asynchronous function to get URLs from a sitemap
async def get_sitemap_urls(sitemap_url):
    import aiohttp
    async with aiohttp.ClientSession() as session:
        async with session.get(sitemap_url) as response:
            sitemap_xml = await response.text()
    soup = BeautifulSoup(sitemap_xml, "xml")
    urls = [loc.get_text(strip=True) for loc in soup.find_all("loc")]
    return urls

# Function to remove unnecessary tags like headers, footers, and sidebars
def remove_header_footer(soup):
    for tag in soup.find_all(["header", "footer", "nav"]):
        tag.decompose()
    for tag in soup.find_all(class_=lambda x: x and ("cookie" in x.lower() or "sidebar" in x.lower())):
        tag.decompose()
    for p in soup.find_all("p"):
        if "cookie" in p.get_text().lower():
            p.decompose()

# Process a URL to extract content and metadata
async def process_url(crawler, url, total_chars, status):
    try:
        print(f"Starting to scrape {url}")  # Log when a URL starts scraping
        status["current_url"] = url
        result = await crawler.arun(url=url)
        html_content = getattr(result, 'html', result.markdown)
        soup = BeautifulSoup(html_content, "html.parser")

        h1_tag = soup.find('h1')
        h1_value = h1_tag.get_text(strip=True) if h1_tag else None

        remove_header_footer(soup)

        title_tag = soup.find('title')
        meta_title = title_tag.get_text(strip=True) if title_tag else None

        meta_desc_tag = soup.find('meta', attrs={'name': 'description'})
        meta_description = (meta_desc_tag['content'].strip() if meta_desc_tag and meta_desc_tag.has_attr('content') else None)

        meta_keywords_tag = soup.find('meta', attrs={'name': 'keywords'})
        meta_keywords = (meta_keywords_tag['content'].strip() if meta_keywords_tag and meta_keywords_tag.has_attr('content') else None)

        content = soup.get_text(separator="\n", strip=True)
        char_count = len(content)
        total_chars += char_count

        status["scraped_pages"] += 1
        status["total_characters_scraped"] = total_chars

        # Create a ScrapedData object and return it
        scraped_data = ScrapedData(
            name=h1_value,
            meta_title=meta_title,
            meta_description=meta_description,
            meta_keywords=meta_keywords,
            url=result.url,
            Learn_More=f"{h1_value} - For more info, go to {result.url}",
            content=content
        )

        status["rem_link"] = status.get("rem_link", 0) - 1
        new_data = scraped_data.dict()

        return new_data, total_chars
    except Exception as e:
        print(f"Error processing {url}: {e}")
        status["error"] = f"Error processing {url}: {str(e)}"
        return None, total_chars

# Function to scrape a single page
async def scrape_single_page_with_status(page_url, status):
    if status.get("rem_link", 0) <= 0:
        status["is_scraping"] = False
        status["error"] = "Your current plan doesn’t support fetching data from this URL — consider upgrading your plan."
        return

    status["is_scraping"] = True
    status["scraped_pages"] = 0
    status["remaining_pages"] = 1
    status["current_url"] = page_url
    status["total_characters_scraped"] = 0

    async with AsyncWebCrawler() as crawler:
        data, total_chars = await process_url(crawler, page_url, 0, status)
        if data:
            append_data_to_file(data, status['user_id'], status)

    status["scraped_pages"] = 1
    status["remaining_pages"] = 0
    status["is_scraping"] = False

# Function to scrape all URLs in a sitemap
async def scrape_sitemap_with_status(sitemap_url, status):
    try:
        sitemap_urls = await get_sitemap_urls(sitemap_url)
        total_urls = len(sitemap_urls)

        if total_urls == 0:
            status["is_scraping"] = False
            status["error"] = "No URLs found in the sitemap."
            return

        status["remaining_pages"] = total_urls
        status["scraped_pages"] = 0
        status["total_characters_scraped"] = 0

        async with AsyncWebCrawler() as crawler:
            for url in sitemap_urls:
                status["current_url"] = url
                try:
                    await process_url_with_status(crawler, url, status)
                except Exception as e:
                    status["error"] = f"Error processing URL {url}: {str(e)}"
                    break  # Stop scraping if there's an error

                status["scraped_pages"] += 1
                status["remaining_pages"] -= 1

                await asyncio.sleep(1)  # Sleep for a short duration to avoid overwhelming the server

        status["is_scraping"] = False
        status["remaining_pages"] = 0
        status["error"] = None
    except Exception as e:
        status["is_scraping"] = False
        status["error"] = f"Error during sitemap scraping: {str(e)}"
        print(f"Error during sitemap scraping: {str(e)}")

# Function to process each URL during sitemap scraping
async def process_url_with_status(crawler, url, status):
    try:
        new_data, total_chars = await process_url(crawler, url, status["total_characters_scraped"], status)
        
        if new_data:
            append_data_to_file(new_data, status['user_id'], status)
        
        status["total_characters_scraped"] += total_chars

    except Exception as e:
        status["error"] = f"Error processing URL {url}: {e}"
        print(f"Error processing {url}: {e}")

# Main scraper runner
async def run_scraper(scrape_mode, scrape_url, status):
    if scrape_mode == "single":
        await scrape_single_page_with_status(scrape_url, status)
    elif scrape_mode == "sitemap":
        await scrape_sitemap_with_status(scrape_url, status)
    else:
        status["is_scraping"] = False
        status["error"] = "Invalid scrape mode. Please choose 'single' or 'sitemap'."

# Threaded function to run the scraper in the background
def run_scraper_thread(scrape_mode, scrape_url, status):
    asyncio.run(run_scraper(scrape_mode, scrape_url, status))
    status["is_scraping"] = False

# API endpoint to start the scraper
@csrf_exempt
def api_scrape(request):
    if request.method == "POST":
        try:
            payload = json.loads(request.body)
            scrape_url = payload.get("scrape_url")
            scrape_mode = payload.get("scrape_mode")
            user_id = payload.get("user_id")
            rem_link = payload.get("rem_link")

            if not scrape_url or not scrape_mode or not user_id:
                return JsonResponse({"error": "Missing required parameters: 'scrape_url', 'scrape_mode', or 'user_id'."}, status=400)

            if rem_link is None or not isinstance(rem_link, int) or rem_link <= 0:
                return JsonResponse({"error": "Invalid or missing 'rem_link' parameter."}, status=400)

            status = {
                "scraped_pages": 0,
                "remaining_pages": 0,
                "current_url": None,
                "total_characters_scraped": 0,
                "is_scraping": True,
                "user_id": user_id,
                "file_size": 0,
                "rem_link": rem_link,
                "error": None,
            }

            scraping_status[user_id] = {"url": scrape_url, "mode": scrape_mode, "status": status}
            thread = threading.Thread(target=run_scraper_thread, args=(scrape_mode, scrape_url, status))
            thread.start()

            return JsonResponse({"status": "Scrape started", "scrape_url": scrape_url, "scrape_mode": scrape_mode})

        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON format in the request body."}, status=400)

    return JsonResponse({"error": "Invalid request method. Only POST requests are allowed."}, status=405)

# Get scrape status
def get_scrape_status(request, user_id):
    entry = scraping_status.get(user_id)
    if not entry:
        return JsonResponse({"error": "No scraping task found for this user_id."}, status=404)

    status = entry.get("status", {})
    all_urls = [status.get("current_url") or entry.get("url")]
    response_data = {
        "user_id": user_id,
        "url": entry.get("url"),
        "mode": entry.get("mode"),
        "scraped_pages": status.get("scraped_pages", 0),
        "total_characters_scraped": status.get("total_characters_scraped", 0),
        "is_scraping": status.get("is_scraping", False),
        "remaining_pages": status.get("remaining_pages", 0),
        "current_url": status.get("current_url") or entry.get("url"),
        "file_size": status.get("file_size", 0),
        "all_urls": all_urls,
        "error": status.get("error", ""),
    }
    return JsonResponse(response_data)

# Function to append scraped data to a file
def append_data_to_file(new_data, agent_id, status):
    try:
        # Convert HttpUrl fields to string if they are not None
        if isinstance(new_data.get('url'), HttpUrl):
            new_data['url'] = str(new_data['url'])
        if isinstance(new_data.get('Learn_More'), HttpUrl):
            new_data['Learn_More'] = str(new_data['Learn_More'])

        # Define the file path using the agent_id
        file_name = f"bol7_data_{agent_id}.json"
        file_path = os.path.join("data", file_name)

        # Create the directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        existing_data = []
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            with open(file_path, "r", encoding="utf-8") as f:
                try:
                    existing_data = json.load(f)
                except json.JSONDecodeError:
                    existing_data = []

        existing_data.append(new_data)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, indent=4, ensure_ascii=False)

        # Calculate the file size after writing
        file_size = os.path.getsize(file_path)
        status['file_size'] = file_size
    except Exception as e:
        print(f"Error writing data to file: {e}")
        status["error"] = f"Error writing data to file: {str(e)}"

# Django view for scrape page
def scrape(request):
    return render(request, 'index.html')
