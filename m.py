import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import nest_asyncio
import json
import requests
from lxml import html
from urllib.parse import urljoin
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from io import BytesIO
import csv
from datetime import datetime
from datetime import timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json
import io
import os
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# Function to generate a unique filename
def generate_unique_filename(base_name):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"{base_name}_{timestamp}"
from datetime import datetime
import random
import time
import string
# Load service account credentials
# Load JSON data from file.json
with open('file.json', 'r') as f:
    key_data = json.load(f)


SCOPES = ['https://www.googleapis.com/auth/drive.file']
credentials = service_account.Credentials.from_service_account_info(key_data, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=credentials)
nest_asyncio.apply()

async def scrape_hrefs(page):
    """Scrape href attributes from the current page."""
    hrefs = await page.locator("//a").evaluate_all(
        "elements => elements.map(el => el.href)"
    )
    room_links = [href for href in hrefs if "/rooms/" in href]  # Filter only Airbnb listing links
    return room_links

async def scrape_single_url(start_url, all_links):
    """Scrape Airbnb links from a single URL."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, slow_mo=50)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        page = await context.new_page()

        try:
            await page.goto(start_url, timeout=120000)
            await page.wait_for_timeout(5000)  # Wait for the page to load

            while True:
                # Scrape hrefs from the current page
                hrefs = await scrape_hrefs(page)

                # Add unique links to the all_links set (no need to manually check duplicates)
                all_links.update(hrefs)  # Using set update() ensures no duplicates

                print(f"Collected {len(all_links)} unique Airbnb links so far.")

                # Locate the "Next" button
                next_button = page.locator('//a[@aria-label="Next"]')
                if await next_button.is_visible():
                    print("Next button found. Moving to next page.")
                    await next_button.click()
                    await page.wait_for_timeout(5000)  # Wait for the next page to load
                else:
                    print("Next button not found or not visible. Exiting loop.")
                    break

        except Exception as e:
            print(f"An error occurred while scraping {start_url}: {e}")

        finally:
            await browser.close()
def upload_file_to_folder(file_name, file_path, folder_id):
    try:
        file_metadata = {
            'name': file_name,
            'parents': [folder_id]
        }

        # Check if file exists before uploading
        if not os.path.exists(file_path):
            print(f"Error: {file_path} does not exist.")
            return None

        with open(file_path, 'rb') as f:
            media = MediaIoBaseUpload(f, mimetype='text/csv')
            file = drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()

        print(f"File ID: {file.get('id')} uploaded to folder ID {folder_id}")
        return file.get('id')
    except Exception as e:
        print(f"Error uploading file: {e}")
        return None



async def main():
    start_urls =[
    "https://www.airbnb.com/s/London--United-Kingdom/homes",
    "https://www.airbnb.com/s/Edinburgh--United-Kingdom/homes",
    "https://www.airbnb.com/s/Manchester--United-Kingdom/homes",
    "https://www.airbnb.com/s/Birmingham--United-Kingdom/homes",
    "https://www.airbnb.com/s/Bristol--United-Kingdom/homes",
    "https://www.airbnb.com/s/Glasgow--United-Kingdom/homes",
    "https://www.airbnb.com/s/Brighton--United-Kingdom/homes",
    "https://www.airbnb.com/s/Liverpool--United-Kingdom/homes",
    "https://www.airbnb.com/s/Oxford--United-Kingdom/homes",
    "https://www.airbnb.com/s/Cambridge--United-Kingdom/homes",
    "https://www.airbnb.com/s/York--United-Kingdom/homes",
    "https://www.airbnb.com/s/Bath--United-Kingdom/homes",
    "https://www.airbnb.com/s/Cardiff--United-Kingdom/homes",
    "https://www.airbnb.com/s/Leeds--United-Kingdom/homes",
    "https://www.airbnb.com/s/Chester--United-Kingdom/homes",
    "https://www.airbnb.com/s/Winchester--United-Kingdom/homes",
    "https://www.airbnb.com/s/Nottingham--United-Kingdom/homes",
    "https://www.airbnb.com/s/Newcastle--United-Kingdom/homes",
    "https://www.airbnb.com/s/Sheffield--United-Kingdom/homes",
    "https://www.airbnb.com/s/Canterbury--United-Kingdom/homes",
    "https://www.airbnb.com/s/Norwich--United-Kingdom/homes",
    "https://www.airbnb.com/s/Exeter--United-Kingdom/homes",
    "https://www.airbnb.com/s/Leicester--United-Kingdom/homes",
    "https://www.airbnb.com/s/Swansea--United-Kingdom/homes",
    "https://www.airbnb.com/s/Portsmouth--United-Kingdom/homes",
    "https://www.airbnb.com/s/Durham--United-Kingdom/homes",
    "https://www.airbnb.com/s/Aberdeen--United-Kingdom/homes",
    "https://www.airbnb.com/s/Inverness--United-Kingdom/homes",
    "https://www.airbnb.com/s/Stirling--United-Kingdom/homes",
    "https://www.airbnb.com/s/Perth--United-Kingdom/homes",
    "https://www.airbnb.com/s/Dundee--United-Kingdom/homes",
    "https://www.airbnb.com/s/Blackpool--United-Kingdom/homes",
    "https://www.airbnb.com/s/Plymouth--United-Kingdom/homes",
    "https://www.airbnb.com/s/Isle-of-Skye--United-Kingdom/homes",
    "https://www.airbnb.com/s/Cotswolds--United-Kingdom/homes",
    "https://www.airbnb.com/s/Lake-District--United-Kingdom/homes",
    "https://www.airbnb.com/s/Snowdonia--United-Kingdom/homes",
    "https://www.airbnb.com/s/Cornwall--United-Kingdom/homes",
    "https://www.airbnb.com/s/Devon--United-Kingdom/homes",
    "https://www.airbnb.com/s/Dorset--United-Kingdom/homes",
    "https://www.airbnb.com/s/Kent--United-Kingdom/homes",
    "https://www.airbnb.com/s/Sussex--United-Kingdom/homes",
    "https://www.airbnb.com/s/Yorkshire-Dales--United-Kingdom/homes",
    "https://www.airbnb.com/s/Northumberland--United-Kingdom/homes",
    "https://www.airbnb.com/s/Peak-District--United-Kingdom/homes",
    "https://www.airbnb.com/s/Isle-of-Wight--United-Kingdom/homes",
    "https://www.airbnb.com/s/Anglesey--United-Kingdom/homes",
    "https://www.airbnb.com/s/Gloucester--United-Kingdom/homes",
    "https://www.airbnb.com/s/Lincoln--United-Kingdom/homes",
    "https://www.airbnb.com/s/Stratford-upon-Avon--United-Kingdom/homes",
    "https://www.airbnb.com/s/Warwick--United-Kingdom/homes",
    "https://www.airbnb.com/s/Loughborough--United-Kingdom/homes",
    "https://www.airbnb.com/s/Sunderland--United-Kingdom/homes",
    "https://www.airbnb.com/s/Derby--United-Kingdom/homes",
    "https://www.airbnb.com/s/Wolverhampton--United-Kingdom/homes",
    "https://www.airbnb.com/s/Lichfield--United-Kingdom/homes",
    "https://www.airbnb.com/s/Belfast--United-Kingdom/homes",
    "https://www.airbnb.com/s/Derry--United-Kingdom/homes",
    "https://www.airbnb.com/s/Newry--United-Kingdom/homes",
    "https://www.airbnb.com/s/Bangor--United-Kingdom/homes",
    "https://www.airbnb.com/s/Coleraine--United-Kingdom/homes",
    "https://www.airbnb.com/s/Larne--United-Kingdom/homes",
    "https://www.airbnb.com/s/Carrickfergus--United-Kingdom/homes",
    "https://www.airbnb.com/s/Falkirk--United-Kingdom/homes",
    "https://www.airbnb.com/s/Oban--United-Kingdom/homes",
    "https://www.airbnb.com/s/Fort-William--United-Kingdom/homes",
    "https://www.airbnb.com/s/Keswick--United-Kingdom/homes",
    "https://www.airbnb.com/s/Windermere--United-Kingdom/homes",
    "https://www.airbnb.com/s/Ambleside--United-Kingdom/homes",
    "https://www.airbnb.com/s/Scarborough--United-Kingdom/homes",
    "https://www.airbnb.com/s/Whitby--United-Kingdom/homes",
    "https://www.airbnb.com/s/Filey--United-Kingdom/homes",
    "https://www.airbnb.com/s/Bridlington--United-Kingdom/homes",
    "https://www.airbnb.com/s/Beverley--United-Kingdom/homes",
    "https://www.airbnb.com/s/Hexham--United-Kingdom/homes",
    "https://www.airbnb.com/s/Alnwick--United-Kingdom/homes",
    "https://www.airbnb.com/s/Berwick-upon-Tweed--United-Kingdom/homes",
    "https://www.airbnb.com/s/Saint-Ives--United-Kingdom/homes",
    "https://www.airbnb.com/s/Newquay--United-Kingdom/homes",
    "https://www.airbnb.com/s/Padstow--United-Kingdom/homes",
    "https://www.airbnb.com/s/Falmouth--United-Kingdom/homes",
    "https://www.airbnb.com/s/Penzance--United-Kingdom/homes",
    "https://www.airbnb.com/s/Truro--United-Kingdom/homes",
    "https://www.airbnb.com/s/St-Austell--United-Kingdom/homes",
    "https://www.airbnb.com/s/Bodmin--United-Kingdom/homes",
    "https://www.airbnb.com/s/Torquay--United-Kingdom/homes",
    "https://www.airbnb.com/s/Paignton--United-Kingdom/homes",
    "https://www.airbnb.com/s/Brixham--United-Kingdom/homes",
    "https://www.airbnb.com/s/Exmouth--United-Kingdom/homes",
    "https://www.airbnb.com/s/Barnstaple--United-Kingdom/homes",
    "https://www.airbnb.com/s/Ilfracombe--United-Kingdom/homes",
    "https://www.airbnb.com/s/Salcombe--United-Kingdom/homes",
    "https://www.airbnb.com/s/Dartmouth--United-Kingdom/homes"
]





    all_links = set()  # Use a set to store unique Airbnb links

    for url in start_urls:
        print(f"Scraping {url}...")
        await scrape_single_url(url, all_links)

    print("\nScraping completed. Saving results...")



    # Upload the CSV file to Google Drive
    folder_id = '1MP5GR_GFxe8x4eEE-A-uOLaLPeq37Yg1'  # Replace with your actual Google Drive folder ID
    # Save the results into a pandas DataFrame and then to CSV
    if all_links:
        links_df = pd.DataFrame(list(all_links), columns=["Airbnb Links"])
        links_df["Airbnb Links"] = links_df["Airbnb Links"].str.split("?").str[0]  # Remove query strings
        links_df = links_df.drop_duplicates()  # Remove duplicate links

        csv_file_name = "airbnb_links_france.csv"  # Define the output file name
        links_df.to_csv(csv_file_name, index=False)
        print(f"Airbnb links have been saved to '{csv_file_name}'.")

        # Upload the CSV file to Google Drive
        folder_id = '1MP5GR_GFxe8x4eEE-A-uOLaLPeq37Yg1'  # Replace with your actual Google Drive folder ID
        upload_file_to_folder(csv_file_name, csv_file_name, folder_id)
    else:
        print("No Airbnb links found to save.")

if __name__ == "__main__":
    asyncio.run(main())
