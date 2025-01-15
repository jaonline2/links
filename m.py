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
    start_urls =['https://www.airbnb.com/s/Madrid--Spain/homes', 'https://www.airbnb.com/s/Barcelona--Spain/homes', 'https://www.airbnb.com/s/Valencia--Spain/homes', 'https://www.airbnb.com/s/Seville--Spain/homes', 'https://www.airbnb.com/s/Zaragoza--Spain/homes', 'https://www.airbnb.com/s/Málaga--Spain/homes', 'https://www.airbnb.com/s/Murcia--Spain/homes', 'https://www.airbnb.com/s/Palma--Spain/homes', 'https://www.airbnb.com/s/Las-Palmas-de-Gran-Canaria--Spain/homes', 'https://www.airbnb.com/s/Bilbao--Spain/homes', 'https://www.airbnb.com/s/Alicante--Spain/homes', 'https://www.airbnb.com/s/Córdoba--Spain/homes', 'https://www.airbnb.com/s/Valladolid--Spain/homes', 'https://www.airbnb.com/s/Vigo--Spain/homes', 'https://www.airbnb.com/s/Gijón--Spain/homes', 'https://www.airbnb.com/s/Eixample--Spain/homes', "https://www.airbnb.com/s/L'Hospitalet-de-Llobregat--Spain/homes", 'https://www.airbnb.com/s/Latina--Spain/homes', 'https://www.airbnb.com/s/Carabanchel--Spain/homes', 'https://www.airbnb.com/s/A-Coruña--Spain/homes', 'https://www.airbnb.com/s/Puente-de-Vallecas--Spain/homes', 'https://www.airbnb.com/s/Granada--Spain/homes', 'https://www.airbnb.com/s/Elche--Spain/homes', 'https://www.airbnb.com/s/Sant-Martí--Spain/homes', 'https://www.airbnb.com/s/Terrassa--Spain/homes', 'https://www.airbnb.com/s/Badalona--Spain/homes', 'https://www.airbnb.com/s/Oviedo--Spain/homes', 'https://www.airbnb.com/s/Cartagena--Spain/homes', 'https://www.airbnb.com/s/Nou-Barris--Spain/homes', 'https://www.airbnb.com/s/San-Sebastián--Spain/homes', 'https://www.airbnb.com/s/Tetuán-de-las-Victorias--Spain/homes', 'https://www.airbnb.com/s/Santander--Spain/homes', 'https://www.airbnb.com/s/Jerez-de-la-Frontera--Spain/homes', 'https://www.airbnb.com/s/Marbella--Spain/homes', 'https://www.airbnb.com/s/Santa-Cruz-de-Tenerife--Spain/homes', 'https://www.airbnb.com/s/Almería--Spain/homes', 'https://www.airbnb.com/s/Burgos--Spain/homes', 'https://www.airbnb.com/s/Salamanca--Spain/homes', 'https://www.airbnb.com/s/Alcalá-de-Henares--Spain/homes', 'https://www.airbnb.com/s/Albacete--Spain/homes', 'https://www.airbnb.com/s/Getafe--Spain/homes', 'https://www.airbnb.com/s/Alcorcón--Spain/homes', 'https://www.airbnb.com/s/Huelva--Spain/homes', 'https://www.airbnb.com/s/Logroño--Spain/homes', 'https://www.airbnb.com/s/Badajoz--Spain/homes', 'https://www.airbnb.com/s/Tarragona--Spain/homes', 'https://www.airbnb.com/s/Parla--Spain/homes', 'https://www.airbnb.com/s/Sabadell--Spain/homes', 'https://www.airbnb.com/s/Móstoles--Spain/homes', 'https://www.airbnb.com/s/Lleida--Spain/homes', 'https://www.airbnb.com/s/Ciudad-Lineal--Spain/homes', 'https://www.airbnb.com/s/Chamberí--Spain/homes', 'https://www.airbnb.com/s/Pamplona--Spain/homes', 'https://www.airbnb.com/s/Fuencarral-El-Pardo--Spain/homes', 'https://www.airbnb.com/s/Chamartín--Spain/homes', 'https://www.airbnb.com/s/Ourense--Spain/homes', 'https://www.airbnb.com/s/Reus--Spain/homes', 'https://www.airbnb.com/s/San-Blas-Canillejas--Spain/homes', 'https://www.airbnb.com/s/Algeciras--Spain/homes', 'https://www.airbnb.com/s/Mataró--Spain/homes', 'https://www.airbnb.com/s/Leganés--Spain/homes', 'https://www.airbnb.com/s/Barakaldo--Spain/homes', 'https://www.airbnb.com/s/Castellón-de-la-Plana--Spain/homes', 'https://www.airbnb.com/s/San-Fernando--Spain/homes', 'https://www.airbnb.com/s/Torremolinos--Spain/homes', 'https://www.airbnb.com/s/Ceuta--Spain/homes', 'https://www.airbnb.com/s/Melilla--Spain/homes', 'https://www.airbnb.com/s/Torrevieja--Spain/homes', 'https://www.airbnb.com/s/Ferrol--Spain/homes', 'https://www.airbnb.com/s/Avilés--Spain/homes', 'https://www.airbnb.com/s/Ibiza--Spain/homes', 'https://www.airbnb.com/s/Elda--Spain/homes', 'https://www.airbnb.com/s/Benidorm--Spain/homes', 'https://www.airbnb.com/s/Santiago-de-Compostela--Spain/homes', 'https://www.airbnb.com/s/Viladecans--Spain/homes', 'https://www.airbnb.com/s/Fuengirola--Spain/homes', 'https://www.airbnb.com/s/Granollers--Spain/homes', 'https://www.airbnb.com/s/Manresa--Spain/homes', 'https://www.airbnb.com/s/Alcobendas--Spain/homes', 'https://www.airbnb.com/s/Calvià--Spain/homes', 'https://www.airbnb.com/s/Majadahonda--Spain/homes', 'https://www.airbnb.com/s/Sant-Cugat-del-Vallès--Spain/homes', 'https://www.airbnb.com/s/Sant-Boi-de-Llobregat--Spain/homes', 'https://www.airbnb.com/s/Cáceres--Spain/homes', 'https://www.airbnb.com/s/Pontevedra--Spain/homes', 'https://www.airbnb.com/s/Telde--Spain/homes', 'https://www.airbnb.com/s/Mijas--Spain/homes', 'https://www.airbnb.com/s/Arrecife--Spain/homes', 'https://www.airbnb.com/s/Benalmádena--Spain/homes', 'https://www.airbnb.com/s/Valdeorras--Spain/homes', 'https://www.airbnb.com/s/San-Pedro-del-Pinatar--Spain/homes', 'https://www.airbnb.com/s/Mula--Spain/homes', 'https://www.airbnb.com/s/Fuensalida--Spain/homes', 'https://www.airbnb.com/s/El-Campello--Spain/homes', 'https://www.airbnb.com/s/Llíria--Spain/homes', 'https://www.airbnb.com/s/Alcalá-la-Real--Spain/homes', 'https://www.airbnb.com/s/Vega-de-Granada--Spain/homes', 'https://www.airbnb.com/s/Albuñol--Spain/homes', 'https://www.airbnb.com/s/Águilas--Spain/homes', 'https://www.airbnb.com/s/Cabo-de-Gata--Spain/homes', 'https://www.airbnb.com/s/Los-Llanos-de-Aridane--Spain/homes', 'https://www.airbnb.com/s/La-Orotava--Spain/homes', 'https://www.airbnb.com/s/Santa-Cruz-de-la-Palma--Spain/homes', 'https://www.airbnb.com/s/Teguise--Spain/homes', 'https://www.airbnb.com/s/Puerto-del-Carmen--Spain/homes', 'https://www.airbnb.com/s/Puerto-Rico--Spain/homes', 'https://www.airbnb.com/s/Mogan--Spain/homes', 'https://www.airbnb.com/s/Playa-Blanca--Spain/homes', 'https://www.airbnb.com/s/Fuerteventura--Spain/homes', 'https://www.airbnb.com/s/La-Laguna--Spain/homes', 'https://www.airbnb.com/s/Lanzarote--Spain/homes', 'https://www.airbnb.com/s/El-Hierro--Spain/homes', 'https://www.airbnb.com/s/Tenerife--Spain/homes', 'https://www.airbnb.com/s/La-Gomera--Spain/homes', 'https://www.airbnb.com/s/San-Miguel-de-Abona--Spain/homes', 'https://www.airbnb.com/s/Vilaflor--Spain/homes', 'https://www.airbnb.com/s/Los-Realejos--Spain/homes', 'https://www.airbnb.com/s/Gran-Tarajal--Spain/homes', 'https://www.airbnb.com/s/Alajeró--Spain/homes', 'https://www.airbnb.com/s/Vega-de-Tenerife--Spain/homes', 'https://www.airbnb.com/s/Vallehermoso--Spain/homes', 'https://www.airbnb.com/s/Candelaria--Spain/homes', 'https://www.airbnb.com/s/Fauna-Canarias--Spain/homes', 'https://www.airbnb.com/s/El-Tamarguillo--Spain/homes', 'https://www.airbnb.com/s/Villas-de-Guancha--Spain/homes', 'https://www.airbnb.com/s/Las-Galletas--Spain/homes', 'https://www.airbnb.com/s/Puerto-de-Santiago--Spain/homes', 'https://www.airbnb.com/s/Santa-BRígida--Spain/homes', 'https://www.airbnb.com/s/San-Bartolomé-de-Tirajana--Spain/homes', 'https://www.airbnb.com/s/Costa-Tequise--Spain/homes', 'https://www.airbnb.com/s/Maspalomas--Spain/homes', 'https://www.airbnb.com/s/Playa-del-Inglés--Spain/homes', 'https://www.airbnb.com/s/Aranjuez--Spain/homes', 'https://www.airbnb.com/s/Chinchón--Spain/homes', 'https://www.airbnb.com/s/San-Vicente-del-Raspeig--Spain/homes', 'https://www.airbnb.com/s/Sueca--Spain/homes', 'https://www.airbnb.com/s/Cheste--Spain/homes', 'https://www.airbnb.com/s/Paterna--Spain/homes', 'https://www.airbnb.com/s/Albufereta--Spain/homes', 'https://www.airbnb.com/s/Xàtiva--Spain/homes', 'https://www.airbnb.com/s/Burjassot--Spain/homes', 'https://www.airbnb.com/s/Quart-de-Poblet--Spain/homes', 'https://www.airbnb.com/s/Torrent--Spain/homes', 'https://www.airbnb.com/s/Gandía--Spain/homes', 'https://www.airbnb.com/s/Oliva--Spain/homes', 'https://www.airbnb.com/s/Xàbia--Spain/homes', 'https://www.airbnb.com/s/Tavernes-de-la-Valldigna--Spain/homes', 'https://www.airbnb.com/s/Denia--Spain/homes', 'https://www.airbnb.com/s/Benissa--Spain/homes']





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
