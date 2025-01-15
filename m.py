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
    start_urls = [
    "https://www.airbnb.com/s/Madrid--Spain",
    "https://www.airbnb.com/s/Barcelona--Spain",
    "https://www.airbnb.com/s/Valencia--Spain",
    "https://www.airbnb.com/s/Seville--Spain",
    "https://www.airbnb.com/s/Zaragoza--Spain",
    "https://www.airbnb.com/s/Málaga--Spain",
    "https://www.airbnb.com/s/Murcia--Spain",
    "https://www.airbnb.com/s/Palma--Spain",
    "https://www.airbnb.com/s/Las-Palmas-de-Gran-Canaria--Spain",
    "https://www.airbnb.com/s/Bilbao--Spain",
    "https://www.airbnb.com/s/Alicante--Spain",
    "https://www.airbnb.com/s/Córdoba--Spain",
    "https://www.airbnb.com/s/Valladolid--Spain",
    "https://www.airbnb.com/s/Vigo--Spain",
    "https://www.airbnb.com/s/Gijón--Spain",
    "https://www.airbnb.com/s/Eixample--Spain",
    "https://www.airbnb.com/s/L'Hospitalet-de-Llobregat--Spain",
    "https://www.airbnb.com/s/Latina--Spain",
    "https://www.airbnb.com/s/Carabanchel--Spain",
    "https://www.airbnb.com/s/A-Coruña--Spain",
    "https://www.airbnb.com/s/Puente-de-Vallecas--Spain",
    "https://www.airbnb.com/s/Granada--Spain",
    "https://www.airbnb.com/s/Elche--Spain",
    "https://www.airbnb.com/s/Sant-Martí--Spain",
    "https://www.airbnb.com/s/Terrassa--Spain",
    "https://www.airbnb.com/s/Badalona--Spain",
    "https://www.airbnb.com/s/Oviedo--Spain",
    "https://www.airbnb.com/s/Cartagena--Spain",
    "https://www.airbnb.com/s/Nou-Barris--Spain",
    "https://www.airbnb.com/s/San-Sebastián--Spain",
    "https://www.airbnb.com/s/Tetuán-de-las-Victorias--Spain",
    "https://www.airbnb.com/s/Santander--Spain",
    "https://www.airbnb.com/s/Jerez-de-la-Frontera--Spain",
    "https://www.airbnb.com/s/Marbella--Spain",
    "https://www.airbnb.com/s/Santa-Cruz-de-Tenerife--Spain",
    "https://www.airbnb.com/s/Almería--Spain",
    "https://www.airbnb.com/s/Burgos--Spain",
    "https://www.airbnb.com/s/Salamanca--Spain",
    "https://www.airbnb.com/s/Alcalá-de-Henares--Spain",
    "https://www.airbnb.com/s/Albacete--Spain",
    "https://www.airbnb.com/s/Getafe--Spain",
    "https://www.airbnb.com/s/Alcorcón--Spain",
    "https://www.airbnb.com/s/Huelva--Spain",
    "https://www.airbnb.com/s/Logroño--Spain",
    "https://www.airbnb.com/s/Badajoz--Spain",
    "https://www.airbnb.com/s/Tarragona--Spain",
    "https://www.airbnb.com/s/Parla--Spain",
    "https://www.airbnb.com/s/Sabadell--Spain",
    "https://www.airbnb.com/s/Móstoles--Spain",
    "https://www.airbnb.com/s/Lleida--Spain",
    "https://www.airbnb.com/s/Ciudad-Lineal--Spain",
    "https://www.airbnb.com/s/Chamberí--Spain",
    "https://www.airbnb.com/s/Pamplona--Spain",
    "https://www.airbnb.com/s/Fuencarral-El-Pardo--Spain",
    "https://www.airbnb.com/s/Chamartín--Spain",
    "https://www.airbnb.com/s/Ourense--Spain",
    "https://www.airbnb.com/s/Reus--Spain",
    "https://www.airbnb.com/s/San-Blas-Canillejas--Spain",
    "https://www.airbnb.com/s/Algeciras--Spain",
    "https://www.airbnb.com/s/Mataró--Spain",
    "https://www.airbnb.com/s/Leganés--Spain",
    "https://www.airbnb.com/s/Barakaldo--Spain",
    "https://www.airbnb.com/s/Castellón-de-la-Plana--Spain",
    "https://www.airbnb.com/s/San-Fernando--Spain",
    "https://www.airbnb.com/s/Torremolinos--Spain",
    "https://www.airbnb.com/s/Ceuta--Spain",
    "https://www.airbnb.com/s/Melilla--Spain",
    "https://www.airbnb.com/s/Torrevieja--Spain",
    "https://www.airbnb.com/s/Ferrol--Spain",
    "https://www.airbnb.com/s/Avilés--Spain",
    "https://www.airbnb.com/s/Ibiza--Spain",
    "https://www.airbnb.com/s/Elda--Spain",
    "https://www.airbnb.com/s/Benidorm--Spain",
    "https://www.airbnb.com/s/Santiago-de-Compostela--Spain",
    "https://www.airbnb.com/s/Viladecans--Spain",
    "https://www.airbnb.com/s/Fuengirola--Spain",
    "https://www.airbnb.com/s/Granollers--Spain",
    "https://www.airbnb.com/s/Manresa--Spain",
    "https://www.airbnb.com/s/Alcobendas--Spain",
    "https://www.airbnb.com/s/Calvià--Spain",
    "https://www.airbnb.com/s/Majadahonda--Spain",
    "https://www.airbnb.com/s/Sant-Cugat-del-Vallès--Spain",
    "https://www.airbnb.com/s/Sant-Boi-de-Llobregat--Spain",
    "https://www.airbnb.com/s/Cáceres--Spain",
    "https://www.airbnb.com/s/Pontevedra--Spain",
    "https://www.airbnb.com/s/Telde--Spain",
    "https://www.airbnb.com/s/Mijas--Spain",
    "https://www.airbnb.com/s/Arrecife--Spain",
    "https://www.airbnb.com/s/Benalmádena--Spain",
    "https://www.airbnb.com/s/Lugo--Spain",
    "https://www.airbnb.com/s/Zamora--Spain",
    "https://www.airbnb.com/s/Huesca--Spain",
    "https://www.airbnb.com/s/Soria--Spain",
    "https://www.airbnb.com/s/Cuenca--Spain",
    "https://www.airbnb.com/s/Segovia--Spain",
    "https://www.airbnb.com/s/Ávila--Spain",
    "https://www.airbnb.com/s/Talavera-de-la-Reina--Spain",
    "https://www.airbnb.com/s/Mérida--Spain",
    "https://www.airbnb.com/s/Guadalajara--Spain",
    "https://www.airbnb.com/s/Cádiz--Spain",
    "https://www.airbnb.com/s/Plasencia--Spain",
    "https://www.airbnb.com/s/Ronda--Spain",
    "https://www.airbnb.com/s/Almuñécar--Spain",
    "https://www.airbnb.com/s/Nerja--Spain",
    "https://www.airbnb.com/s/Estepona--Spain",
    "https://www.airbnb.com/s/La-Línea-de-la-Concepción--Spain",
    "https://www.airbnb.com/s/Manilva--Spain",
    "https://www.airbnb.com/s/Ribadeo--Spain",
    "https://www.airbnb.com/s/Fisterra--Spain",
    "https://www.airbnb.com/s/Arenas-de-San-Pedro--Spain",
    "https://www.airbnb.com/s/San-Javier--Spain",
    "https://www.airbnb.com/s/Roquetas-de-Mar--Spain",
    "https://www.airbnb.com/s/Águilas--Spain",
    "https://www.airbnb.com/s/Cartaya--Spain",
    "https://www.airbnb.com/s/Almonte--Spain",
    "https://www.airbnb.com/s/Isla-Cristina--Spain",
    "https://www.airbnb.com/s/Baños-de-la-Encina--Spain",
    "https://www.airbnb.com/s/Baeza--Spain",
    "https://www.airbnb.com/s/Ubeda--Spain",
    "https://www.airbnb.com/s/Cazorla--Spain",
    "https://www.airbnb.com/s/Alcázar-de-San-Juan--Spain",
    "https://www.airbnb.com/s/Tomelloso--Spain",
    "https://www.airbnb.com/s/Valdepeñas--Spain",
    "https://www.airbnb.com/s/Almagro--Spain",
    "https://www.airbnb.com/s/Santa-Pola--Spain",
    "https://www.airbnb.com/s/Guardamar-del-Segura--Spain",
    "https://www.airbnb.com/s/Cullera--Spain",
    "https://www.airbnb.com/s/Denia--Spain",
    "https://www.airbnb.com/s/Jávea--Spain",
    "https://www.airbnb.com/s/Calpe--Spain",
    "https://www.airbnb.com/s/Peñíscola--Spain",
    "https://www.airbnb.com/s/Morella--Spain",
    "https://www.airbnb.com/s/Vinaròs--Spain",
    "https://www.airbnb.com/s/Oropesa-del-Mar--Spain",
    "https://www.airbnb.com/s/Cebreros--Spain",
    "https://www.airbnb.com/s/Pedraza--Spain",
    "https://www.airbnb.com/s/Ainsa--Spain",
    "https://www.airbnb.com/s/Benasque--Spain",
    "https://www.airbnb.com/s/Torla--Spain",
    "https://www.airbnb.com/s/Jaca--Spain",
    "https://www.airbnb.com/s/Graus--Spain",
    "https://www.airbnb.com/s/Aínsa--Spain",
    "https://www.airbnb.com/s/Canfranc--Spain",
    "https://www.airbnb.com/s/Formigal--Spain",
    "https://www.airbnb.com/s/Sallent-de-Gállego--Spain",
    "https://www.airbnb.com/s/Lanjarón--Spain",
    "https://www.airbnb.com/s/Capileira--Spain",
    "https://www.airbnb.com/s/Trevélez--Spain",
    "https://www.airbnb.com/s/Pampaneira--Spain",
    "https://www.airbnb.com/s/Bubión--Spain",
    "https://www.airbnb.com/s/Laujar-de-Andarax--Spain",
    "https://www.airbnb.com/s/Olvera--Spain",
    "https://www.airbnb.com/s/Setenil-de-las-Bodegas--Spain",
    "https://www.airbnb.com/s/Grazalema--Spain",
    "https://www.airbnb.com/s/Zahara-de-la-Sierra--Spain",
    "https://www.airbnb.com/s/Arcos-de-la-Frontera--Spain",
    "https://www.airbnb.com/s/Vejer-de-la-Frontera--Spain",
    "https://www.airbnb.com/s/Conil-de-la-Frontera--Spain",
    "https://www.airbnb.com/s/Chiclana-de-la-Frontera--Spain",
    "https://www.airbnb.com/s/El-Puerto-de-Santa-María--Spain",
    "https://www.airbnb.com/s/Sanlúcar-de-Barrameda--Spain",
    "https://www.airbnb.com/s/Chipiona--Spain",
    "https://www.airbnb.com/s/Tarifa--Spain"]








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
