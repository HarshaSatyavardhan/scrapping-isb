import pandas as pd
import requests
from bs4 import BeautifulSoup
import logging
import random
import time
from tqdm import tqdm
import os
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load the input CSV file
input_file_path = '/scratch/harsha.vasamsetti/Drug Safety-related Labeling Changes (SrLC).csv'
drug_data = pd.read_csv(input_file_path)

# Define columns for the output CSV
columns = [
    'Drug', 'Application Number', 'Date', 'Boxed Warning', 'Contraindications',
    'Warnings and Precautions', 'Adverse Reactions', 'Drug Interactions',
    'Use in Specific Populations', 'PCI/PI/MG'
]

# List of User-Agent strings
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36'
]

# Set up a requests Session
session = requests.Session()

# Common headers for a real browser
base_headers = {
    'User-Agent': random.choice(user_agents),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://www.accessdata.fda.gov/scripts/cder/safetylabelingchanges/',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-User': '?1',
    'Host': 'www.accessdata.fda.gov',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache'
}

def extract_sections(url):
    updates = []
    try:
        headers = base_headers.copy()
        headers['User-Agent'] = random.choice(user_agents)
        response = session.get(url, headers=headers, timeout=10, allow_redirects=True)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            # Find the h3 tag containing the drug name and application number
            h3_tags = soup.find_all('h3')
            drug_name = ''
            app_number = ''
            for h3 in h3_tags:
                if 'NDA-' in h3.get_text() or 'BLA-' in h3.get_text():
                    drug_app_info = h3.get_text(strip=True)
                    logging.debug(f"Drug application info: {drug_app_info}")
                    drug_name = drug_app_info.split(' (')[0].strip()
                    app_number_match = re.search(r'(NDA|BLA)-(\d+)', drug_app_info)
                    if app_number_match:
                        app_number = app_number_match.group(2)
                    break
            # Find all update headers
            update_headers = soup.find_all('h3', class_='background_text accordion-header ui-accordion-header ui-helper-reset ui-state-default ui-accordion-icons ui-corner-all')
            for header in update_headers:
                date_str = header.find(text=True, recursive=False).strip()
                content_div = header.find_next('div', class_='ui-accordion-content ui-helper-reset ui-widget-content ui-corner-bottom')
                if content_div:
                    sections = {
                        'Boxed Warning': '',
                        'Contraindications': '',
                        'Warnings and Precautions': '',
                        'Adverse Reactions': '',
                        'Drug Interactions': '',
                        'Use in Specific Populations': '',
                        'PCI/PI/MG': ''
                    }
                    for h4 in content_div.find_all('h4'):
                        header_text = h4.get_text(strip=True).lower()
                        if 'boxed warning' in header_text:
                            sections['Boxed Warning'] = 'x'
                        elif 'contraindications' in header_text:
                            sections['Contraindications'] = 'x'
                        elif 'warnings and precautions' in header_text:
                            sections['Warnings and Precautions'] = 'x'
                        elif 'adverse reactions' in header_text:
                            sections['Adverse Reactions'] = 'x'
                        elif 'drug interactions' in header_text:
                            sections['Drug Interactions'] = 'x'
                        elif 'use in specific populations' in header_text:
                            sections['Use in Specific Populations'] = 'x'
                        elif 'pci' in header_text or 'patient counseling information' in header_text:
                            sections['PCI/PI/MG'] = 'x'
                    update_data = {
                        'Drug': drug_name,
                        'Application Number': app_number,
                        'Date': date_str,
                        **sections
                    }
                    updates.append(update_data)
        else:
            logging.error(f"Failed to retrieve URL {url}: Status code {response.status_code}")
    except Exception as e:
        logging.error(f"Error processing URL {url}: {e}")
    return updates

# Define the output file path
output_file_path = 'Drug_Safety_Labels_Output1.csv'

# Check if the file exists; if not, write the header
if not os.path.exists(output_file_path):
    pd.DataFrame(columns=columns).to_csv(output_file_path, index=False, encoding='utf-8')

# Process each drug in the input file
for index, row in tqdm(drug_data.iterrows(), total=len(drug_data)):
    drug = row.get('Drug', '')
    application_number = row.get('Application Number', '')
    link = row.get('Link', '')

    if pd.notna(link) and isinstance(link, str) and link.strip():
        logging.info(f"Processing {drug} ({application_number}) => {link}")
        updates = extract_sections(link)
        for update in updates:
            df = pd.DataFrame([update], columns=columns)
            df.to_csv(output_file_path, mode='a', header=False, index=False, encoding='utf-8')
        time.sleep(random.uniform(2, 5))

logging.info(f"Data extraction complete. Output saved to {output_file_path}")