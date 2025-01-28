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
input_file_path = '/scratch/harsha.vasamsetti/latest/code-2/Drug Safety-related Labeling Changes (SrLC).csv'
drug_data = pd.read_csv(input_file_path)

# We will store both an 'x' marker to indicate that a section was found
# AND the text extracted from that section.

# Define the columns for the output CSV. We include new columns for the extracted text.
columns = [
    'Drug', 
    'Application Number', 
    'Date', 
    'Boxed Warning',
    'Boxed Warning Content',
    'Contraindications',
    'Contraindications Content',
    'Warnings and Precautions',
    'Warnings and Precautions Content',
    'Adverse Reactions',
    'Adverse Reactions Content',
    'Drug Interactions',
    'Drug Interactions Content',
    'Use in Specific Populations',
    'Use in Specific Populations Content',
    'PCI/PI/MG',
    'PCI/PI/MG Content'
]

# A list of possible user-agent strings
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36'
]

# Set up a requests Session
session = requests.Session()

# Common headers to mimic a real browser
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
    """
    Retrieves the page, searches for labeling update sections (in h3/h4),
    and extracts both presence (marked with 'x') and text from each section.
    """
    updates = []
    try:
        headers = base_headers.copy()
        headers['User-Agent'] = random.choice(user_agents)
        response = session.get(url, headers=headers, timeout=10, allow_redirects=True)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Attempt to extract drug name and application number from h3 tags
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
            
            # Each major "accordion" content block is one dated update.
            # We find them by looking for the special h3 with class = 'background_text ...'
            update_headers = soup.find_all(
                'h3', 
                class_='background_text accordion-header ui-accordion-header ui-helper-reset ui-state-default ui-accordion-icons ui-corner-all'
            )
            
            for header in update_headers:
                # The date is in the h3's direct text
                date_str = header.find(text=True, recursive=False).strip()
                
                # This is the container with all the updated sections
                content_div = header.find_next(
                    'div', 
                    class_='ui-accordion-content ui-helper-reset ui-widget-content ui-corner-bottom'
                )
                
                if content_div:
                    # We will store whether a section was found ("x") 
                    # and the associated text from that section.
                    sections_found = {
                        'Boxed Warning': '',
                        'Contraindications': '',
                        'Warnings and Precautions': '',
                        'Adverse Reactions': '',
                        'Drug Interactions': '',
                        'Use in Specific Populations': '',
                        'PCI/PI/MG': ''
                    }
                    
                    sections_text = {
                        'Boxed Warning': '',
                        'Contraindications': '',
                        'Warnings and Precautions': '',
                        'Adverse Reactions': '',
                        'Drug Interactions': '',
                        'Use in Specific Populations': '',
                        'PCI/PI/MG': ''
                    }
                    
                    # Map certain keywords in the <h4> text to our known section names
                    section_map = {
                        'boxed warning': 'Boxed Warning',
                        'contraindications': 'Contraindications',
                        'warnings and precautions': 'Warnings and Precautions',
                        'adverse reactions': 'Adverse Reactions',
                        'drug interactions': 'Drug Interactions',
                        'use in specific populations': 'Use in Specific Populations',
                        'pci': 'PCI/PI/MG',
                        'patient counseling information': 'PCI/PI/MG'  # Common naming
                    }
                    
                    # We'll gather all h4 headings within this date block.
                    all_h4s = content_div.find_all('h4')
                    
                    for i, h4 in enumerate(all_h4s):
                        header_text = h4.get_text(strip=True).lower()
                        matched_section = None
                        
                        # Check which of our known sections this h4 might match
                        for key, val in section_map.items():
                            if key in header_text:
                                matched_section = val
                                break
                        
                        if matched_section:
                            # We know this h4 belongs to one of the relevant sections.
                            # Mark presence with 'x'.
                            sections_found[matched_section] = 'x'
                            
                            # Now gather text from the siblings of this h4,
                            # until we reach the next h4 or run out of siblings.
                            content_parts = []
                            current_sibling = h4.next_sibling
                            
                            while current_sibling:
                                # If we hit the next h4, stop collecting
                                if current_sibling.name == 'h4':
                                    break
                                # If it's a Tag or NavigableString, we can extract text
                                if hasattr(current_sibling, 'get_text'):
                                    text = current_sibling.get_text(strip=True)
                                    if text:
                                        content_parts.append(text)
                                else:
                                    # A NavigableString or something else
                                    temp_text = str(current_sibling).strip()
                                    if temp_text:
                                        content_parts.append(temp_text)
                                current_sibling = current_sibling.next_sibling
                            
                            # Join all text fragments
                            combined_text = " ".join(content_parts)
                            sections_text[matched_section] = combined_text
                    
                    # Build our dictionary for this date-block update
                    update_data = {
                        'Drug': drug_name,
                        'Application Number': app_number,
                        'Date': date_str,
                        'Boxed Warning': sections_found['Boxed Warning'],
                        'Boxed Warning Content': sections_text['Boxed Warning'],
                        'Contraindications': sections_found['Contraindications'],
                        'Contraindications Content': sections_text['Contraindications'],
                        'Warnings and Precautions': sections_found['Warnings and Precautions'],
                        'Warnings and Precautions Content': sections_text['Warnings and Precautions'],
                        'Adverse Reactions': sections_found['Adverse Reactions'],
                        'Adverse Reactions Content': sections_text['Adverse Reactions'],
                        'Drug Interactions': sections_found['Drug Interactions'],
                        'Drug Interactions Content': sections_text['Drug Interactions'],
                        'Use in Specific Populations': sections_found['Use in Specific Populations'],
                        'Use in Specific Populations Content': sections_text['Use in Specific Populations'],
                        'PCI/PI/MG': sections_found['PCI/PI/MG'],
                        'PCI/PI/MG Content': sections_text['PCI/PI/MG']
                    }
                    
                    updates.append(update_data)
        else:
            logging.error(f"Failed to retrieve URL {url}: Status code {response.status_code}")
    
    except Exception as e:
        logging.error(f"Error processing URL {url}: {e}")
    
    return updates

# Define the output file path
output_file_path = 'Drug_Safety_Labels_Output2.csv'

# If the file does not exist, write the header first
if not os.path.exists(output_file_path):
    pd.DataFrame(columns=columns).to_csv(output_file_path, index=False, encoding='utf-8')

# Iterate over each row in the input file
for index, row in tqdm(drug_data.iterrows(), total=len(drug_data)):
    drug = row.get('Drug Name', '')
    application_number = row.get('Application Number', '')
    link = row.get('Link', '')

    # Only process valid links
    if pd.notna(link) and isinstance(link, str) and link.strip():
        logging.info(f"Processing {drug} ({application_number}) => {link}")
        updates = extract_sections(link)
        
        # For every date-block update we found, append it to our output CSV
        for update in updates:
            df = pd.DataFrame([update], columns=columns)
            df.to_csv(output_file_path, mode='a', header=False, index=False, encoding='utf-8')
        
        # A random pause to avoid overwhelming the server
        time.sleep(random.uniform(2, 5))

logging.info(f"Data extraction complete. Output saved to {output_file_path}")
