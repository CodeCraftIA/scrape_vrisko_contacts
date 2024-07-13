import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd
import re
import time

# List of regions to search in
#regions = ['Λάρισα', 'Πάτρα', 'Θεσσαλονίκη', 'Γλυφάδα', 'Ιωάννινα', 'Αθήνα', 'Χαλάνδρι', 'Περιστέρι', 'Κορυδαλλός', 'Ηράκλειο', 'Χολαργός', 'Χαλκίδα', 'Νέα Σμύρνη', 'Ξάνθη', 'Ζάκυνθος', 'Λαμία', 'Πειραιάς', 'Νικήτη', 'Κρανίδι', 'Θέρμη', 'Πυλαία', 'Πύργος Ηλείας', 'Θεσσαλονίκη - Ντεπώ', 'Νέα Ιωνία', 'Κόρινθος', 'Βριλήσσια', 'Χανιά', 'Μαρούσι', 'Βόλος', 'Ζωγράφος', 'Αλεξανδρούπολη', 'Νέο Ηράκλειο', 'Μεταμόρφωση', 'Νέα Καλλικράτεια', 'Λοιπές Περιοχές']

# Create empty lists to store data
names = []
addresses = []
emails = []
mobiles = []
telephones = []

def extract_lines(input_file_path, start_line, end_line, output_file_path):
    """Extract lines from an input file and save to an output file."""
    try:
        with open(input_file_path, 'r') as input_file, open(output_file_path, 'w') as output_file:
            lines = input_file.readlines()
            # Adjusting start_line and end_line to be within the valid range
            start_line = max(1, min(start_line, len(lines)))
            end_line = max(start_line, min(end_line, len(lines)))
            output_file.writelines(lines[start_line - 1:end_line])
        print(f"Lines {start_line} to {end_line} extracted and saved to {output_file_path}")
    except Exception as e:
        print(f"Error: {e}")

def fetch_soup(url):
    """Fetch the HTML content of a URL and return a BeautifulSoup object."""
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}
    response = requests.get(url, headers=headers)
    if not response.ok:
        print('Status Code:', response.status_code)
        raise Exception('Failed to fetch')
    return BeautifulSoup(response.text, 'html.parser')

def update_lists(soup):
    """Extract data from the soup object and update the lists."""
    content = soup.find('div', class_='ResultsMain')
    res_content = content.find('div', id='SearchResults')
    boxes = res_content.find_all('div', class_='DetailsArea_L')

    for box in boxes:
        name = box.find('h2', class_='CompanyName')
        name = name.text.strip() if name else ''

        address = box.find('div', class_='AdvAddress') or box.find('div', class_='FreeListingAddress') or box.find('div', class_='LightAdvAddress')
        address = address.text.strip() if address else ''

        more_info = box.find('div', attrs={"name": "extraInfo"})
        email = ''
        telephone = ''
        mobile = ''
        if more_info:
            find_phones = more_info.find_all('div', attrs={'itemprop': 'telephone'})
            for phone in find_phones:
                phone_number = phone.text.strip()
                if phone_number.startswith('2'):
                    telephone = phone_number
                elif phone_number.startswith('69'):
                    mobile = phone_number
            email = more_info.find('a', class_='AdvSiteEmailLink noemail')
            email = email.text.strip() if email else ''
        
        names.append(name)
        addresses.append(address)
        emails.append(email)
        mobiles.append(str(mobile))
        telephones.append(str(telephone))
    
    return len(boxes) == 20

def check_results(url):
    """Check and update results from the given URL."""
    with open('errors.txt', 'a', encoding='utf-8') as file_er:
        try:
            soup = fetch_soup(url)
            results = soup.find('span', class_='mainCountTitle').text.strip().replace('.', '').replace(',', '')
            num_res = int(re.findall(r'\d+', results)[0])
            num_pages = min(10, num_res // 20 + (1 if num_res % 20 != 0 else 0))

            next_move = update_lists(soup)
            if next_move:
                for page in range(1, num_pages):
                    time.sleep(8)
                    new_url = f"{url}/?page={page}"
                    soup2 = fetch_soup(new_url)
                    next_move = update_lists(soup2)
                    if not next_move:
                        break
        except Exception as e:
            print("Error on URL:", url, "Error:", e)
            file_er.write(url + '\n')

# List of URLs to scrape
urls = [
    'https://www.vrisko.gr/search/Τεχνικά-Γραφεία-Εταιρίες/',
    'https://www.vrisko.gr/search/Χωματουργικές-Εργασίες-Εργολάβοι',
    'https://www.vrisko.gr/search/Χωματουργικά-Μηχανήματα',
    'https://www.vrisko.gr/search/Μηχανήματα-Οδοποιίας/',
    'https://www.vrisko.gr/search/Κατεδαφίσεις/',
    'https://www.vrisko.gr/search/Ανακύκλωση/',
    'https://www.vrisko.gr/search/Άσφαλτος-Παράγωγα-Προϊόντα/',
    'https://www.vrisko.gr/search/Λατομεία/',
    'https://www.vrisko.gr/search/Μηχανήματα-Λατομείων-Μεταλλείων/',
    'https://www.vrisko.gr/search/Τοπογράφοι-Μηχανικοί',
    'https://www.vrisko.gr/search/Πολιτικοί-Μηχανικοί/',
    'https://www.vrisko.gr/search/Σύμβουλοι-Μηχανικοί/',
    'https://www.vrisko.gr/search/Αρχιτέκτονες-Αρχιτεκτονικά-Γραφεία/',
    'https://www.vrisko.gr/search/Ανακαίνιση-Αναπαλαίωση-Κτιρίων/',
    'https://www.vrisko.gr/search/Σχεδιαστές-Αρχιτεκτονικού-Μηχανολογικού-σχεδίου/',
    'https://www.vrisko.gr/search/Αρχιτέκτονες-Τοπίου/',
    'https://www.vrisko.gr/search/Οικοδομικές-Εργασίες-Εργολάβοι/',
    'https://www.vrisko.gr/search/κατασκευαστικη/',
    'https://www.vrisko.gr/search/Γραφεία-Μελετών-Εταιρίες/',
    'https://www.vrisko.gr/search/Κατασκευή-Διαμόρφωση-Κήπου/',
    'https://www.vrisko.gr/search/Εξοπλισμοί-Κατασκευές-Αθλητικών-Εγκαταστάσεων/',
    'https://www.vrisko.gr/search/Ανανεώσιμες-Πηγές-Ενέργειας-ΑΠΕ/',
    'https://www.vrisko.gr/search/Σύμβουλοι-Περιβαλλοντικών-Έργων/',
]

counter = 0
for url in tqdm(urls[12:]):
    counter += 1
    check_results(url)
    time.sleep(12)
    if counter == 5:
        counter = 0
        time.sleep(70)

def write_excel(path):
    """Write the scraped data to an Excel file."""
    df = pd.DataFrame({
        'Company Name': names,
        'Phone': telephones,
        'Mobile': mobiles,
        'Email': emails,
        'Address': addresses,
    })
    with pd.ExcelWriter(path, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        print("Data scraped successfully and saved.")
        print("Processing complete. Check the generated files.")

def create_distinct_excel(input_path, output_path):
    """Create an Excel file with distinct rows."""
    df = pd.read_excel(input_path)
    df_distinct = df.drop_duplicates()
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        df_distinct.to_excel(writer, index=False, sheet_name='Sheet1')
    print(f"Distinct data saved to {output_path}")

# Define the file names
input_file_name = 'vrisko2.xlsx'
output_file_name = 'vrisko_distinct2.xlsx'

# Create the original Excel file
write_excel(input_file_name)

# Create a new Excel file with distinct rows
create_distinct_excel(input_file_name, output_file_name)
