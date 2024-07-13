import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd
import re
import time


regions = ['Λάρισα' ,'Πάτρα', 'Θεσσαλονίκη', 'Γλυφάδα', 'Ιωάννινα', 'Αθήνα', 'Χαλάνδρι', 'Περιστέρι', 'Κορυδαλλός' ,'Ηράκλειο', 'Χολαργός', 'Χαλκίδα', 'Νέα Σμύρνη', 'Ξάνθη', 'Ζάκυνθος', 'Λαμία', 'Πειραιάς', 'Νικήτη', 'Κρανίδι' , 'Θέρμη' , 'Πυλαία' , 'Πύργος Ηλείας', 'Θεσσαλονίκη - Ντεπώ' , 'Νέα Ιωνία', 'Κόρινθος', 'Βριλήσσια', 'Χανιά', 'Μαρούσι', 'Βόλος', 'Ζωγράφος', 'Αλεξανδρούπολη', 'Νέο Ηράκλειο', 'Μεταμόρφωση', 'Νέα Καλλικράτεια', 'Λοιπές Περιοχές']


# Create empty lists to store data
names = []
addresses = []
emails = []
mobiles = []
telephones = []



def extract_lines(input_file_path, start_line, end_line, output_file_path):
    try:
        with open(input_file_path, 'r') as input_file, open(output_file_path, 'w') as output_file:
            lines = input_file.readlines()
            # Adjusting start_line and end_line to be within the valid range
            start_line = max(1, min(start_line, len(lines)))
            end_line = max(start_line, min(end_line, len(lines)))

            # Writing the selected lines to the output file
            output_file.writelines(lines[start_line - 1:end_line])

        print(f"Lines {start_line} to {end_line} extracted and saved to {output_file_path}")

    except Exception as e:
        print(f"Error: {e}")


def fetch_soup(url):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}
    '''
    headers = {
        "authority": "www.vrisko.gr",
        "method": "GET",
        "path": "/search/%CE%A4%CE%B5%CF%87%CE%BD%CE%B9%CE%BA%CE%AC-%CE%93%CF%81%CE%B1%CF%86%CE%B5%CE%AF%CE%B1-%CE%95%CF%84%CE%B1%CE%B9%CF%81%CE%AF%CE%B5%CF%82/Regions=%CE%98%CE%B5%CF%83%CF%83%CE%B1%CE%BB%CE%BF%CE%BD%CE%AF%CE%BA%CE%B7",
        "scheme": "https",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en",
        "Cookie": "__Newsphone_AdServer=__AdServer_UniqueID=65636458-31ec-46a7-a695-e79b4d0e66f3; usprivacy=1Y--; _gcl_au=1.1.826782335.1715509462; _ga=GA1.1.1327774357.1715509462; euconsent-v2=CP-gEEAP-gEEAAKA1AELA0EsAP_gAEPgAAyIKKtV_H__bW1r8X73aft0eY1P9_j77sQxBhfJE-4FzLvW_JwXx2ExNA36tqIKmRIEu3bBIQNlHJDUTVCgaogVryDMakWcoTNKJ6BkiFMRO2dYCF5vmwtj-QKY5vr993dx2D-t_dv83dzyz4VHn3a5_2e0WJCdA58tDfv9bROb-9IPd_58v4v0_F_rE2_eT1l_tevp7D9-ct87_XW-9_fff79Ll9-goqAWYaFRAHWRISEGgYRQIAVBWEBFAgAAABIGiAgBMGBTsDAJdYSIAQAoABggBAACjIAEAAAkACEQAQAFAgAAgECgABAAgEAgAYGAAMAFgIBAACA6BCmBBAoFgAkZkRCmBCFAkEBLZUIJAECCuEIRZ4EEAiJgoAAASACsAAQFgsDiSQErEggS4g2gAAIAEAghAqEUnZgCCBM2WqvFE2jK0gLR84WAAAAA.YAAAAAAAAAAA; addtl_consent=1~43.3.9.6.9.13.6.4.15.9.5.2.11.1.7.1.3.2.10.33.4.6.9.17.2.9.20.7.20.5.20.9.2.1.4.40.4.14.9.3.10.6.2.9.6.6.9.8.33.5.3.1.27.1.17.10.9.1.8.6.2.8.3.4.146.65.1.17.1.18.25.35.5.18.9.7.41.2.4.18.24.4.9.6.5.2.14.18.7.3.2.2.8.28.8.6.3.10.4.20.2.17.10.11.1.3.22.16.2.6.8.6.11.6.5.33.11.8.11.28.12.1.5.2.17.9.6.40.17.4.9.15.8.7.3.12.7.2.4.1.7.12.13.22.13.2.6.8.10.1.4.15.2.4.9.4.5.4.7.13.5.15.17.4.14.10.15.2.5.6.2.2.1.2.14.7.4.8.2.9.10.18.12.13.2.18.1.1.3.1.1.9.7.2.16.5.19.8.4.8.5.4.8.4.4.2.14.2.13.4.2.6.9.6.3.2.2.3.5.2.3.6.10.11.6.3.19.8.3.3.1.2.3.9.19.26.3.10.13.4.3.4.6.3.3.3.3.1.1.1.6.11.3.1.1.11.6.1.10.5.8.3.2.2.4.3.2.2.7.15.7.14.1.3.3.4.5.4.3.2.2.5.5.1.2.9.7.9.1.5.3.7.10.11.1.3.1.1.2.1.3.2.6.1.12.8.1.3.1.1.2.2.7.7.1.4.3.6.1.2.1.4.1.1.4.1.1.2.1.8.1.7.4.3.3.3.5.3.15.1.15.10.28.1.2.2.12.3.4.1.6.3.4.7.1.3.1.4.1.5.3.1.3.4.1.5.2.3.1.2.2.6.2.1.2.2.2.4.1.1.1.2.2.1.1.1.1.2.1.1.1.2.2.1.1.2.1.2.1.7.1.7.1.1.1.1.2.1.4.2.1.1.9.1.6.2.1.6.2.3.2.1.1.1.2.5.2.4.1.1.2.2.1.1.7.1.2.2.1.2.1.2.3.1.1.2.4.1.1.1.5.1.3.6.4.5.5.4.1.2.3.1.4.3.2.2.3.1.1.1.1.1.11.1.3.1.1.2.2.1.6.3.3.5.2.7.1.1.2.5.1.9.5.1.3.1.8.4.5.1.9.1.1.1.2.1.1.1.4.2.13.1.1.3.1.2.2.3.1.2.1.1.1.2.1.3.1.1.1.1.2.4.1.5.1.2.4.3.10.2.9.7.2.2.1.3.3.1.6.1.2.5.1.1.2.6.4.2.1.200.200.100.100.200.400.100.100.100.400.1700.100.204.596.100.1000.800.500.400.200.200.500.1300.801.99.506.95.1399.1100.4402.1798.1400.1300.200.100.800.900.300.700.100.800",
        "Priority": "u=0, i",
        "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
    '''
    response = requests.get(url, headers=headers)

    if not response.ok:
        print('Status Code:', response.status_code)
        raise Exception('Failed to fetch')

    soup = BeautifulSoup(response.text, 'html.parser')
    return soup



def update_lists(soup):
    content = soup.find('div', class_='ResultsMain')
    res_content = content.find('div', id='SearchResults')
    boxes = res_content.find_all('div', class_='DetailsArea_L')
    for box in boxes:
        name = ''
        email=''
        telephone=''
        mobile=''
        address=''
        name = box.find('h2', class_='CompanyName')
        if name:
            name = name.text.strip()
        else:
            name=''
        address = box.find('div', class_='AdvAddress')
        if address:
            address = address.text.strip()
        else:
            address=box.find('div', class_='FreeListingAddress')
            if address:
                address = address.text.strip()
            else:
                address = box.find('div', class_='LightAdvAddress')
                if address:
                    address = address.text.strip()
                else:
                  address=''

        more_info = box.find('div', attrs={"name":"extraInfo"})
        if more_info:
            find_phones = more_info.find_all('div', attrs={'itemprop':'telephone'})
            for phone in find_phones:
                phone_number = phone.text.strip()
                if phone_number.startswith('2'):
                    telephone = phone_number
                elif phone_number.startswith('69'):
                    mobile = phone_number
            email = more_info.find('a', class_='AdvSiteEmailLink noemail')
            if email:
                email = email.text.strip()
            else:
                email = ''
        names.append(name)
        addresses.append(address)
        emails.append(email)
        mobiles.append(str(mobile))
        telephones.append(str(telephone))
    if len(boxes)==20:
        return True
    else:
        return False
        

def check_results(url):
  with open('errors.txt', 'a', encoding='utf-8') as file_er:
    try:
        soup = fetch_soup(url)
        results = soup.find('span',class_='mainCountTitle').text.strip()
        results = results.replace('.','')
        results = results.replace(',','')
        num = re.findall(r'\d+', results)
        num_res = int(num[0])
        if num_res >= 200:
            num_pages = 10
        else:
            num_pages = num_res // 20  # Integer division to get the number of full pages
            # Check if there's a remainder when dividing by 20
            if num_res % 20 != 0:
                num_pages += 1
        next_move = update_lists(soup)
        if next_move:
            for page in range(1, num_pages):
                time.sleep(8)
                new_url = url+'/?page='+str(page)
                soup2 = fetch_soup(new_url)
                next_move = update_lists(soup2)
                if not next_move:
                    break
                
    except Exception as e:
        print("Error on URL:", url, "Error:", e)
        file_er.write(url +'\n')


#url = "https://www.vrisko.gr/search/Τεχνικά-Γραφεία-Εταιρίες/"
'''
urls = []
urls.append(url)

for r in regions[20:]:
    urls.append(url+"Regions="+r)
'''

urls= ['https://www.vrisko.gr/search/Τεχνικά-Γραφεία-Εταιρίες/',
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
    counter+=1
    check_results(url)
    time.sleep(12)
    if counter==5:
        counter=0
        time.sleep(70)
    '''
    try:
    
    except Exception as e:
        print("Error on URL:", url, "Error:", e)
    '''




def write_excel(path):
    # Create DataFrame
    df = pd.DataFrame({
        'Company Name': names,
        'Phone': telephones,
        'Mobile': mobiles,
        'Email': emails,
        'Address': addresses,
    })
    # Write DataFrame to Excel
    with pd.ExcelWriter(path, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        print("Data scraped successfully and saved.")
        print("Processing complete. Check the generated files.")

def create_distinct_excel(input_path, output_path):
    # Read the existing Excel file
    df = pd.read_excel(input_path)
    
    # Drop duplicate rows
    df_distinct = df.drop_duplicates()
    
    # Write the distinct DataFrame to a new Excel file
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

