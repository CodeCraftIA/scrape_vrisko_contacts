# scrape_vrisko_contacts

# Description
This script scrapes company data from specified URLs on the vrisko.gr website. It extracts information such as company names, addresses, emails, mobile numbers, and telephone numbers. The data is then saved into an Excel file, with an option to create a second Excel file containing only distinct rows.

# Key functionalities include:

Fetching HTML content using requests and parsing it with BeautifulSoup.
Extracting relevant information from the parsed HTML.
Saving the extracted data to an Excel file.
Creating a distinct version of the Excel file to remove duplicates.

## Prerequisites

# Ensure you have the following installed:

- Python 3.x
- `requests`
- `beautifulsoup4`
- `pandas`
- `tqdm`
- `openpyxl`
- `xlsxwriter`
