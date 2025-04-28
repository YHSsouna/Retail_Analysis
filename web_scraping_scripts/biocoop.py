from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support import expected_conditions as EC
import math
from datetime import datetime
import pickle  # For saving cookies
import json  # For saving local storage data
import re
from selenium.webdriver.common.keys import Keys
import shutil
import psycopg2
import os
import re
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time



COOKIES_FILE = "/opt/airflow/web_scraping_scripts/cookies_bio.pkl"
LOCAL_STORAGE_FILE = "/opt/airflow/web_scraping_scripts/local_storage_bio.json"

STORE_LOCATION_DATA = {
    "journeySearches": '[{"id":"YOUR_ID_HERE","geoSpatialData":{"address":{"zipcode":"YOUR_ZIPCODE","city":"YOUR_CITY","country":"France"},"location":{"latitude":"YOUR_LATITUDE","longitude":"YOUR_LONGITUDE"},"accuracy":"MUNICIPALITY"},"position":"1"}]'
}

SHOP_LOCAL = {
    "shoplocal": '[{"id":"700","code":"CHORPA","name":"Biocoop Choron","address":"4, Rue Choron<br/>75009, Paris 9e Arrondissement","phone":"01 40 38 61 08","magasin_url":null,"update":"2025-03-08 02:01:55","region":"CENTRE NORD-EST","departement":"paris","postcode":"75009","city":"Paris 9e Arrondissement","email":"bonjour@biocoopchoron.fr","maps_url":"https://www.google.com/maps?q=4,+rue+Choron+75009+PARIS+9E+ARRONDISSEMENT","extra_message":null,"storelocator_url":"#","pickup_slot":"jeudi 10 avril à 15:00","is_pickup":"1","pickup_slots":[{"clickandcollect":"2025-04-10 15:00:00","next_slot":"jeudi 10 avril à 15:00","slot_time":1,"is_pickup":"1"}],"shipping_method":"clickandcollect","next_force_update":1744284600,"data_id":1744283203,"timestamp":1744283625,"delivery_address":"NjMgUnVlIFLDqWF1bXVyLCA3NTAwMiBQYXJpcywgRnJhbmNl","can_deliver":true,"cache_key":"CHORPA_SHIPFROMSTORE_3461f67733df9cc63b316ea4526f81f8_1741399315_cfcd208495d565ef66e7dff9f98764da","found_by_address":true}]'
}

SHOP = {
    "shop": '[{"id": "700", "code": "CHORPA", "name": "Biocoop Choron", "address": "4, Rue Choron<br/>75009, Paris 9e Arrondissement", "phone": "01 40 38 61 08", "magasin_url": null, "update": "2025-03-08 02:01:55", "region": "CENTRE NORD-EST", "departement": "paris", "postcode": "75009", "city": "Paris 9e Arrondissement", "email": "bonjour@biocoopchoron.fr", "maps_url": "https://www.google.com/maps?q=4,+rue+Choron+75009+PARIS+9E+ARRONDISSEMENT", "extra_message": null, "storelocator_url": "#", "pickup_slot": "jeudi 10 avril \u00e0 14:00", "is_pickup": "1", "pickup_slots": [{"clickandcollect": "2025-04-10 14:00:00", "next_slot": "jeudi 10 avril \u00e0 14:00", "slot_time": 1, "is_pickup": "1"}], "shipping_method": "clickandcollect", "next_force_update": 1744282800, "data_id": 1744281512, "timestamp": 1744281933, "can_deliver": true, "cache_key": "CHORPA_SHIPFROMSTORE_6ee4441b9bfddd3f0398cc748da820e0_1741399315_cfcd208495d565ef66e7dff9f98764da", "found_by_address": false}]'
}

RESERVE_SLOT = {
    "reserveSlot": '[{"biocoop_pickup_biocoop_pickup":{"day_selected":{"label":"Friday 11/04","code":"110425_","format":"ven. 11 avr.","formatLong":"<span>vendredi</span> 11 avril"},"slot_selected":{"label":"11h00 - 12h00","value":"11h00","full_date":"110425_11h00","system_date":"2025-04-11 11:00:00"},"store_code":"CHORPA","shipping_method":"clickandcollect","specific_shipping_method":"clickandcollect"}}]'
}


def load_session_data(driver):
    # Load cookies
    try:
        with open(COOKIES_FILE, "rb") as file:
            cookies = pickle.load(file)
            for cookie in cookies:
                driver.add_cookie(cookie)

        print("Cookies loaded.")
    except FileNotFoundError:
        print("No saved cookies found.")
    for name, value in SHOP.items():
        cookie = {"name": name, "value": value, "domain": "www.biocoop.fr"}  # Add the correct domain
        driver.add_cookie(cookie)
    # Inject store location into local storage again
    for key, value in STORE_LOCATION_DATA.items():
        driver.execute_script(f"window.localStorage.setItem('{key}', '{value}');")
    for key, value in SHOP_LOCAL.items():
        driver.execute_script(f"window.localStorage.setItem('{key}', '{value}');")
    for key, value in RESERVE_SLOT.items():
        driver.execute_script(f"window.localStorage.setItem('{key}', '{value}');")

    print("Local storage restored.")
    driver.refresh()  # Refresh to apply session data
def wait_and_click(driver, xpath, description):
    """Waits for an element, scrolls to it, and clicks it."""
    try:
        # Wait for the element to be clickable
        element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, xpath)))

        # Scroll into view
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)

        # Click the element
        element.click()
        print(f"✅ {description} clicked successfully!")
    except Exception as e:
        print(f"❌ Error clicking {description}: {str(e)}")

product_types_url = [
    "https://www.biocoop.fr/magasin-biocoop_choron/fruits-et-legumes.html",
    "https://www.biocoop.fr/magasin-biocoop_choron/epicerie-sucree.html?categorie_select%5B0%5D=Tablettes+de+chocolat&categorie_select%5B1%5D=Cookies&p=2",
    "https://www.biocoop.fr/magasin-biocoop_choron/cremerie.html?categorie_select%5B0%5D=Lait+de+vache&categorie_select%5B1%5D=Cr%C3%A8mes+fra%C3%AEches&categorie_select%5B2%5D=Laits+de+vache&categorie_select%5B3%5D=Desserts",
    "https://www.biocoop.fr/magasin-biocoop_choron/traiteur-boucherie-poissonnerie.html",
    "https://www.biocoop.fr/magasin-biocoop_choron/boissons.html"
]


def scrape_page(filtered_hrefs,thread_id):
    # Initialize WebDriver Options
    edge_driver_path = '/usr/local/bin/msedgedriver'
    options = webdriver.EdgeOptions()
    options.add_argument("--headless")  # Run without GUI (important for Docker)
    options.add_argument("--disable-gpu")  # Prevent GPU issues
    options.add_argument("--no-sandbox")  # Required for running as root in Docker
    
    options.add_argument("--disable-dev-shm-usage")  # Avoid shared memory issues
    profile_dir = f"/opt/airflow/edge_profile_{thread_id+200}"
    os.makedirs(profile_dir, exist_ok=True)
    options.add_argument(f"--user-data-dir={profile_dir}")

    # Start Microsoft Edge browser
    service = Service(edge_driver_path)
    driver = webdriver.Edge(service=service, options=options)


    # === Set Geolocation Override ===
    # Grant permission for geolocation to the Carrefour site.
    driver.execute_cdp_cmd("Browser.grantPermissions", {
        "origin": "https://www.biocoop.fr/",
        "permissions": ["geolocation"]
    })

    # Override the geolocation with your provided coordinates.
    driver.execute_cdp_cmd("Emulation.setGeolocationOverride", {
        "latitude": 48.86663002824408,
        "longitude": 2.3505984152328954,
        "accuracy": 100  # Adjust accuracy (in meters) if necessary.
    })

    local_stock, local_name, local_img, local_price, local_price_per_quantity, local_quantity = [], [], [], [], [], []

    driver.get(filtered_hrefs)
    driver.maximize_window()
    load_session_data(driver)
    driver.execute_script("document.body.style.zoom='25%'")

    try:
        # Locate the element containing the total number of products
        toolbar_amount = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="maincontent"]/div[3]/div[1]/div[2]/p/span'))
        )

        text_content = toolbar_amount.text.strip()  # Example: "192 produits"
        print(f"Extracted Text: '{text_content}'")  # Debugging

        # Split and take the first part (number)
        total_products = int(text_content.split(" ")[0])

        # Calculate the number of pages (assuming 12 products per page)
        products_per_page = 12
        num_pages = math.ceil(total_products / products_per_page)
        for page in range(1, num_pages + 1):
            driver.get(f'{filtered_hrefs}?p={page}')
            driver.execute_script("document.body.style.zoom='33%'")
            time.sleep(5)
            all_prod = driver.find_elements(By.XPATH, '//*[@id="items-wrapper"]/li')

            for prod, product in enumerate(all_prod):
                try:
                    dont_add = 0
                    product_class = product.get_attribute("class")

                    # Skip sponsored products
                    if "product-fourth" in product_class:
                        print("⏭️ Skipping sponsored product...")
                        continue

                    button = driver.find_element(By.XPATH,
                                                 f'//*[@id="items-wrapper"]/li[{prod + 1}]/div/div[last()]/div/div/form[1]/button/span[1]')
                    button.click()
                except:
                    print("can't click ajouter")
                    dont_add = 1
                try:
                    price_xpath = driver.find_element(By.XPATH,
                                                      f'//*[@id="items-wrapper"]/li[{prod + 1}]/div/div[last()-1]/div[1]/span/span')
                    local_price.append(price_xpath.get_attribute('data-price-amount'))
                except:
                    print(f"Error fetching price for product {prod + 1}")
                    local_price.append(None)  # Append None if price is not found

                    # Quantity extraction
                try:
                    quantity_xpath = driver.find_element(By.XPATH,
                                                         f'//*[@id="items-wrapper"]/li[{prod + 1}]/div/div[last()-1]/div[last()]/div/span')
                    local_quantity.append(quantity_xpath.text)
                except:
                    print(f"Error fetching quantity for product {prod + 1}")
                    local_quantity.append(None)

                    # Price per quantity extraction
                try:
                    price_per_quantity_xpath = driver.find_element(By.XPATH,
                                                                   f'//*[@id="items-wrapper"]/li[{prod + 1}]/div/div[last()-1]/div[3]/div/span')
                    local_price_per_quantity.append(price_per_quantity_xpath.text)
                except:
                    print(f"Error fetching price per quantity for product {prod + 1}")
                    local_price_per_quantity.append(None)

                try:
                    name_xpath = driver.find_element(By.XPATH,
                                                     f'//*[@id="items-wrapper"]/li[{prod + 1}]/div/div[last()-3]/a')
                    local_name.append(name_xpath.text)
                except:
                    print(f"Error fetching name for product {prod + 1}")
                    local_name.append(None)

                try:
                    img_xpath = driver.find_element(By.XPATH,
                                                    f'//*[@id="items-wrapper"]/li[{prod + 1}]/div/a/span/span/img')
                    local_img.append(img_xpath.get_attribute('src'))
                except:
                    print(f"Error fetching image for product {prod + 1}")
                    local_img.append(None)
                if dont_add == 0:
                    try:
                        # Try clicking "Add to Cart" button
                        for add in range(5):
                            add_button = WebDriverWait(driver, 3).until(EC.element_to_be_clickable(
                                (By.XPATH,
                                 f'//*[@id="items-wrapper"]/li[{prod + 1}]/div/div[last()]/div/div/form[2]/div/button[2]')))
                            add_button.click()
                        local_stock.append("In Stock")  # Track stock as 'In Stock' when clicked successfully
                    except Exception as e:
                        print(f"Error clicking add {prod + 1}")
                        try:
                            # Try alternative "Add to Cart" button
                            ajout_button = WebDriverWait(driver, 3).until(EC.element_to_be_clickable(
                                (By.XPATH,
                                 f'//*[@id="items-wrapper"]/li[{prod + 1}]/div/div[last()]/div/div/form[2]/div/div[2]/button/span[1]')))
                            ajout_button.click()
                            local_stock.append("In Stock")  # Track stock as 'In Stock' when clicked successfully
                        except Exception as e:
                            print(f"Product {prod + 1} is out of stock. Adding None.")
                            local_stock.append(None)  # Mark out-of-stock products as None
                elif dont_add == 1:
                    local_stock.append(None)

            try:
                WebDriverWait(driver, 5).until(EC.element(
                    (By.XPATH,
                     f'//*[@id="items-wrapper"]/li[12]/div/div[last()]/div/div/div/div/span/span')))
            except:
                time.sleep(0.5)
            time.sleep(3)
            stc_len = len(local_stock) - len(all_prod)
            driver.get('https://www.biocoop.fr/magasin-biocoop_choron/checkout/cart/')
            driver.execute_script("document.body.style.zoom='25%'")
            time.sleep(5)
            all_prod = driver.find_elements(By.XPATH, '//*[@id="shopping-cart-table"]/tbody/tr')
            for prod in range(len(all_prod)):

                try:
                    stock_path = driver.find_element(By.XPATH,
                                                     f'//*[@id="shopping-cart-table"]/tbody/tr[{prod + 1}]/td[2]/div[2]/div/div/input')
                    local_stock[stc_len + prod] = stock_path.get_attribute('value')  # Replace None with actual stock if available
                except Exception as e:
                    print(f"Error fetching stock for product {prod + 1}")
            try:
                driver.find_element(By.XPATH, '//*[@id="maincontent"]/div[2]/div/div[3]/div[1]/div[3]/a').click()
            except:
                print('error clearing panier')

    finally:
        driver.quit()
        shutil.rmtree(profile_dir, ignore_errors=True)
        print('deleted folder')

    return {
        'price': local_price,
        'img': local_img,
        'name': local_name,
        'price_per_q': local_price_per_quantity,
        'stock': local_stock,
        'quantity': local_quantity,
    }

# Build argument list with unique thread IDs
args = [(url, i) for i, url in enumerate(product_types_url)]

# Wrapper so map only takes one argument
def wrapper(arg):
    return scrape_page(*arg)

# Run threads and collect per-thread results
all_product_price = []
all_imgs = []
all_product_name = []
all_price_per_quantity = []
all_stock = []
all_quantity = []

with ThreadPoolExecutor(max_workers=5) as executor:
    results = list(executor.map(wrapper, args))  # Pages 1–10

# Combine all thread results into final lists
for res in results:
    all_product_price.extend(res['price'])
    all_imgs.extend(res['img'])
    all_product_name.extend(res['name'])
    all_price_per_quantity.extend(res['price_per_q'])
    all_stock.extend(res['stock'])
    all_quantity.extend(res['quantity'])


DB_PARAMS = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",  # Service name in Docker Compose
    "port": "5432"
}


df = pd.DataFrame({
    'Date': datetime.today().strftime('%Y-%m-%d'),  # Add date as a column
    'Store': 'Biocoop',  # Modify store name as needed
    'Name': all_product_name,
    'Price': all_product_price,  # Ensure this variable exists
    'Quantity in Stock': all_stock,
    'Price per kg': all_price_per_quantity,
    'Image URL': all_imgs
})
# Define file path
conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()

# Insert DataFrame rows into PostgreSQL
for _, row in df.iterrows():
    cur.execute("""
              INSERT INTO biocoop (name, price, stock, price_per_quantity, date, img, store)
              VALUES (%s, %s, %s, %s, %s, %s, %s)
          """, (row["Name"], row["Price"], row["Quantity in Stock"],
                row["Price per kg"], row["Date"], row["Image URL"], row["Store"]))

# Commit and close connection
conn.commit()
cur.close()
conn.close()


