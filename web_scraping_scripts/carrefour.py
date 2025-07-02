from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import shutil
import psycopg2
import re
import pandas as pd
import pickle  # For saving cookies
import json  # For saving local storage data
import shutil
import os
import time
import pandas as pd
from urllib.parse import urlparse



COOKIES_FILE = "/opt/airflow/web_scraping_scripts/cookies_car.pkl"
LOCAL_STORAGE_FILE = "/opt/airflow/web_scraping_scripts/local_storage_car.json"

product_name = []
product_price = []
imgs=[]
quantity=[]
price_per_quantity=[]
promotion=[]
stock=[]
prod_id=[]
date=[]
store = []


STORE_LOCATION_DATA = {
    "journeySearches": '[{"id":"YOUR_ID_HERE","geoSpatialData":{"address":{"zipcode":"YOUR_ZIPCODE","city":"YOUR_CITY","country":"France"},"location":{"latitude":"YOUR_LATITUDE","longitude":"YOUR_LONGITUDE"},"accuracy":"MUNICIPALITY"},"position":"1"}]'
}
# Path to Edge WebDriver
EDGE_DRIVER_PATH = "/usr/local/bin/msedgedriver"
# Initialize WebDriver Options
options = Options()
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36")

options.add_argument("--headless")  # Run without GUI (important for Docker)
options.add_argument("--disable-gpu")  # Prevent GPU issues
options.add_argument("--no-sandbox")  # Required for running as root in Docker
options.add_argument("--disable-dev-shm-usage")  # Avoid shared memory issues
options.add_argument("--user-data-dir=/opt/airflow/new_folder24")

# Set the window size to match your screen resolution
options.add_argument("--start-maximized")  # Maximizes the browser window
options.add_argument("--window-size=1920,1080")  # Change based on your screen resolution
# Start Microsoft Edge browser
service = Service(EDGE_DRIVER_PATH)
driver = webdriver.Edge(service=service, options=options)

# === Set Geolocation Override ===
# Grant permission for geolocation to the Carrefour site.
driver.execute_cdp_cmd("Browser.grantPermissions", {
    "origin": "https://www.carrefour.fr",
    "permissions": ["geolocation"]
})

# Override the geolocation with your provided coordinates.
driver.execute_cdp_cmd("Emulation.setGeolocationOverride", {
    "latitude": 48.83514963601109,
    "longitude": 2.3646013309764187,
    "accuracy": 100  # Adjust accuracy (in meters) if necessary.
})
# =================================


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

try:
    driver.maximize_window()
    #
    # # Open Carrefour's "Bio et Écologie" page
    # driver.get("https://www.carrefour.fr/r/bio-et-ecologie?noRedirect=1&page=1")
    # driver.execute_script("document.body.style.zoom='25%'")
    # # Wait for the "Reject All" button to appear and click it
    # # print(driver.page_source)
    # # Click the first element ("FERMER")
    # try:
    #     first_element_xpath = '//span[contains(text(), "FERMER")]'
    #     first_element = WebDriverWait(driver, 10).until(
    #         EC.element_to_be_clickable((By.XPATH, first_element_xpath))
    #     )
    #     driver.execute_script("arguments[0].scrollIntoView();", first_element)
    #     time.sleep(1)
    #     first_element.click()
    #     all_prod = WebDriverWait(driver, 10).until(
    #         EC.presence_of_all_elements_located((By.XPATH, '//*[@id="data-plp_produits"]/li'))
    #     )
    # except Exception as e:
    #     print('fermer not found')
    # try:
    #     reject_button = WebDriverWait(driver, 5).until(
    #         EC.element_to_be_clickable((By.XPATH, '//*[@id="onetrust-reject-all-handler"]'))
    #     )
    #     reject_button.click()
    # except TimeoutException:
    #     print("No cookie popup found.")
    #
    # #Steps to chose locations
    # add_to_panier_xpath = '//*[@id="data-plp_produits"]/li[13]/article/div[2]/div[3]/div[2]/div/button/span[2]'
    # add_to_panier = WebDriverWait(driver, 10).until(
    #     EC.element_to_be_clickable((By.XPATH, add_to_panier_xpath))
    # )
    # add_to_panier.click()
    #
    # # Steps to chose locations
    # drive_xpath = '//*[@id="sub-header"]/div/div/div[2]/div/div/div/div[1]/ul/li[1]/button/span[2]'
    # drive = WebDriverWait(driver, 10).until(
    #     EC.element_to_be_clickable((By.XPATH, drive_xpath))
    # )
    # drive.click()
    #
    # # Click the button containing "Autour de moi"
    # button_xpath = "//button[.//span[contains(text(), 'Autour de moi')]]"
    # button_element = WebDriverWait(driver, 10).until(
    #     EC.element_to_be_clickable((By.XPATH, button_xpath))
    # )
    # driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button_element)
    # time.sleep(1)
    # button_element.click()
    #
    # # Now click the final element using the provided XPath
    # final_element_xpath = '//*[@id="sub-header"]/div/div/div[2]/div/div/span/div/section/div[1]/div[4]/div/ul/li[1]/div/div[2]/ul/li/div/button'
    # final_element = WebDriverWait(driver, 10).until(
    #     EC.element_to_be_clickable((By.XPATH, final_element_xpath))
    # )
    # driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", final_element)
    # time.sleep(1)
    # final_element.click()
    #
    # # Now click the final element using the provided XPath
    # confirm_xpath = '//*[@id="sub-header"]/div/div/div[2]/div/div/span/div/div/section[3]/button[1]/span'
    # confirm = WebDriverWait(driver, 10).until(
    #     EC.element_to_be_clickable((By.XPATH, confirm_xpath))
    # )
    # time.sleep(1)
    # confirm.click()
    #
    # time.sleep(5)  # Small delay after rejecting cookies
    #
    # with open(COOKIES_FILE, "wb") as file:
    #     pickle.dump(driver.get_cookies(), file)
    #
    # # Save local storage
    # local_storage_data = driver.execute_script("return window.localStorage;")
    # with open(LOCAL_STORAGE_FILE, "w") as file:
    #     json.dump(local_storage_data, file)
    #
    # print("Session data saved.")



    for l in range(1,11):
        driver.get(f'https://www.carrefour.fr/r/bio-et-ecologie?noRedirect=1&page={l}')

        # Load cookies
        try:
            with open(COOKIES_FILE, "rb") as file:
                cookies = pickle.load(file)
                for cookie in cookies:
                    driver.add_cookie(cookie)
            print("Cookies loaded.")
        except FileNotFoundError:
            print("No saved cookies found.")

        # Inject store location into local storage again
        for key, value in STORE_LOCATION_DATA.items():
            driver.execute_script(f"window.localStorage.setItem('{key}', '{value}');")

        print("Local storage restored.")
        driver.refresh()

        driver.execute_script("document.body.style.zoom='25%'")
        all_prod = driver.find_elements(By.XPATH, '//*[@id="data-plp_produits"]/li')
        print(len(all_prod))

        for prod in range (1,len(all_prod)+1):
            try:
                add_button_xpath = f'//*[@id="data-plp_produits"]/li[{prod}]/article/div[last()]/div[last()-1]/div[2]/div/div/button[2]'
                add_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, add_button_xpath))
                )
                add_button.click()

                add_button_xpath = f'//*[@id="data-plp_produits"]/li[{prod}]/article/div[last()]/div[last()-1]/div[2]/div/div/button[2]'
                add_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, add_button_xpath))
                )
                for i in range (20):
                    add_button.click()
            except:
                print('cant add')

        for prod in range(1, len(all_prod) + 1):
            try:
                descendant_element = driver.find_element(By.XPATH,
                                                         f'//*[@id="data-plp_produits"]/li[{prod}]/article/div[last()]/div[last()-1]/div[1]/div/div/div')
                paragraphs = descendant_element.find_elements(By.TAG_NAME, "p")
                combined_text = " ".join([p.text.strip() for p in paragraphs])
                product_price.append(combined_text)
            except:
                product_price.append(None)

            # Extract promotion text
            try:
                promo_elements = driver.find_elements(By.XPATH,
                                                      f"//*[@id='{prod}']/div[last()]/div[3]/div[2]/div[1]/div/div/div")
                if len(promo_elements) > 1:
                    descendant_element = promo_elements[1]
                    paragraphs = descendant_element.find_elements(By.TAG_NAME, "p")
                    combined_text = " ".join([p.text.strip() for p in paragraphs])
                    promotion.append(combined_text)
                else:
                    promotion.append(None)
            except:
                promotion.append(None)

            # Extract product image
            try:
                img_src = driver.find_element(By.XPATH, f'//*[@id="data-plp_produits"]/li[{prod}]/article/div[last()-1]/a/img').get_attribute(
                    'src')
            except:
                img_src = None
            imgs.append(img_src)

            # Extract product name
            try:
                name = driver.find_element(By.XPATH, f'//*[@id="data-plp_produits"]/li[{prod}]/article/div[last()]/a/h3').text.strip()
            except:
                name = None
            product_name.append(name)

            # # Extract quantity
            # try:
            #     paragraph_element = driver.find_element(By.XPATH,
            #                                             f'//*[@id="{prod}"]/div[last()]/div[2]/div[2]/div[last()-1]//p')
            #     extracted_text = paragraph_element.text.strip()
            # except:
            #     extracted_text = None
            # quantity.append(extracted_text)

            # Extract price per quantity
            try:
                paragraph_element = driver.find_element(By.XPATH,
                                                        f'//*[@id="data-plp_produits"]/li[{prod}]/article/div[last()]/div[1]/span[2]')
                price_per_q = paragraph_element.text.strip()
            except:
                price_per_q = None
            price_per_quantity.append(price_per_q)

            # Extract stock information
            try:
                quantity_element = driver.find_element(
                    By.XPATH,
                    f'//*[@id="data-plp_produits"]/li[{prod}]/article/div[last()]/div[last()-1]/div[2]/div/div/div'
                )
                quantity_element = quantity_element.text.split()[0]

            except:
                print('not found')
                quantity_element = None
            stock.append(quantity_element)
            #
            # # Handle second button click if available
            #
            date.append(datetime.today().strftime('%Y-%m-%d'))
            store.append('Carrefour')
        print(len(stock),len(product_price),len(product_name),len(imgs),len(price_per_quantity),len(promotion))


        basket_panel_xpath = '//*[@id="data-basket-panel"]/div/div[1]'
        confirm_xpath = '//*[@id="checkout-empty-basket-confirm"]/span[2]'

        # Execute actions
        wait_and_click(driver, basket_panel_xpath, "Basket Panel")

        page_basket_xpath = '//*[@id="basket-panel__drawer"]/div[3]/section/div[4]/div[3]/button'

        wait_and_click(driver, page_basket_xpath, "go to basket page")

        clear_basket_xpath = '//*[@id="data-clear-basket"]/button'

        wait_and_click(driver, clear_basket_xpath, "clearing basket")

        wait_and_click(driver, confirm_xpath, "'Confirm Empty Basket' Button")


finally:
    print('gg')

# Define PostgreSQL connection parameters
DB_PARAMS = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",  # Service name in Docker Compose
    "port": "5432"
}

df = pd.DataFrame({
    'Name': product_name,
    'Price': product_price,
    'Promotion': promotion,
    'Quantity in stock': stock,
    'Price per kg': price_per_quantity,
    'Date': date,
    'Image URL': imgs,
    'Store': store,
})


conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()

# Insert DataFrame rows into PostgreSQL
for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO carrefour (name, price, promotion, quantity_stock, price_per_quantity, date, image_url, store)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (row["Name"], row["Price"], row["Promotion"], row["Quantity in stock"],
             row["Price per kg"], row["Date"], row["Image URL"], row["Store"]))

# Commit and close connection
conn.commit()
cur.close()
conn.close()


folder_path = "/opt/airflow/new_folder24"
# Check if the folder exists before deleting
if os.path.exists(folder_path):
    shutil.rmtree(folder_path, ignore_errors=True)
    print(f"Deleted folder: {folder_path}")
else:
    print(f"Folder does not exist: {folder_path}")