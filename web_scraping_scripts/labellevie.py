from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support import expected_conditions as EC
import shutil
import psycopg2
import os
from datetime import datetime
import pandas as pd
import time

# Path to Edge WebDriver
EDGE_DRIVER_PATH = "/usr/local/bin/msedgedriver"

# Initialize WebDriver Options
options = Options()
options.add_argument("--headless")  # Run without GUI (important for Docker)
options.add_argument("--disable-gpu")  # Prevent GPU issues
options.add_argument("--no-sandbox")  # Required for running as root in Docker
options.add_argument("--disable-dev-shm-usage")  # Avoid shared memory issues
options.add_argument("--user-data-dir=/opt/airflow/new_folder3")

# Set the window size to match your screen resolution
options.add_argument("--start-maximized")  # Maximizes the browser window
options.add_argument("--window-size=1920,1080")  # Change based on your screen resolution

# options.add_experimental_option("detach", True)  # Keeps the browser open

# Start Microsoft Edge browser
service = Service(EDGE_DRIVER_PATH)
driver = webdriver.Edge(service=service, options=options)

# Lists to store scraped data
categories, imgs, product_name, price = [], [], [], []
quantity_stock, promotion, date, expiration_date, store = [], [], [], [], []

try:
    driver.maximize_window()
    MAX_RETRIES = 3

    for attempt in range(MAX_RETRIES):
        try:
            driver.set_page_load_timeout(180)  # Increase to 180 seconds
            driver.get("https://www.labellevie.com/categorie/2227/produits-frais")
            break  # Success, exit loop
        except TimeoutException:
            print(f"Retry {attempt + 1}/{MAX_RETRIES} due to timeout")
            time.sleep(2 ** attempt)  # Exponential backoff
    time.sleep(2)
    # Wait for the cookie popup and reject it
    try:
        reject_button = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="confidentiality-refuse-all"]'))
        )
        reject_button.click()
        print("Rejected cookies successfully!")
    except TimeoutException:
        print("No cookie popup found.")

    # Get all sectors (categories)
    sectors = driver.find_elements(By.XPATH, '//*[@id="cooklistitems"]/div')

    for i in range(1, len(sectors) + 1):
        # Get category name
        try:
            name_element = driver.find_element(By.XPATH, f'//*[@id="cooklistitems"]/div[{i}]/h2/span')
            category_name = name_element.text.strip()
        except NoSuchElementException:
            category_name = "Unknown"

        # Get products in category
        elements = driver.find_elements(By.XPATH, f'//*[@id="cooklistitems"]/div[{i}]/section/article')

        for n in range(1, len(elements) + 1):
            categories.append(category_name)

            # Get product image URL
            try:
                img_src = driver.find_element(By.XPATH,
                                              f'//*[@id="cooklistitems"]/div[{i}]/section/article[{n}]/a/div[last()-1]/img').get_attribute('src')
            except NoSuchElementException:
                img_src = None
            imgs.append(img_src)

            # Get product name
            try:
                name = driver.find_element(By.XPATH,
                                           f'//*[@id="cooklistitems"]/div[{i}]/section/article[{n}]/a/div[last()]/h1').text.strip()
            except NoSuchElementException:
                name = "No Name"
            product_name.append(name)

            # Get price
            try:
                prices_value = driver.find_elements(By.XPATH,
                                                    f'//*[@id="cooklistitems"]/div[{i}]/section/article[{n}]/a/div[last()]/p/span')
                price_value = prices_value[-2].text.strip() if len(prices_value) >= 2 else None
            except:
                price_value = None
            price.append(price_value)

            # Get expiration date (if exists)
            try:
                if len(elements) > 3:
                    garantie_element = driver.find_element(By.XPATH,
                                                           f'//*[@id="cooklistitems"]/div[{i}]/section/article[{n}]/a/div[2]')
                    expiration_text = garantie_element.text.strip()
                else:
                    expiration_text = None
            except NoSuchElementException:
                expiration_text = None
            expiration_date.append(expiration_text)

            # Get promotion (if exists)
            try:
                if len(elements) > 2:
                    promo_element = driver.find_elements(By.XPATH,
                                                         f'//*[@id="cooklistitems"]/div[{i}]/section/article[{n}]/a/div[1]')
                    promo_text = promo_element[0].text.strip() if promo_element else None
                else:
                    promo_text = None
            except NoSuchElementException:
                promo_text = None
            promotion.append(promo_text)

            # Get stock availability
            try:
                stk = driver.find_element(By.XPATH, f'//*[@id="cooklistitems"]/div[{i}]/section/article[{n}]/button')
                stock = stk.get_attribute("data-stock")  # Get the value of the 'data-stock' attribute
            except NoSuchElementException:
                stock = None
            quantity_stock.append(stock)

            # Append current date
            date.append(datetime.today().strftime('%Y-%m-%d'))
            store.append('La belle vie')

finally:
    # Close the browser
    driver.quit()

# Define PostgreSQL connection parameters
DB_PARAMS = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",  # Service name in Docker Compose
    "port": "5432"
}

# Create DataFrame
df = pd.DataFrame({
    'Name': product_name,
    'Price': price,
    'Promotion': promotion,
    'Quantity in stock': quantity_stock,
    'Category': categories,
    'Date': date,
    "Expiration Date": expiration_date,
    'Image URL': imgs,
    'Store': store,
})

# Connect to PostgreSQL
conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()

# Insert DataFrame rows into PostgreSQL
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO labellevie (name, price, promotion, quantity_stock, category, date, expiration_date, image_url, store)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (row["Name"], row["Price"], row["Promotion"], row["Quantity in stock"],
          row["Category"], row["Date"], row["Expiration Date"], row["Image URL"], row["Store"]))

# Commit and close connection
conn.commit()
cur.close()
conn.close()


folder_path = "/opt/airflow/new_folder3"

# Check if the folder exists before deleting
if os.path.exists(folder_path):
    shutil.rmtree(folder_path)
    print(f"Deleted folder: {folder_path}")
else:
    print(f"Folder does not exist: {folder_path}")

