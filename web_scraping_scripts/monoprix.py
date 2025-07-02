from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import shutil
import os
import psycopg2
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
options.add_argument("--user-data-dir=/opt/airflow/new_folder4")

# Set the window size to match your screen resolution
options.add_argument("--start-maximized")  # Maximizes the browser window
options.add_argument("--window-size=1920,1080")  # Change based on your screen resolution

# options.add_experimental_option("detach", True)  # Keeps the browser open

# Start Microsoft Edge browser
service = Service(EDGE_DRIVER_PATH)
driver = webdriver.Edge(service=service, options=options)
imgs=[]
product_name=[]
price=[]
weight=[]
price_per_kg=[]
promotion=[]
quantity_stock=[]
categories=[]
date=[]
expiration_date=[]
urls=[]
store=[]
try:
    driver.set_page_load_timeout(300)  # Increase timeout to 5 minutes

    driver.maximize_window()
    # Open Monoprix's page
    driver.get("https://courses.monoprix.fr/categories/mes-marques-monoprix/monoprix/produits-frais/42125bd9-72ae-437e-82bc-e7e7a1589879?sortBy=favorite")

    # Wait for the "Reject All" button to appear and click it
    try:
        reject_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="onetrust-reject-all-handler"]'))
        )
        reject_button.click()
        print("Rejected cookies successfully!")
    except TimeoutException:
        print("No cookie popup found.")

    time.sleep(2)  # Small delay after rejecting cookies
    hrefs = []
    texts = []

    # Find all <a> tags with relevant attributes (like data-test="root-category-link")
    links = driver.find_elements(By.XPATH, '//*[@data-test="root-category-link"]')

    # Loop through each link and extract href and text
    for link in links:
        href = link.get_attribute('href')  # Get href attribute
        text = link.text  # Get the text content
        hrefs.append(href)
        texts.append(text)
    for u,href in enumerate(hrefs):
        driver.get(href)  # Go to each link
        time.sleep(2)

        for m in range(0,5):
        # Scroll down to the bottom of the page
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)


        elements = driver.find_elements(By.XPATH, '//*[@id="product-page"]/div/div[2]/div/div[1]/div')
        for n in range(2, (len(elements)+1)):


            try:
                img_src = driver.find_element(By.XPATH,
                                              f'//*[@id="product-page"]/div/div[2]/div/div[1]/div[{n}]/div[2]/div[1]/a/div/img').get_attribute(
                    'src')
            except:
                img_src = None
            imgs.append(img_src)

            try:
                name = driver.find_element(By.XPATH,
                                              f'//*[@id="product-page"]/div/div[2]/div/div[1]/div[{n}]/div[2]/div[2]/div[1]/div[1]/a/h3').text
            except:
                name = None
            product_name.append(name)

            try:
                price_value = driver.find_element(By.XPATH,
                                                  f'//*[@id="product-page"]/div/div[2]/div/div[1]/div[{n}]/div[2]/div[2]/div[2]/div[1]/div/span[2]').text
            except:
                price_value = None
            price.append(price_value)

            try:
                garantie_element = driver.find_element(By.XPATH,
                                                             f'//*[@id="product-page"]/div/div[2]/div/div[1]/div[{n}]/div[2]/div[1]/div[2]/div/div[2]/div/span[1]')
                expiration_date.append(garantie_element.text.strip())
            except NoSuchElementException:
                expiration_date.append(None)

            try:
                weight_value = driver.find_element(By.XPATH,
                                                   f'//*[@id="product-page"]/div/div[2]/div/div[1]/div[{n}]/div[2]/div[2]/div[1]/div[3]/span[1]').text
            except:
                weight_value = None
            weight.append(weight_value)

            try:
                spans = driver.find_elements(By.XPATH,
                                             f'//*[@id="product-page"]/div/div[2]/div/div[1]/div[{n}]/div[2]/div[2]/div[1]//span')


                price_kg = spans[-1].text
                price_kg = spans[-1].text

            except:
                price_kg = None

            price_per_kg.append(price_kg)

            try:
                promo_text = driver.find_element(By.XPATH,
                                                 f'//*[@id="product-page"]/div/div[2]/div/div[1]/div[{n}]/div[2]/div[2]/div[1]/div[2]/div/a/span[1]').text
            except:
                promo_text = None
            promotion.append(promo_text)

            try:
                url_element = driver.find_element(By.XPATH, f'//*[@id="product-page"]/div/div[2]/div/div[1]/div[{n}]/div[2]/div[1]/a')

                url = url_element.get_attribute('href')  # Get href attribute
            except:
                url = None
            urls.append(url)
            try:
                button = driver.find_element(By.XPATH,
                                             f'//*[@id="product-page"]/div/div[2]/div/div[1]/div[{n}]/div[2]/div[2]/div[2]/div[2]/div/button')

                ActionChains(driver).move_to_element(button).click().perform()
                time.sleep(2)
                input_field = driver.find_element(By.XPATH,
                                                  f'//*[@id="product-page"]/div/div[2]/div/div[1]/div[{n}]/div[2]/div[2]/div[2]/div[2]/div/div/input')

                input_field.click()  # Click to focus
                time.sleep(1)

                input_field.send_keys(Keys.CONTROL + "a")  # Select all text
                input_field.send_keys("100")  # Enter new value
                input_field.send_keys(Keys.ENTER)
                time.sleep(2)

                # Get updated value
                stock_quantity = input_field.get_attribute("value")
                quantity_stock.append(stock_quantity)  # ✅ Append to the list

                input_field.send_keys(Keys.CONTROL + "a")  # Select all text
                input_field.send_keys("0")  # Reset value

            except NoSuchElementException:
                quantity_stock.append(None)
            date.append(datetime.today().strftime('%Y-%m-%d'))
            store.append('Monoprix')
            categories.append(texts[u])
            time.sleep(2)
finally:
    # Close the browser after the actions
    driver.quit()

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
    'Price': price,
    'Weight': weight,
    'Price per kg': price_per_kg,
    'Promotion': promotion,
    'Quantity in stock': quantity_stock,
    'Category': categories,
    'Date': date,
    "Expiration Date": expiration_date,
    'Image URL': imgs,
    'URL': urls ,
    'Store': store
})


# Connect to PostgreSQL
conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()

# Insert DataFrame rows into PostgreSQL
for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO monoprix (name, price, weight, price_per_kg, promotion, quantity_stock, category, date, expiration_date, image_url, url, store)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (row["Name"], row["Price"], row["Weight"], row["Price per kg"], row["Promotion"],
              row["Quantity in stock"], row["Category"], row["Date"], row["Expiration Date"],
              row["Image URL"], row["URL"], row["Store"]))

# Commit and close connection
conn.commit()
cur.close()
conn.close()

folder_path = "/opt/airflow/new_folder4"
# Check if the folder exists before deleting
if os.path.exists(folder_path):
    shutil.rmtree(folder_path)
    print(f"Deleted folder: {folder_path}")
else:
    print(f"Folder does not exist: {folder_path}")