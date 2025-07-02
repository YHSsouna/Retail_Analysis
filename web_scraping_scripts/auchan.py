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
import pickle  # For saving cookies
import os
import time
import pandas as pd
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.common.by import By
from concurrent.futures import ThreadPoolExecutor

# Lists to store scraped data
COOKIES_FILE = "/opt/airflow/web_scraping_scripts/cookies.pkl"
LOCAL_STORAGE_FILE = "/opt/airflow/web_scraping_scripts/local_storage.json"

STORE_LOCATION_DATA = {
    "journeySearches": '[{"id":"YOUR_ID_HERE","geoSpatialData":{"address":{"zipcode":"YOUR_ZIPCODE","city":"YOUR_CITY","country":"France"},"location":{"latitude":"YOUR_LATITUDE","longitude":"YOUR_LONGITUDE"},"accuracy":"MUNICIPALITY"},"position":"1"}]'
}
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

    # Inject store location into local storage again
    for key, value in STORE_LOCATION_DATA.items():
        driver.execute_script(f"window.localStorage.setItem('{key}', '{value}');")

    print("Local storage restored.")
    driver.refresh()  # Refresh to apply session data
# Path to Edge WebDriver
EDGE_DRIVER_PATH = "/usr/local/bin/msedgedriver"

# Initialize WebDriver Options
options = webdriver.EdgeOptions()
options.add_argument("--headless")  # Run without GUI (important for Docker)
options.add_argument("--disable-gpu")  # Prevent GPU issues
options.add_argument("--no-sandbox")  # Required for running as root in Docker
options.add_argument("--disable-dev-shm-usage")  # Avoid shared memory issues
options.add_argument("--user-data-dir=/opt/airflow/new_folder18")

# Set the window size to match your screen resolution
options.add_argument("--start-maximized")  # Maximizes the browser window
options.add_argument("--window-size=1920,1080")  # Change based on your screen resolution

# options.add_experimental_option("detach", True)  # Keeps the browser open

# Start Microsoft Edge browser
service = Service(EDGE_DRIVER_PATH)
driver = webdriver.Edge(service=service, options=options)


driver.execute_cdp_cmd("Browser.grantPermissions", {
    "origin": "https://www.auchan.fr/",
    "permissions": ["geolocation"]
})

# Override the geolocation with your provided coordinates.
driver.execute_cdp_cmd("Emulation.setGeolocationOverride", {
    "latitude": 48.83514963601109,
    "longitude": 2.3646013309764187,
    "accuracy": 100  # Adjust accuracy (in meters) if necessary.
})
# Lists to store scraped data
hrefs=[]
sub_categories=[]


try:
    driver.maximize_window()
    # Open the website
    driver.get("https://www.auchan.fr/produits-de-nos-regions/ca-b202406051513")


    # Wait for the cookie popup and reject it
    try:
        reject_button = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="onetrust-reject-all-handler"]'))
        )
        reject_button.click()
        print("Rejected cookies successfully!")
    except TimeoutException:
        print("No cookie popup found.")
    time.sleep(1)
    start_time = time.time()
    while time.time() - start_time < 0.1:
        driver.execute_script("window.scrollBy(0, 500);")  # Scroll

    time.sleep(0.1)

    afficher_prix_xpath = '//*[@id="wrapper"]/div[5]/article[1]/div[2]/footer/button'
    wait_and_click(driver, afficher_prix_xpath, 'afficher_prix_xpath')


    find_location_xpath = '//*[@id="journey-update-modal_desc"]/div[1]/div[1]/div/div[1]/button[1]'
    wait_and_click(driver, find_location_xpath, 'find location')
    time.sleep(5)

    chose_location_xpath = '//*[@id="journey-update-modal_desc"]/div[1]/div[2]/div[2]/section/div[1]/div/div/div[2]/form/button'
    wait_and_click(driver, chose_location_xpath, 'chose location')





    time.sleep(1)
    # Get all sectors (categories)
    rayons_xpath = '//*[@id="navigation"]'
    wait_and_click(driver, rayons_xpath, "access rayons")

    categories_xpath = driver.find_elements(By.XPATH, '//*[@id="navigation_layer"]/div/main/nav/div/div[2]/div/div')
    for category in range(1,len(categories_xpath)+1):
        element = driver.find_element(By.XPATH, f'//*[@id="navigation_layer"]/div/main/nav/div/div[2]/div/div[{category}]/a')
        # Get the href attribute
        try:
            element.click()
            print(category)
            time.sleep(1)
            sub_categories_xpath = driver.find_elements(By.XPATH, f'//*[@id="navigation_layer"]/div/main/nav/div/div[2]/div/div[{category}]/div[1]/div[3]/div')
            for sub_category in range(1,len(sub_categories_xpath)+1):
                element = driver.find_element(By.XPATH,
                                              f'//*[@id="navigation_layer"]/div/main/nav/div/div[2]/div/div[{category}]/div[1]/div[3]/div[{sub_category}]')
                element.click()

                sub1_categories_xpath = driver.find_elements(By.XPATH,
                                                            f'//*[@id="navigation_layer"]/div/main/nav/div/div[2]/div/div[{category}]/div[3]/div/div[2]/a')
                print(len(sub1_categories_xpath))
                for sub1_category in range(1,len(sub1_categories_xpath)+1):
                    element = driver.find_element(By.XPATH,
                                                                 f'//*[@id="navigation_layer"]/div/main/nav/div/div[2]/div/div[{category}]/div[3]/div/div[2]/a[{sub1_category}]')
                    href_value = element.get_attribute("href")
                    hrefs.append(href_value)
                    sub_value = element.text
        except:
            print('collection over')
        print(len(hrefs))
        filtered_hrefs = [
            href for href in hrefs if not any(
                keyword in href for keyword in [
                    "hygiene-beaute-parapharmacie",
                    "bebe",
                    "animalerie",
                    "produits-de-nos-regions-et-du-monde",
                    "entretien-maison",
                    "bio-et-nutrition",
                    "vins-bieres-alcool"
                ]
            )
        ]
        print(len(filtered_hrefs))
        return_rayons_xpath = f'//*[@id="navigation_layer"]/div/main/nav/div/div[2]/div/div[{category}]/div[1]/div[1]/button'
        wait_and_click(driver, return_rayons_xpath, "return rayons")
finally:
    driver.quit()


def scrape_page(filtered_hrefs,thread_id):
    # Initialize WebDriver Options
    edge_driver_path = '/usr/local/bin/msedgedriver'
    options = webdriver.EdgeOptions()
    options.add_argument("--headless")  # Run without GUI (important for Docker)
    options.add_argument("--disable-gpu")  # Prevent GPU issues
    options.add_argument("--no-sandbox")  # Required for running as root in Docker
    options.add_argument("--disable-dev-shm-usage")  # Avoid shared memory issues
    profile_dir = f"/opt/airflow/edge_profile_{thread_id+1000}"
    os.makedirs(profile_dir, exist_ok=True)
    options.add_argument(f"--user-data-dir={profile_dir}")
    # Start Microsoft Edge browser
    service = Service(edge_driver_path)
    driver = webdriver.Edge(service=service, options=options)

    # === Set Geolocation Override ===
    # Grant permission for geolocation to the Carrefour site.
    driver.execute_cdp_cmd("Browser.grantPermissions", {
        "origin": "https://www.auchan.fr/",
        "permissions": ["geolocation"]
    })
    driver.execute_cdp_cmd("Emulation.setGeolocationOverride", {
        "latitude": 48.83514963601109,
        "longitude": 2.3646013309764187,
        "accuracy": 100
    })

    local_product_price = []
    local_promotion = []
    local_imgs = []
    local_marque_name = []
    local_product_name = []
    local_price_per_quantity = []
    local_stock = []
    local_quantity = []
    local_cagnottes = []
    local_categories = []
    local_sub_categories = []

    driver.get(filtered_hrefs)
    driver.maximize_window()
    load_session_data(driver)
    driver.execute_script("document.body.style.zoom='25%'")

    try:
        while True:
            # Scroll down by 100px every 0.1 second
            start_time = time.time()
            while time.time() - start_time < 0.1:
                driver.execute_script("window.scrollBy(0, 500);")  # Scroll

            time.sleep(0.1)

            # Check if 1 second has passed and reset timer
            try:
                # Find the next page link
                next_page_element = driver.find_element(By.XPATH, '//*[@aria-label="Aller sur la page suivante"]')
                next_page_href = next_page_element.get_attribute("href")
                print(f"Next page href: {filtered_hrefs}")
                all_elemnest = driver.find_elements(By.XPATH, '//*[@id="wrapper"]/div[5]/article')
                print(len(all_elemnest))
                for i in range(1, len(all_elemnest) + 1):

                    try:
                        img_element = driver.find_element(By.XPATH,
                                                          f'//*[@id="wrapper"]/div[5]/article[{i}]/div[2]/a/span/span/picture/meta')
                        local_imgs.append(img_element.get_attribute('content'))
                    except NoSuchElementException:
                        print('No image found.')
                        local_imgs.append(None)

                    try:
                        marque_element = driver.find_element(By.XPATH,
                                                             f'//*[@id="wrapper"]/div[5]/article[{i}]/div[2]/a/div/p/strong')
                        local_marque_name.append(marque_element.text)
                    except NoSuchElementException:
                        print('No name found.')
                        local_marque_name.append(None)

                    try:
                        name_element = driver.find_element(By.XPATH,
                                                           f'//*[@id="wrapper"]/div[5]/article[{i}]/div[2]/a/div/p')
                        local_product_name.append(name_element.text)
                    except NoSuchElementException:
                        print('No name found.')
                        local_product_name.append(None)

                    try:
                        price_element = driver.find_element(By.XPATH,
                                                            f'//*[@id="wrapper"]/div[5]/article[{i}]/div[2]/footer/div[last()]/div[1]/div')
                        local_product_price.append(price_element.text)
                    except NoSuchElementException:
                        print('No price found.')
                        local_product_price.append(None)

                    try:
                        price_per_quantity_element = driver.find_element(By.XPATH,
                                                                         f'//*[@id="wrapper"]/div[5]/article[{i}]/div[2]/a/div/div[1]/span[2]')
                        local_price_per_quantity.append(price_per_quantity_element.text)
                    except NoSuchElementException:
                        print('No price per quantity found.')
                        local_price_per_quantity.append(None)

                    try:
                        quantity_element = driver.find_element(By.XPATH,
                                                               f'//*[@id="wrapper"]/div[5]/article[{i}]/div[2]/a/div/div[1]/span[1]')
                        local_quantity.append(quantity_element.text)
                    except NoSuchElementException:
                        print('No price per quantity found.')
                        local_quantity.append(None)

                    try:
                        stock_element = driver.find_element(By.XPATH,
                                                            f'//*[@id="wrapper"]/div[5]/article[{i}]/div[2]/footer/div[last()]/div[2]/div')
                        local_stock.append(stock_element.get_attribute('data-stock'))
                    except NoSuchElementException:
                        print('No stock found.')
                        local_stock.append(None)

                    try:
                        cagnottes_element = driver.find_element(By.XPATH,
                                                                f'//*[@id="wrapper"]/div[5]/article[{i}]/div[2]/footer/div[1]/div/div/span')
                        local_cagnottes.append(cagnottes_element.text)
                    except NoSuchElementException:

                        local_cagnottes.append(None)

                    local_categories.append(category)

                    path_parts = urlparse(filtered_hrefs).path.split("/")
                    cate = path_parts[-2].replace("-", " ")

                    local_sub_categories.append(cate)

                if next_page_href:
                    driver.get(next_page_href)  # Navigate to the next page
                    time.sleep(1)
            except Exception as e:

                break

    finally:
        driver.quit()
        shutil.rmtree(profile_dir, ignore_errors=True)
        print('deleted folder')

    return {
        'price': local_product_price,
        'promo': local_promotion,
        'img': local_imgs,
        'name': local_product_name,
        'price_per_q': local_price_per_quantity,
        'stock': local_stock,
        'quantity': local_quantity,
        'categories': local_categories,
        'cagnotes': local_cagnottes,
        'sub_category': local_sub_categories,
        'marque': local_marque_name
    }



# Build argument list with unique thread IDs
args = [(url, i) for i, url in enumerate(filtered_hrefs)]

# Wrapper so map only takes one argument
def wrapper(arg):
    return scrape_page(*arg)


# Run threads and collect per-thread results
all_product_price = []
all_imgs = []
all_product_name = []
all_price_per_quantity = []
all_stock = []
all_cagnottes = []
all_sub_category = []
all_category = []
all_quantity = []
all_marque = []

# ThreadPool execution
with ThreadPoolExecutor(max_workers=5) as executor:
    results = list(executor.map(wrapper, args))

# Combine all thread results into final lists
for res in results:
    all_product_price.extend(res['price'])
    all_cagnottes.extend(res['cagnotes'])
    all_imgs.extend(res['img'])
    all_product_name.extend(res['name'])
    all_price_per_quantity.extend(res['price_per_q'])
    all_stock.extend(res['stock'])
    all_marque.extend(res['marque'])
    all_quantity.extend(res['quantity'])
    all_category.extend(res['categories'])
    all_sub_category.extend(res['sub_category'])

print("Prices:", len(all_product_price))
print("Cagnottes:", len(all_cagnottes))
print("Images:", len(all_imgs))
print("Product Names:", len(all_product_name))
print("Price per Quantity:", len(all_price_per_quantity))
print("Stock:", len(all_stock))
print("Marques:", len(all_marque))
print("Quantities:", len(all_quantity))
print("Categories:", len(all_category))
print("Sub-Categories:", len(all_sub_category))

# Define PostgreSQL connection parameters
DB_PARAMS = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",  # Service name in Docker Compose
    "port": "5432"
}
# # Create DataFrame
df = pd.DataFrame({
    'Name': all_product_name,
    'Price': all_product_price,
    'Promotion ou cagnotte': all_cagnottes,
    'Quantity in stock': all_stock,
    'Category': all_category,
    'Sub Category': all_sub_category,
    'Quantity': all_quantity,
    'Price_per_quantity': all_price_per_quantity,
    'Marque': all_marque,
    'Date':  datetime.today().strftime('%Y-%m-%d'),
    'Image URL': all_imgs,
    'Store': 'Auchan'
})



conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()

sql = """
    INSERT INTO auchan (name, price, promotion, quantity_stock, category, sub_category, quantity, 
                        price_per_quantity, marque, date, image_url, store)  
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Insert DataFrame rows into PostgreSQL
for _, row in df.iterrows():
    cur.execute(sql, (
        row["Name"],
        row["Price"],
        row["Promotion ou cagnotte"],
        row["Quantity in stock"],
        row["Category"],
        row["Sub Category"],
        row["Quantity"],
        row["Price_per_quantity"],
        row["Marque"],
        row["Date"],
        row["Image URL"],
        row["Store"]
    ))
# Commit and close connection
conn.commit()
cur.close()
conn.close()

folder_path = "/opt/airflow/new_folder18"
# Check if the folder exists before deleting
if os.path.exists(folder_path):
    shutil.rmtree(folder_path)
    print(f"Deleted folder: {folder_path}")
else:
    print(f"Folder does not exist: {folder_path}")