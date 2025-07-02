import psycopg2
import time
import requests
import json
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()


DB_PARAMS = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": "5432"
}


def fetch_biocoop_data():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # Query to fetch data
        cursor.execute("SELECT DISTINCT(product_name), section FROM biocoop_section;")
        rows = cursor.fetchall()

        df = pd.DataFrame(rows, columns=["name", "section"])

        # Close connection
        cursor.close()
        conn.close()
        return df
    except Exception as e:
        print(f"Error: {e}")


# Run the function
df = fetch_biocoop_data()


def fetch_biocoop_data():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # Query to fetch data
        cursor.execute("SELECT DISTINCT(product_name) FROM biocoop_cat;")
        rows = cursor.fetchall()

        df = pd.DataFrame(rows, columns=["name"])

        # Close connection
        cursor.close()
        conn.close()
        return df
    except Exception as e:
        print(f"Error: {e}")


# Run the function
df_norm = fetch_biocoop_data()
df_filtered = df[~df['name'].isin(df_norm['name'])]
df = df_filtered.reset_index(drop=True)

category_dict = {
    "Grocery / Food": [
        "Pasta & Rice",
        "Canned Foods",
        "Sauces & Spices",
        "Snacks",
        "Breakfast Cereals",
        "Biscuits & Cookies"
    ],
    "Beverages": [
        "Water",
        "Sodas",
        "Juices",
        "Coffee & Tea",
        "Alcoholic Beverages"
    ],
    "Dairy": [
        "Eggs",
        "Milk",
        "Yogurt",
        "Cheese",
        "Butter & Cream",
        "Plant-Based Alternatives"
    ],
    "Frozen": [
        "Frozen Vegetables",
        "Frozen Meals",
        "Ice Cream",
        "Frozen Meat"
    ],
    "Meat & Fish": [
        "Poultry",
        "Beef",
        "Pork",
        "Fish",
        "Deli Meats"
    ],
    "Bakery & Pastry": [
        "Bread",
        "Croissants & Viennoiseries",
        "Cakes & Tarts"
    ],
    "Fruits & Vegetables": [
        "Fresh Fruits",
        "Fresh Vegetables",
        "Herbs & Aromatics"
    ],
    "Baby & Kids": [
        "Baby Food",
        "Diapers & Wipes",
        "Baby Care"
    ],
    "Beauty & Personal Care": [
        "Hair Care",
        "Skin Care",
        "Oral Care",
        "Shaving & Deodorants",
        "Feminine Hygiene"
    ],
    "Household / Cleaning": [
        "Laundry Products",
        "Dishwashing",
        "Surface Cleaners",
        "Trash Bags",
        "Air Fresheners"
    ],
    "Home & Kitchen": [
        "Kitchen Utensils",
        "Storage",
        "Cookware",
        "Décor",
        "Lighting"
    ],
    "Clothing & Accessories": [
        "Men’s Clothing",
        "Women’s Clothing",
        "Underwear",
        "Socks",
        "Accessories"
    ],
    "Leisure & Toys": [
        "Board Games",
        "Toys",
        "Sports & Fitness",
        "Outdoor",
        "Candles"
    ],
    "Autre": [
    ]
}


# Create separate DataFrames for each section and store in a dictionary
section_dfs = {}

for section in df["section"].dropna().unique():
    section_dfs[section] = df[df["section"] == section].copy()


def insert_auchan_categories(df, DB_PARAMS, verbose=True):
    """
    Inserts product names and categories into the 'auchan_cat' table.

    Parameters:
    - df: DataFrame with 'Product Name' and 'Categories' columns
    - DB_PARAMS: dict containing PostgreSQL connection parameters
    - verbose: if True, prints status messages
    """
    if df.empty:
        if verbose:
            print("⚠️ DataFrame is empty. No rows to insert.")
        return

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()

        sql = """
            INSERT INTO biocoop_cat (product_name, category)  
            VALUES (%s, %s)
        """

        count = 0
        for _, row in df.iterrows():
            cur.execute(sql, (
                row["Product Name"],
                row["Categories"]
            ))
            count += 1

        conn.commit()
        cur.close()
        conn.close()

        if verbose:
            print(f"✅ {count} categories inserted into 'auchan_cat'.")

    except Exception as e:
        print(f"❌ Error inserting rows: {e}")


API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
HEADERS = {"Authorization": f"Bearer {GROQ_API_KEY}"}


def categorize_all_products(category_dict, section_dfs):
    all_results = {}

    for section, categories in category_dict.items():
        print(f"\nProcessing section: {section}")

        try:
            unique_names = section_dfs[section]['name'].unique().tolist()
        except KeyError:
            print(f"❌ Warning: No dataframe found for section '{section}' in section_dfs.")
            continue

        all_categories = []

        for i in range(0, len(unique_names), 10):
            product_batch = unique_names[i:i + 10]
            prompt = f"""
I will give you a list of product names. These are in the '{section}' section. Your task is to categorize each product into one of the following categories: 
{categories}
Return only the category names in a JSON array format.

**Instructions:**
- Assign each product to the most relevant category.
- If no exact match is found, put "Autre".
- Just give the output list with no other words.
- Each time I will give you 10 product names, so just give me a list of 10 categories.
- Don't say anything like "Here is the categorized list:"
Here are the product names to categorize:

{product_batch}
"""

            data = {
                "model": "llama3-70b-8192",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0
            }

            success = False
            while not success:
                response = requests.post(API_URL, headers=HEADERS, json=data)
                if response.status_code == 200:
                    try:
                        raw_content = response.json()["choices"][0]["message"]["content"].strip()
                        categories_batch = json.loads(raw_content)
                        all_categories.extend(categories_batch)
                        print(f"✓ Processed {len(all_categories)} / {len(unique_names)} in '{section}'")
                        success = True
                    except Exception as parse_err:
                        print(f"❌ Failed to parse JSON: {raw_content}")
                        time.sleep(3)
                elif response.status_code == 429:
                    print("⏳ Rate limit hit. Waiting 5 seconds...")
                    time.sleep(5)
                else:
                    print(f"❌ API Error: {response.status_code} - {response.text}")
                    break

        # Match lengths in case of slight mismatch
        df_len = min(len(unique_names), len(all_categories))

        df = pd.DataFrame({
            'Product Name': unique_names[:df_len],
            'Categories': all_categories[:df_len]
        })

        # Insert into DB
        insert_auchan_categories(df, DB_PARAMS)
        print(f"✅ Inserted {df_len} categorized products from section '{section}' into the database.")

        # Store for reference (optional)
        all_results[section] = df


categorize_all_products(category_dict, section_dfs)
