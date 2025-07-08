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
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        cursor.execute("SELECT DISTINCT(name) FROM auchan;")
        rows = cursor.fetchall()

        df = pd.DataFrame(rows, columns=["name"])

        cursor.close()
        conn.close()
        return df
    except Exception as e:
        print(f"Error: {e}")


df = fetch_biocoop_data()


def fetch_biocoop_data():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # Query to fetch data
        cursor.execute("SELECT DISTINCT(product_name) FROM auchan_section;")
        rows = cursor.fetchall()

        df = pd.DataFrame(rows, columns=["name"])

        # Close connection
        cursor.close()
        conn.close()
        return df
    except Exception as e:
        print(f"Error: {e}")


# Run the function
df_cat = fetch_biocoop_data()

df_filtered = df[~df['name'].isin(df_cat['name'])]

df = df_filtered.reset_index(drop=True)

unique_names = df['name'].unique().tolist()
API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
HEADERS = {"Authorization": f"Bearer {GROQ_API_KEY}"}

HEADERS = {"Authorization": f"Bearer {GROQ_API_KEY}"}

all_categories = []  # List to store all categorized results
print(len(unique_names))

for i in range(0, len(unique_names), 10):
    prompt = f"""
    I will give you a list of product names. Your task is to assign a section to each product into one of the following sections:
    Grocery / Food
    Beverages
    Frozen
    Bakery & Pastry
    Dairy
    Meat & Fish
    Fruits & Vegetables
    Baby & Kids
    Household / Cleaning
    Home & Kitchen
    Clothing & Accessories
    Autre
    Return only the section names in a JSON array format.

    Example:
    Input: ["product1", "product2", "product3"]
    Output: ["section1", "section2", "section3"]
    **Instructions:**
    - Assign each product to the most relevant category.
    - If no exact match is found put Autre.
    - Just give the output list with no other words.
    - each time I will give you 10 product names so just give me a list of 10 categories.
    - don't say this Here is the categorized list:
    Here are the product names to categorize:

    {unique_names[i:i + 10]}
    """

    data = {
        "model": "llama3-70b-8192",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0
    }
    my = 0
    while my == 0:
        response = requests.post(API_URL, headers=HEADERS, json=data)
        response_json = response.json()

        if response.status_code == 200:
            raw_content = response_json["choices"][0]["message"]["content"]
            print(raw_content)
            # Ensure it's a valid JSON array before appending
            categories = json.loads(raw_content)

            # Append the batch result to the main list
            all_categories.extend(categories)
            print(len(all_categories))
            my = 1

        elif response.status_code == 429:  # Rate Limit Exceeded
            print("Rate limit atteint. Attente de 5 secondes...")
            time.sleep(4)  # Attente avant de réessayer
        else:
            print(f"Erreur API: {response.text}")

df_len = min(len(unique_names), len(all_categories))
number = 0

try:
    df = pd.DataFrame({
        'Product Name': unique_names[:df_len],
        'Section': all_categories[:df_len]
    })
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    sql = """
            INSERT INTO auchan_section (product_name, section)  
            VALUES (%s, %s)
    """

    # Insert DataFrame rows into PostgreSQL
    for _, row in df.iterrows():
        cur.execute(sql, (
            row["Product Name"],
            row["Section"]
        ))
        number += 1
    # Commit and close connection
    conn.commit()
    cur.close()
    conn.close()
    print(number, ': of sections added')
except:
    print('No new products found')