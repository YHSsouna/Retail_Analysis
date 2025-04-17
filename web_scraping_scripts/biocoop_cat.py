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
    "host": "postgres",  # Service name in Docker Compose
    "port": "5432"
}


def fetch_biocoop_data():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # Query to fetch data
        cursor.execute("SELECT DISTINCT(name) FROM biocoop;")
        rows = cursor.fetchall()

        df = pd.DataFrame(rows, columns=["name"])


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
        cursor.execute("SELECT DISTINCT(name) FROM biocoop_categories;")
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

# Reset index if needed
df = df_filtered.reset_index(drop=True)
unique_names = df['name'].unique().tolist()
API_URL = "https://api.groq.com/openai/v1/chat/completions"  # Example endpoint
GROQ_API_KEY = os.getenv("GROQ_API_KEY")


HEADERS = {"Authorization": f"Bearer {GROQ_API_KEY}"}

all_categories = []  # List to store all categorized results
print(len(unique_names))

for i in range(0, len(unique_names), 10):
    # Create a prompt instructing the model
    prompt = f"""
    I will give you a list of product names. Your task is to categorize each product into one of the following categories:
    Céréales & Tartinables
    Lait
    Charcuterie
    Beurre, Margarine & Crèmes
    Eaux & Infusions
    Fruits Secs & Graines
    Glaces & Desserts
    Poissons Surgelés
    Boissons Végétales
    Jus & Smoothies
    Pain & Viennoiseries
    Autre
    Riz, Pâtes & Légumineuses
    Sauces & Épices
    Fruits Frais
    Yaourts
    Plats Cuisinés & Snacking
    Oeufs
    Légumes Frais
    Fromages
    Poissons & Fruits de Mer
    Café & Chocolats Chauds
    Viandes
    Légumes Surgelés
    Conserves & Soupes
    Boissons Gazeuses

    Return only the category names in a JSON array format.

    Example:
    Input: ["product1", "product2", "product3"]
    Output: ["category1", "category2", "category3"]
    **Instructions:**
    - Assign each product to the most relevant category.
    - If no exact match is found put Autre.
    - Just give the output.
    - each time I will give you 10 product names so just give me a list of 10 categories.
    Here are the product names to categorize:

    {unique_names[i:i + 10]}
    """

    data = {
        "model": "gemma2-9b-it",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0
    }
    my = 0
    while my == 0:
        response = requests.post(API_URL, headers=HEADERS, json=data)
        response_json = response.json()

        if response.status_code == 200:
            raw_content = response_json["choices"][0]["message"]["content"]

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
            'Category': all_categories[:df_len]
        })


    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    sql = """
        INSERT INTO biocoop_categories (name, category)  
        VALUES (%s, %s)
    """

    # Insert DataFrame rows into PostgreSQL
    for _, row in df.iterrows():
        cur.execute(sql, (
            row["Product Name"],
            row["Category"]
        ))
        number =+ 1
    # Commit and close connection
    conn.commit()
    cur.close()
    conn.close()
    print(number, ': of categories added')
except :
    print('No new products found')
