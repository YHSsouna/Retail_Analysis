import psycopg2
import time
import requests
import json
import pandas as pd
import re
from dotenv import load_dotenv
import os

load_dotenv()

DB_PARAMS = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "localhost",
    "port": "5432"
}


def fetch_biocoop_data():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # Query to fetch data
        cursor.execute("SELECT DISTINCT(name),quantity FROM auchan;")
        rows = cursor.fetchall()

        df = pd.DataFrame(rows, columns=["name","u_quantity"])


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
        cursor.execute("SELECT DISTINCT(name) FROM auchan_norm;")
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


API_URL = "https://api.groq.com/openai/v1/chat/completions"  # Example endpoint
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

HEADERS = {"Authorization": f"Bearer {GROQ_API_KEY}"}

unique_quantity = df['u_quantity'].unique().tolist()

all_unit = []
all_quantity = []

for i in range(0, len(unique_quantity), 10):
    # Create a prompt instructing the model
    prompt = f"""
You are a data extractor.

Your task is to extract the **unit** and **total quantity** from each item in the list below.

items often include quantity and unit information, like: `4 x 100 g`, `300 ml`, or `x15, 300 g`.

Follow these rules strictly:

1. If the quantity is in `ml`, convert it to **liters (L)** and round to 3 decimals (e.g., 300 ml → 0.300 L).
2. If the quantity is in `kg`, convert it to **grams (g)** (e.g., 4 x 1 kg → 4000 g).
3. If the unit is already in `g` or `L`, keep it.
4. If multiple units are mentioned (e.g., `4 x 110 ml`), **calculate the total quantity**.
5. If the quantity is given after a comma (e.g., `x15, 300 ml`)-->0.3 L.
6. If there's no recognizable quantity or unit, return: `unit: ""`, `quantity: 0`.
7. If the product is not in grams or liters (e.g., sold by piece), return: `unit: "UNITE"`, `quantity: 1`.
8. If 'environ 300 - 400g',--> `unit: g`, `quantity: 350`.
9. Anything that is not a normal unit (12 oeufs,...), return: `unit: "UNITE"`, `quantity: '12'.

**Examples:**
- `"300 g"` → `{{"quantity": 300, "unit": "g"}}`
- `"None"` → `{{"quantity": 0, "unit": ""}}`
- `"4 x 110 ml"` → `{{"quantity": 0.440, "unit": "L"}}`
- `"x15, 300 ml"` → `{{"quantity": 0.300, "unit": "L"}}`
- `"4 x 1 kg"` → `{{"quantity": 4000, "unit": "g"}}`
- `"Lot de 6 assiettes"` → `{{"quantity": 6, "unit": "UNITE"}}`
- `"Dentifrice Colgate"` → `{{"quantity": 0, "unit": ""}}`
- `""` → `{{"quantity": 0, "unit": "}}`

**Return only** the result in **JSON array format**, like this don't write anything else:

[
  {{ "quantity": 200, "unit": "g" }},
  {{ "quantity": 1.000, "unit": "L" }},
  {{ "quantity": 1, "unit": "UNITE" }},
  ...
]

Here is the list of product names:

{unique_quantity[i:i + 10]}
"""

    data = {
        "model": "meta-llama/llama-4-maverick-17b-128e-instruct",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0
    }
    my = 0
    success = False
    while my == 0:
        response = requests.post(API_URL, headers=HEADERS, json=data)
        response_json = response.json()

        if response.status_code == 200:
            raw_content = response_json["choices"][0]["message"]["content"]

            # Ensure it's a valid JSON array before appending
            print(raw_content)
            categories = json.loads(raw_content)

            # Extract units and quantities
            all_unit.extend([item["unit"] for item in categories])
            all_quantity.extend([item["quantity"] for item in categories])

            print(len(all_unit))
            my = 1

        elif response.status_code == 429:  # Rate Limit Exceeded
            print("Rate limit atteint. Attente de 5 secondes...")
            time.sleep(4)  # Attente avant de réessayer

        else:
            print(f"Erreur API: {response.text}")


df_trans = pd.DataFrame({
    'unique_quantity': unique_quantity[:len(all_unit)],
    'unit': all_unit,
    'quantity': all_quantity
})

merged_df = df.merge(df_trans, left_on='u_quantity', right_on='unique_quantity', how='left')
merged_df = merged_df.drop(columns=['unique_quantity'])


df_len = min(len(all_unit), len(all_quantity))
number = 0


try:
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    sql = """
        INSERT INTO auchan_norm (name, u_quantity, unit, quantity)  
        VALUES (%s, %s, %s, %s)
    """

    # Insert DataFrame rows into PostgreSQL
    for _, row in merged_df.iterrows():
        cur.execute(sql, (
            row["name"],
            row["u_quantity"],
            row["unit"],
            row["quantity"]
        ))
        number +=1
    # Commit and close connection
    conn.commit()
    cur.close()
    conn.close()

    print(number, ': of categories added')
except:
    print('No new products found')