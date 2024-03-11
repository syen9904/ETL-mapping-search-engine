import os
import pandas as pd
import sqlite3
from typing import Optional, List
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
import asyncio
import nest_asyncio
import aiosqlite
import uvicorn
from datetime import datetime

def get_db_columns(db_path, table_name):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns = [row[1] for row in cursor.fetchall()] 
    conn.close()
    return columns
    
def create_fake_df():
    data = {
        'Name': ['John Doe', 'Jane Doe', 'Jim Brown', 'Jake Blues'],
        'Age': [28, 34, 23, 45],
        'City': ['New York', 'Los Angeles', 'Chicago', 'New Orleans'],
        'Occupation': ['Developer', 'Scientist', 'Manager', 'Musician'],
        'Name1': ['John Doe', 'Jane Doe', 'Jim Brown', 'Jake Blues'],
        'Age1': [28, 34, 23, 45],
        'City1': ['New York', 'Los Angeles', 'Chicago', 'New Orleans'],
        'Occupation1': ['Developer', 'Scientist', 'Manager', 'Musician'],
        'Name2': ['John Doe', 'Jane Doe', 'Jim Brown', 'Jake Blues'],
        'Age2': [28, 34, 23, 45],
        'City2': ['New York', 'Los Angeles', 'Chicago', 'New Orleans'],
        'Occupation2': ['Developer', 'Scientist', 'Manager', 'Musician']
    }
    df = pd.DataFrame(data)
    for i in range(14):
        df = pd.concat([df, df])
    return df

def create_db(csv_path, db_path, table_name) -> List:
    if os.path.exists(db_path): 
        print(f'db already exist at {db_path}')
        return get_db_columns(db_path=db_path, table_name=table_name)
    if os.path.exists(csv_path):
        cteVocabMapDF = pd.read_csv(csv_path, on_bad_lines="skip", delimiter="|")
        print('csv was read successfully. Size:', cteVocabMapDF.shape)
    else:
        cteVocabMapDF = create_fake_df()
        print('test df was created successfully. Size:', cteVocabMapDF.shape)
    conn = sqlite3.connect(db_path)
    cteVocabMapDF.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    print(f'db created at {db_path}')
    print(cteVocabMapDF.columns)
    return cteVocabMapDF.columns.tolist()

def indexing(db_path, columns, table_name):
    conn = sqlite3.connect(db_path)
    for column in columns:     
        index_name = f'idx_{column}'
        conn.execute(f'CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column})')
        print(column)
    conn.close()

def str_to_query(search_str, columns, table_name) -> str:
    select_clause = ", ".join([f'"{col}"' for col in columns])
    like_clauses = [f'"{col}" LIKE "%{search_str}%"' for col in columns]
    query_condition = " OR ".join(like_clauses)
    query = f"SELECT {select_clause} FROM {table_name} WHERE {query_condition}"
    return query 

def current_time():
    return datetime.now().strftime("%H:%M:%S")

async def search(search_str, columns, table_name, retry_count=3, timeout_duration=10):
    query = str_to_query(search_str, columns, table_name)
    for attempt in range(retry_count):
        try:
            async with aiosqlite.connect(db_path) as db:
                # Attempt to execute the query with a timeout
                print(f'[{current_time()}]: {attempt}/3 attempt for searching "{search_str}"')
                cursor = await asyncio.wait_for(db.execute(query), timeout=timeout_duration)
                print(f'[{current_time()}]: query executed')
                result = await asyncio.wait_for(cursor.fetchall(), timeout=timeout_duration)
                print(f'[{current_time()}]: result fetched')
                return {"result": result}
        except asyncio.TimeoutError:
            print(f"Query timeout! Retrying {attempt + 1}/{retry_count}...")
            await asyncio.sleep(1)
        except Exception as e:
            print(f"An error occurred: {e}")
            break  # Stop retrying on non-timeout errors
    return {result: []}  # Return an empty list or appropriate error response if retries exceeded

nest_asyncio.apply()
app = FastAPI()
"""@app.get("/", response_class=HTMLResponse)
async def root():
    # Read the HTML file
    with open("static/template.html", 'r') as file:
        html_content = file.read()

    # Inject the columns into the JavaScript code within the HTML
    columns_js_array = str(columns).replace("'", '"')  # Convert Python list to JS array string
    html_content = html_content.replace("['col1', 'col2']", columns_js_array)
    return HTMLResponse(content=html_content)
"""
@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    with open(os.getcwd() + "/static/template.html", 'r') as file:
        html_content = file.read()
    search_str = request.query_params.get('search_str', '') 
    if search_str:
        results = await search(search_str, columns, table_name)
    else:
        results = {"result": []}
    num_results = len(results["result"])
    return HTMLResponse(content=html_content.format(search_str=search_str, num_results=num_results))

"""
@app.get("/api/search/")
async def api_search(search_str: str):
    # Correctly awaiting the search coroutine
    result = await search(search_str=search_str, columns=columns, table_name=table_name)
    print(f'[{current_time()}]: result returned to html for rendering')
    return result"""

parent_dir = os.path.dirname(os.getcwd())
csv_path = parent_dir + "/cteVocabMapDF.csv"
db_path = parent_dir + "/database.db"
table_name = 'data'
COLUMNS = ['source_code', 'source_concept_id', 'source_code_description', 'source_vocabulary_id', 'source_domain_id', 'source_concept_class_id','target_concept_id', 'target_concept_name', 'target_vocabulary_id', 'target_domain_id', 'target_concept_class_id']

if __name__ == "__main__":
    columns = create_db(csv_path=csv_path, db_path=db_path, table_name=table_name)
    if os.path.exists(csv_path): columns = COLUMNS
    #indexing(db_path=db_path, columns=columns, table_name=table_name)
    uvicorn.run(app, host = "0.0.0.0", port=9487)   