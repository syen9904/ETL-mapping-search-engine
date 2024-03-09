import os
import pandas as pd
import sqlite3
from typing import Optional, List
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
import aiosqlite
import nest_asyncio
import uvicorn

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
        'Occupation': ['Developer', 'Scientist', 'Manager', 'Musician']
    }
    df = pd.DataFrame(data)
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
    return cteVocabMapDF.columns

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

async def search(search_str, columns, table_name) -> List:
    query = str_to_query(search_str, columns, table_name)
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute(query)
        result = await cursor.fetchall()
        return result
 
async def construct_html_table(search_str: str, columns: List[str], table_name) -> str:
    result = await search(search_str, columns, table_name)
    if not result:
        return '<p>No results found.</p>'

    table_html = '<table border="1"><tr>'
    for col in columns:
        table_html += f'<th>{col}</th>'
    table_html += '</tr>'

    for row in result:
        table_html += '<tr>' + ''.join([f'<td>{cell}</td>' for cell in row]) + '</tr>'
    table_html += '</table>'

    return table_html

nest_asyncio.apply()
app = FastAPI()
@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    with open("template.html", 'r') as file:
        html_content = file.read()

    search_str = request.query_params.get('search_str', '') 
    if search_str:
        results = await construct_html_table(search_str, columns, table_name)
    else:
        results = ""
    return HTMLResponse(content=html_content.format(search_str=search_str, results=results))

parent_dir = os.path.dirname(os.getcwd())
csv_path = parent_dir + "/cteVocabMapDF.csv"
db_path = parent_dir + "/database.db"
table_name = 'data'
COLUMNS = ['source_code', 'source_concept_id', 'source_code_description', 'source_vocabulary_id', 'source_domain_id', 'source_concept_class_id','target_concept_id', 'target_concept_name', 'target_vocabulary_id', 'target_domain_id', 'target_concept_class_id']

if __name__ == "__main__":
    columns = create_db(csv_path=csv_path, db_path=db_path, table_name=table_name)
    if os.path.exists(csv_path): columns = COLUMNS
    #indexing(db_path=db_path, columns=columns, table_name=table_name)
    uvicorn.run(app, host = "0.0.0.0", port=8000)   