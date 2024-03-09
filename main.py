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

def csv_to_sqlite(csv_path, db_path):
    cteVocabMapDF = pd.read_csv(csv_path, on_bad_lines="skip", delimiter="|")
    print(cteVocabMapDF.shape)
    #sample_cteVocabMapDF = cteVocabMapDF.sample(frac = 1, random_state = 1)
    #print(sample_cteVocabMapDF.shape)
    conn = sqlite3.connect(db_path)
    cteVocabMapDF.to_sql('data', conn, if_exists='replace', index=False)
    #sample_cteVocabMapDF.to_sql('data', conn, if_exists='replace', index=False)
    conn.close()

def indexing(db_path, columns):
    conn = sqlite3.connect(db_path)
    for column in columns:     
        index_name = f'idx_{column}'
        conn.execute(f'CREATE INDEX IF NOT EXISTS {index_name} ON data ({column})')
        print(column)
    conn.close()

def str_to_query(search_str, columns):
    select_clause = ", ".join([f'"{col}"' for col in columns])
    like_clauses = [f'"{col}" LIKE "%{search_str}%"' for col in columns]
    query_condition = " OR ".join(like_clauses)
    query = f"SELECT {select_clause} FROM data WHERE {query_condition}"
    return query 

async def search(search_str, columns):
    query = str_to_query(search_str, columns)
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute(query)
        result = await cursor.fetchall()
        return result
 
async def construct_html_table(search_str: str, columns: List[str]) -> str:
    result = await search(search_str, columns)
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
        results = await construct_html_table(search_str, columns)
    else:
        results = ""
    return HTMLResponse(content=html_content.format(search_str=search_str, results=results))

csv_path = "cteVocabMapDF.csv"
db_path = "database.db"
columns = ['source_code', 'source_concept_id', 'source_code_description'
    , 'source_vocabulary_id', 'source_domain_id', 'source_concept_class_id'
    ,'target_concept_id', 'target_concept_name', 'target_vocabulary_id'
    , 'target_domain_id', 'target_concept_class_id']

#csv_to_sqlite(csv_path=csv_path, db_path=db_path)
#indexing(db_path=db_path, columns=columns)

if __name__ == "__main__":
    uvicorn.run(app, host = "0.0.0.0", port=8000)   