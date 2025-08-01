from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
import pandas as pd
import psycopg2
import logging
import os
from typing import Optional, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI(
    title="Australian Company Data API",
    description="API for unified Australian company data",
    version="1.0.0"
)

class Company(BaseModel):
    unified_id: int
    abn: Optional[str]
    company_name: str
    entity_type: Optional[str]
    entity_status: Optional[str]
    address: Optional[str]
    postcode: Optional[str]
    state: Optional[str]
    start_date: Optional[str]
    industry: Optional[str]
    website_url: Optional[str]
    created_at: Optional[str]
    updated_at: Optional[str]
    llm_confidence: Optional[int] = Field(None)
    llm_reasoning: Optional[str] = Field(None)

def connect_db():
    return psycopg2.connect(
        dbname="company_db",
        user=os.getenv("DB_USER", "readonly_user"),
        password=os.getenv("DB_PASSWORD", "readonly_password"),
        host="localhost",
        port="5432"
    )

@app.get("/companies", response_model=List[Company])
async def get_companies(
    name: Optional[str] = Query(None, description="Partial match for company name"),
    abn: Optional[str] = Query(None, description="Exact match for ABN"),
    industry: Optional[str] = Query(None, description="Filter by industry"),
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0)
):
    try:
        query = "SELECT * FROM unified_companies WHERE 1=1"
        params = {}
        if name:
            query += " AND company_name ILIKE %(name)s"
            params['name'] = f"%{name}%"
        if abn:
            query += " AND abn = %(abn)s"
            params['abn'] = abn
        if industry:
            query += " AND industry ILIKE %(industry)s"
            params['industry'] = f"%{industry}%"
        query += " LIMIT %(limit)s OFFSET %(offset)s"
        params['limit'] = limit
        params['offset'] = offset

        conn = connect_db()
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        if df.empty: return []
        results = df.to_dict(orient='records')
        return results
    except Exception as e:
        logging.error(f"Error querying companies: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")
