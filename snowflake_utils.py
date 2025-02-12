import os
import snowflake.connector
from typing import List, Dict, Any

def query_snowflake(query: str) -> List[Dict[str, Any]]:
    """Execute one or multiple queries on Snowflake and return structured results."""
    conn = None
    cursor = None
    try:
        # Establish connection
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse="COMPUTE_WH",
            database="PRODUCTS",
            schema="PRODUCT",
        )
        cursor = conn.cursor()

        # Split multiple queries
        queries = [q.strip() for q in query.split(";") if q.strip()]

        results = []
        for q in queries:
            cursor.execute(q)
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            results.append({"query": q, "data": [dict(zip(column_names, row)) for row in result]})

        # If only one query, return directly for backward compatibility
        return results if len(results) > 1 else results[0]["data"]

    except Exception as e:
        return [{"error": str(e)}]

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()



def get_schema_details() -> Dict[str, List[str]]:
    """Fetch schema details dynamically from Snowflake."""
    conn = None
    cursor = None
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse="COMPUTE_WH",
            database="PRODUCTS",
            schema="PRODUCT",
        )
        cursor = conn.cursor()

        # Fetch table names
        cursor.execute("SHOW TABLES;")
        tables = [row[1] for row in cursor.fetchall()]

        # Fetch column details for each table
        schema_details = {}
        for table in tables:
            cursor.execute(f"DESCRIBE TABLE {table};")
            schema_details[table] = [row[0] for row in cursor.fetchall()]

        return schema_details

    except Exception as e:
        return {"error": str(e)}

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
