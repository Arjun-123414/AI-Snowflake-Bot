# main.py
import os
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from dotenv import load_dotenv
from modelz import SessionLocal, QueryResult
from snowflake_utils import query_snowflake, get_schema_details
from groq_utils import get_groq_response
from action_utils import parse_action_response, execute_action

# Load environment variables
load_dotenv()

# Function to sync SQLite data to Snowflake
from sqlalchemy import text  # Ensure this import is present

def sync_sqlite_to_snowflake():
    try:
        DATABASE_URL = "sqlite:///log.db"
        local_engine = create_engine(DATABASE_URL)
        table_name = "query_result"

        with local_engine.connect() as conn:
            # Fetch only unsynced rows
            df = pd.read_sql(f"SELECT * FROM {table_name} WHERE synced_to_snowflake = FALSE", conn)

        if df.empty:
            print("No new data to sync.")
            return

        SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
        SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
        SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
        SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
        SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
        SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
        SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

        if not all([SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
                    SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE]):
            print("Missing Snowflake credentials in environment variables.")
            return

        snowflake_engine = create_engine(URL(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            warehouse=SNOWFLAKE_WAREHOUSE,
            role=SNOWFLAKE_ROLE
        ))

        snowflake_table_name = "LoginTable"
        print(f"Syncing data to Snowflake table: {snowflake_table_name}")

        with snowflake_engine.connect() as conn:
            # Append new rows to Snowflake
            df.to_sql(
                name=snowflake_table_name,
                con=conn,
                if_exists='append',
                index=False,
                method='multi'
            )
            print(f"Synced {len(df)} new rows to Snowflake.")

            # Mark rows as synced in SQLite
            with local_engine.connect() as local_conn:
                for id in df['id']:
                    # Use text() and pass parameters as a dictionary
                    local_conn.execute(
                        text(f"UPDATE {table_name} SET synced_to_snowflake = TRUE WHERE id = :id"),
                        {"id": id}  # Pass parameters as a dictionary
                    )
                local_conn.commit()

    except Exception as e:
        print(f"Error syncing data to Snowflake: {e}")

# Get schema details
schema_details = get_schema_details()
schema_text = "\n".join(
    [f"Table: {table}, Columns: {', '.join(columns)}" for table, columns in schema_details.items()]
)

react_system_prompt = f"""
    You are a Snowflake SQL assistant. Use the schema below:  
    {schema_text}  

    1. Use exact table/column names, valid joins, and correct foreign keys.  
    2. Handle time queries (`DATEADD`, `DATEDIFF`), NULLs, and incomplete data.  
    3. Ensure Snowflake syntax, proper aggregation (`SUM`, `COUNT`), and `GROUP BY`.  
    4. Optimize queries, avoid unnecessary joins/subqueries, and use aliases.  
    5. Never use `ORDER BY` in UNION subqueries—use `LIMIT` instead.  
    6. Use `DISTINCT` only when necessary.  
    7. Merge multiple queries into one when possible.  
    8. Respond **only with a JSON object** in the following format(never respond in any other format except json):  
    {{
      "function_name": "query_snowflake",
      "function_parms": {{"query": "<Your SQL Query Here>"}}
    }}
"""

available_actions = {"query_snowflake": query_snowflake}

# query_memory = {}

def save_query_result(user_query, natural_language_response, result, sql_query, response_text, tokens_first_call=None, tokens_second_call=None, total_tokens_used=None, error_message=None):
    db_session = SessionLocal()
    try:
        query_result = QueryResult(
            query=user_query,
            answer=str(natural_language_response) if natural_language_response else None,
            sfresult=str(result) if result else None,
            sqlquery=str(sql_query) if sql_query else None,
            raw_response=str(response_text),  # Save raw response
            tokens_first_call=tokens_first_call,  # Tokens used in the first call
            tokens_second_call=tokens_second_call,  # Tokens used in the second call
            total_tokens_used=total_tokens_used,  # Total tokens used
            error_message=str(error_message) if error_message else None  # Save error if present
        )
        db_session.add(query_result)
        db_session.commit()
        sync_sqlite_to_snowflake()
    except Exception as e:
        print(f"Error saving query and result to database: {e}")
    finally:
        db_session.close()


if __name__ == "__main__":
    messages = [{"role": "system", "content": react_system_prompt}]
    query_memory = {}  # Ensuring query memory is properly managed
    total_tokens_used = 0  # Initialize a variable to track cumulative token usage

    while True:
        user_query = input("Enter your query (To exit type exit or quit):  ")
        if user_query.lower() in ["exit", "quit"]:
            print("Exiting chat.")
            print(f"Total Tokens Used in this session: {total_tokens_used}")  # Display total tokens used
            break

        # Append user query to conversation history
        messages.append({"role": "user", "content": user_query})

        # Check query memory for a quick response
        if user_query in query_memory:
            print(query_memory[user_query])
            continue

        # Get raw response from LLM (First Call)
        response_text, token_usage_first_call = get_groq_response(react_system_prompt, messages)
        total_tokens_used += token_usage_first_call  # Add tokens from the first call
        print("Raw Response:", response_text)  # Debugging output
        print(f"Tokens Used (First Call): {token_usage_first_call}")  # Display tokens used in the first call

        # Parse action from the response
        action = parse_action_response(response_text)
        if not action:
            print("Error parsing response.")
            save_query_result(user_query, None, None, None, response_text, error_message="Error parsing response.")
            continue

        # Execute SQL or any function based on action
        result = execute_action(action, available_actions)
        sql_query = action.get("function_parms", {}).get("query", "")

        print("Executed SQL Query:", sql_query)  # Debugging output
        print("Execution Result:", result)  # Debugging output

        # Process result & generate natural language response (Second Call)
        if isinstance(result, list) and result:
            # Maintain full chat history while generating response
            messages.append({"role": "assistant", "content": str(result)})  # Store the raw result

            # Improved prompt with explicit guidance
            natural_language_response, token_usage_second_call = get_groq_response(
                f"User: {user_query}. Result: {result}. Summarize concisely without assumptions. Use chat history for follow-ups; if unclear, infer the last mentioned entity/metric. Exclude SQL and JSON.",
                messages
            )

            total_tokens_used += token_usage_second_call  # Add tokens from the second call
            print(f"Tokens Used (Second Call): {token_usage_second_call}")  # Display tokens used in the second call

            # Store and print the final response
            print("Generated Response:", natural_language_response)  # Debugging output
            query_memory[user_query] = natural_language_response

            # Save query result with token usage data
            save_query_result(
                user_query,
                natural_language_response,
                result,
                sql_query,
                response_text,
                tokens_first_call=token_usage_first_call,
                tokens_second_call=token_usage_second_call,
                total_tokens_used=total_tokens_used,
            )

            # Append assistant response to chat history
            messages.append({"role": "assistant", "content": natural_language_response})
        else:
            print("Error occurred or no results found.")
            save_query_result(
                user_query,
                None,
                None,
                sql_query,
                response_text,
                tokens_first_call=token_usage_first_call,
                tokens_second_call=None,
                total_tokens_used=total_tokens_used,
                error_message="No valid result returned.",
            )

        # Display cumulative token usage after each query
        print(f"Total Tokens Used So Far: {total_tokens_used}")