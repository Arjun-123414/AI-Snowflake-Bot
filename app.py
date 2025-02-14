import os
import pandas as pd
from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL
from dotenv import load_dotenv
from modelz import SessionLocal, QueryResult
from snowflake_utils import query_snowflake, get_schema_details
from groq_utils import get_groq_response
from action_utils import parse_action_response, execute_action
import streamlit as st
from PIL import Image

# Load environment variables
load_dotenv()

# Streamlit app configuration
st.set_page_config(
    page_title="❄️ Snowflake Data Assistant",
    page_icon="❄️",
    layout="wide"
)

# Custom CSS for styling
def local_css(file_name):
    with open(file_name) as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

local_css("style.css")

# Connect to Snowflake
def get_snowflake_connection():
    return create_engine(URL(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE")
    ))

# Authenticate user
def authenticate_user(email, password):
    if not email.endswith("@ahs.com"):
        return False  # Restrict access to emails ending with @ahc.com

    engine = get_snowflake_connection()
    with engine.connect() as conn:
        query = text("SELECT COUNT(*) FROM UserPasswordName WHERE username = :email AND password = :password")
        result = conn.execute(query, {"email": email, "password": password}).fetchone()
        return result[0] > 0  # Returns True if user exists, False otherwise

# Check if user needs to change password
def needs_password_change(email):
    engine = get_snowflake_connection()
    with engine.connect() as conn:
        query = text("SELECT initial FROM UserPasswordName WHERE username = :email")
        result = conn.execute(query, {"email": email}).fetchone()
        return result[0] if result else False

# Update password in Snowflake
def update_password(email, new_password):
    engine = get_snowflake_connection()
    with engine.connect() as conn:
        query = text("UPDATE UserPasswordName SET password = :new_password, initial = FALSE WHERE username = :email")
        conn.execute(query, {"new_password": new_password, "email": email})
        conn.commit()

# Password Change Page
def password_change_page():
    st.title("🔐 Change Password")
    email = st.session_state["user"]
    current_password = st.text_input("Current Password", type="password", placeholder="Enter your current password")
    new_password = st.text_input("New Password", type="password", placeholder="Enter your new password")
    confirm_password = st.text_input("Confirm New Password", type="password", placeholder="Confirm your new password")

    if st.button("Change Password"):
        if authenticate_user(email, current_password):
            if new_password == confirm_password:
                update_password(email, new_password)
                st.success("Password changed successfully!")
                st.session_state["password_changed"] = True
                st.rerun()
            else:
                st.error("New passwords do not match!")
        else:
            st.error("Incorrect current password!")

# Login Page
def login_page():
    st.title("🔐 Login to Snowflake Assistant")

    email = st.text_input("Email", placeholder="Enter your email")
    password = st.text_input("Password", type="password", placeholder="Enter your password")

    if st.button("Login"):
        if authenticate_user(email, password):
            st.session_state["authenticated"] = True
            st.session_state["user"] = email
            st.rerun()
        else:
            st.error("Invalid credentials! Please try again.")

# Main Application
def main_app():
    with st.sidebar:
        logo = Image.open("logo.png")  # Replace with your logo (500x500px)
        st.image(logo, width=200)
        st.markdown("""
        ❄️ Snowflake Data Assistant
        Powered by Groq & Streamlit
        Version 1.0
        Learn More | Documentation
        """)

        # Logout button
        if st.button("Logout"):
            st.session_state["authenticated"] = False
            st.session_state["user"] = None
            st.rerun()

        # Token usage stats
        st.divider()
        st.subheader("Session Stats")
        if 'total_tokens' not in st.session_state:
            st.session_state.total_tokens = 0
        st.metric("Total Tokens Used", st.session_state.total_tokens)

    # Main chat interface
    st.title("❄️ Snowflake Data Assistant")
    st.caption("Ask natural language questions about your Snowflake data")

    # Get schema details (from main.py)
    schema_details = get_schema_details()
    schema_text = "\n".join(
        [f"Table: {table}, Columns: {', '.join(columns)}" for table, columns in schema_details.items()]
    )

    # System prompt (from main.py)
    react_system_prompt = f"""  
        You are a Snowflake SQL assistant. Use the schema below:    
        {schema_text}    
        **STRICT RULES** (Violating these will be considered a failure):  
        1. Use exact table/column names, valid joins, and correct foreign keys.    
        2. Handle time queries (DATEADD, DATEDIFF), NULLs, and incomplete data.    
        3. Ensure Snowflake syntax, proper aggregation (SUM, COUNT), and GROUP BY.    
        4. Optimize queries, avoid unnecessary joins/subqueries, and use aliases.    
        5. **NEVER use ORDER BY before UNION. Instead, use ORDER BY inside a subquery with LIMIT, then select from that subquery.**  
        6. Use DISTINCT only when necessary.    
        7. Merge multiple queries into one when possible.    
        8. Respond **only with a JSON object** in the following format(never respond in any other format except json):    
        {{  
          "function_name": "query_snowflake",  
          "function_parms": {{"query": "<Your SQL Query Here>"}}  
        }}  
    """

    # Available actions (from main.py)
    available_actions = {"query_snowflake": query_snowflake}

    # Function to sync SQLite data to Snowflake (from main.py)
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

    # Function to save query results (from main.py)
    def save_query_result(user_query, natural_language_response, result, sql_query, response_text,
                          tokens_first_call=None,
                          tokens_second_call=None, total_tokens_used=None, error_message=None):
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

    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = [{"role": "system", "content": react_system_prompt}]  # System prompt
        st.session_state.chat_history = []  # Separate list for chat history (user + assistant messages)

    # Display chat messages
    for message in st.session_state.chat_history:  # Only display user and assistant messages
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Chat input
    if prompt := st.chat_input("Ask about your Snowflake data..."):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": prompt})
        st.session_state.chat_history.append({"role": "user", "content": prompt})

        with st.chat_message("user"):
            st.markdown(prompt)

        # Generate response
        with st.spinner("Analyzing your query..."):
            try:
                # Get raw response from LLM (First Call)
                response_text, token_usage_first_call = get_groq_response(react_system_prompt,
                                                                          st.session_state.messages)
                st.session_state.total_tokens += token_usage_first_call

                # Parse action from the response
                action = parse_action_response(response_text)
                if not action:
                    raise Exception("Error parsing response.")

                # Execute SQL or any function based on action
                result = execute_action(action, available_actions)
                sql_query = action.get("function_parms", {}).get("query", "")

                # Generate natural language response (Second Call)
                natural_response, token_usage_second_call = get_groq_response(
                    f"User: {prompt}. Result: {result}. Summarize concisely without assumptions. Use chat history for follow-ups; if unclear, infer the last mentioned entity/metric. Exclude SQL and JSON.",
                    st.session_state.messages
                )
                st.session_state.total_tokens += token_usage_second_call

                # Save query result
                save_query_result(
                    prompt,
                    natural_response,
                    result,
                    sql_query,
                    response_text,
                    tokens_first_call=token_usage_first_call,
                    tokens_second_call=token_usage_second_call,
                    total_tokens_used=st.session_state.total_tokens,
                )

                # Add assistant response to chat history
                st.session_state.messages.append({"role": "assistant", "content": natural_response})
                st.session_state.chat_history.append({"role": "assistant", "content": natural_response})

            except Exception as e:
                # Save error details
                save_query_result(
                    prompt,
                    None,  # No natural language response
                    None,  # No result
                    sql_query if 'sql_query' in locals() else None,  # SQL query if available
                    response_text if 'response_text' in locals() else str(e),  # Raw response or error message
                    tokens_first_call=token_usage_first_call if 'token_usage_first_call' in locals() else None,
                    tokens_second_call=None,  # No second call tokens
                    total_tokens_used=st.session_state.total_tokens,
                    error_message=str(e)  # Save the error message
                )

                # Display error message
                natural_response = f"Error: {str(e)}"
                st.session_state.messages.append({"role": "assistant", "content": natural_response})
                st.session_state.chat_history.append({"role": "assistant", "content": natural_response})

        # Display assistant response
        with st.chat_message("assistant"):
            st.markdown(natural_response)

    # Sync button in sidebar
    if st.sidebar.button("🔄 Sync to Snowflake"):
        with st.spinner("Syncing data..."):
            sync_sqlite_to_snowflake()
        st.sidebar.success("Sync completed!")

# Run the App
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if st.session_state["authenticated"]:
    if needs_password_change(st.session_state["user"]):
        password_change_page()
    else:
        main_app()
else:
    login_page()