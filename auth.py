import os
import streamlit as st
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

# Function to validate login credentials
def validate_login(username, password):
    try:
        # Connect to Snowflake
        SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
        SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
        SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
        SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
        SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

        engine = create_engine(URL(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        ))
        conn = engine.connect()

        # Query to check if username and password exist in the table
        query = f"SELECT * FROM UandP WHERE username = '{username}' AND password = '{password}'"
        result = conn.execute(query).fetchone()

        conn.close()
        return result is not None
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return False

# Login form
def login_form():
    st.title("Login to Snowflake Data Assistant")
    username = st.text_input("Username (Email)")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        if not username.endswith('@ahc.com'):
            st.error("Invalid domain. Please use your company email.")
        else:
            if validate_login(username, password):
                st.session_state.logged_in = True
                st.success("Login successful!")
                st.experimental_rerun()  # Refresh the app
            else:
                st.error("Invalid credentials. Please try again.")