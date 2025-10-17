# streamlit_app.py
import streamlit as st
import requests
import json


st.set_page_config(page_title="SQL LLM Query Interface", layout="wide")
st.title("DataSage")

# Input for the user question
question = st.text_area("Enter your question:", height=100)


if st.button("Run Query"):
    if not question.strip():
        st.warning("Please enter a question")
    else:
        try:
            response = requests.post(
                "http://127.0.0.1:8000/query_stream",
                json={"question": question},
                stream=True
            )
            placeholder = st.empty()
            full_text = ""
            for line in response.iter_lines():
                if line:
                    data = json.loads(line)
                    if "chunk" in data:
                        full_text += data["chunk"]
                        placeholder.code(full_text, language="sql")
                    elif "error" in data:
                        st.error(data["error"])

        except Exception as e:
            st.error(f"Failed to connect to API: {e}")

