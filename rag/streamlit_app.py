import streamlit as st
from langchain_community.vectorstores import Chroma
from langchain_community.llms import Groq
from langchain.chains import RetrievalQA
from embeddings import get_embeddings
import os
from dotenv import load_dotenv

load_dotenv()

st.title("🚖 NYC Taxi AI Lakehouse Expert")
st.markdown("Ask natural language questions about the NYC Taxi Gold Data!")

@st.cache_resource
def load_rag():
    embeddings = get_embeddings()
    vectorstore = Chroma(persist_directory="./chroma_db", embedding_function=embeddings)
    retriever = vectorstore.as_retriever(search_kwargs={"k": 2})
    llm = Groq(temperature=0, groq_api_key=os.getenv("GROQ_API_KEY"), model_name="mixtral-8x7b-32768")
    return RetrievalQA.from_chain_type(llm=llm, retriever=retriever)

qa_chain = load_rag()

example_queries = [
    "What was the total revenue for January 1st?",
    "Which borough experiences the highest average fare?",
    "When are the peak hours for trips?",
    "What is the average fare for trips with 3 passengers?",
    "How many trips occurred yesterday?",
    "Summarize daily metrics.",
    "Show fare analysis constraints.",
    "What is the delta between maximum and average fare?"
]

selected_q = st.selectbox("Or choose an example query:", [""] + example_queries)
user_q = st.text_input("Enter your business question manually:")

query = user_q or selected_q

if query:
    response = qa_chain.run(query)
    st.write("### AI Response:")
    st.success(response)\n