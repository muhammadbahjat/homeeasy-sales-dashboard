# main.py
import streamlit as st
from sales_leads import show_sales_leads
from client_stage_progression import show_client_stage_progression

st.sidebar.title("Homeeasy Sales Leads Monitoring System")
page = st.sidebar.selectbox("Choose a report", ["Sales Leads Monitoring", "Client Stage Progression Report"])

if page == "Sales Leads Monitoring":
    show_sales_leads()
else:
    show_client_stage_progression()