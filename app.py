import streamlit as st
from sales_leads import show_sales_leads
from client_stage_progression import show_client_stage_progression
from low_sales_progression import show_low_sales_progression

# favicon = "1.jpg"
favicon = "2.png"

st.set_page_config(page_title='Homeeasy Sales Dashboard', page_icon=favicon, layout='wide', initial_sidebar_state='auto')

st.sidebar.title("Homeeasy Sales Leads Monitoring System")
page = st.sidebar.selectbox("Choose a report", ["Sales Leads Monitoring", "Client Stage Progression Report", "Low Sales Progression"])  # Add the new option

if page == "Sales Leads Monitoring":
    show_sales_leads()
elif page == "Client Stage Progression Report":
    show_client_stage_progression()
else:
    show_low_sales_progression() 
