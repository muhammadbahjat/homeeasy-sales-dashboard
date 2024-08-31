import streamlit as st
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime


def show_client_stage_progression():
    st.title("Client Stage Progression Report")
    db_params = {
        'dbname': st.secrets["database"]["DB_NAME"],
        'user': st.secrets["database"]["DB_USER"],
        'password': st.secrets["database"]["DB_PASSWORD"],
        'host': st.secrets["database"]["DB_HOST"],
        'port': st.secrets["database"]["DB_PORT"]
    }

    fetch_leads_stage_4_and_beyond_query = """
    SELECT 
        csp.client_id,
        c.fullname AS client_name,
        e.fullname AS employee_name,
        csp.current_stage,
        csp.created_on AS time_entered_stage
    FROM 
        public.client_stage_progression csp
    JOIN 
        public.client c ON csp.client_id = c.id
    JOIN 
        public.employee e ON c.assigned_employee = e.id
    WHERE 
        csp.current_stage >= 4
    ORDER BY 
        csp.client_id;
    """

    fetch_sales_reps_count_query = """
    SELECT 
        e.fullname AS employee_name,
        DATE(csp.created_on) AS date_moved,
        COUNT(*) AS count_of_leads
    FROM 
        public.client_stage_progression csp
    JOIN 
        public.client c ON csp.client_id = c.id
    JOIN 
        public.employee e ON c.assigned_employee = e.id
    WHERE 
        csp.current_stage >= 4
    GROUP BY 
        e.fullname, DATE(csp.created_on)
    ORDER BY 
        date_moved DESC, count_of_leads DESC;
    """

    def fetch_data(query):
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(**db_params)
            cursor = connection.cursor()
            cursor.execute(query)
            records = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(records, columns=column_names)
            
            return df
            
        except Exception as error:
            st.error(f"Error fetching records: {error}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def plot_leads_stage_4_and_beyond(df):
        st.subheader("Graph: Leads in Stage 4 and Beyond")
        fig, ax = plt.subplots(figsize=(14, 8)) 
        df['time_entered_stage'] = pd.to_datetime(df['time_entered_stage'])
        daily_counts = df.set_index('time_entered_stage').resample('D').size()
        daily_counts.plot(ax=ax, kind='line')
        ax.set_xlabel('Date', fontsize=12)
        ax.set_ylabel('Number of Leads', fontsize=12)
        ax.set_title('Leads Entering Stage 4 and Beyond Over Time', fontsize=16)
        plt.xticks(rotation=45, ha='right', fontsize=10)
        plt.yticks(fontsize=10)
        st.pyplot(fig)

    def plot_sales_reps_moving_leads(df):
        st.subheader("Graph: Sales Reps Moving Leads to Stage 4 and Beyond")
        fig, ax = plt.subplots(figsize=(14, 8)) 
        pivot_data = df.pivot(index='date_moved', columns='employee_name', values='count_of_leads').fillna(0)
        pivot_data.plot(kind='bar', stacked=True, ax=ax)
        ax.set_xlabel('Date', fontsize=12)
        ax.set_ylabel('Number of Leads', fontsize=12)
        ax.set_title('Sales Reps Moving Leads to Stage 4 and Beyond', fontsize=16)
        plt.xticks(rotation=45, ha='right', fontsize=10)
        plt.yticks(fontsize=10)
        ax.legend(loc='center left', bbox_to_anchor=(1.0, 0.5), fontsize=10) 
        st.pyplot(fig)

    if st.button('Show Data / Refresh Data'):
        today = datetime.today().strftime('%Y-%m-%d')
        st.write(f"Till: {today}")

        leads_data = fetch_data(fetch_leads_stage_4_and_beyond_query)

        if leads_data is not None:
            st.subheader("Leads in Stage 4 and Beyond")
            st.dataframe(leads_data)
            st.write(f"Total leads in Stage 4 and beyond: {len(leads_data)}")
            
            plot_leads_stage_4_and_beyond(leads_data)

        sales_reps_data = fetch_data(fetch_sales_reps_count_query)

        if sales_reps_data is not None:
            st.subheader("Sales Reps Moving Leads to Stage 4 and Beyond")
            st.dataframe(sales_reps_data)
            st.write(f"Total entries: {len(sales_reps_data)}")
            plot_sales_reps_moving_leads(sales_reps_data)
