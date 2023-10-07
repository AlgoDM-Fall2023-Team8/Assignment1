from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from datetime import datetime

from dotenv import load_dotenv 
from function import *
from urllib import parse
import streamlit as st
import pandas as pd 
import numpy as np
import os

#loads all the environment variables 
load_dotenv()

# creating the engine
engine = create_engine(URL(
    account=os.getenv('ACCOUNT_IDENTIFIER'),
    user =os.getenv('USER'),
    password=parse.quote(os.getenv('PASSWORD')),
    database = 'SNOWFLAKE_SAMPLE_DATA',
    schema = 'TPCDS_SF10TCL',
    warehouse = 'STREAMLIT',
    role='ACCOUNTADMIN',
    timezone = 'America/Los_Angeles',
))

#connecting with snowflake
connection = engine.connect()

# creating an aray of all the questions
question_queries = [
    "Find customers and their detailed customer data who have returned items bought from the catalog more than 20 percent the average customer returns for customers in a given state in a given time period. Order output by customer data.",
    "Find customers who tend to spend more money (net-paid) on-line than in stores.",
    "Retrieve the items with the highest number of returns where the number of returns was approximately equivalent across all store, catalog and web channels (within a tolerance of +/- 10%), within the week ending a given date"
]

##initally storing the session variables
if 'stage' not in st.session_state:
    st.session_state.stage = 0

if 'runquery' not in st.session_state:
     st.session_state.runquery =0


#function to update the session variables 
def param(runquery):
     st.session_state.runquery = runquery
def dis(stage):
      st.session_state.stage = stage


# making the form 
option_selected=st.selectbox("Pick one", question_queries)    
get_parameters= st.button('Get parameters',on_click=dis,args=(1,)) 

def show_parameters_1():
        with st.form(key='parameters_1'):
            col1,col2=st.columns(2)

            time_period_year=pd.read_sql_query("Select distinct(d_year) from date_dim order by d_year;",engine)
            ca_state=pd.read_sql_query(" Select distinct(ca_state) from customer_address order by ca_state",engine)

            with col1:
             time_period_year_select=st.selectbox("Pick the time period",time_period_year)
 
                        

            
            with col2:
             ca_state_select=st.selectbox("Pick the state",ca_state)
        
            submit_query_1=st.form_submit_button('Submit the paramters',on_click=param,args=(1,))
        
        return time_period_year_select,ca_state_select

def show_parameters_2():
        with st.form(key='paramers_2'):
            manx_id=pd.read_sql_query("select Distinct(i_manufact_id) from item order by i_manufact_id;",engine)
            manx_id.dropna(subset=['i_manufact_id'], inplace=True)

            

            manx_id['i_manufact_id'] = manx_id['i_manufact_id'].astype(int)

            print(manx_id)
            # st.write(man_id)
            # print(man_id)
            i_price=pd.read_sql_query(" select distinct(i_current_price) from item order by i_current_price;",engine)
            col1,col2=st.columns(2)
            max_date = datetime(2100, 1, 1) 
            min_date=datetime(1900,1,2)
            with col1:
                  m_id_1=st.selectbox("Pick the first manufacture",manx_id,key="1")
                  m_id_2=st.selectbox("Pick the second manufacture id",manx_id,key="2")
                  first_date = st.date_input("Select first date ")

            with col2:
                m_id_3=st.selectbox("Pick the third manufacture id",manx_id)
                m_id_4=st.selectbox("Pick the fouth manufacture id",manx_id)
                price=st.selectbox("Pick the fouth price id",i_price)

            submit_query_2=st.form_submit_button('Submit the paramters',on_click=param,args=(1,))

        return m_id_1,m_id_2,m_id_3,m_id_4,price,first_date

def show_parameters_3():
        with st.form(key='parameters_3'):
            col1,col2=st.columns(2)
            max_date = datetime(2100, 1, 1) 
            min_date=datetime(1900,1,2)
            with col1:
                  first_date = st.date_input("Select first date ",max_value=max_date,min_value=min_date)
                  second_date=st.date_input("Select second date ",max_value=max_date,min_value=min_date)

            with col2:
                  third_date=st.date_input("Select third date",min_value=min_date,max_value=max_date)

            submit_query_2=st.form_submit_button('Submit the paramters',on_click=param,args=(1,))

        return first_date,second_date,third_date

if st.session_state.stage==1:

    if(option_selected==question_queries[0]):
         time_period_year_select,ca_state_select=show_parameters_1()

    if(option_selected==question_queries[1]):
         m_id_1,m_id_2,m_id_3,m_id_4,price,first_date=show_parameters_2()

    if(option_selected==question_queries[2]):
         first_date,second_date,third_date=show_parameters_3()


if st.session_state.runquery==1:
    if(option_selected==question_queries[0]):
                        try:
                             df=pd.read_sql_query(run_query_1(time_period_year_select,ca_state_select),engine)
                             
                             if df.empty:
                                st.write("No results found")
                             else:
                                st.write(df)                             
                             
                        finally:

                            connection.close()
                            engine.dispose()
    
    if(option_selected==question_queries[1]):
                        try:
                            add_price=int(price)
                            add_price=add_price+30

                            df=pd.read_sql_query(run_query_2(m_id_1,m_id_2,m_id_3,m_id_4,first_date,price,add_price),engine)
                            print(run_query_2(m_id_1,m_id_2,m_id_3,m_id_4,first_date,price,add_price)) 
                            if df.empty:
                                st.write("No results found")
                            else:
                                st.write(df)
                             
                        finally:

                            connection.close()
                            engine.dispose()


    if(option_selected==question_queries[2]):
                        try:
                             df=pd.read_sql_query(run_query_3(first_date,second_date,third_date),engine)
                             
                             print(first_date,second_date,third_date)
                             if df.empty:
                                st.write("No results found")
                             else:
                                st.write(df)
                             
                        finally:

                            connection.close()
                            engine.dispose()        
        



