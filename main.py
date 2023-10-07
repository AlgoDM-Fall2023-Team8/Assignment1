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
    "Retrieve the items with the highest number of returns where the number of returns was approximately equivalent across all store, catalog and web channels (within a tolerance of +/- 10%), within the week ending a given date", 
    "List all customers living in a specified city, with an income between 2 values.",
    "For all web return reason calculate the average sales, average refunded cash and average return fee by different combinations of customer and sales types (e.g., based on marital status, education status, state and sales profit).",
    "Rollup the web sales for a given year by category and class, and rank the sales among peers within the parent, for each group compute sum of sales, location with the hierarchy and rank within the group.",
    "Count how many customers have ordered on the same day items on the web and the catalog and on the same day have bought items in a store",
    "How many items do we sell between pacific times of a day in certain stores to customers with one dependent count and 2 or less vehicles registered or 2 dependents with 4 or fewer vehicles registered or 3 dependents and five or less vehicles registered. In one row break the counts into sells from 8:30 to 9, 9 to 9:30, 9:30 to 10 ... 12 to 12:30",
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




def show_parameters_5():
        with st.form(key='parameters_4'):
            col1,col2=st.columns(2)

            time_period_year=pd.read_sql_query("Select distinct(d_year) from date_dim order by d_year;",engine)
            ca_state=pd.read_sql_query(" Select distinct(ca_state) from customer_address order by ca_state",engine)
              
            maratial_status=pd.read_sql_query("Select distinct(cd_marital_status) from customer_demographics order by cd_marital_status;",engine)
            education_status=pd.read_sql_query("Select distinct(cd_education_status) from customer_demographics order by cd_education_status;",engine)

            with col1:
             ca_state_1=st.selectbox("Pick the first state",ca_state)
             ca_state_2=st.selectbox("Pick the second state",ca_state)
             ca_satte_3=st.selectbox("Pick the third state",ca_state)

            
            with col2:
             maratial_status_select=st.selectbox("Pick the second state",maratial_status)
             education_status_select=st.selectbox("Pick the education",education_status)
             year_select=st.selectbox("Pick the year",time_period_year)
        
            submit_query_1=st.form_submit_button('Submit the paramters',on_click=param,args=(1,))
        
        return ca_state_1,ca_state_2,ca_satte_3,maratial_status_select,education_status_select,year_select

def show_parameters_4():
        with st.form(key='parameters_5'):
            col1,col2=st.columns(2)

            city=pd.read_sql_query("Select distinct(ca_city) from customer_address order by ca_city;",engine)
           # income=pd.read_sql_query(" Select distinct(ib_lower_bound) from income_band order by ib_lower_bound",engine)

            with col1:
             city_select=st.selectbox("Pick the city",city)
 
                        

            
            with col2:
             income=st.text_input("Pick the income")
        
            submit_query_1=st.form_submit_button('Submit the paramters',on_click=param,args=(1,))
        
        return city_select,income
            
def show_parameters_6():
        with st.form(key='6'):
             get_dms=pd.read_sql_query("Select distinct(d_month_seq) from date_dim order by d_month_seq;",engine)
             no_of_dms=st.selectbox("Pick DMS",get_dms)
             
             submit_query_1=st.form_submit_button('Submit the paramters',on_click=param,args=(1,))

        

        return no_of_dms     
            
def show_parameters_7():
        with st.form(key='7'):
             get_dms_7=pd.read_sql_query("Select distinct(d_month_seq) from date_dim order by d_month_seq;",engine)
             no_of_dms_7=st.selectbox("Pick DMS",get_dms_7)
             
             submit_query_1=st.form_submit_button('Submit the paramters',on_click=param,args=(1,))

        

        return no_of_dms_7

def show_parameters_8():
        with st.form(key='8'):
            

            hd_dep_count_8=pd.read_sql_query("Select distinct(hd_dep_count) from household_demographics order by hd_dep_count;",engine)
            hd_vehicle_count_8=pd.read_sql_query(" Select distinct(hd_vehicle_count) from household_demographics order by hd_vehicle_count;",engine)
            store=pd.read_sql_query("Select distinct(s_store_name) from store order by s_store_name;",engine)
        
            hd_dep_count_select_1=st.selectbox("Pick Househould demographics count",hd_dep_count_8,key="81")
            hd_dep_count_select_2=st.selectbox("Pick Househould demographics count ",hd_dep_count_8,key="82")
            hd_dep_count_select_3=st.selectbox("Pick Househould demographics count ",hd_dep_count_8,key="83")
            store=st.selectbox("Pick the store",store)


                        

            



             
            
            submit_query_1=st.form_submit_button('Submit the paramters',on_click=param,args=(1,))
        return hd_dep_count_select_1,hd_dep_count_select_2,hd_dep_count_select_3,store

def show_parameters_9():
        with st.form(key='8'):
            

            item=pd.read_sql_query("Select distinct(i_category) from item order by i_category;",engine)
            i_class=pd.read_sql_query("Select distinct(i_class) from item order by i_class;",engine)
            time_period_year=pd.read_sql_query("Select distinct(d_year) from date_dim order by d_year;",engine)

            # hd_dep_count_select_1=st.selectbox("Pick Househould demographics count",hd_dep_count_8,key="81")

            option_item_select = st.multiselect(
                'What are your favorite colors',item)

            i_class_select = st.multiselect(
                'What are your favorite colors',i_class)
            
            time_period_year=st.selectbox("Pick the first manufacture",time_period_year)
                        

            



             
            
            submit_query_1=st.form_submit_button('Submit the paramters',on_click=param,args=(1,))
        return option_item_select,i_class_select,time_period_year      

if st.session_state.stage==1:

    if(option_selected==question_queries[0]):
         time_period_year_select,ca_state_select=show_parameters_1()

    if(option_selected==question_queries[1]):
         m_id_1,m_id_2,m_id_3,m_id_4,price,first_date=show_parameters_2()

    if(option_selected==question_queries[2]):
         first_date,second_date,third_date=show_parameters_3()

    if(option_selected==question_queries[4]):
        ca_state_1,ca_state_2,ca_satte_3,maratial_status_select,education_status_select,year_select=show_parameters_5()

    if(option_selected==question_queries[3]):
         city_select,income=show_parameters_4()

    if(option_selected==question_queries[5]):
         no_of_dms=show_parameters_6()


    if(option_selected==question_queries[6]):
         no_of_dms_7=show_parameters_7()

    if(option_selected==question_queries[7]):
         hd_dep_count_select_1,hd_dep_count_select_2,hd_dep_count_select_3,store=show_parameters_8()

    if(option_selected==question_queries[8]):
         option_item_select,i_class_select,time_period_year=show_parameters_9()

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
      
    if(option_selected==question_queries[3]):
                        try:
                             income=int(income)
                             add_income=income+50000
                             df=pd.read_sql_query(run_query_4(city_select,income,add_income),engine)
                             
                             print(city_select,income)
                             print(run_query_4(city_select,income,add_income))
                             if df.empty:
                                st.write("No results found")
                             else:
                                st.write(df)
                             
                        finally:

                            connection.close()
                            engine.dispose()        
    if(option_selected==question_queries[4]):
                        try:
                             df=pd.read_sql_query(run_query_5(maratial_status_select,education_status_select,year_select,ca_state_1,ca_state_2,ca_satte_3,),engine)
                             
                             
                             if df.empty:
                                st.write("No results found")
                             else:
                                st.write(df)
                             
                        finally:

                            connection.close()
                            engine.dispose()
                            
    if(option_selected==question_queries[5]):
                        try:
                             no_of_dms=int(no_of_dms)
                             add_dms=no_of_dms+11
                             df=pd.read_sql_query(run_query_6(no_of_dms,add_dms),engine)
                             
                             print(add_dms,no_of_dms)
                             print(run_query_6(no_of_dms,add_dms))
                             if df.empty:
                                st.write("No results found")
                             else:
                                st.write(df)
                             
                        finally:

                            connection.close()
                            engine.dispose()        

    if(option_selected==question_queries[6]):
                        try:
                             no_of_dms_7=int(no_of_dms_7)
                             add_dms_7=no_of_dms_7+11
                             df=pd.read_sql_query(run_query_7(no_of_dms_7,  add_dms_7),engine)
                             
                             print(add_dms_7,no_of_dms_7)
                             if df.empty:
                                st.write("No results found")
                             else:
                                st.write(df)
                             
                        finally:

                            connection.close()
                            engine.dispose()

    if(option_selected==question_queries[7]):
                        try:
                             hour1=int(hd_dep_count_select_1)
                             add_hour1=hour1+2

                             hour2=int(hd_dep_count_select_2)
                             add_hour2=hour2+2

                             hour3=-int(hd_dep_count_select_3)
                             add_hour3=hour3+2                         
                             print(hour3)

                             df=pd.read_sql_query(run_query_8(store,hour1,add_hour1,hour2,add_hour2,hour3,add_hour3),engine)
                             
                             if df.empty:
                                st.write("No results found")
                             else:
                                st.write(df)
                             
                        finally:

                            connection.close()
                            engine.dispose()


    if(option_selected==question_queries[8]):
                        try:
                             df=pd.read_sql_query(run_query_9(option_item_select,i_class_select,time_period_year),engine)
                             if df.empty:
                                st.write("No results found")
                             else:
                                st.write(df)
                             
                             
                             
                        finally:

                            connection.close()
                            engine.dispose()