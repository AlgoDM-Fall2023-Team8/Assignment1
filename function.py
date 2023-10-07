def run_query_1(time_period_year,customer_state):
      sql_query_1=f"with customer_total_return as (select cr_returning_customer_sk as ctr_customer_sk ,ca_state as ctr_state, sum(cr_return_amt_inc_tax) as ctr_total_return from catalog_returns ,date_dim ,customer_address where cr_returned_date_sk = d_date_sk and d_year ={time_period_year} and cr_returning_addr_sk = ca_address_sk group by cr_returning_customer_sk ,ca_state ) select  c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset ,ca_location_type,ctr_total_return from customer_total_return ctr1 ,customer_address ,customer where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2 from customer_total_return ctr2 where ctr1.ctr_state = ctr2.ctr_state) and ca_address_sk = c_current_addr_sk and ca_state = '{customer_state}' and ctr1.ctr_customer_sk = c_customer_sk order by c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset ,ca_location_type,ctr_total_return limit 100;"

      return sql_query_1

def run_query_2(mid1,mid2,mid3,mid4,date,price,add_price):
  sql_query_2=f"select  i_item_id ,i_item_desc ,i_current_price from item, inventory, date_dim, store_sales where i_current_price between {price} and {add_price} and inv_item_sk = i_item_sk and d_date_sk=inv_date_sk and d_date between cast('{date}' as date) and dateadd(day,60,to_date('{date}')) and i_manufact_id in ({mid1},{mid2},{mid3},{mid4}) and inv_quantity_on_hand between 100 and 500 and ss_item_sk = i_item_sk group by i_item_id,i_item_desc,i_current_price order by i_item_id limit 100;"
  return sql_query_2


def run_query_3(return_date_1, return_date_2, return_date_3):
  
  sql_query_3=f"with sr_items as (select i_item_id item_id, sum(sr_return_quantity) sr_item_qty from store_returns, item, date_dim where sr_item_sk = i_item_sk and   d_date    in (select d_date from date_dim where d_week_seq in (select d_week_seq from date_dim where d_date in ('{return_date_1}', '{return_date_2}', '{return_date_3}'))) and   sr_returned_date_sk   = d_date_sk group by i_item_id), cr_items as (select i_item_id item_id, sum(cr_return_quantity) cr_item_qty from catalog_returns, item, date_dim where cr_item_sk = i_item_sk and   d_date    in (select d_date from date_dim where d_week_seq in (select d_week_seq from date_dim where d_date in ('{return_date_1}', '{return_date_2}',' {return_date_3}'))) and   cr_returned_date_sk   = d_date_sk group by i_item_id), wr_items as (select i_item_id item_id, sum(wr_return_quantity) wr_item_qty from web_returns, item, date_dim where wr_item_sk = i_item_sk and   d_date    in (select d_date from date_dim where d_week_seq in (select d_week_seq from date_dim where d_date in ('1998-02-20','1998-09-28','1998-11-14'))) and   wr_returned_date_sk   = d_date_sk group by i_item_id) select  sr_items.item_id ,sr_item_qty ,sr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 sr_dev ,cr_item_qty ,cr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 cr_dev ,wr_item_qty ,wr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 wr_dev ,(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 average from sr_items ,cr_items ,wr_items where sr_items.item_id=cr_items.item_id and sr_items.item_id=wr_items.item_id order by sr_items.item_id ,sr_item_qty limit 100;"
  return sql_query_3

def run_query_4(city, income,add_income):
  sql_query_4=f"select  c_customer_id as customer_id , coalesce(c_last_name,'') || ', ' || coalesce(c_first_name,'') as customername from customer ,customer_address ,customer_demographics ,household_demographics ,income_band ,store_returns where ca_city	        =  '{city}'and c_current_addr_sk = ca_address_sk and ib_lower_bound   >=  {income} and ib_upper_bound   <= {add_income}   and ib_income_band_sk = hd_income_band_sk and cd_demo_sk = c_current_cdemo_sk and hd_demo_sk = c_current_hdemo_sk and sr_cdemo_sk = cd_demo_sk order by c_customer_id limit 100;"
  
  return sql_query_4

def run_query_5(maratial_status,education_status,year,state_1,state_2,state_3):
   sql_query_5 = f" select  substr(r_reason_desc,1,20) ,avg(ws_quantity) ,avg(wr_refunded_cash) ,avg(wr_fee) from web_sales, web_returns, web_page, customer_demographics cd1, customer_demographics cd2, customer_address, date_dim, reason where ws_web_page_sk = wp_web_page_sk and ws_item_sk = wr_item_sk and ws_order_number = wr_order_number and ws_sold_date_sk = d_date_sk and d_year = {year} and cd1.cd_demo_sk = wr_refunded_cdemo_sk and cd2.cd_demo_sk = wr_returning_cdemo_sk and ca_address_sk = wr_refunded_addr_sk and r_reason_sk = wr_reason_sk and ( cd1.cd_marital_status = '{maratial_status}' and cd1.cd_marital_status = cd2.cd_marital_status and cd1.cd_education_status = '{education_status}' and cd1.cd_education_status = cd2.cd_education_status and ws_sales_price between 100.00 and 150.00 ) and ( ca_country = 'United States' and ca_state in ('{state_1}', '{state_2}','{state_3}') and ws_net_profit between 100 and 200 ) group by r_reason_desc order by substr(r_reason_desc,1,20) ,avg(ws_quantity) ,avg(wr_refunded_cash) ,avg(wr_fee) limit 100;"

   return sql_query_5
   
def run_query_6(DMS,add_dms):
  sql_query_6=f"select sum(ws_net_paid) as total_sum ,i_category ,i_class ,grouping(i_category)+grouping(i_class) as lochierarchy ,rank() over ( partition by grouping(i_category)+grouping(i_class), case when grouping(i_class) = 0 then i_category end order by sum(ws_net_paid) desc) as rank_within_parent from web_sales ,date_dim       d1 ,item where d1.d_month_seq between {DMS} and {add_dms}and d1.d_date_sk = ws_sold_date_sk and i_item_sk  = ws_item_sk group by rollup(i_category,i_class) order by lochierarchy desc, case when lochierarchy = 0 then i_category end, rank_within_parent limit 100;"
  return sql_query_6

def run_query_7(DMS2,add_dms2):
  sql_query_7=f"select count(*) from ((select distinct c_last_name, c_first_name, d_date from store_sales, date_dim, customer where store_sales.ss_sold_date_sk = date_dim.d_date_sk and store_sales.ss_customer_sk = customer.c_customer_sk and d_month_seq between {DMS2} and {add_dms2}) except (select distinct c_last_name, c_first_name, d_date from catalog_sales, date_dim, customer where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk and d_month_seq between {DMS2} and {add_dms2}) except (select distinct c_last_name, c_first_name, d_date from web_sales, date_dim, customer where web_sales.ws_sold_date_sk = date_dim.d_date_sk and web_sales.ws_bill_customer_sk = customer.c_customer_sk and d_month_seq between {DMS2} and {add_dms2}) ) cool_cust ;"
  return sql_query_7

def run_query_8(store,hour1,add_hour1,hour2,add_hour2,hour3,add_hour3):
  sql_query_8=f"select  * from (select count(*) h8_30_to_9 from store_sales, household_demographics , time_dim, store where ss_sold_time_sk = time_dim.t_time_sk and ss_hdemo_sk = household_demographics.hd_demo_sk and ss_store_sk = s_store_sk and time_dim.t_hour = 8 and time_dim.t_minute >= 30 and ((household_demographics.hd_dep_count = {hour1} and household_demographics.hd_vehicle_count<={add_hour1}) or (household_demographics.hd_dep_count = {hour2} and household_demographics.hd_vehicle_count<={add_hour2}) or (household_demographics.hd_dep_count = {hour3} and household_demographics.hd_vehicle_count<={add_hour3})) and store.s_store_name = '{store}') s1, (select count(*) h9_to_9_30 from store_sales, household_demographics , time_dim, store where ss_sold_time_sk = time_dim.t_time_sk and ss_hdemo_sk = household_demographics.hd_demo_sk and ss_store_sk = s_store_sk and time_dim.t_hour = 9 and time_dim.t_minute < 30 and ((household_demographics.hd_dep_count = {hour1} and household_demographics.hd_vehicle_count<={add_hour1}) or (household_demographics.hd_dep_count = {hour2} and household_demographics.hd_vehicle_count<={add_hour2}) or (household_demographics.hd_dep_count = {hour3} and household_demographics.hd_vehicle_count<={add_hour3})) and store.s_store_name = '{store}') s2, (select count(*) h9_30_to_10 from store_sales, household_demographics , time_dim, store where ss_sold_time_sk = time_dim.t_time_sk and ss_hdemo_sk = household_demographics.hd_demo_sk and ss_store_sk = s_store_sk and time_dim.t_hour = 9 and time_dim.t_minute >= 30 and ((household_demographics.hd_dep_count = {hour1} and household_demographics.hd_vehicle_count<={add_hour1}) or (household_demographics.hd_dep_count = {hour2} and household_demographics.hd_vehicle_count<={add_hour2}) or (household_demographics.hd_dep_count = {hour3} and household_demographics.hd_vehicle_count<={add_hour3})) and store.s_store_name = '{store}') s3, (select count(*) h10_to_10_30 from store_sales, household_demographics , time_dim, store where ss_sold_time_sk = time_dim.t_time_sk and ss_hdemo_sk = household_demographics.hd_demo_sk and ss_store_sk = s_store_sk and time_dim.t_hour = 10 and time_dim.t_minute < 30 and ((household_demographics.hd_dep_count = {hour1} and household_demographics.hd_vehicle_count<={hour1}) or (household_demographics.hd_dep_count = {hour2} and household_demographics.hd_vehicle_count<={hour2}) or (household_demographics.hd_dep_count = {hour3} and household_demographics.hd_vehicle_count<={hour3})) and store.s_store_name = '{store}') s4, (select count(*) h10_30_to_11 from store_sales, household_demographics , time_dim, store where ss_sold_time_sk = time_dim.t_time_sk and ss_hdemo_sk = household_demographics.hd_demo_sk and ss_store_sk = s_store_sk and time_dim.t_hour = 10 and time_dim.t_minute >= 30 and ((household_demographics.hd_dep_count = {hour1} and household_demographics.hd_vehicle_count<={add_hour1}) or (household_demographics.hd_dep_count = {hour2} and household_demographics.hd_vehicle_count<={add_hour2}) or (household_demographics.hd_dep_count = {hour3} and household_demographics.hd_vehicle_count<={add_hour3})) and store.s_store_name = '{store}') s5, (select count(*) h11_to_11_30 from store_sales, household_demographics , time_dim, store where ss_sold_time_sk = time_dim.t_time_sk and ss_hdemo_sk = household_demographics.hd_demo_sk and ss_store_sk = s_store_sk and time_dim.t_hour = 11 and time_dim.t_minute < 30 and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or (household_demographics.hd_dep_count = 1 and household_demographics.hd_vehicle_count<=1+2) or (household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2)) and store.s_store_name = '{store}') s6, (select count(*) h11_30_to_12 from store_sales, household_demographics , time_dim, store where ss_sold_time_sk = time_dim.t_time_sk and ss_hdemo_sk = household_demographics.hd_demo_sk and ss_store_sk = s_store_sk and time_dim.t_hour = 11 and time_dim.t_minute >= 30 and ((household_demographics.hd_dep_count = {hour1} and household_demographics.hd_vehicle_count<={add_hour1}) or (household_demographics.hd_dep_count = {hour2} and household_demographics.hd_vehicle_count<={add_hour2}) or (household_demographics.hd_dep_count = {hour2} and household_demographics.hd_vehicle_count<={add_hour2})) and store.s_store_name = '{store}') s7, (select count(*) h12_to_12_30 from store_sales, household_demographics , time_dim, store where ss_sold_time_sk = time_dim.t_time_sk and ss_hdemo_sk = household_demographics.hd_demo_sk and ss_store_sk = s_store_sk and time_dim.t_hour = 12 and time_dim.t_minute < 30 and ((household_demographics.hd_dep_count = {hour1} and household_demographics.hd_vehicle_count<={add_hour1}) or (household_demographics.hd_dep_count = {hour2} and household_demographics.hd_vehicle_count<={add_hour2}) or (household_demographics.hd_dep_count = {hour3} and household_demographics.hd_vehicle_count<={add_hour3})) and store.s_store_name = '{store}') s8"
  return sql_query_8

def run_query_9(categories,classes,year):
   # Lists of categories and classes


      # Join the lists into comma-separated strings
      categories_str = "', '".join(categories)
      classes_str = "', '".join(classes)

      # Modify your query string to include multiple categories and classes
      sql_query = f"""
      SELECT *
      FROM (
      SELECT
            i_category, i_class, i_brand,
            s_store_name, s_company_name,
            d_moy,
            SUM(ss_sales_price) sum_sales,
            AVG(SUM(ss_sales_price)) OVER (
                  PARTITION BY i_category, i_brand, s_store_name, s_company_name
            ) avg_monthly_sales
      FROM
            item, store_sales, date_dim, store
      WHERE
            ss_item_sk = i_item_sk
            AND ss_sold_date_sk = d_date_sk
            AND ss_store_sk = s_store_sk
            AND d_year IN ({year})
            AND (
                  (i_category IN ('{categories_str}') AND i_class IN ('{classes_str}'))
            )
      GROUP BY
            i_category, i_class, i_brand,
            s_store_name, s_company_name, d_moy
      ) tmp1
      WHERE
      CASE
            WHEN (avg_monthly_sales <> 0) THEN (ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales)
            ELSE NULL
      END > 0.1
      ORDER BY
      sum_sales - avg_monthly_sales, s_store_name
      LIMIT 100;
      """

      return sql_query

def run_query_10(morning_start_time_select,morning_end_time_select,evening_start_time_select,evening_end_time_select,no_of_dependents):
      sql_query_10=f"select  cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio from ( select count() amc from web_sales, household_demographics , time_dim, web_page where ws_sold_time_sk = time_dim.t_time_sk and ws_ship_hdemo_sk = household_demographics.hd_demo_sk and ws_web_page_sk = web_page.wp_web_page_sk and time_dim.t_hour between {morning_start_time_select}and {morning_end_time_select} and household_demographics.hd_dep_count = {no_of_dependents} and web_page.wp_char_count between 5000 and 5200) at, ( select count() pmc from web_sales, household_demographics , time_dim, web_page where ws_sold_time_sk = time_dim.t_time_sk and ws_ship_hdemo_sk = household_demographics.hd_demo_sk and ws_web_page_sk = web_page.wp_web_page_sk and time_dim.t_hour between {evening_start_time_select} and {evening_end_time_select} and household_demographics.hd_dep_count = {no_of_dependents} and web_page.wp_char_count between 5000 and 5200) pt order by am_pm_ratio limit 100;"
      
      return sql_query_10

