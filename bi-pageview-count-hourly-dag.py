from datetime import date, timedelta
from airflow import DAG


# Default dag args
default_args = {
    'owner': 'bi team',
    'retries': 0,
    'retry_delay': timedelta(minutes=0),
    'catchup': False,
    'start_date': datetime(2020, 1, 1)}
}


dag = DAG('bi-pageview-count-hourly-dag',
          schedule_interval='',  #  make it running once an hour
          catchup=False,
          max_active_runs=1,
          default_args=default_args
          )
          
          

#run the task - new

# function 1 : run SQL script with no parameters
def bi_fnc_run_sql_script(date_cutoff, hour_cutoff, file_name, fnc_cursor ):
    logging.info('START bi_fnc_run_sql_script')
    path = '/etl_sql_scripts/' + file_name   #the path to SQL file  it depends on how its implemented in Airflow
    sql_file = open(dag_folder + path , 'r')
    query_text = sql_file.read()
    sql_file.close()
    fnc_cursor.execute(query_text)
    logging.info('END bi_fnc_run_sql_script')



# function 2 : run SQL script  using two parameters Date and hours
def bi_fnc_run_sql_script_date_hour_cutoff(date_cutoff, hour_cutoff, file_name, fnc_cursor ):
    logging.info('START bi_fnc_run_sql_script_date_hour_cutoff')
    path = '/etl_sql_scripts/' + file_name   #the path to SQL file  it depends on how its implemented in Airflow
    sql_file = open(dag_folder + path , 'r')
    query_text = sql_file.read()
    sql_file.close()
    fnc_cursor.execute(query_text, { "date_cutoff" : date_cutoff , "hour_cutoff" : hour_cutoff  })
    logging.info('END bi_fnc_run_sql_script_date_hour_cutoff')


#function 3: insert into pageviews_user_history
def bi_fnc_run_pageviews_user_history(ds, **kwargs):
    logging.info('START bi_fnc_run_pageviews_user_history')
    db_1 = SnowflakeHook('snowflake_bi')
    db1_conn = db_1.get_conn()
    db1_cursor = db1_conn.cursor()

    bi_fnc_run_sql_script(date_cutoff, hour_cutoff, 'insert_Pageviews_user_history.sql',db1_cursor)

    db1_cursor.close()
    db1_conn.commit()
    db1_conn.close()
    logging.info('END bi_fnc_run_pageviews_postcode_history_count')


#function 4: delete recent entries in pageviews_postcode_history_count and re-populate it
def bi_fnc_run_pageviews_postcode_count_hourly(ds, **kwargs):
    logging.info('START bi_fnc_run_pageviews_postcode_count_hourly')
    db_1 = SnowflakeHook('snowflake_bi')
    db1_conn = db_1.get_conn()
    db1_cursor = db1_conn.cursor()

    #define  cutoff date
    select_SQL_string = """select max(pageview_datetime)::date from bi_analytic.Pageviews_user_history """
    db1_cursor.execute(select_SQL_string)
    date_cutoff = db1_cursor.fetchone()[0]  #select first element of the list

    #define  cutoff hour with 1 hour of buffer
    select_SQL_string = """select hour(max(pageview_datetime)) -1  from bi_analytic.Pageviews_user_history """
    db1_cursor.execute(select_SQL_string)
    hour_cutoff = db1_cursor.fetchone()[0]
    if hour_cutoff < 0:  #since buffer in day cut off can cause negatives at start of the day
        hour_cutoff = 0

    # populate the historical postcode count table
    logging.info('START delete_pageviews_postcode_history_count')
    bi_fnc_run_sql_script_date_hour_cutoff(date_cutoff, hour_cutoff, 'delete_pageviews_postcode_history_count.sql', db1_cursor)  # see git repository for SQL script
    logging.info('END delete_pageviews_postcode_history_count')

    ogging.info('START insert_pageviews_postcode_history_count')
    bi_fnc_run_sql_script_date_hour_cutoff(date_cutoff, hour_cutoff, 'insert_pageviews_postcode_history_count.sql', db1_cursor)   # see git repository for SQL script
    ogging.info('START insert_pageviews_postcode_history_count')

    # populate the current(now) postcode count table
    logging.info('START delete_pageviews_postcode_now_count_hourly')
    bi_fnc_run_sql_script_date_hour_cutoff(date_cutoff, hour_cutoff, 'delete_pageviews_postcode_now_count_hourly.sql', db1_cursor)  # see git repository for SQL script
    logging.info('END delete_pageviews_postcode_now_count_hourly')

    ogging.info('START insert_pageviews_postcode_now_count_hourly')
    bi_fnc_run_sql_script_date_hour_cutoff(date_cutoff, hour_cutoff, 'insert_pageviews_postcode_now_count_hourly.sql', db1_cursor)   # see git repository for SQL script
    ogging.info('START insert_pageviews_postcode_now_count_hourly')

    db1_cursor.close()
    db1_conn.commit()
    db1_conn.close()
    logging.info('END bi_fnc_run_pageviews_postcode_count_hourly')



# create tasks

# first is  DAG dependencies to ensure it run after insert_pageviews_extract is completed

wait_for_extract = ExternalTaskSensor(
        task_id='wait_for_extract',
        external_dag_id='insert_pageviews_extract',
        external_task_id='insert_hour_pageviews',  # assume the task is named that way in  insert_pageviews_extract dag.
        execution_delta=None,  # Same day as today
        mode='reschedule',
        dag=dag
    )

#once the extract is completed for pageviews, dump the data into  pageviews_user_history  table
run_pageviews_user_history_hourly= PythonOperator(
    task_id='run_pageviews_user_history_hourly',
    provide_context=True,
    python_callable=bi_fnc_run_pageviews_user_history,
    on_failure_callback=slack_failed_task,
    dag=dag)

# once the table pageviews_user_history is populated with recent data, update pageviews_postcode_history_count  table
run_pageviews_postcode_count_hourly = PythonOperator(
    task_id='run_pageviews_postcode_count_hourly',
    provide_context=True,
    python_callable=bi_fnc_run_pageviews_postcode_count_hourly,
    on_failure_callback=slack_failed_task,
    dag=dag)
""" 
----------------------------------------------
DAG hierarchy here
----------------------------------------------
"""

wait_for_extract >> run_pageviews_user_history_hourly >> run_pageviews_postcode_count_hourly
