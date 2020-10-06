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


dag = DAG('bi_pageview_postcode_count_daily',
          schedule_interval='@daily', #run it daily after user extract is done
          catchup=False,
          max_active_runs=1,
          default_args=default_args
          )


# function 1 : run SQL script with no parameters
def bi_fnc_run_sql_script(file_name, fnc_cursor ):
    logging.info('START bi_fnc_run_sql_script')
    path = '/etl_sql_scripts/' + file_name   #the path to SQL file  it depends on how its implemented in Airflow
    sql_file = open(dag_folder + path , 'r')
    query_text = sql_file.read()
    sql_file.close()
    fnc_cursor.execute(query_text)
    logging.info('END bi_fnc_run_sql_script')



#function 2: truncate and repopulate all the count table with current (now) user postcode
def bi_fnc_run_pageviews_postcode_now_count_daily(ds, **kwargs):
    logging.info('START bi_fnc_run_pageviews_postcode_now_count_daily')
    db_1 = SnowflakeHook('snowflake_bi')
    db1_conn = db_1.get_conn()
    db1_cursor = db1_conn.cursor()

    # truncate all bi_analytic.pageviews_postcode_now_count table
    logging.info('START truncate_pageviews_postcode_now_count_daily')
    bi_fnc_run_sql_script_date_hour_cutoff('truncate_pageviews_postcode_now_count_daily.sql', db1_cursor)  # see git repository for SQL script
    logging.info('END truncate_pageviews_postcode_now_count_daily')

    # repopulate all bi_analytic.pageviews_postcode_now_count table with up to date postcode from user table
    #NOTE: all historical data from all date will be repopulated , see sql file
    ogging.info('START insert_pageviews_postcode_now_count_daily')
    bi_fnc_run_sql_script_date_hour_cutoff('insert_pageviews_postcode_now_count_daily.sql', db1_cursor)   # see git repository for SQL script
    ogging.info('START insert_pageviews_postcode_now_count_daily')

    db1_cursor.close()
    db1_conn.commit()
    db1_conn.close()
    logging.info('END bi_fnc_run_pageviews_postcode_now_count_daily')


# create tasks

# first is  DAG dependencies to make sure that  user_extract is completed
wait_for_user_extract = ExternalTaskSensor(
        task_id='wait_for_user_extract',
        external_dag_id='users_extract_daily',
        external_task_id='run_users_extract_daily',  # assume the task is named that way in  insert_pageviews_extract dag.
        execution_delta=None,
        dag=dag
    )

# once the table user extract is truncated and repopulated with recent data, truncate all pageviews_postcode_now_count and repopulate  it
run_truncate_populate_pageviews_postcode_now_count_daily = PythonOperator(
    task_id='run_truncate_populate_pageviews_postcode_now_count_daily',
    provide_context=True,
    python_callable=bi_fnc_run_pageviews_postcode_now_count_daily,
    on_failure_callback=slack_failed_task,
    dag=dag)

""" 
----------------------------------------------
DAG hierarchy here
----------------------------------------------
"""
wait_for_user_extract >> run_truncate_populate_pageviews_postcode_now_count_daily
