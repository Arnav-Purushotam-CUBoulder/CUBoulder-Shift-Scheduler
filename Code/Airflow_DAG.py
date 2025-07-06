# REFERENCES:
    # https://github.com/hnawaz007/pythondataanalysis/blob/main/ETL%20Pipeline/automate_etl_with_airflow.py
    # https://www.youtube.com/watch?v=ZET50M20hkU&ab_channel=AmazonWebServices

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta, time
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from sqlalchemy import create_engine
from utils import transform_time_inout, create_working_flag, create_remaining_hours, alert_employee_shortage 
import pandas as pd
import io
from sqlalchemy import create_engine
from greeter_allocation import allocate_greeter
from register_salesfloor_acclocation import allocate_register_salesfloor
from email_scripts import covert_rds_to_json,  upload_data_to_dynamodb, send_data_to_sqs, process_sqs_messages

############## Transformation Functions ##############
@task()
def fetch_emp_avail_from_s3():
    # Initialize S3Hook with the default AWS connection
    s3_hook = S3Hook(aws_conn_id='bookstore_aws')
    
    # Fetch the file content from S3
    file_content = s3_hook.read_key(
        key='Processed/Daily Employee Availability/Emp_Availability_Initial.csv',  # Specify the S3 path
        bucket_name='bookstore-scheduling-bucket-arnav',  # Specify your S3 bucket
    )

    # Validate that content is not None or empty
    if not file_content:
        raise ValueError("File content is empty or missing")
    
    # Convert the file content to a pandas DataFrame
    # We use StringIO to treat the string as file-like for pandas to read
    file_like_object = io.StringIO(file_content)
    
    # Read the content into a DataFrame
    df = pd.read_csv(file_like_object) #, names=['Name', 'Responsibility', 'Time in', 'Time out'])
    
    # Return the DataFrame
    return df

@task()
def fetch_shift_req_from_s3():
    # Initialize S3Hook with the default AWS connection
    s3_hook = S3Hook(aws_conn_id='bookstore_aws')
    
    # Fetch the file content from S3
    file_content = s3_hook.read_key(
        key='Processed/Daily Shift Requirements/Emp_Count_Requirement.csv',  # Specify the S3 path
        bucket_name='bookstore-scheduling-bucket-arnav',  # Specify your S3 bucket
    )
    
    # Validate that content is not None or empty
    if not file_content:
        raise ValueError("File content is empty or missing")
    
    # Convert the file content to a pandas DataFrame
    # We use StringIO to treat the string as file-like for pandas to read
    file_like_object = io.StringIO(file_content)
    
    # Read the content into a DataFrame
    emp_count_req = pd.read_csv(file_like_object)
    
    # Return the DataFrame
    return emp_count_req

@task()
def prepare_emp_aval(df):
    # 1. Read Emp Availability table, filter and format Time columns
    # path="00_Input/01_Emp_Availability_Initial.xlsx" # TODO: Read from S3
    # df = pd.read_excel(path, names=['Name', 'Responsibility', 'Time in', 'Time out'])
    filtered_df = df[~df['Responsibility'].isin(['Technology', 'Office Work']) & ~df['Name'].str.contains('Available')] # Filter out not-required roles and names
    filtered_df= transform_time_inout(filtered_df)

    # 2. Create Working Flag and Remaining Hours Left
    work_status_df = create_working_flag(filtered_df)
    work_status_df= work_status_df[work_status_df['Working Flag']==1] # Filter only working hours for every employee
    work_status_df= create_remaining_hours(work_status_df, filtered_df)
    work_status_df['Start_time'] = pd.to_datetime(work_status_df['Start_time'], format='%H:%M:%S').dt.time
    work_status_df['End_time'] = pd.to_datetime(work_status_df['End_time'], format='%H:%M:%S').dt.time

    return work_status_df

@task()
def prepare_shift_req(work_status_df, emp_count_req):
    # 1. Read Shift Req table, format Time columns
    # emp_count_req= pd.read_excel('00_Input/02_Emp_Count_Requirement.xlsx') # TODO: Read from S3
    emp_count_req['From_Time'] = pd.to_datetime(emp_count_req['From_Time'], format='%H:%M:%S').dt.time
    emp_count_req['To_Time'] = pd.to_datetime(emp_count_req['To_Time'], format='%H:%M:%S').dt.time

    # 2. Alert if available employees are insufficient to satisfy the required count
    emp_requirements= alert_employee_shortage(work_status_df, emp_count_req)

    return emp_requirements

@task()
def store_work_status_in_rds(work_status_df):
    print("INPUT DATA.")
    print("Data:")
    print(work_status_df.head())
    print("Data Info:")
    print(work_status_df.info())

    # 1. Format datatype and convert the dataframe into list of tuple
    work_status_df["Start_time"] = work_status_df["Start_time"].astype(str)
    work_status_df["End_time"] = work_status_df["End_time"].astype(str)

    insert_values= [] # Will be a list of tuples
    for index,row in work_status_df.iterrows():
        insert_values.append(tuple(row.values))
    print("Dataformatted successfully.")
    print("Data:")
    print(work_status_df.head())
    print("Data Info:")
    print(work_status_df.info())

    # 2. Create database connection
    db_url = 
    engine = create_engine(db_url)
    print("Database connection successfully.")

    # 3. Truuncate the table and insert new data row by row
    truncate_query= "TRUNCATE TABLE transformed.work_status; "
    insert_query= "INSERT INTO transformed.work_status  VALUES (%s, %s, %s, %s, %s)"
    with engine.connect() as conn:
        conn.execute(truncate_query) 
        for record in insert_values:
            conn.execute(insert_query, record)
    print("Data replaced/inserted successfully.")

    return

@task()
def store_emp_requirements_in_rds(emp_requirements):

    print("INPUT DATA.")
    print("Data:")
    print(emp_requirements.head())
    print("Data Info:")
    print(emp_requirements.info())

    # 1. Format datatype and convert the dataframe into list of tuple
    emp_requirements["From_Time"] = emp_requirements["From_Time"].astype(str)
    emp_requirements["To_Time"] = emp_requirements["To_Time"].astype(str)

    insert_values= [] # Will be a list of tuples
    for index,row in emp_requirements.iterrows():
        insert_values.append(tuple(row.values))
    print("Dataformatted successfully.")
    print("Data:")
    print(emp_requirements.head())
    print("Data Info:")
    print(emp_requirements.info())

    # 2. Create database connection
    db_url = 
    engine = create_engine(db_url)
    print("Database connection successfully.")

    # 3. Truuncate the table and insert new data row by row
    truncate_query= "TRUNCATE TABLE transformed.emp_req; "
    insert_query= "INSERT INTO transformed.emp_req  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    with engine.connect() as conn:
        conn.execute(truncate_query) 
        for record in insert_values:
            conn.execute(insert_query, record)
    print("Data replaced/inserted successfully.")
    return


############## Scheduling Functions ##############
@task()
def fetch_work_status_df_transformed_from_RDS():
    
    # 1. Create database connection
    db_url = 
    engine = create_engine(db_url)

    # 2. Query data from DB
    query= 'SELECT * FROM transformed.work_status;'
    with engine.connect() as conn:
        sql_query = pd.read_sql(
            sql=query,
            con=conn.connection
        )
    work_status_df = pd.DataFrame(sql_query)

    # 3. Format queried data

    # Rename the columns
    work_status_df.columns= ['Name', 'Start_time',  'End_time', 'Working Flag', 'Remaining_hours_left']

    # Fix time datatypes
    work_status_df['Start_time'] = pd.to_datetime(work_status_df['Start_time'], format='%H:%M:%S').dt.time
    work_status_df['End_time'] = pd.to_datetime(work_status_df['End_time'], format='%H:%M:%S').dt.time

    return work_status_df

@task()
def fetch_emp_requirements_transformed_from_RDS():

    # 1. Create database connection
    db_url = 
    engine = create_engine(db_url)

    # 2. Query data from DB
    query= 'SELECT * FROM transformed.emp_req;'
    with engine.connect() as conn:
        sql_query = pd.read_sql(
            sql=query,
            con=conn.connection
        )
    emp_requirements = pd.DataFrame(sql_query)

    # 3. Format queried data
    
    # Rename the columns
    emp_requirements.columns= ['From_Time', 'To_Time',  'Reg_Up_Needed', 'Reg_Down_Needed', 'Greeter_Up_Needed', 'Greeter_Down_Needed', 'Min_Total_Emp_Needed', 'Total_Avl_Emp', 'Availability_Check_Flag']

    # Fix time datatypes
    emp_requirements['From_Time'] = pd.to_datetime(emp_requirements['From_Time'], format='%H:%M:%S').dt.time
    emp_requirements['To_Time'] = pd.to_datetime(emp_requirements['To_Time'], format='%H:%M:%S').dt.time

    return emp_requirements

@task()
def greeter_allocation(work_status_df, emp_requirements):
    greeter_assignment, greeter_shift_done_dict = allocate_greeter(work_status_df, emp_requirements)
    return greeter_assignment

@task()
def register_salesfloor_allocation(emp_requirements, work_status_df, greeter_assignment):
    register_allocation= allocate_register_salesfloor(emp_requirements, work_status_df, greeter_assignment)
    final_allocation= pd.merge(greeter_assignment, register_allocation, how='outer', left_on=['From_Time', 'To_Time'], right_on=['From_Time', 'To_Time'])
    return final_allocation

@task()
def store_final_allocation_in_rds(final_allocation):
    
    print("INPUT DATA.")
    print("Data:")
    print(final_allocation.head())
    print("Data Info:")
    print(final_allocation.info())

    # 1. Format datatype, replace nulls to empty string and convert the dataframe into list of tuple
    # Format time to string
    final_allocation["From_Time"] = final_allocation["From_Time"].astype(str)
    final_allocation["To_Time"] = final_allocation["To_Time"].astype(str)

    # Format nulls
    final_allocation['Upstairs Greeter']= final_allocation['Upstairs Greeter'].fillna('')
    final_allocation['Downstairs Greeter']= final_allocation['Downstairs Greeter'].fillna('')
    final_allocation['Register Up']= final_allocation['Register Up'].fillna('')
    final_allocation['Register Down']= final_allocation['Register Down'].fillna('')
    final_allocation['SF Up']= final_allocation['SF Up'].fillna('')
    final_allocation['SF Down']= final_allocation['SF Down'].fillna('')

    # Format name array to string
    def array_to_str(arr):
        if isinstance(arr, str) and arr == '':
            return arr
        result=''
        for i in arr:
            result+=str(i)+", "
        return result[:-2]
    
    final_allocation["Upstairs Greeter"] = final_allocation["Upstairs Greeter"].astype(str)
    final_allocation["Downstairs Greeter"] = final_allocation["Downstairs Greeter"].astype(str)
    final_allocation["Register Up"] = final_allocation["Register Up"].apply(array_to_str)
    final_allocation["Register Down"] = final_allocation["Register Down"].apply(array_to_str)
    final_allocation["SF Up"] = final_allocation["SF Up"].apply(array_to_str)
    final_allocation["SF Down"] = final_allocation["SF Down"].apply(array_to_str)

    insert_values= [] # Will be a list of tuples
    for index,row in final_allocation.iterrows():
        insert_values.append(tuple(row.values))

    print("Dataformatted successfully.")
    print("Data:")
    print(final_allocation.head())
    print("Data Info:")
    print(final_allocation.info())

    # 2. Create database connection
    db_url =
    engine = create_engine(db_url)

    # 3. Truuncate the table and insert new data row by row
    truncate_query= "TRUNCATE TABLE scheduled.final_allocation; "
    insert_query= "INSERT INTO scheduled.final_allocation  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    with engine.connect() as conn:
        conn.execute(truncate_query) 
        for record in insert_values:
            conn.execute(insert_query, record)
    print("Data replaced/inserted successfully.")

    return

############## Emailing Functions ##############
@task()
def convert_scheduled_data_to_json():
    records= covert_rds_to_json()
    return records

@task()
def upload_json_to_dynamo(records):
    upload_data_to_dynamodb(records)
    return

@task()
def migrate_dynamo_to_sqs():
    send_data_to_sqs()
    return

@task()
def email_with_ses_from_sqs():
    process_sqs_messages()
    return

with DAG(dag_id="Bookstore_Scheduling_DAG", schedule_interval="0 9 * * *", start_date=datetime(2022, 3, 5), catchup=False, tags=["Bookstore"]) as dag:

    with TaskGroup("Transformation_ETL", tooltip="Extract & transform Employee Availability and Shift Requirement data") as transformation:
        df = fetch_emp_avail_from_s3()
        work_status_df = prepare_emp_aval(df)
        emp_count_req = fetch_shift_req_from_s3()
        emp_requirements = prepare_shift_req(work_status_df, emp_count_req)
        
        # Define tasks to store transformed data in RDS
        store_work_status_task = store_work_status_in_rds(work_status_df)
        store_emp_requirements_task = store_emp_requirements_in_rds(emp_requirements)

        # Define the task order
        [df, emp_count_req] >> work_status_df >> emp_requirements
        emp_requirements >> [store_work_status_task, store_emp_requirements_task]
    
    with TaskGroup("Scheduling", tooltip="Run scheduling algorithm using the transformed data and store results in RDS.") as scheduling:
        work_status_df_task = fetch_work_status_df_transformed_from_RDS()
        emp_requirements_task = fetch_emp_requirements_transformed_from_RDS()

        # Ensure these functions return the actual task object, not the DataFrame
        greeter_assignment_task = greeter_allocation(work_status_df_task, emp_requirements_task)
        final_allocation_task = register_salesfloor_allocation(emp_requirements_task, work_status_df_task, greeter_assignment_task)

        # Define tasks to store scheduling data back in RDS
        store_final_allocation_rds_task = store_final_allocation_in_rds(final_allocation_task)

        # Define the task order using task objects
        [work_status_df_task, emp_requirements_task] >> greeter_assignment_task >> final_allocation_task >> store_final_allocation_rds_task

    with TaskGroup("Emailing", tooltip="Convert RDS to JSON, Stores them to Dynamo DB, Uploads to SQS and Emails employee's.") as emailing:
        rds_to_json = convert_scheduled_data_to_json()
        json_to_dynamo = upload_json_to_dynamo(rds_to_json)
        dynamo_to_sqs= migrate_dynamo_to_sqs()
        sqs_to_email= email_with_ses_from_sqs() # Sends email only if email ID is available

        # Define the task order using task objects
        rds_to_json >> json_to_dynamo >> dynamo_to_sqs >> sqs_to_email

    transformation >> scheduling >> emailing

