import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import boto3
import json
import os
import uuid
import json
from dotenv import load_dotenv

load_dotenv()

def covert_rds_to_json():
    # 1. Create database connection
    db_url = 
    engine = create_engine(db_url)
    print("RDS Database Connection Successfull")

    # 2. Query the table and convert to dataframe
    query= 'SELECT * FROM scheduled.final_allocation;'
    with engine.connect() as conn:
        sql_query = pd.read_sql(
            sql=query,
            con=conn.connection
        )
    df = pd.DataFrame(sql_query)
    print("Final Allocation data query Successfull")

    # 3. Format by time and sort ascending

    # Step 1: Convert to datetime objects
    df["from_time"] = pd.to_datetime(df["from_time"], format="%H:%M:%S")
    df["to_time"] = pd.to_datetime(df["to_time"], format="%H:%M:%S")

    # Step 2: Sort by `from_time` in ascending order
    df = df.sort_values(by="from_time")

    # Step 3: Convert to AM/PM format
    def convert_time_format(time_obj):
        return time_obj.strftime("%I:%M%p").lstrip("0")  # Remove leading 0 for hours
    df["from_time"] = df["from_time"].apply(convert_time_format)
    df["to_time"] = df["to_time"].apply(convert_time_format)
    print("Data sorting and formating Successfull")

    # 4. Convert to dictionary
    dynamo_db_input=dict()

    email_lookup={
        'Arnav Purushotam': 'arnavpsusa@gmail.com'
    }

    def check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name):
        if (emp_name not in dynamo_db_input): # Add name to dictionary if not exist and create template
            email=''
            if emp_name in email_lookup:  # Add email if exist
                email= email_lookup[emp_name]
            dynamo_db_input[emp_name]= {
                "Name": emp_name,
                "Data": [],
                "Email": email
            }
            return dynamo_db_input
        else:
            return dynamo_db_input

    for index,row in df.iterrows():
        # Convert Greeter Up
        if row['upstairs_greeter'] !='': 
            emp_name= row['upstairs_greeter']
            dynamo_db_input= check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name)
            # Data row to append
            data_row={
                'Location': 'Greeter Upstair',
                'TimeIn': row['from_time'],
                'TimeOut': row['to_time']
                }
            dynamo_db_input[emp_name]['Data'].append(data_row)

        # Convert Greeter Down
        if row['downstairs_greeter'] !='': 
            emp_name= row['downstairs_greeter']
            dynamo_db_input= check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name)
            # Data row to append
            data_row={
                'Location': 'Greeter Downstair',
                'TimeIn': row['from_time'],
                'TimeOut': row['to_time']
                }
            dynamo_db_input[emp_name]['Data'].append(data_row)

        # Convert Register Up
        if row['register_up'] !='': 
            emp_names= row['register_up'].split(", ")
            for emp_name in emp_names:
                dynamo_db_input= check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name)
                # Data row to append
                data_row={
                    'Location': 'Register Upstair',
                    'TimeIn': row['from_time'],
                    'TimeOut': row['to_time']
                    }
                dynamo_db_input[emp_name]['Data'].append(data_row)
        
        # Convert Register Down
        if row['register_down'] !='': 
            emp_names= row['register_down'].split(", ")
            for emp_name in emp_names:
                dynamo_db_input= check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name)
                # Data row to append
                data_row={
                    'Location': 'Register Downstair',
                    'TimeIn': row['from_time'],
                    'TimeOut': row['to_time']
                    }
                dynamo_db_input[emp_name]['Data'].append(data_row)
        
        # Convert Salesfloor Up
        if row['sf_up'] !='': 
            emp_names= row['sf_up'].split(", ")
            for emp_name in emp_names:
                dynamo_db_input= check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name)
                # Data row to append
                data_row={
                    'Location': 'Salesfloor Upstair',
                    'TimeIn': row['from_time'],
                    'TimeOut': row['to_time']
                    }
                dynamo_db_input[emp_name]['Data'].append(data_row)
        
        if row['sf_down'] !='': 
            emp_names= row['sf_down'].split(", ")
            for emp_name in emp_names:
                dynamo_db_input= check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name)
                # Data row to append
                data_row={
                    'Location': 'Salesfloor Downstair',
                    'TimeIn': row['from_time'],
                    'TimeOut': row['to_time']
                    }
                dynamo_db_input[emp_name]['Data'].append(data_row)
        
    # Remove names as keys and convert to a list of dictionaries
    dynamo_db_input_records= []

    for key,value in dynamo_db_input.items():
        dynamo_db_input_records.append(value)

    print("Data Conversion Successfull \n\n")

    return dynamo_db_input_records


def convert_df_to_emp_view(df):
    # 1. Format by time and sort ascending

    # Step 1: Convert to datetime objects
    df["From_Time"] = pd.to_datetime(df["From_Time"], format="%H:%M:%S")
    df["To_Time"] = pd.to_datetime(df["To_Time"], format="%H:%M:%S")

    # Step 2: Sort by `From_Time` in ascending order
    df = df.sort_values(by="From_Time")

    # Step 3: Convert to AM/PM format
    def convert_time_format(time_obj):
        return time_obj.strftime("%I:%M%p").lstrip("0")  # Remove leading 0 for hours
    df["From_Time"] = df["From_Time"].apply(convert_time_format)
    df["To_Time"] = df["To_Time"].apply(convert_time_format)
    print("Data sorting and formating Successfull")

    # 4. Convert to dictionary
    dynamo_db_input=dict()

    email_lookup={
        'Arnav Purushotam': 'arnavpsusa@gmail.com'
    }

    def check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name):
        if (emp_name not in dynamo_db_input): # Add name to dictionary if not exist and create template
            dynamo_db_input[emp_name]= {
                "Name": emp_name,
                "Data": [],
            }
            return dynamo_db_input
        else:
            return dynamo_db_input

    for index,row in df.iterrows():
        # Convert Greeter Up
        if row['upstairs_greeter'] !='': 
            emp_name= row['upstairs_greeter']
            dynamo_db_input= check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name)
            # Data row to append
            data_row={
                'Location': 'Greeter Upstair',
                'TimeIn': row['from_time'],
                'TimeOut': row['to_time']
                }
            dynamo_db_input[emp_name]['Data'].append(data_row)

        # Convert Greeter Down
        if row['downstairs_greeter'] !='': 
            emp_name= row['downstairs_greeter']
            dynamo_db_input= check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name)
            # Data row to append
            data_row={
                'Location': 'Greeter Downstair',
                'TimeIn': row['from_time'],
                'TimeOut': row['to_time']
                }
            dynamo_db_input[emp_name]['Data'].append(data_row)

        # Convert Register Up
        if row['register_up'] !='': 
            emp_names= row['register_up'].split(", ")
            for emp_name in emp_names:
                dynamo_db_input= check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name)
                # Data row to append
                data_row={
                    'Location': 'Register Upstair',
                    'TimeIn': row['from_time'],
                    'TimeOut': row['to_time']
                    }
                dynamo_db_input[emp_name]['Data'].append(data_row)
        
        # Convert Register Down
        if row['register_down'] !='': 
            emp_names= row['register_down'].split(", ")
            for emp_name in emp_names:
                dynamo_db_input= check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name)
                # Data row to append
                data_row={
                    'Location': 'Register Downstair',
                    'TimeIn': row['from_time'],
                    'TimeOut': row['to_time']
                    }
                dynamo_db_input[emp_name]['Data'].append(data_row)
        
        # Convert Salesfloor Up
        if row['sf_up'] !='': 
            emp_names= row['sf_up'].split(", ")
            for emp_name in emp_names:
                dynamo_db_input= check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name)
                # Data row to append
                data_row={
                    'Location': 'Salesfloor Upstair',
                    'TimeIn': row['from_time'],
                    'TimeOut': row['to_time']
                    }
                dynamo_db_input[emp_name]['Data'].append(data_row)
        
        if row['sf_down'] !='': 
            emp_names= row['sf_down'].split(", ")
            for emp_name in emp_names:
                dynamo_db_input= check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name)
                # Data row to append
                data_row={
                    'Location': 'Salesfloor Downstair',
                    'TimeIn': row['from_time'],
                    'TimeOut': row['to_time']
                    }
                dynamo_db_input[emp_name]['Data'].append(data_row)
        
    # Remove names as keys and convert to a list of dictionaries
    dynamo_db_input_records= []

    for key,value in dynamo_db_input.items():
        dynamo_db_input_records.append(value)

    print("Data Conversion Successfull V1 \n\n")
    print(dynamo_db_input)

    print("Data Conversion Successfull V2 \n\n")
    print(dynamo_db_input_records)

    return dynamo_db_input_records


def upload_data_to_dynamodb(records):

    # Get AWS credentials from environment variables
    region = 
    aws_access_key_id = 
    aws_secret_access_key = 

    # Initialize DynamoDB client and table with credentials
    dynamodb = boto3.resource(
        'dynamodb',
        region_name=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    table = dynamodb.Table('ShiftsTable')

    # Upload each record one at a time
    for record in records:
        #formatted_item = format_item_for_dynamodb(record)
        formatted_item= record
        table.put_item(Item=formatted_item)
        print(f"Item uploaded successfully: {formatted_item}")

    return

def delete_all_records_from_dynamodb():
    # Get AWS credentials from environment variables
    region = 
    aws_access_key_id = 
    aws_secret_access_key = 

    # Initialize DynamoDB client and table with credentials
    dynamodb = boto3.resource(
        'dynamodb',
        region_name=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    table = dynamodb.Table('ShiftsTable')

    # Extract key attribute names from the key schema
    key_names = [key['AttributeName'] for key in table.key_schema]

    # Scan the table to retrieve all items
    response = table.scan()
    items = response.get('Items', [])

    # Delete each item
    for item in items:
        # Construct the key from the item using the key names
        key = {key_name: item[key_name] for key_name in key_names}
        table.delete_item(Key=key)

    # Handle pagination if the table has more items than the scan limit
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items = response.get('Items', [])
        for item in items:
            key = {key_name: item[key_name] for key_name in key_names}
            table.delete_item(Key=key)

    print("All items deleted in Dynamo DB successfully.")

def connect_to_dynamodb():
    dynamodb = boto3.resource(
        'dynamodb',
        region_name=,
        aws_access_key_id=,
        aws_secret_access_key=
    )
    table = dynamodb.Table('ShiftsTable')
    return table

def connect_to_sqs():
    try:
        sqs = boto3.client('sqs',
                region_name=,
                aws_access_key_id=,
                aws_secret_access_key=)

        queue_url = 
        return sqs, queue_url
    except Exception as e:
        print(f"Error connecting to SQS: {e}")
        return None, None
     
def send_data_to_sqs():
    def get_items_from_dynamodb(table):
        try:
            # Scan all items from the table
            response = table.scan()
            items = response.get('Items', [])
            
            # Paginate as there are more items
            while 'LastEvaluatedKey' in response:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                items.extend(response.get('Items', []))
            
            return items
        
        except Exception as e:
            print(f"Error retrieving data from DynamoDB: {e}")
            return []
    
    def send_to_sqs(items, sqs, queue_url):
        """Send each item to the SQS queue."""
        try:
            for item in items:
                # Send message to SQS
                response = sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(item),
                    MessageGroupId="schedule_group",  
                    MessageDeduplicationId=str(uuid.uuid4())  
                )
                print(f"Message sent with ID: {response['MessageId']}")
        
        except Exception as e:
            print(f"Error sending message to SQS: {e}")
    
    table = connect_to_dynamodb()
    if table:
        sqs, queue_url = connect_to_sqs()
        
        if sqs and queue_url:
            items = get_items_from_dynamodb(table)
            
            if items:
                send_to_sqs(items, sqs, queue_url)
            else:
                print("No items to send.")
        else:
            print("Failed to connect to SQS.")
    else:
        print("Failed to connect to DynamoDB.")

    delete_all_records_from_dynamodb()

    pass

def connect_to_ses():
    try:
        ses = boto3.client('ses',
                region_name=,
                aws_access_key_id=,
                aws_secret_access_key=)
        return ses
    except Exception as e:
        print(f"Error connecting to SES: {e}")
        return None
    
def process_sqs_messages():
    def send_email_via_ses(item, ses):
        try:
            name = item['Name']
            email = item['Email']
            shift_details = item['Data']
            
            body = f"Hi {name},\n\nHere are your shift details:\n"
            for shift in shift_details:
                body += f"- {shift['TimeIn']} to {shift['TimeOut']} at {shift['Location']}\n"
            
            # Send email via SES
            ses.send_email(
                Source='nitrox919@gmail.com',  
                Destination={'ToAddresses': [email]},
                Message={
                    'Subject': {'Data': 'Your Shift Details'},
                    'Body': {'Text': {'Data': body}}
                }
            )
            print(f"Email sent to {name} at {email}")
        except Exception as e:
            print(f"Error sending email via SES: {e}")

    sqs, queue_url = connect_to_sqs()

    # Send emails for a batch of 10 at a time
    if sqs:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,  
            WaitTimeSeconds=10
        )

        while 'Messages' in response:
            ses = connect_to_ses()
            if ses:
                for message in response['Messages']:
                    #print("Message Body:", message['Body'])

                    try:
                        item = json.loads(message['Body'])
                        send_email_via_ses(item, ses)

                        # Delete the processed message from SQS
                        sqs.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        print(f"Message deleted from SQS: {message['MessageId']}")

                    except Exception as e:
                        print(f"Error processing message: {e}")
            print("\n\n Emails Sent For Batch\n\n")
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,  
                WaitTimeSeconds=10
            )
    else:
        print("Failed to connect to SQS.")
    return



# if __name__ == "__main__":
#     records= covert_rds_to_json()
#     upload_data_to_dynamodb(records)
#     send_data_to_sqs()
#     process_sqs_messages()
