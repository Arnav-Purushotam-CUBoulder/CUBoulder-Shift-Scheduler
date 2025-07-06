import boto3
from flask import Flask, request, jsonify, render_template, send_file
import os
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO

# Load environment variables from .env file
load_dotenv()

# Access AWS credentials and region
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

# Database connection
DB_URL = "enter your connection DB URL"
engine = create_engine(DB_URL)
print("RDS DATABSE ENGINE CONNECTED")

app = Flask(__name__)

# S3 client setup
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Bucket name and folder where files will be uploaded
S3_BUCKET_NAME = 'bookstore-scheduling-bucket-arnav'
S3_FOLDER = 'Input/'

# Dictionary to simulate storage for uploaded files
uploaded_files = {
    'daily_employee_files': [],
    'daily_shift_files': []
}

# Route to render the upload form
@app.route('/')
def home():
    return render_template('upload_form.html')  # This will render the HTML form

# File upload route
@app.route('/upload-files', methods=['POST'])
def upload_files():
    # Check if both required files are present in the request
    if 'daily_employee_files' not in request.files or 'daily_shift_files' not in request.files:
        return jsonify({"error": "Both 'Daily Employee Availability' and 'Daily Shift Requirements' files are required."}), 400

    # Retrieve multiple files
    daily_employee_files = request.files.getlist('daily_employee_files')
    daily_shift_files = request.files.getlist('daily_shift_files')

    # Retrieve the shift date from the form
    shift_date = request.form.get('shift_date')
    
    if not shift_date:
        return jsonify({"error": "Shift date is required."}), 400

    # Convert the shift date to a proper format (e.g., YYYY-MM-DD)
    try:
        formatted_date = datetime.strptime(shift_date, '%Y-%m-%d').strftime('%Y-%m-%d')
    except ValueError:
        return jsonify({"error": "Invalid date format. Please use YYYY-MM-DD."}), 400

    # Validate file content (ensure files are provided)
    if not daily_employee_files or not daily_shift_files:
        return jsonify({"error": "One or both file sets are empty."}), 400

    # Upload the files to S3 with the combined date in the file name
    for file in daily_employee_files + daily_shift_files:
        # Determine the correct folder based on the file type
        if file in daily_employee_files:
            folder = 'Daily Employee Availability/'
        else:
            folder = 'Daily Shift Requirements/'

        # Generate a unique file name using the folder, file name, and shift date
        s3_file_name = os.path.join(S3_FOLDER, folder, f"{formatted_date}_{file.filename}")

        try:
            # Convert the file to CSV if it is an .xlsx file
            if file.filename.endswith('.xlsx'):
                # Read the Excel file into a DataFrame
                df = pd.read_excel(file)
                
                # Create a temporary CSV file
                csv_file_name = f"/tmp/{formatted_date}_{os.path.splitext(file.filename)[0]}.csv"
                df.to_csv(csv_file_name, index=False)

                # Upload the CSV file to S3
                with open(csv_file_name, 'rb') as csv_file:
                    s3_client.upload_fileobj(csv_file, S3_BUCKET_NAME, s3_file_name.replace('.xlsx', '.csv'))

                print(f"File {file.filename} converted to CSV and uploaded successfully to S3 at {s3_file_name.replace('.xlsx', '.csv')}.")
                os.remove(csv_file_name)  # Clean up temporary file
            else:
                # Upload the original file to S3
                s3_client.upload_fileobj(file, S3_BUCKET_NAME, s3_file_name)
                print(f"File {file.filename} uploaded successfully to S3 at {s3_file_name}.")

            # Store the filename in the dictionary to simulate storage
            if file in daily_employee_files:
                uploaded_files['daily_employee_files'].append(s3_file_name)
            else:
                uploaded_files['daily_shift_files'].append(s3_file_name)

        except Exception as e:
            return jsonify({"error": f"Error uploading file {file.filename}: {e}"}), 500

    # Return success message once both file sets are uploaded
    return jsonify({"message": f"{len(daily_employee_files)} Daily Employee Availability files and {len(daily_shift_files)} Daily Shift Requirements files uploaded successfully to S3!"}), 200


@app.route('/retrieve-files', methods=['POST'])
def retrieve_files():
    # Get the date from the form
    selected_date = request.form.get('date')

    # Validate the date
    if not selected_date:
        return jsonify({"error": "Date is required."}), 400

    try:
        # Query the database
        query = "SELECT * FROM scheduled.final_allocation"
        with engine.connect() as conn:
            sql_query = pd.read_sql(
                sql=query,
                con=conn.connection
            )
        df = pd.DataFrame(sql_query)
        # Convert the DataFrame to HTML for rendering
        table_html = df.to_html(classes='table table-bordered', index=False)
        return render_template('retrieve_results.html', table_html=table_html)

    except Exception as e:
        return render_template('error.html', error_message=str(e))

# Route to download Excel
@app.route('/download-excel')
def download_excel():
    try:
        # Query the database
        query = "SELECT * FROM scheduled.final_allocation"
        with engine.connect() as conn:
            sql_query = pd.read_sql(
                sql=query,
                con=conn.connection
            )
        df = pd.DataFrame(sql_query)

        # Create an Excel file
        output = BytesIO()
        df.to_excel(output, index=False, engine='openpyxl')
        output.seek(0)

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='schedule.xlsx'
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)


