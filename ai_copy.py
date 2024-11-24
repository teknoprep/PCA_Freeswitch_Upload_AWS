"""
========================= Instructions to Build the .env File =========================

Create a file named '.env' in the same directory as this script or in a directory accessible by the script.

The .env file should contain the following variables:

# AWS Credentials
AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key

# AWS S3 Configuration
S3_BUCKET_NAME=your_s3_bucket_name
S3_REGION_NAME=your_s3_region_name
S3_KEY_PREFIX=optional_s3_key_prefix  # Leave blank if not needed

# AWS Step Functions Configuration
STEP_FUNCTION_ARN=your_step_function_arn

# File and Directory Paths
FREESWITCH_RECORDING_PATH=/path/to/your/freeswitch/recordings
DOMAIN_NAME=your_domain_name
COPIED_DATA_FILE=/path/to/your/copied_data.json

# Upload Configuration
RECORD_RETENTION_DAYS=number_of_days_to_retain_records  # e.g., 30
EXTENSIONS_TO_UPLOAD=.wav,.mp3  # Comma-separated list of file extensions to upload
MIN_FILE_LENGTH_SECONDS=minimum_duration_in_seconds  # e.g., 15

# Initial Seed Configuration (Optional)
INITIAL_SEED_DAYS=number_of_days_to_process_on_initial_seed  # e.g., 5

# Agent Upload Filter Array
AGENT_UPLOAD_FILTER_ARRAY=201,202,212

=======================================================================================

Example .env file:

AWS_ACCESS_KEY_ID=ABCD1234EFGH5678IJKL
AWS_SECRET_ACCESS_KEY=abcd1234efgh5678ijkl9012mnop3456qrst7890
S3_BUCKET_NAME=my-s3-bucket
S3_REGION_NAME=us-east-1
S3_KEY_PREFIX=optional/prefix
STEP_FUNCTION_ARN=arn:aws:states:us-east-1:123456789012:stateMachine:MyStateMachine
FREESWITCH_RECORDING_PATH=/usr/local/freeswitch/recordings
DOMAIN_NAME=pbx.example.com
COPIED_DATA_FILE=/usr/src/ai_s3_copy/pbx.example.com/pbx.example.com.json
RECORD_RETENTION_DAYS=30
EXTENSIONS_TO_UPLOAD=.wav,.mp3
MIN_FILE_LENGTH_SECONDS=15
INITIAL_SEED_DAYS=5
AGENT_UPLOAD_FILTER_ARRAY=201,202,212

Make sure to replace the example values with your actual configuration.

Note:

- The .env file should be kept secure and not committed to version control systems like Git, as it contains sensitive information.
- Ensure that the user running this script has read access to the .env file.
- You may need to install the 'python-dotenv' package if it's not already installed:

    pip install python-dotenv

=======================================================================================

"""

import os
import json
import boto3
import hashlib
from datetime import datetime, timedelta
from urllib.parse import quote  # For URL encoding
import traceback  # For detailed error information
from dotenv import load_dotenv  # To load variables from .env file
import re  # For regex operations

# ---------------------------- Load Environment Variables ----------------------------
load_dotenv()

# ---------------------------- Variables ----------------------------

# File and Directory Paths
freeswitch_recording_path = os.getenv('FREESWITCH_RECORDING_PATH', '/usr/local/freeswitch/recordings')
domain_name = os.getenv('DOMAIN_NAME', 'your_domain_name')  # Company domain name to formulate the folder path
copied_data_file = os.getenv('COPIED_DATA_FILE', '/path/to/your/copied_data.json')  # JSON file to track uploaded files

# Upload Configuration
record_retention_days = int(os.getenv('RECORD_RETENTION_DAYS', '30'))  # Number of days to retain records in the JSON file
extensions_to_upload = os.getenv('EXTENSIONS_TO_UPLOAD', '.wav,.mp3').split(',')  # List of file extensions to upload

# Minimum Duration Configuration
min_file_length_seconds = int(os.getenv('MIN_FILE_LENGTH_SECONDS', '15'))  # Minimum duration in seconds for both .wav and .mp3 files

# Initial Seed Configuration (Optional)
initial_seed_days = int(os.getenv('INITIAL_SEED_DAYS', str(record_retention_days)))  # Number of days to process on initial seed

# AWS S3 Configuration
# Credentials are loaded from the .env file
s3_access_id = os.getenv('AWS_ACCESS_KEY_ID')  # Loaded from .env
s3_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')  # Loaded from .env
s3_bucket_name = os.getenv('S3_BUCKET_NAME', 'your_s3_bucket_name')  # Your S3 bucket name
s3_region_name = os.getenv('S3_REGION_NAME', 'your_s3_region_name')  # AWS region where your bucket is located
s3_key_prefix = os.getenv('S3_KEY_PREFIX', '')  # Optional: Set a prefix for S3 keys if needed

# AWS Step Functions Configuration
step_function_arn = os.getenv('STEP_FUNCTION_ARN', 'your_step_function_arn')  # Replace with your Step Function's State Machine ARN

# Agent Upload Filter Configuration
agent_upload_filter_array_env = os.getenv('AGENT_UPLOAD_FILTER_ARRAY', '')
if agent_upload_filter_array_env:
    # Split by commas, strip whitespace, and filter out empty strings
    agent_upload_filter_array = [agent.strip() for agent in agent_upload_filter_array_env.split(',') if agent.strip()]
    print(f"Agent upload filter array loaded: {agent_upload_filter_array}")
else:
    agent_upload_filter_array = []
    print("Agent upload filter array is empty. All agent files will be uploaded.")

# -------------------------------------------------------------------

# ------------------------ Duration Checks ------------------------
try:
    from mutagen.mp3 import MP3
    from mutagen.wave import WAVE
except ImportError:
    print("Error: The 'mutagen' library is not installed. Please install it using:")
    print("pip install mutagen")
    sys.exit(1)

def get_audio_duration(file_path, extension):
    """
    Returns the duration of an audio file in seconds.
    If the duration cannot be determined, returns 0.
    """
    try:
        if extension == '.mp3':
            audio = MP3(file_path)
        elif extension == '.wav':
            audio = WAVE(file_path)
        else:
            return 0  # Unsupported extension
        return audio.info.length
    except Exception as e:
        print(f"Error reading {extension} file {file_path}: {e}")
        traceback.print_exc()
        return 0

# ------------------------ Filename Normalization ------------------------
def normalize_filename(filename):
    """
    Normalizes the filename by removing any special characters
    except '_', '.', and '-'. Ensures no '+' signs are present.

    Args:
        filename (str): The original filename without extension.

    Returns:
        str: The normalized filename.
    """
    # Remove any character that is not alphanumeric, '_', '.', or '-'
    normalized = re.sub(r'[^a-zA-Z0-9_.-]', '', filename)
    return normalized

# ------------------------ Validate and Fix IDs ------------------------
def validate_and_fix_ids(filename):
    """
    Validates and fixes the _CUST_ and _AGENT_ parts of the filename.
    - For _CUST_, removes leading '1' if the number is 11 digits starting with '1',
      then ensures the ID is exactly 10 digits.
    - For _AGENT_, leaves the ID unchanged if it's all digits.
      If it contains non-digit characters, removes them and uses the remaining digits.

    Args:
        filename (str): The original filename.

    Returns:
        str: The filename with corrected _CUST_ and _AGENT_ parts.
    """
    # Regex patterns to find _CUST_ and _AGENT_ parts
    cust_pattern = r'(_CUST_)([^_]+)'
    agent_pattern = r'(_AGENT_)([^_]+)'

    # Function to correct _CUST_ ID
    def correct_cust_id(match):
        prefix = match.group(1)
        id_value = match.group(2)
        # Remove non-digit characters
        digits = re.sub(r'\D', '', id_value)
        # If digits are 11 digits and start with '1', remove the leading '1'
        if len(digits) == 11 and digits.startswith('1'):
            digits = digits[1:]
            print(f"Removed leading '1' from {prefix} ID: {id_value} -> {digits}")
        # Ensure ID is exactly 10 digits
        if len(digits) >= 10:
            corrected_id = digits[-10:]
        else:
            corrected_id = digits.zfill(10)
        print(f"Corrected {prefix} ID: {id_value} -> {corrected_id}")
        return prefix + corrected_id

    # Function to correct _AGENT_ ID
    def correct_agent_id(match):
        prefix = match.group(1)
        id_value = match.group(2)
        if id_value.isdigit():
            # ID contains only digits; leave it unchanged
            corrected_id = id_value
            print(f"Leaving {prefix} ID unchanged: {id_value}")
        else:
            # Remove non-digit characters
            digits = re.sub(r'\D', '', id_value)
            corrected_id = digits
            print(f"Corrected {prefix} ID: {id_value} -> {corrected_id}")
        return prefix + corrected_id

    # Replace _CUST_ IDs
    corrected_filename = re.sub(cust_pattern, correct_cust_id, filename, count=1)

    # Replace _AGENT_ IDs
    corrected_filename = re.sub(agent_pattern, correct_agent_id, corrected_filename, count=1)

    return corrected_filename
# ------------------------ End Validate and Fix IDs ------------------------

# -------------------------------------------------------------------

# Initialize S3 client with enforced signature version
s3_client = boto3.client(
    's3',
    aws_access_key_id=s3_access_id,
    aws_secret_access_key=s3_access_key,
    region_name=s3_region_name,
    config=boto3.session.Config(signature_version='s3v4')
)

# Initialize Step Functions client
stepfunctions_client = boto3.client(
    'stepfunctions',
    aws_access_key_id=s3_access_id,
    aws_secret_access_key=s3_access_key,
    region_name=s3_region_name
)

# Load or initialize copied data
if os.path.exists(copied_data_file):
    try:
        with open(copied_data_file, 'r') as f:
            copied_data = json.load(f)
            print(f"Loaded copied data from {copied_data_file}.")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {copied_data_file}: {e}")
        copied_data = {}
else:
    copied_data = {}
    print(f"No existing copied data file found. Starting fresh.")

# Initialize or update retention period
if 'record_retention_days' not in copied_data:
    copied_data['record_retention_days'] = record_retention_days
    print(f"Set record retention days to {record_retention_days}.")
elif copied_data['record_retention_days'] != record_retention_days:
    print(f"Updating record retention days from {copied_data['record_retention_days']} to {record_retention_days}.")
    copied_data['record_retention_days'] = record_retention_days

# Determine the start time for processing
if 'last_run_time' in copied_data:
    try:
        last_run_time = datetime.fromisoformat(copied_data['last_run_time'])
        print(f"Last run time found: {last_run_time.isoformat()} UTC.")
    except ValueError as e:
        print(f"Invalid last_run_time format in JSON: {e}. Starting fresh.")
        last_run_time = None
else:
    last_run_time = None

# Check if we need to perform an initial seed
do_initial_seed = False
if last_run_time is None:
    # First run or invalid last_run_time
    do_initial_seed = True
    print(f"Performing initial seed: Processing the last {initial_seed_days} day(s).")
else:
    # Calculate the difference between now and last_run_time
    now = datetime.utcnow()
    if now - last_run_time > timedelta(days=1):
        print(f"Last run was more than a day ago. Processing all files from {last_run_time.isoformat()} to now.")
    else:
        print(f"Last run was within the last day. Processing files from {last_run_time.isoformat()} to now.")

# Generate dates to process
dates_to_process = []

if do_initial_seed:
    for i in range(initial_seed_days):
        date = datetime.today() - timedelta(days=i)
        dates_to_process.append(date)
else:
    # Process files from last_run_time to now
    # Assuming the directory structure is organized by year/month/day
    # We'll generate all dates from last_run_time.date() to today
    start_date = last_run_time.date() if last_run_time else datetime.today().date()
    end_date = datetime.today().date()
    delta = end_date - start_date
    for i in range(delta.days + 1):
        date = datetime.combine(start_date + timedelta(days=i), datetime.min.time())
        dates_to_process.append(date)

# Initialize a list to track uploads in the current run
uploaded_files_current_run = []

# Initialize uploaded_files as a dictionary if not present
if 'uploaded_files' not in copied_data:
    copied_data['uploaded_files'] = {}
    print("Initialized 'uploaded_files' dictionary in copied data.")

# Convert uploaded_files to a dictionary
uploaded_files_dict = copied_data['uploaded_files']

# ---------------------- Retention Mechanism ----------------------

# Calculate the cutoff datetime for retention
cutoff_datetime = datetime.utcnow() - timedelta(days=record_retention_days)
print(f"Removing records older than {cutoff_datetime.isoformat()} UTC.")

# Create a list of files to remove
files_to_remove = [
    file_path for file_path, details in uploaded_files_dict.items()
    if datetime.fromisoformat(details['uploaded_at']) < cutoff_datetime
]

# Remove old records
for file_path in files_to_remove:
    del uploaded_files_dict[file_path]
    print(f"Removed old record: {file_path}")

# ---------------------- File Processing ----------------------

# Process each date
for date in dates_to_process:
    year = date.strftime('%Y')
    month = date.strftime('%b')  # 'Oct', 'Sep', etc.
    day = date.strftime('%d')    # '01', '30', etc.

    folder_path = os.path.join(
        freeswitch_recording_path,
        domain_name,
        'archive',
        year,
        month,
        day
    )

    print(f"Scanning folder: {folder_path}")

    # Check if the folder exists
    if not os.path.exists(folder_path):
        print(f"Folder does not exist: {folder_path}. Skipping.")
        continue  # Skip if the folder doesn't exist

    # Walk through the folder and find files with specified extensions
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_lower = file.lower()
            if any(file_lower.endswith(ext) for ext in extensions_to_upload):
                file_path = os.path.abspath(os.path.join(root, file))
                file_path = os.path.normpath(file_path)

                # Get current file size
                try:
                    current_size = os.path.getsize(file_path)
                except OSError as e:
                    print(f"Error accessing file size for {file_path}: {e}")
                    traceback.print_exc()
                    continue  # Skip files that can't be accessed

                # Check if the file has already been uploaded
                if file_path in uploaded_files_dict:
                    previous_size = uploaded_files_dict[file_path]['file_size']
                    if current_size != previous_size:
                        print(f"File size changed for {file_path}: {previous_size} -> {current_size}. Re-uploading.")
                    else:
                        print(f"Skipping file {file_path}: No size change detected.")
                        continue  # Skip files with unchanged size
                else:
                    print(f"Found new file to upload: {file_path}")

                # Check if the file meets the minimum duration requirement
                _, file_extension = os.path.splitext(file_lower)
                duration = get_audio_duration(file_path, file_extension)
                if duration < min_file_length_seconds:
                    print(f"Skipping file {file_path}: Duration {duration:.2f} seconds is less than {min_file_length_seconds} seconds.")
                    continue  # Skip files shorter than the minimum duration

                # Normalize the base filename
                original_filename = file
                corrected_filename = validate_and_fix_ids(original_filename)
                file_base_name, file_extension = os.path.splitext(corrected_filename)
                normalized_base_name = normalize_filename(file_base_name)

                # -------------------- Agent Filtering Logic --------------------
                # Extract _AGENT_ ID from the corrected filename
                agent_match = re.search(r'_AGENT_(\d+)', corrected_filename)
                if agent_match:
                    agent_id = agent_match.group(1)
                    if agent_upload_filter_array:
                        if agent_id not in agent_upload_filter_array:
                            print(f"Skipping file {file_path}: Agent ID {agent_id} is not in the allowed list {agent_upload_filter_array}.")
                            continue  # Skip this file
                        else:
                            print(f"File {file_path} matches allowed Agent ID {agent_id}. Proceeding to upload.")
                    else:
                        print(f"Agent upload filter is empty. Proceeding to upload file {file_path} with Agent ID {agent_id}.")
                else:
                    print(f"Skipping file {file_path}: No _AGENT_ ID found in filename.")
                    continue  # Skip files without _AGENT_ ID
                # -------------------- End Agent Filtering Logic --------------------

                # Flatten the S3 key (filename only)
                # To prevent collisions, append a unique identifier
                relative_path = os.path.relpath(file_path, freeswitch_recording_path)
                unique_identifier = hashlib.md5(relative_path.encode('utf-8')).hexdigest()[:8]
                s3_filename = f"{normalized_base_name}_{unique_identifier}{file_extension}"

                # URL-encode the S3 key to handle special characters
                s3_key = quote(s3_filename)

                if s3_key_prefix:
                    s3_key = f"{s3_key_prefix}/{s3_key}"
                else:
                    s3_key = s3_key

                # Print the file being uploaded
                print(f"Uploading {file_path} to s3://{s3_bucket_name}/{s3_key}...")

                # Upload the file to S3
                try:
                    s3_client.upload_file(file_path, s3_bucket_name, s3_key)
                    print(f"Uploaded {file_path} to s3://{s3_bucket_name}/{s3_key}")

                    # Record the upload with the current UTC timestamp and file size
                    uploaded_files_dict[file_path] = {
                        'uploaded_at': datetime.utcnow().isoformat(),
                        'file_size': current_size
                    }

                    # Add to the current run's upload list
                    uploaded_files_current_run.append(file_path)
                except Exception as e:
                    print(f"Failed to upload {file_path}: {e}")
                    traceback.print_exc()

# ---------------------- Trigger AWS Step Function ----------------------

if uploaded_files_current_run:
    print(f"{len(uploaded_files_current_run)} file(s) uploaded/re-uploaded. Triggering AWS Step Function: {step_function_arn}")

    # Prepare input for the Step Function if needed
    # For example, you can pass the list of uploaded files
    step_function_input = {
        "uploaded_files": uploaded_files_current_run,
        "timestamp": datetime.utcnow().isoformat()
    }

    try:
        response = stepfunctions_client.start_execution(
            stateMachineArn=step_function_arn,
            input=json.dumps(step_function_input)
        )
        execution_arn = response['executionArn']
        print(f"Step Function execution started: {execution_arn}")
    except Exception as e:
        print(f"Failed to start Step Function execution: {e}")
        traceback.print_exc()
else:
    print("No new or updated files were uploaded/re-uploaded. Step Function will not be triggered.")

# ---------------------- Save Tracking Data ----------------------

# Update copied_data with the latest uploaded_files
copied_data['uploaded_files'] = uploaded_files_dict

# Update last_run_time to current UTC time
copied_data['last_run_time'] = datetime.utcnow().isoformat()

# Save the updated copied_data to the JSON file
try:
    # Ensure the directory exists
    os.makedirs(os.path.dirname(copied_data_file), exist_ok=True)

    with open(copied_data_file, 'w') as f:
        json.dump(copied_data, f, indent=4)
    print(f"Updated copied data file: {copied_data_file}")
except Exception as e:
    print(f"Failed to write to copied data file {copied_data_file}: {e}")
    traceback.print_exc()
