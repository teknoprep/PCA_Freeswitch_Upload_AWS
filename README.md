### PCA_Freeswitch_Upload_AWS
This code takes your Freeswitch / FusionPBX call recordings and uploads them to S3 for processing





### Example .env File
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

Make sure to replace the example values with your actual configuration.

Note:

- The .env file should be kept secure and not committed to version control systems like Git, as it contains sensitive information.
- Ensure that the user running this script has read access to the .env file.
- You may need to install the 'python-dotenv' package if it's not already installed:

    pip install python-dotenv

=======================================================================================

"""
