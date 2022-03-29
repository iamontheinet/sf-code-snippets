### Pre-reqs: 
# pip install boto3 awscli json snowflake.snowpark
# Set AWS credentials via environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
###

from snowflake.snowpark.session import Session
import json
import boto3
from botocore.exceptions import ClientError

# Load Snowflake connection details from AWS Secrets Manager
def get_aws_sf_connection_details(secret_name,region_name):
    
    # Create a Secrets Manager boto3 client
    boto3_session = boto3.session.Session()
    client = boto3_session.client(service_name='secretsmanager',region_name=region_name)
    
    get_secret_value_response = None

    try:
        # Get secret values(s) based on the passed in secret name
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)['SecretString']
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            raise e

    return get_secret_value_response
        
# Create Snowflake Session object
connection_parameters = json.loads(get_aws_sf_connection_details('AWS_SECRET_NAME','AWS_REGION'))
session = Session.builder.configs(connection_parameters).create()
print(session.sql('select current_warehouse(), current_database(), current_schema()').collect())
