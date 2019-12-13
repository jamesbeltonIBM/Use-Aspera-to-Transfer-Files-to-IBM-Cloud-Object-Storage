# This python code will upload a file from the host computer
# to a bucket in IBM Cloud Object Storage using IBM Aspera File transfer
# For usage details, please read the guide that accompanies this code
# from https://github.com/jamesbeltonIBM/Use-Aspera-to-Transfer-Files-to-IBM-Cloud-Object-Storage
# Code is offered 'as is' and used at your own risk, with no support etc. 


#Import required Libraries

import sys
import ibm_boto3
from ibm_botocore.client import Config
from ibm_s3transfer.aspera.manager import AsperaTransferManager
from ibm_s3transfer.aspera.manager import AsperaConfig


# Set required variables with info about the COS instance
# You must change these values (except COS_AUTH_ENDPOINT) to match your set up

COS_ENDPOINT = "https://<<YOUR COS ENDPOINT>>"
COS_API_KEY_ID = "<<YOUR COS API KEY>>"
COS_AUTH_ENDPOINT = "https://iam.cloud.ibm.com/identity/token"
COS_RESOURCE_CRN = "<<YOUR COS RESOURCE CRN>>"
COS_BUCKET_LOCATION = "<<YOUR COS BUCKET LOCATION"

# Create the client resource

cos = ibm_boto3.client("s3",
    ibm_api_key_id=COS_API_KEY_ID,
    ibm_service_instance_id=COS_RESOURCE_CRN,
    ibm_auth_endpoint=COS_AUTH_ENDPOINT,
    config=Config(signature_version="oauth"),
    endpoint_url=COS_ENDPOINT
)

#initiate the client

transfer_manager = AsperaTransferManager(cos)

# use multiple sessions/threads for the upload

ms_transfer_config = AsperaConfig(multi_session="all",
                                  target_rate_mbps=2500,
                                  multi_session_threshold_mb=100)

# File upload information

bucket_name = "<<NAME OF YOUR BUCKET>>"
upload_filename =  sys.argv[1] 
object_name =  sys.argv[2] 


print ("Uploading file to " + bucket_name)

# Create Transfer manager
with AsperaTransferManager(cos) as transfer_manager:

    # Perform upload
    future = transfer_manager.upload(upload_filename, bucket_name, object_name)

    # Wait for upload to complete
    future.result()

print ("Upload Complete!")
