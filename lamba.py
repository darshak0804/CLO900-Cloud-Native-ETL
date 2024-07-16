import boto3

glue = boto3.client('glue')

def lambda_handler(event, context):
    # Trigger AWS Glue Job
    response = glue.start_job_run(JobName='your-glue-job-name')
    job_run_id = response['JobRunId']
    
    print(f"Started AWS Glue Job '{job_run_id}'")
    
    # Optionally, you can monitor job status or handle other tasks here
    # based on the response from Glue.
