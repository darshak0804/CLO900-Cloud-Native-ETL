import boto3

# Initialize Glue client
glue = boto3.client('glue')

# Create a Glue job
response = glue.create_job(
    Name='glueetl',
    Role='GlueServiceRole',  # Replace with your Glue service role ARN
    Command={
        'Name': 'glueetl',
        'ScriptLocation': 's3://aws-glue-assets-211125509507-us-east-1/scripts.py'
    },
    DefaultArguments={
        '--job-language': 'python',
        '--your-custom-argument': 'value'
    },
    ExecutionProperty={
        'MaxConcurrentRuns': 1
    },
    Description='Your Glue job description',
    Timeout=60,  # Timeout in minutes
    MaxCapacity=2.0  # Max capacity for the job
)

print(f"Created Glue job: {response['Name']}")
