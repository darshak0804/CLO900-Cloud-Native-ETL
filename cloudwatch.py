import boto3
 
cloudwatch = boto3.client('cloudwatch')
 
# CloudWatch Alarm
response = cloudwatch.put_metric_alarm(
    AlarmName='YourCloudWatchAlarmName',
    ComparisonOperator='GreaterThanThreshold',
    EvaluationPeriods=1,
    MetricName='your-metric-name',
    Namespace='AWS/Glue',
    Period=300,  # 5 minutes
    Threshold=100.0,  # Adjust threshold
    AlarmDescription='Monitoring Transforming Data',
    ActionsEnabled=True,
    AlarmActions=[
        'arn:aws:sns:region:account-id:your-sns-topic'
    ],
)
 
print(f"Created CloudWatch alarm: {response['AlarmName']}")