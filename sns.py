import boto3
 
sns = boto3.client('sns')
 
# Create SNS Topic
response = sns.create_topic(Name='YourSNSTopicName')
topic_arn = response['TopicArn']
 
print(f"Created SNS topic: {topic_arn}")
 
# Subscribe to Topic
response = sns.subscribe(
    TopicArn=topic_arn,
    Protocol='email',
    Endpoint='skytechsquad.caa900@gmail.com'
)
 
print(f"Subscribed {response['SubscriptionArn']} to topic {topic_arn}")