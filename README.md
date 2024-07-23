# Steps of ETL in AWS

1. python3 -m venv venv

2. source venv/bin/activate

3. pip install --upgrade pip

4. pip install -r requirements.txt

5. Create 2 S3 buckets - etl-clo900-s3bucket , transform-etl-clo900-s3bucket

6. Running connection.py = python3 connection.py

7. Drive files are visible in S3 bucket.

8. Setup Glue and Script to transform data and store it into S3 - transform-etl-clo900-s3bucket

9. To monitor this we are using CloudWatch and AWS SNS services.

10. We are working to fixing issue in our transformation script in our ETL pipeline.
