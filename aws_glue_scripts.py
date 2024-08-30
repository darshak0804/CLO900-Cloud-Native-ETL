import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
 
# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
 
# Parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
 
# Job initialization
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
 
# Input and output paths
input_path = "s3://etl-clo900-s3bucket/GoogleDrivedata/"
output_path = "s3://transform-etl-clo900-s3bucket/Transformed_Google_Drive_Data/"
 
# Glue DynamicFrame from S3 data
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="png",
    connection_options={"paths": [input_path]},
)
 
# Convert DynamicFrame to DataFrame for processing
data_frame = dynamic_frame.toDF()
 
# Perform transformations
transformed_df = data_frame.select("col1", "col2").filter(data_frame["col1"] == "value")
 
# Convert DataFrame back to DynamicFrame
transformed_dynamic_frame = DynamicFrame.fromDF(transformed_df, glueContext, "transformed_df")
 
# Write transformed data back to S3
glueContext.write_dynamic_frame.from_options(
    frame=transformed_dynamic_frame,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet"
)
 
# Commit the job
job.commit()