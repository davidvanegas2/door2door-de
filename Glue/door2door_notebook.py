import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
import sys
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'day'])
import boto3

s3 = boto3.client('s3')
bucket_name = 'door2door-de'
prefix = 'data/' + args['day']
objects = []

# Paginate over the objects in the S3 bucket
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
    if 'Contents' in page:
        for obj in page['Contents']:
            if obj['Key'] != prefix:
                # Append the object's key to the list of objects
                objects.append('s3://' + bucket_name + '/' + obj['Key'])
dyf = glueContext.create_dynamic_frame.from_options(
    's3',
    {'paths': objects},
    format='json'
)
dyf.printSchema()
# dyf = glueContext.create_dynamic_frame.from_catalog(database='door2door', table_name='data')
# dyf.printSchema()
# Filter the input DynamicFrame based on the "on" column
vehicle_dyf = dyf.filter(lambda x: x["on"] == "vehicle")
period_dyf = dyf.filter(lambda x: x["on"] == "operating_period")
# Define the mapping to extract fields from the "data" struct
mapping_vehicle = [
    ("data.id", "string", "id", "string"),
    ("data.location.lat", "double", "lat", "double"),
    ("data.location.lng", "double", "lng", "double"),
    ("data.location.at", "string", "location_at", "timestamp"),
    ("event", "string", "event", "string"),
]

# Use the ApplyMapping transform to extract fields and create a new DynamicFrame
dyf_vehicle = ApplyMapping.apply(
    frame=vehicle_dyf,
    mappings=mapping_vehicle
)
dyf_vehicle.toDF().show()
# Define the mapping to extract fields from the "data" struct
mapping_period = [
    ("data.id", "string", "id", "string"),
    ("data.start", "string", "start", "timestamp"),
    ("data.finish", "string", "finish", "timestamp"),
    ("event", "string", "event", "string"),
]

# Use the ApplyMapping transform to extract fields and create a new DynamicFrame
dyf_period = ApplyMapping.apply(
    frame=period_dyf,
    mappings=mapping_period
)
dyf_period.toDF().show()
from pyspark.sql.functions import monotonically_increasing_id


mapping = [
    ("on", "string", "on", "string"),
    ("at", "string", "date", "timestamp"),
    ("data.id", "string", "data_id", "string"),
    ("organization_id", "string", "organization_id", "string"),
]

# Use the ApplyMapping transform to add a new column for the "id" field
new_dyf = ApplyMapping.apply(
    frame=dyf,
    mappings=mapping
)

new_dyf = new_dyf.withColumn("id", monotonically_increasing_id())
new_dyf.toDF().show()
glueContext.write_dynamic_frame.from_catalog(frame=new_dyf, database="door2door", table_name="fact", transformation_ctx="datasink")
glueContext.write_dynamic_frame.from_catalog(frame=dyf_vehicle, database="door2door", table_name="vehicle", transformation_ctx="datasink")
glueContext.write_dynamic_frame.from_catalog(frame=dyf_period, database="door2door", table_name="period", transformation_ctx="datasink")
job.commit()