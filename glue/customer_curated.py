import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1683753467309 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-project/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1683753467309",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-project/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=AccelerometerLanding_node1683753467309,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Fields
DropFields_node1683753610756 = DropFields.apply(
    frame=Join_node2,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1683753610756",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1683753610756,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse-project/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node3",
)

job.commit()
