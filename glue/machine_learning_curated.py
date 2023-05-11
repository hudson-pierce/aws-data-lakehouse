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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-project/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1683824067986 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-project/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1683824067986",
)

# Script generated for node Timestamp Join
TimestampJoin_node1683824163916 = Join.apply(
    frame1=StepTrainerTrusted_node1,
    frame2=AccelerometerTrusted_node1683824067986,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="TimestampJoin_node1683824163916",
)

# Script generated for node Drop Duplicate Timestamp
DropDuplicateTimestamp_node1683824290286 = DropFields.apply(
    frame=TimestampJoin_node1683824163916,
    paths=["timeStamp"],
    transformation_ctx="DropDuplicateTimestamp_node1683824290286",
)

# Script generated for node ML Curated
MLCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicateTimestamp_node1683824290286,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse-project/ml/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MLCurated_node3",
)

job.commit()
