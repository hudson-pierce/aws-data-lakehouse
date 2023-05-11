import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Curated
CustomerCurated_node1683755081136 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-project/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1683755081136",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-project/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1683823658145 = DynamicFrame.fromDF(
    CustomerCurated_node1683755081136.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1683823658145",
)

# Script generated for node Rename Key
RenameKey_node1683823345262 = RenameField.apply(
    frame=DropDuplicates_node1683823658145,
    old_name="serialNumber",
    new_name="customerSerialNumber",
    transformation_ctx="RenameKey_node1683823345262",
)

# Script generated for node Privacy Filter
PrivacyFilter_node2 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=RenameKey_node1683823345262,
    keys1=["serialNumber"],
    keys2=["customerSerialNumber"],
    transformation_ctx="PrivacyFilter_node2",
)

# Script generated for node Drop Fields
DropFields_node1683820501073 = DropFields.apply(
    frame=PrivacyFilter_node2,
    paths=[
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
        "customerSerialNumber",
    ],
    transformation_ctx="DropFields_node1683820501073",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1683820501073,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse-project/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node3",
)

job.commit()
