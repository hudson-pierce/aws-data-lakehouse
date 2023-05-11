spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-project/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1683744658558 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-project/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1683744658558",
)

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node2 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrusted_node1683744658558,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyFilter_node2",
)

# Script generated for node Drop Fields
DropFields_node1683747796220 = DropFields.apply(
    frame=CustomerPrivacyFilter_node2,
    paths=[
        "email",
        "phone",
        "customerName",
        "lastUpdateDate",
        "shareWithFriendsAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "birthDay",
        "shareWithPublicAsOfDate",
        "serialNumber",
    ],
    transformation_ctx="DropFields_node1683747796220",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1683747796220,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse-project/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node3",
)

job.commit()
