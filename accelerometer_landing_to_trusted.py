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

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dapostoli-stedi-lakehouse/customers/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1677861744822 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dapostoli-stedi-lakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1677861744822",
)

# Script generated for node Join Customer and Accelerometer
JoinCustomerandAccelerometer_node2 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=AccelerometerLanding_node1677861744822,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinCustomerandAccelerometer_node2",
)

# Script generated for node Drop Fields
DropFields_node1677864054972 = DropFields.apply(
    frame=JoinCustomerandAccelerometer_node2,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1677864054972",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1677864054972,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dapostoli-stedi-lakehouse/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node3",
)

job.commit()
