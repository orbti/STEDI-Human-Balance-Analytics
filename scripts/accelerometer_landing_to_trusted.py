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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1680637159901 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1680637159901",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1680637158735 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AWSGlueDataCatalog_node1680637158735",
)

# Script generated for node Join Customer and Accelerometer
JoinCustomerandAccelerometer_node2 = Join.apply(
    frame1=AWSGlueDataCatalog_node1680637158735,
    frame2=AWSGlueDataCatalog_node1680637159901,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomerandAccelerometer_node2",
)

# Script generated for node Drop Fields
DropFields_node1677864054972 = DropFields.apply(
    frame=JoinCustomerandAccelerometer_node2,
    paths=[
        "email",
        "phone",
        "customername",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1677864054972",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node3 = glueContext.getSink(
    path="s3://dapostoli-stedi-lakehouse/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node3",
)
AccelerometerTrusted_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node3.setFormat("json")
AccelerometerTrusted_node3.writeFrame(DropFields_node1677864054972)
job.commit()
