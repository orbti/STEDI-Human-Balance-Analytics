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

# Script generated for node step_trainer_landing
step_trainer_landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dapostoli-stedi-lakehouse/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1680624477241 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customers_curated",
    transformation_ctx="AWSGlueDataCatalog_node1680624477241",
)

# Script generated for node Join
Join_node1680624418851 = Join.apply(
    frame1=step_trainer_landing_node1,
    frame2=AWSGlueDataCatalog_node1680624477241,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1680624418851",
)

# Script generated for node Drop Fields
DropFields_node1680624559087 = DropFields.apply(
    frame=Join_node1680624418851,
    paths=[
        "serialnumber",
        "birthday",
        "sharewithresearchasofdate",
        "registrationdate",
        "customername",
        "sharewithfriendsasofdate",
        "email",
        "lastupdatedate",
        "phone",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1680624559087",
)

# Script generated for node Amazon S3
AmazonS3_node1680624514202 = glueContext.getSink(
    path="s3://dapostoli-stedi-lakehouse/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1680624514202",
)
AmazonS3_node1680624514202.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
AmazonS3_node1680624514202.setFormat("json")
AmazonS3_node1680624514202.writeFrame(DropFields_node1680624559087)
job.commit()
