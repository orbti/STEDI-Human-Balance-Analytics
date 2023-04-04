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

# Script generated for node Acceleromter Landing
AcceleromterLanding_node1680619711143 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dapostoli-stedi-lakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AcceleromterLanding_node1680619711143",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=AcceleromterLanding_node1680619711143,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1680619785893 = DropFields.apply(
    frame=ApplyMapping_node2,
    paths=["x", "y", "z", "user", "timeStamp"],
    transformation_ctx="DropFields_node1680619785893",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1680621987000 = DynamicFrame.fromDF(
    DropFields_node1680619785893.toDF().dropDuplicates(
        ["serialNumber", "birthDay", "customerName", "email", "phone"]
    ),
    glueContext,
    "DropDuplicates_node1680621987000",
)

# Script generated for node Customers Curated
CustomersCurated_node3 = glueContext.getSink(
    path="s3://dapostoli-stedi-lakehouse/customers/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomersCurated_node3",
)
CustomersCurated_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customers_curated"
)
CustomersCurated_node3.setFormat("json")
CustomersCurated_node3.writeFrame(DropDuplicates_node1680621987000)
job.commit()
