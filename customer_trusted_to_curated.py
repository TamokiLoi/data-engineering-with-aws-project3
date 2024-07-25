import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1721880959255 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://loinlt1/customer_trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1721880959255")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1721880960586 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://loinlt1/accelerometer_trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1721880960586")

# Script generated for node Join
Join_node1721881422430 = Join.apply(frame1=CustomerTrusted_node1721880959255, frame2=AccelerometerTrusted_node1721880960586, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1721881422430")

# Script generated for node Select Fields
SelectFields_node1721881467860 = SelectFields.apply(frame=Join_node1721881422430, paths=["registrationdate", "customername", "birthday", "sharewithfriendsasofdate", "sharewithpublicasofdate", "lastupdatedate", "email", "serialnumber", "sharewithresearchasofdate", "phone"], transformation_ctx="SelectFields_node1721881467860")

# Script generated for node Drop Duplicates
DropDuplicates_node1721881880024 =  DynamicFrame.fromDF(SelectFields_node1721881467860.toDF().dropDuplicates(["email"]), glueContext, "DropDuplicates_node1721881880024")

# Script generated for node Customer Curated
CustomerCurated_node1721881501833 = glueContext.getSink(path="s3://loinlt1/customers_curated/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1721881501833")
CustomerCurated_node1721881501833.setCatalogInfo(catalogDatabase="database-1",catalogTableName="customers_curated")
CustomerCurated_node1721881501833.setFormat("json")
CustomerCurated_node1721881501833.writeFrame(DropDuplicates_node1721881880024)
job.commit()