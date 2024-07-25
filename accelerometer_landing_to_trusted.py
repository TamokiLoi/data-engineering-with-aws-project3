import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1721880328858 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://loinlt1/accelerometer/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1721880328858")

# Script generated for node Customer Trusted
CustomerTrusted_node1721880330249 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://loinlt1/customer_trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1721880330249")

# Script generated for node Join
Join_node1721880456720 = Join.apply(frame1=AccelerometerLanding_node1721880328858, frame2=CustomerTrusted_node1721880330249, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1721880456720")

# Script generated for node Select Fields
SelectFields_node1721880487396 = SelectFields.apply(frame=Join_node1721880456720, paths=["user", "timestamp", "x", "y", "z"], transformation_ctx="SelectFields_node1721880487396")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1721880543261 = glueContext.getSink(path="s3://loinlt1/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1721880543261")
AccelerometerTrusted_node1721880543261.setCatalogInfo(catalogDatabase="database-1",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1721880543261.setFormat("json")
AccelerometerTrusted_node1721880543261.writeFrame(SelectFields_node1721880487396)
job.commit()