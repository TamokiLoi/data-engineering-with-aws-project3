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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1721888279095 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://loinlt1/accelerometer_trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1721888279095")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1721888261254 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://loinlt1/step_trainer_trusted/"], "recurse": True}, transformation_ctx="StepTrainerTrusted_node1721888261254")

# Script generated for node Join
Join_node1721888308675 = Join.apply(frame1=StepTrainerTrusted_node1721888261254, frame2=AccelerometerTrusted_node1721888279095, keys1=["right_sensorreadingtime"], keys2=["timestamp"], transformation_ctx="Join_node1721888308675")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1721891641672 = glueContext.getSink(path="s3://loinlt1/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1721891641672")
MachineLearningCurated_node1721891641672.setCatalogInfo(catalogDatabase="database-1",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1721891641672.setFormat("json")
MachineLearningCurated_node1721891641672.writeFrame(Join_node1721888308675)
job.commit()