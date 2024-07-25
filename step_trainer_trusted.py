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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1721882712612 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://loinlt1/step_trainer/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1721882712612")

# Script generated for node Customers Curated
CustomersCurated_node1721882714630 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://loinlt1/customers_curated/"], "recurse": True}, transformation_ctx="CustomersCurated_node1721882714630")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1721884556733 = ApplyMapping.apply(frame=StepTrainerLanding_node1721882712612, mappings=[("sensorreadingtime", "long", "right_sensorreadingtime", "long"), ("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "int", "right_distancefromobject", "int")], transformation_ctx="RenamedkeysforJoin_node1721884556733")

# Script generated for node Join
Join_node1721884274902 = Join.apply(frame1=CustomersCurated_node1721882714630, frame2=RenamedkeysforJoin_node1721884556733, keys1=["serialnumber"], keys2=["right_serialnumber"], transformation_ctx="Join_node1721884274902")

# Script generated for node Drop Fields
DropFields_node1721884329547 = DropFields.apply(frame=Join_node1721884274902, paths=["registrationdate", "customername", "birthday", "sharewithfriendsasofdate", "sharewithpublicasofdate", "lastupdatedate", "email", "serialnumber", "phone", "sharewithresearchasofdate"], transformation_ctx="DropFields_node1721884329547")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1721884614864 = glueContext.getSink(path="s3://loinlt1/step_trainer_trusted/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1721884614864")
StepTrainerTrusted_node1721884614864.setCatalogInfo(catalogDatabase="database-1",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1721884614864.setFormat("json")
StepTrainerTrusted_node1721884614864.writeFrame(DropFields_node1721884329547)
job.commit()