import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Landing
CustomerLanding_node1721877027112 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://loinlt1/customer/"], "recurse": True}, transformation_ctx="CustomerLanding_node1721877027112")

# Script generated for node Change Schema
ChangeSchema_node1721878566434 = ApplyMapping.apply(frame=CustomerLanding_node1721877027112, mappings=[("customername", "string", "customername", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("birthday", "string", "birthday", "string"), ("serialnumber", "string", "serialnumber", "string"), ("registrationdate", "bigint", "registrationdate", "long"), ("lastupdatedate", "bigint", "lastupdatedate", "long"), ("sharewithresearchasofdate", "bigint", "sharewithresearchasofdate", "long"), ("sharewithpublicasofdate", "bigint", "sharewithpublicasofdate", "long"), ("sharewithfriendsasofdate", "bigint", "sharewithfriendsasofdate", "long")], transformation_ctx="ChangeSchema_node1721878566434")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT * 
FROM myDataSource 
WHERE sharewithresearchasofdate IS NOT NULL 
AND sharewithresearchasofdate <> 0
'''
SQLQuery_node1721879402029 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":ChangeSchema_node1721878566434}, transformation_ctx = "SQLQuery_node1721879402029")

# Script generated for node Customer Trusted
CustomerTrusted_node1721879522021 = glueContext.getSink(path="s3://loinlt1/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1721879522021")
CustomerTrusted_node1721879522021.setCatalogInfo(catalogDatabase="database-1",catalogTableName="customer_trusted")
CustomerTrusted_node1721879522021.setFormat("json")
CustomerTrusted_node1721879522021.writeFrame(SQLQuery_node1721879402029)
job.commit()