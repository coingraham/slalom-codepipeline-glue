####################################################################################
# File Name: wkf_nv_mstr_material_ods_stgc_material_uom.py                         #
# Author: Slalom LLC, Kendra Billings                                              #
# Version: 1.0                                                                     #
# Create Date: 02/19/2019                                                          #
# Update Date: 05/01/2019   by Roald Gomes                                         #
#                                                                                  #
# Description: The purpose of this process is to consume the raw extract file from #
#              the S3 landing zone and perform the ETL process to load the records #
#              into the parquet file: stgc_material_uom.  					       #
####################################################################################
# Import Python libraries
import sys

# Import PPM Function Library
from wrk_ppm_general import *

################################################################################
#  Defined Function area.                                                      #
################################################################################


################################################################################
# This area will have customized values specific for each ETL process.         #
################################################################################
dataLake_s3Bucket = ''
ingest_s3Bucket = ''
system_s3Bucket = ''
app_name = 'Python Spark Session - NV MATERIAL_UOM - Staging'
env = 'Staging'
# Define the UDF
filename1 = 'NV_MSTR_MATERIAL.TXT'
landingfolder1 = 'Landing/Received/NV/MSTR_MATERIAL'
filename1_count = 0
filename_list = filename1
filename_count = ' '
transformedfolder = 'Transformed/'
now = datetime.datetime.now()
stagingfile1 = 'stgc_material_uom'
startTime = datetime.datetime.now()
sysID = 'NV'
sysName = 'FMS'
systemMessage = ''
timeStamp = now.isoformat()
workFlowName = 'wkf_nv_mstr_material_ods_stgc_material_uom'

print("START: Begin ETL process... \n")
print("START: Starting a spark session \n")

spark = SparkSession \
	.builder \
	.appName(app_name) \
	.enableHiveSupport() \
	.config('hive.metastore.client.factory.class',
			'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory') \
	.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

if __name__ == '__main__':
	###############################################################################################
	# Check to ensure that the bucket name parameters are passes into the program.                #
	###############################################################################################
	if len(sys.argv) != 4:
		print("ERROR: Missing parameters for S3 bucket names and directories \n", file=sys.stderr)
		exit(-1)
	else:
		dataLake_s3Bucket = sys.argv[1]  # wrk-datalake-ppm-dev
		ingest_s3Bucket = sys.argv[2]  # wrk-ingest-ppm-dev
		system_s3Bucket = sys.argv[3]  # wrk-system-ppm-dev
	
	try:
		# ------------------------------------------------------------------------------------------
		# Start custom development.                                                                -
		# ------------------------------------------------------------------------------------------
		############################################################################################
		# Step 1. Read the 1st input file and format only the required fields and perform ltrim,   #
		#         rtrim, uppercase and convert empty strings to NULL on the individual fields.     #
		############################################################################################
		print("START: Read the input file(s) from the S3 landing/received bucket \n")
		inputFile1 = 's3://' + ingest_s3Bucket + '/' + landingfolder1 + '/' + filename1
		inputFile1DF = spark.read.text(inputFile1)
		input1DF = inputFile1DF.select(
			inputFile1DF.value.substr(1, 21).alias('no'),
			inputFile1DF.value.substr(2252, 13).alias('grammatur'),
			inputFile1DF.value.substr(2724, 10).alias('company_code')
		)
		
		# Informatica transformations
		input1DF = input1DF.transform(trim_and_upper).transform(convert_empty_to_null)
		
		# Get number of records
		filename1_count = input1DF.count()
		print("SUCCESS: Created the dataframe for the 1st input file \n")
		
		############################################################################################
		# Step 2. Apply expression transformations to input1DF.           						   #
		############################################################################################
		exp1aDF = input1DF.withColumn('v_MATERIAL_ID', func.concat(func.col('no'),
																   func.lit('-'),
																   func.col('company_code')))
		exp1aDF = exp1aDF.withColumn('MATERIAL_ID1', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-R')))
		exp1aDF = exp1aDF.withColumn('MATERIAL_ID2', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-S')))
		exp1aDF = exp1aDF.withColumn('SYSTEM_ID', func.lit(sysID))
		exp1aDF = exp1aDF.withColumn('LOCAL_SCE_UOM_ID', func.lit('M2'))
		exp1aDF = exp1aDF.withColumn('LOCAL_DEST_UOM_ID', func.lit('KG'))
		exp1aDF = exp1aDF.withColumn('EXCHANGE_RATE', (func.col('grammatur') / 1000).cast(DecimalType(30, 15)))
		exp1aDF = exp1aDF.withColumn('SOURCE_TYPE', func.lit(sysName))
		print("SUCCESS: Applied expression transformations to input1DF dataframe \n")
		
		############################################################################################
		# Step 3. Apply normalizer transformations to exp1aDF.           						   #
		############################################################################################
		exp1aDF.createOrReplaceTempView('temp_normalize_tbl')
		norm1aDF = spark.sql("""
								SELECT	MATERIAL_ID1 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_SCE_UOM_ID,
										LOCAL_DEST_UOM_ID,
										EXCHANGE_RATE,
										SOURCE_TYPE
								FROM temp_normalize_tbl
								union all
								SELECT	MATERIAL_ID2 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_SCE_UOM_ID,
										LOCAL_DEST_UOM_ID,
										EXCHANGE_RATE,
										SOURCE_TYPE
								FROM temp_normalize_tbl""")
		print("SUCCESS: Applied normalizer transformations to exp1aDF dataframe \n")
		
		############################################################################################
		# Step 4. Create an ETL dataframe from norm1aDF and append new columns.  				   #
		############################################################################################
		etl1DF = norm1aDF.withColumn('material_id', func.col('MATERIAL_ID').cast(StringType()))
		etl1DF = etl1DF.withColumn('system_id', func.col('SYSTEM_ID').cast(StringType()))
		etl1DF = etl1DF.withColumn('local_sce_uom_id', func.col('LOCAL_SCE_UOM_ID').cast(StringType()))
		etl1DF = etl1DF.withColumn('sce_uom_id', func.lit(None).cast(StringType()))
		etl1DF = etl1DF.withColumn('local_dest_uom_id', func.col('LOCAL_DEST_UOM_ID').cast(StringType()))
		etl1DF = etl1DF.withColumn('dest_uom_id', func.lit(None).cast(StringType()))
		etl1DF = etl1DF.withColumn('exchange_rate', func.col('EXCHANGE_RATE').cast(DecimalType(30, 15)))
		etl1DF = etl1DF.withColumn('source_type', func.col('SOURCE_TYPE').cast(StringType()))
		etl1DF = etl1DF.withColumn('datestamp', func.lit(format_current_dt()).cast(TimestampType()))
		etl1DF = etl1DF.withColumn('processing_flag', func.lit(None).cast(StringType()))
		print("SUCCESS: Processed norm1aDF and added columns to match final table layout \n")
		etl1DF.show(20)
		
		############################################################################################
		# Step 4. Write the staging dataframe to the staging parquet file				           #
		############################################################################################
		etl1DF.select('material_id', 'system_id', 'local_sce_uom_id', 'sce_uom_id', 'local_dest_uom_id', 'dest_uom_id',
					  'exchange_rate', 'source_type', 'datestamp', 'processing_flag') \
			.write.parquet('s3://' + dataLake_s3Bucket + '/' + transformedfolder + stagingfile1 + '/',
						   mode='append',
						   compression='snappy')
		print("SUCCESS: Wrote to the staging parquet file \n")
		
		############################################################################################
		# Step 5. Write out the log details.                                                       #
		############################################################################################
		filename_count = str(filename1_count)
		write_log_file(workFlowName, sysID, filename_list, filename_count, env, startTime, '', 'Success',
					   system_s3Bucket,
					   spark)
		print("SUCCESS: Write log dataframe to parquet file\n")
		print("SUCCESS: End of job \n")
		system_cleanup(spark)
	
	except Exception as e:
		filename_count = str(filename1_count)
		write_log_file(workFlowName, sysID, filename_list, filename_count, env, startTime, e, 'Error', system_s3Bucket,
					   spark)
		print("ERROR: Workflow processing failed. See message below for details \n")
		print(e)
		system_cleanup(spark)

############################################################################################
# End of script                                                                            #
############################################################################################
