####################################################################################
# File Name: wkf_nv_mstr_material_ods_stgm_material.py                             #
# Author: Slalom LLC, Kendra Billings                                              #
# Version: 1.0                                                                     #
# Create Date: 02/19/2019                                                          #
# Update Date: 05/31/2019   by Roald Gomes                                         #
#                                                                                  #
# Description: The purpose of this process is to consume the raw extract file from #
#              the S3 landing zone and perform the ETL process to load the records #
#              into the parquet file: stgm_material and stge_material.             #
####################################################################################
# Import Python libraries
import sys

# Import PPM Function Library
from wrk_ppm_general import *


################################################################################
#  Defined Function area.                                                      #
################################################################################
def isdate(date_value, date_separator):
	"""
	Description: This function will check of the value passed in dateCode is a valid date.
				  If it is a valid date, the date value will be returned to the calling routine.
				  If it is not a valid date, a NULL value will be returned.
	"""
	# default value for century
	century = 20
	
	try:
		# Calculate current date and its components
		date_current = datetime.datetime.today().date()
		
		# Set date_value to empty string if NULL
		date_value = date_value or ''
		# date_value components
		date_value_day, date_value_month, date_value_yy = date_value.split(date_separator)
		date_value_yyyy = str(century) + date_value_yy + '-' + date_value_month + '-' + date_value_day
		date_value_calculated = datetime.datetime.strptime(date_value_yyyy, '%Y-%m-%d').date()
		
		if date_value_calculated > date_current:
			century = 19
			date_value_yyyy = str(century) + date_value_yy + '-' + date_value_month + '-' + date_value_day
			date_value_calculated = datetime.datetime.strptime(date_value_yyyy, '%Y-%m-%d').date()
		
		date_formatted = datetime.datetime.strptime(str(date_value_calculated), '%Y-%m-%d')
		v_return = str(date_formatted)
	
	except TypeError:
		v_return = None
	except ValueError:
		v_return = None
	
	return v_return


################################################################################
# This area will have customized values specific for each ETL process.         #
################################################################################
dataLake_s3Bucket = ''
ingest_s3Bucket = ''
system_s3Bucket = ''
app_name = 'Python Spark Session - NV MSTR_MATERIAL - Staging'
env = 'Staging'
# Define the UDF
isdate_udf = func.udf(isdate)
filename1 = 'NV_MSTR_MATERIAL.TXT'
landingfolder1 = 'Landing/Received/NV/MSTR_MATERIAL'
filename1_count = 0
filename_list = filename1
filename_count = ' '
transformedfolder = 'Transformed/'
now = datetime.datetime.now()
stagingfile1 = 'stge_material'
stagingfile2 = 'stgm_material'
startTime = datetime.datetime.now()
sysID = 'NV'
sysName = 'FMS'
systemMessage = ''
timeStamp = now.isoformat()
workFlowName = 'wkf_nv_mstr_material_ods_stgm_material'

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
			inputFile1DF.value.substr(43, 31).alias('description'),
			inputFile1DF.value.substr(158, 11).alias('base_unit_of_measure'),
			inputFile1DF.value.substr(298, 13).alias('standard_cost'),
			inputFile1DF.value.substr(2199, 11).alias('micron'),
			inputFile1DF.value.substr(2210, 31).alias('service_item_group_code'),
			inputFile1DF.value.substr(2265, 12).alias('created_on'),
			inputFile1DF.value.substr(2299, 13).alias('grammatur'),
			inputFile1DF.value.substr(2312, 21).alias('quality_code'),
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
																 func.lit('-R')))
		exp1aDF = exp1aDF.withColumn('MATERIAL_ID3', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-S')))
		exp1aDF = exp1aDF.withColumn('MATERIAL_ID4', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-S')))
		exp1aDF = exp1aDF.withColumn('MATERIAL_ID5', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-R')))
		exp1aDF = exp1aDF.withColumn('MATERIAL_ID6', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-R')))
		exp1aDF = exp1aDF.withColumn('MATERIAL_ID7', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-S')))
		exp1aDF = exp1aDF.withColumn('MATERIAL_ID8', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-S')))
		exp1aDF = exp1aDF.withColumn('SYSTEM_ID', func.lit(sysID))
		exp1aDF = exp1aDF.withColumn('LOCAL_EXT_ATTRIBUTE_TYPE1', func.lit('CALIPER'))
		exp1aDF = exp1aDF.withColumn('LOCAL_EXT_ATTRIBUTE_TYPE2', func.lit('BASIS WEIGHT'))
		exp1aDF = exp1aDF.withColumn('LOCAL_EXT_ATTRIBUTE_TYPE3', func.lit('CALIPER'))
		exp1aDF = exp1aDF.withColumn('LOCAL_EXT_ATTRIBUTE_TYPE4', func.lit('BASIS WEIGHT'))
		exp1aDF = exp1aDF.withColumn('LOCAL_EXT_ATTRIBUTE_TYPE5', func.lit('GRADE'))
		exp1aDF = exp1aDF.withColumn('LOCAL_EXT_ATTRIBUTE_TYPE6', func.lit('FORM TYPE'))
		exp1aDF = exp1aDF.withColumn('LOCAL_EXT_ATTRIBUTE_TYPE7', func.lit('GRADE'))
		exp1aDF = exp1aDF.withColumn('LOCAL_EXT_ATTRIBUTE_TYPE8', func.lit('FORM TYPE'))
		exp1aDF = exp1aDF.withColumn('EXT_ATTRIBUTE_VALUE1', func.col('micron'))
		exp1aDF = exp1aDF.withColumn('EXT_ATTRIBUTE_VALUE2', func.col('grammatur'))
		exp1aDF = exp1aDF.withColumn('EXT_ATTRIBUTE_VALUE3', func.col('micron'))
		exp1aDF = exp1aDF.withColumn('EXT_ATTRIBUTE_VALUE4', func.col('grammatur'))
		exp1aDF = exp1aDF.withColumn('EXT_ATTRIBUTE_VALUE5', func.col('No'))
		exp1aDF = exp1aDF.withColumn('EXT_ATTRIBUTE_VALUE6', func.lit('ROLL'))
		exp1aDF = exp1aDF.withColumn('EXT_ATTRIBUTE_VALUE7', func.col('No'))
		exp1aDF = exp1aDF.withColumn('EXT_ATTRIBUTE_VALUE8', func.lit('SHEET'))
		exp1aDF = exp1aDF.withColumn('LOCAL_UOM_ID1', func.lit('MICROMETER'))
		exp1aDF = exp1aDF.withColumn('LOCAL_UOM_ID2', func.lit('GMS PER SQM'))
		exp1aDF = exp1aDF.withColumn('LOCAL_UOM_ID3', func.lit('MICROMETER'))
		exp1aDF = exp1aDF.withColumn('LOCAL_UOM_ID4', func.lit('GMS PER SQM'))
		exp1aDF = exp1aDF.withColumn('LOCAL_UOM_ID5', func.lit(None))
		exp1aDF = exp1aDF.withColumn('LOCAL_UOM_ID6', func.lit(None))
		exp1aDF = exp1aDF.withColumn('LOCAL_UOM_ID7', func.lit(None))
		exp1aDF = exp1aDF.withColumn('LOCAL_UOM_ID8', func.lit(None))
		exp1aDF = exp1aDF.withColumn('SOURCE_TYPE', func.lit(sysName))
		print("SUCCESS: Applied expression transformations to input1DF dataframe \n")
		
		############################################################################################
		# Step 3. Apply normalizer transformations to exp1aDF.           						   #
		############################################################################################
		exp1aDF.createOrReplaceTempView('temp_normalize_tbl')
		norm1aDF = spark.sql("""
								SELECT	MATERIAL_ID1 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_EXT_ATTRIBUTE_TYPE1 as LOCAL_EXT_ATTRIBUTE_TYPE,
										EXT_ATTRIBUTE_VALUE1 as EXT_ATTRIBUTE_VALUE,
										LOCAL_UOM_ID1 as LOCAL_UOM_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl
								union all
								SELECT	MATERIAL_ID2 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_EXT_ATTRIBUTE_TYPE2 as LOCAL_EXT_ATTRIBUTE_TYPE,
										EXT_ATTRIBUTE_VALUE2 as EXT_ATTRIBUTE_VALUE,
										LOCAL_UOM_ID2 as LOCAL_UOM_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl
								union all
								SELECT	MATERIAL_ID3 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_EXT_ATTRIBUTE_TYPE3 as LOCAL_EXT_ATTRIBUTE_TYPE,
										EXT_ATTRIBUTE_VALUE3 as EXT_ATTRIBUTE_VALUE,
										LOCAL_UOM_ID3 as LOCAL_UOM_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl
								union all
								SELECT	MATERIAL_ID4 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_EXT_ATTRIBUTE_TYPE4 as LOCAL_EXT_ATTRIBUTE_TYPE,
										EXT_ATTRIBUTE_VALUE4 as EXT_ATTRIBUTE_VALUE,
										LOCAL_UOM_ID4 as LOCAL_UOM_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl
								union all
								SELECT	MATERIAL_ID5 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_EXT_ATTRIBUTE_TYPE5 as LOCAL_EXT_ATTRIBUTE_TYPE,
										EXT_ATTRIBUTE_VALUE5 as EXT_ATTRIBUTE_VALUE,
										LOCAL_UOM_ID5 as LOCAL_UOM_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl
								union all
								SELECT	MATERIAL_ID6 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_EXT_ATTRIBUTE_TYPE6 as LOCAL_EXT_ATTRIBUTE_TYPE,
										EXT_ATTRIBUTE_VALUE6 as EXT_ATTRIBUTE_VALUE,
										LOCAL_UOM_ID6 as LOCAL_UOM_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl
								union all
								SELECT	MATERIAL_ID7 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_EXT_ATTRIBUTE_TYPE7 as LOCAL_EXT_ATTRIBUTE_TYPE,
										EXT_ATTRIBUTE_VALUE7 as EXT_ATTRIBUTE_VALUE,
										LOCAL_UOM_ID7 as LOCAL_UOM_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl
								union all
								SELECT	MATERIAL_ID8 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_EXT_ATTRIBUTE_TYPE8 as LOCAL_EXT_ATTRIBUTE_TYPE,
										EXT_ATTRIBUTE_VALUE8 as EXT_ATTRIBUTE_VALUE,
										LOCAL_UOM_ID8 as LOCAL_UOM_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl""")
		print("SUCCESS: Applied normalizer transformations to exp1aDF dataframe \n")
		
		############################################################################################
		# Step 4. Create an ETL dataframe from norm1aDF and append new columns.  				   #
		############################################################################################
		etl1DF = norm1aDF.withColumn('material_id', func.col('MATERIAL_ID').cast(StringType())) \
			.withColumn('system_id', func.col('SYSTEM_ID').cast(StringType())) \
			.withColumn('local_ext_attribute_type', func.col('LOCAL_EXT_ATTRIBUTE_TYPE').cast(StringType())) \
			.withColumn('ext_attribute_type', func.lit(None).cast(StringType())) \
			.withColumn('ext_attribute_value', func.col('EXT_ATTRIBUTE_VALUE').cast(StringType())) \
			.withColumn('local_uom_id', func.col('LOCAL_UOM_ID').cast(StringType())) \
			.withColumn('uom_id', func.lit(None).cast(StringType())) \
			.withColumn('local_currency_id', func.lit(None).cast(StringType())) \
			.withColumn('currency_id', func.lit(None).cast(StringType())) \
			.withColumn('source_type', func.col('SOURCE_TYPE').cast(StringType())) \
			.withColumn('datestamp', func.lit(format_current_dt()).cast(TimestampType())) \
			.withColumn('processing_flag', func.lit(None).cast(StringType()))
		print("SUCCESS: Processed norm1aDF and added columns to match final table layout \n")
		etl1DF.show(20)
		
		############################################################################################
		# Step 4. Write the staging dataframe to the staging parquet file				           #
		############################################################################################
		etl1DF.select('material_id', 'system_id', 'local_ext_attribute_type', 'ext_attribute_type',
					  'ext_attribute_value', 'local_uom_id', 'uom_id', 'local_currency_id', 'currency_id',
					  'source_type', 'datestamp', 'processing_flag') \
			.write.parquet('s3://' + dataLake_s3Bucket + '/' + transformedfolder + stagingfile1 + '/',
						   mode='append',
						   compression='snappy')
		print("SUCCESS: Wrote to the staging parquet file \n")
		
		############################################################################################
		# Step 6. Apply expression transformations to input1DF.           						   #
		############################################################################################
		exp2aDF = input1DF.withColumn('v_MATERIAL_ID', func.concat(func.col('No'),
																   func.lit('-'),
																   func.col('company_code')))
		exp2aDF = exp2aDF.withColumn('MATERIAL_ID1', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-R')))
		exp2aDF = exp2aDF.withColumn('MATERIAL_ID2', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-S')))
		exp2aDF = exp2aDF.withColumn('SYSTEM_ID', func.lit(sysID))
		exp2aDF = exp2aDF.withColumn('MATERIAL_DESC', func.col('description'))
		exp2aDF = exp2aDF.withColumn('LOCAL_BASE_UOM_ID', func
									 .when(func.coalesce(func.col('base_unit_of_measure'), func.lit('')) == '',
										   func.col('v_MATERIAL_ID'))
									 .otherwise(func.col('base_unit_of_measure')))
		exp2aDF = exp2aDF.withColumn('FIRST_SHIP_DATE', isdate_udf(func.col('created_on'), func.lit('.'))
									 .cast(TimestampType()))
		exp2aDF = exp2aDF.withColumn('LOCAL_KPG_ID1', func.concat(func.col('No'),
																  func.lit('-R')))
		exp2aDF = exp2aDF.withColumn('LOCAL_KPG_ID2', func.concat(func.col('No'),
																  func.lit('-S')))
		exp2aDF = exp2aDF.withColumn('SOURCE_TYPE', func.lit(sysName))
		print("SUCCESS: Applied expression transformations to input1DF dataframe \n")
		
		############################################################################################
		# Step 7. Apply normalizer transformations to exp2aDF.           						   #
		############################################################################################
		exp2aDF.createOrReplaceTempView('temp_normalize_tbl')
		norm2aDF = spark.sql("""
								SELECT	MATERIAL_ID1 as MATERIAL_ID,
										SYSTEM_ID,
										MATERIAL_DESC,
										LOCAL_BASE_UOM_ID,
										FIRST_SHIP_DATE,
										LOCAL_KPG_ID1 as LOCAL_KPG_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl
								union all
								SELECT	MATERIAL_ID2 as MATERIAL_ID,
										SYSTEM_ID,
										MATERIAL_DESC,
										LOCAL_BASE_UOM_ID,
										FIRST_SHIP_DATE,
										LOCAL_KPG_ID2 as LOCAL_KPG_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl""")
		print("SUCCESS: Applied normalizer transformations to exp2aDF dataframe \n")
		
		############################################################################################
		# Step 8. Create an ETL dataframe from norm2aDF and append new columns.  				   #
		############################################################################################
		etl2DF = norm2aDF.withColumn('material_id', func.col('MATERIAL_ID').cast(StringType())) \
			.withColumn('system_id', func.col('SYSTEM_ID').cast(StringType())) \
			.withColumn('material_desc', func.col('MATERIAL_DESC').cast(StringType())) \
			.withColumn('local_base_uom_id', func.col('LOCAL_BASE_UOM_ID').cast(StringType())) \
			.withColumn('base_uom_id', func.lit(None).cast(StringType())) \
			.withColumn('first_ship_date', func.col('FIRST_SHIP_DATE').cast(TimestampType())) \
			.withColumn('local_kpg_id', func.col('LOCAL_KPG_ID').cast(StringType())) \
			.withColumn('kpg_id', func.lit(None).cast(StringType())) \
			.withColumn('local_pfl_id', func.lit(None).cast(StringType())) \
			.withColumn('pfl_id', func.lit(None).cast(StringType())) \
			.withColumn('local_spl_id', func.lit(None).cast(StringType())) \
			.withColumn('spl_id', func.lit(None).cast(StringType())) \
			.withColumn('source_type', func.col('SOURCE_TYPE').cast(StringType())) \
			.withColumn('datestamp', func.lit(format_current_dt()).cast(TimestampType())) \
			.withColumn('processing_flag', func.lit(None).cast(StringType()))
		print("SUCCESS: Processed norm2aDF and added columns to match final table layout \n")
		etl2DF.show(20)
		
		############################################################################################
		# Step 9. Write the staging dataframe to the staging parquet file 				  		   #
		############################################################################################
		etl2DF.select('material_id', 'system_id', 'material_desc', 'local_base_uom_id', 'base_uom_id',
					  'first_ship_date', 'local_kpg_id', 'kpg_id', 'local_pfl_id', 'pfl_id', 'local_spl_id', 'spl_id',
					  'source_type', 'datestamp', 'processing_flag') \
			.write.parquet('s3://' + dataLake_s3Bucket + '/' + transformedfolder + stagingfile2 + '/',
						   mode='append',
						   compression='snappy')
		print("SUCCESS: Wrote to the staging parquet file \n")
		
		############################################################################################
		# Step 10. Write out the log details.                                                      #
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
