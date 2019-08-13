####################################################################################
# File Name: wkf_nv_tran_invoice_header_ods_stgt_invoice_header.py                 #
# Author: Slalom LLC, Roald Gomes                                                  #
# Version: 1.0                                                                     #
# Update Date: 05/13/2019                                                          #
#                                                                                  #
# Description: The purpose of this process is to consume the raw extract file from #
#              the S3 landing zone and perform the ETL process to load the records #
#              into the parquet file: stgt_invoice_header.                         #
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
app_name = 'Python Spark Session - NV INVOICE_HEADER - Staging'
env = 'Staging'
# Define the UDF
isdate_udf = func.udf(isdate)
filename1 = 'NV_TRAN_INVOICE_HEADER.TXT'
landingfolder1 = 'Landing/Received/NV/TRAN_INVOICE_HEADER'
filename1_count = 0
filename_list = filename1
filename_count = ' '
transformedfolder = 'Transformed/'
now = datetime.datetime.now()
stagingfile1 = 'stgt_invoice_header'
startTime = datetime.datetime.now()
sysID = 'NV'
sysName = 'FMS'
systemMessage = ''
timeStamp = now.isoformat()
workFlowName = 'wkf_nv_tran_invoice_header_ods_stgt_invoice_header'

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
		# Remove non-printable characters that dislocates subsequent columns
		inputFile1DF = inputFile1DF.withColumn('value', func.regexp_replace(func.col('value'), '/[^ -~]+/g', ''))
		
		input1DF = inputFile1DF.select(
			inputFile1DF.value.substr(1, 2).alias('invoice_type'),
			inputFile1DF.value.substr(23, 20).alias('no'),
			inputFile1DF.value.substr(43, 20).alias('bill_to_customer_no'),
			inputFile1DF.value.substr(463, 11).alias('posting_date'),
			inputFile1DF.value.substr(649, 10).alias('currency_code'),
			inputFile1DF.value.substr(811, 12).alias('amount'),
			inputFile1DF.value.substr(1774, 10).alias('invoice_type102'),
			inputFile1DF.value.substr(1784, 10).alias('segment_code'),
			inputFile1DF.value.substr(1985, 10).alias('company_code')
		)
		
		# Informatica transformations
		input1DF = input1DF.transform(trim_and_upper).transform(convert_empty_to_null)
		
		# Get number of records
		filename1_count = input1DF.count()
		print("SUCCESS: Created the dataframe for the 1st input file \n")
		
		############################################################################################
		# Step 2. Apply expression transformations to exp1aDF.           					       #
		############################################################################################
		exp1aDF = input1DF.withColumn('LOCAL_INVOICE_ID', func.concat(func.col('no'),
																	  func.lit('-'),
																	  func.col('company_code')))
		exp1aDF = exp1aDF.withColumn('SYSTEM_ID', func.lit(sysID))
		exp1aDF = exp1aDF.withColumn('SOLD_CUSTOMER_ID', func.concat(func.col('bill_to_customer_no'),
																	 func.lit('-'),
																	 func.col('company_code')))
		exp1aDF = exp1aDF.withColumn('LOCAL_INVOICE_TYPE', func
									 .when(func.col('invoice_type') == '',
										   func.lit('IX'))
									 .otherwise(func.col('invoice_type')))
		exp1aDF = exp1aDF.withColumn('LOCAL_BUSINESS_UNIT_ID', func.concat(func.col('segment_code'),
																		   func.lit('-'),
																		   func.col("company_code")))
		exp1aDF = exp1aDF.withColumn('amount', func.regexp_replace(func.col('amount'), ',', ''))
		exp1aDF = exp1aDF.withColumn('INVOICE_VALUE', func
									 .when(func.col('invoice_type') == func.lit('MX'),
										   func.col('amount') * -1)
									 .otherwise(func.col('amount')).cast(DecimalType(20, 2)))
		exp1aDF = exp1aDF.withColumn('LOCAL_CURRENCY_ID', func
									 .when(func.col('currency_code') == '',
										   func.lit('EUR'))
									 .otherwise(func.col('currency_code')))
		exp1aDF = exp1aDF.withColumn('INVOICE_DATE', isdate_udf(func.col('posting_date'), func.lit('-')))
		exp1aDF = exp1aDF.withColumn('POST_DATE', isdate_udf(func.col('posting_date'), func.lit('-')))
		exp1aDF = exp1aDF.withColumn('SOURCE_TYPE', func.lit(sysName))
		print("SUCCESS: Applied expression transformations to exp1aDF dataframe \n")
		
		############################################################################################
		# Step 3. Create an ETL dataframe from the exp1aDF dataframe and append new columns.       #
		############################################################################################
		etl2DF = exp1aDF.withColumn('local_invoice_id', func.col('LOCAL_INVOICE_ID').cast(StringType())) \
			.withColumn('system_id', func.col('SYSTEM_ID').cast(StringType())) \
			.withColumn('sold_customer_id', func.col('SOLD_CUSTOMER_ID').cast(StringType())) \
			.withColumn('local_invoice_type', func.col('LOCAL_INVOICE_TYPE').cast(StringType())) \
			.withColumn('invoice_type', func.lit(None).cast(StringType())) \
			.withColumn('local_business_unit_id', func.col('LOCAL_BUSINESS_UNIT_ID').cast(StringType())) \
			.withColumn('business_unit_id', func.lit(None).cast(StringType())) \
			.withColumn('invoice_value', func.col('INVOICE_VALUE').cast(DecimalType(20, 4))) \
			.withColumn('local_currency_id', func.col('LOCAL_CURRENCY_ID').cast(StringType())) \
			.withColumn('currency_id', func.lit(None).cast(StringType())) \
			.withColumn('invoice_date', func.col('INVOICE_DATE').cast(TimestampType())) \
			.withColumn('post_date', func.col('POST_DATE').cast(TimestampType())) \
			.withColumn('source_type', func.col('SOURCE_TYPE').cast(StringType())) \
			.withColumn('datestamp', func.lit(format_current_dt()).cast(TimestampType())) \
			.withColumn('processing_flag', func.lit(None).cast(StringType()))
		print("SUCCESS: Processed exp1aDF and added columns to match final table layout \n")
		etl2DF.show(20)
		
		############################################################################################
		# Step 4. Write the staging dataframe to the staging parquet file				           #
		############################################################################################
		etl2DF.select('local_invoice_id', 'system_id', 'sold_customer_id', 'local_invoice_type', 'invoice_type',
					  'local_business_unit_id', 'business_unit_id', 'invoice_value', 'local_currency_id', 'currency_id',
					  'invoice_date', 'post_date', 'source_type', 'datestamp', 'processing_flag') \
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
