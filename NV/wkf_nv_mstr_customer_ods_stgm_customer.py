####################################################################################
# File Name: wkf_nv_mstr_customer_ods_stgm_customer.py                             #
# Author: Slalom LLC, Kendra Billings                                              #
# Version: 1.0                                                                     #
# Create Date: 02/19/2019                                                          #
# Update Date: 04/30/2019   by Roald Gomes                                         #
#                                                                                  #
# Description: The purpose of this process is to consume the raw extract file from #
#              the S3 landing zone and perform the ETL process to load the records #
#              into the parquet file: stgm_customer.                               #
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
app_name = 'Python Spark Session - NV Customer - Staging'
env = 'Staging'
# Define the UDF
isdate_udf = func.udf(isdate)
filename1 = 'NV_MSTR_CUSTOMER.TXT'
landingfolder1 = 'Landing/Received/NV/MSTR_CUSTOMER'
filename1_count = 0
filename_list = filename1
filename_count = ' '
transformedfolder = 'Transformed/'
now = datetime.datetime.now()
stagingfile1 = 'stgm_customer'
startTime = datetime.datetime.now()
sysID = 'NV'
sysName = 'FMS'
systemMessage = ''
timeStamp = now.isoformat()
workFlowName = 'wkf_nv_mstr_customer_ods_stgm_customer'

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
		#         rtrim, and uppercase on the individual fields - CUSTOMER.              		   #
		############################################################################################
		print("START: Read the input file(s) from the S3 landing/received bucket \n")
		inputFile1 = 's3://' + ingest_s3Bucket + '/' + landingfolder1 + '/' + filename1
		inputFile1DF = spark.read.text(inputFile1)
		input1DF = inputFile1DF.select(
			inputFile1DF.value.substr(1, 20).alias('no'),
			inputFile1DF.value.substr(21, 30).alias('name'),
			inputFile1DF.value.substr(111, 30).alias('address'),
			inputFile1DF.value.substr(141, 30).alias('address_2'),
			inputFile1DF.value.substr(171, 30).alias('city'),
			inputFile1DF.value.substr(231, 30).alias('phone_no'),
			inputFile1DF.value.substr(261, 30).alias('telex_no'),
			inputFile1DF.value.substr(405, 10).alias('customer_posting_group'),
			inputFile1DF.value.substr(452, 10).alias('payment_terms_code'),
			inputFile1DF.value.substr(472, 10).alias('salesperson_code'),
			inputFile1DF.value.substr(552, 10).alias('country_code'),
			inputFile1DF.value.substr(655, 10).alias('payment_method_code'),
			inputFile1DF.value.substr(665, 11).alias('last_date_modified'),
			inputFile1DF.value.substr(706, 30).alias('fax_no'),
			inputFile1DF.value.substr(816, 20).alias('post_code'),
			inputFile1DF.value.substr(836, 30).alias('county'),
			inputFile1DF.value.substr(1286, 30).alias('address_3'),
			inputFile1DF.value.substr(2909, 30).alias('customer_group'),
			inputFile1DF.value.substr(2940, 30).alias('company_code')
		)
		
		# Informatica transformations
		input1DF = input1DF.transform(trim_and_upper).transform(convert_empty_to_null)
		
		# Get number of records
		filename1_count = input1DF.count()
		print("SUCCESS: Created the dataframe  - CUSTOMER \n")
		
		############################################################################################
		# Step 2. Apply expression transformations to input1DF.           						   #
		############################################################################################
		exp1aDF = input1DF.withColumn('CUSTOMER_ID', func.concat(func.col('No'),
																 func.lit('-'),
																 func.col('company_code')))
		exp1aDF = exp1aDF.withColumn('SYSTEM_ID', func.lit(sysID))
		exp1aDF = exp1aDF.withColumn('LOCAL_CUSTOMER_TYPE', func
									 .when(func.coalesce(func.col('customer_group'), func.lit('')) == '',
										   func.col('CUSTOMER_ID'))
									 .otherwise(func.col('customer_group')))
		exp1aDF = exp1aDF.withColumn('FIRST_SHIP_DATE', isdate_udf(func.col('last_date_modified'), func.lit('-')))
		exp1aDF = exp1aDF.withColumn('LOCAL_STATE_ID', func.when(input1DF['country_code'].isin(['US', 'CA', 'BR']),
																 func.col('CUSTOMER_ID'))
									 .otherwise(func.lit(None)))
		exp1aDF = exp1aDF.withColumn('SOURCE_TYPE', func.lit(sysName))
		print("SUCCESS: Applied expression transformations to input1DF dataframe \n")
		
		############################################################################################
		# Step 3. Create an ETL dataframe from the aboved dataframe and append new columns.		   #
		#                                                                     				       #
		############################################################################################
		etl1DF = exp1aDF.withColumn('customer_id', func.col('CUSTOMER_ID').cast(StringType())) \
			.withColumn('system_id', func.col('SYSTEM_ID').cast(StringType())) \
			.withColumn('customer_desc', func.col('name').cast(StringType())) \
			.withColumn('local_customer_type', func.col('LOCAL_CUSTOMER_TYPE').cast(StringType())) \
			.withColumn('customer_type', func.lit(None).cast(StringType())) \
			.withColumn('local_payment_terms_id', func.col('payment_terms_code').cast(StringType())) \
			.withColumn('payment_terms_id', func.lit(None).cast(StringType())) \
			.withColumn('local_payment_method_id', func.col('payment_method_code').cast(StringType())) \
			.withColumn('payment_method_id', func.lit(None).cast(StringType())) \
			.withColumn('db_nbr', func.lit(None).cast(StringType())) \
			.withColumn('sales_rep', func.col('salesperson_code').cast(StringType())) \
			.withColumn('first_ship_date', func.col('FIRST_SHIP_DATE').cast(TimestampType())) \
			.withColumn('address_1', func.col('address').cast(StringType())) \
			.withColumn('address_2', func.col('address_2').cast(StringType())) \
			.withColumn('address_3', func.col('address_3').cast(StringType())) \
			.withColumn('city', func.col('city').cast(StringType())) \
			.withColumn('county', func.col('county').cast(StringType())) \
			.withColumn('local_state_id', func.col('LOCAL_STATE_ID').cast(StringType())) \
			.withColumn('state_id', func.lit(None).cast(StringType())) \
			.withColumn('local_country_id', func.col('country_code').cast(StringType())) \
			.withColumn('country_id', func.lit(None).cast(StringType())) \
			.withColumn('postal_code', func.col('post_code').cast(StringType())) \
			.withColumn('telephone_1', func.col('phone_no').cast(StringType())) \
			.withColumn('telephone_2', func.col('fax_no').cast(StringType())) \
			.withColumn('source_type', func.col('SOURCE_TYPE').cast(StringType())) \
			.withColumn('datestamp', func.lit(format_current_dt()).cast(TimestampType())) \
			.withColumn('processing_flag', func.lit(None).cast(StringType()))
		print("SUCCESS: Processed exp1aDF and added columns to match final table layout \n")
		etl1DF.show(20)
		
		############################################################################################
		# Step 4. Select the columns for the final parquet table layout.                           #
		############################################################################################
		etl2DF = etl1DF.select('customer_id', 'system_id', 'customer_desc', 'local_customer_type', 'customer_type',
							   'local_payment_terms_id', 'payment_terms_id', 'local_payment_method_id',
							   'payment_method_id', 'db_nbr', 'sales_rep', 'first_ship_date', 'address_1', 'address_2',
							   'address_3', 'city', 'county', 'local_state_id', 'state_id', 'local_country_id',
							   'country_id', 'postal_code', 'telephone_1', 'telephone_2', 'source_type', 'datestamp',
							   'processing_flag')
		print("SUCCESS: Select final columns for staging table \n")
		
		############################################################################################
		# Step 5. Write the staging dataframe to the staging parquet file 						   #
		############################################################################################
		etl2DF.write.parquet('s3://' + dataLake_s3Bucket + '/' + transformedfolder + stagingfile1 + '/',
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
