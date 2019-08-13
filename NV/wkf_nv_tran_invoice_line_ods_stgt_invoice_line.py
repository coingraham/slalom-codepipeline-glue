####################################################################################
# File Name: wkf_nv_tran_invoice_line_ods_stgt_invoice_line.py                 #
# Author: Slalom LLC, Roald Gomes                                                  #
# Version: 1.0                                                                     #
# Update Date: 05/15/2019                                                          #
#                                                                                  #
# Description: The purpose of this process is to consume the raw extract file from #
#              the S3 landing zone and perform the ETL process to load the records #
#              into the parquet file: stgt_invoice_line.                         #
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
app_name = 'Python Spark Session - NV INVOICE_LINE - Staging'
env = 'Staging'
# Define the UDF
isdate_udf = func.udf(isdate)
filename1 = 'NV_MSTR_MATERIAL.TXT'
filename2 = 'NV_TRAN_INVOICE_LINE.TXT'
landingfolder1 = 'Landing/Received/NV/MSTR_MATERIAL'
landingfolder2 = 'Landing/Received/NV/TRAN_INVOICE_LINE'
filename1_count = 0
filename2_count = 0
filename_list = filename1 + ',' + filename2
filename_count = ' '
transformedfolder = 'Transformed/'
now = datetime.datetime.now()
stagingfile1 = 'stgt_invoice_line'
stagingfile2 = 'stge_invoice_line'
startTime = datetime.datetime.now()
sysID = 'NV'
sysName = 'FMS'
systemMessage = ''
timeStamp = now.isoformat()
workFlowName = 'wkf_nv_tran_invoice_line_ods_stgt_invoice_line'

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
		# Step 2. Read the 2nd input file and format only the required fields and perform ltrim,   #
		#         rtrim, uppercase and convert empty strings to NULL on the individual fields.     #
		############################################################################################
		print("START: Read the input file(s) from the S3 landing/received bucket \n")
		inputFile2 = 's3://' + ingest_s3Bucket + '/' + landingfolder2 + '/' + filename2
		inputFile2DF = spark.read.text(inputFile2)
		# Remove non-printable characters that dislocates subsequent columns
		inputFile2DF = inputFile2DF.withColumn('value', func.regexp_replace(func.col('value'), '/[^ -~]+/g', ''))
		
		input2DF = inputFile2DF.select(
			inputFile2DF.value.substr(1, 2).alias('document_type'),
			inputFile2DF.value.substr(3, 20).alias('sell_to_customer_no'),
			inputFile2DF.value.substr(23, 20).alias('document_no'),
			inputFile2DF.value.substr(43, 7).alias('line_no'),
			inputFile2DF.value.substr(50, 10).alias('type'),
			inputFile2DF.value.substr(60, 20).alias('no'),
			inputFile2DF.value.substr(111, 50).alias('description'),
			inputFile2DF.value.substr(211, 30).alias('unit_of_measure'),
			inputFile2DF.value.substr(241, 12).alias('quantity'),
			inputFile2DF.value.substr(313, 12).alias('amount'),
			inputFile2DF.value.substr(359, 12).alias('net_weight'),
			inputFile2DF.value.substr(422, 20).alias('shortcut_dimension_2_code'),
			inputFile2DF.value.substr(591, 10).alias('gen_prod_posting_group'),
			inputFile2DF.value.substr(895, 10).alias('unit_of_measure_code'),
			inputFile2DF.value.substr(1162, 10).alias('return_reason_code'),
			inputFile2DF.value.substr(1192, 11).alias('posting_date_header'),
			inputFile2DF.value.substr(1311, 12).alias('width'),
			inputFile2DF.value.substr(1323, 12).alias('length_mm_m'),
			inputFile2DF.value.substr(1347, 12).alias('qty_sheets_rolls_to_invoice'),
			inputFile2DF.value.substr(1359, 12).alias('net_weight_to_invoice'),
			inputFile2DF.value.substr(1395, 12).alias('core'),
			inputFile2DF.value.substr(1751, 10).alias('currency_code'),
			inputFile2DF.value.substr(1761, 10).alias('invoice_type'),
			inputFile2DF.value.substr(1771, 10).alias('company_code')
		)
		
		# Informatica transformations
		input2DF = input2DF.transform(trim_and_upper).transform(convert_empty_to_null)
		
		# Get number of records
		filename2_count = input2DF.count()
		print("SUCCESS: Created the dataframe for the 2nd input file \n")
		
		############################################################################################
		# Step 3. Join the input1DF dataframe with the input2DF dataframe       				   #
		############################################################################################
		join2aDF = input1DF.join(input2DF,
								 (input1DF.no == input2DF.no) & (input1DF.company_code == input2DF.company_code),
								 how='right').select(input1DF.grammatur,
													 input2DF.document_type,
													 input2DF.sell_to_customer_no,
													 input2DF.document_no,
													 input2DF.line_no,
													 input2DF.type,
													 input2DF.no,
													 input2DF.description,
													 input2DF.quantity,
													 input2DF.amount,
													 input2DF.net_weight,
													 input2DF.shortcut_dimension_2_code,
													 input2DF.return_reason_code,
													 input2DF.posting_date_header,
													 input2DF.width,
													 input2DF.length_mm_m,
													 input2DF.qty_sheets_rolls_to_invoice,
													 input2DF.net_weight_to_invoice,
													 input2DF.core,
													 input2DF.currency_code,
													 input2DF.invoice_type,
													 input2DF.company_code,
													 input2DF.unit_of_measure_code,
													 input2DF.gen_prod_posting_group)
		print("SUCCESS: Joined the input1DF dataframe with the input2DF dataframe \n")
		
		############################################################################################
		# Step 4. Apply expression transformations to join2aDF.           					       #
		############################################################################################
		exp2aDF = join2aDF.withColumn('quantity', func.regexp_replace(func.col('quantity'), ',', ''))
		exp2aDF = exp2aDF.withColumn('amount', func.regexp_replace(func.col('amount'), ',', ''))
		exp2aDF = exp2aDF.withColumn('net_weight', func.regexp_replace(func.col('net_weight'), ',', ''))
		exp2aDF = exp2aDF.withColumn('net_weight_to_invoice',
									 func.regexp_replace(func.col('net_weight_to_invoice'), ',', ''))
		exp2aDF = exp2aDF.withColumn('grammatur', func.regexp_replace(func.col('grammatur'), ',', ''))
		exp2aDF = exp2aDF.withColumn('qty_sheets_rolls_to_invoice',
									 func.regexp_replace(func.col('qty_sheets_rolls_to_invoice'), ',', ''))
		exp2aDF = exp2aDF.withColumn('LOCAL_INVOICE_ID', func.concat(func.col('no'),
																	 func.lit('-'),
																	 func.col('company_code')))
		exp2aDF = exp2aDF.withColumn('SYSTEM_ID', func.lit(sysID))
		exp2aDF = exp2aDF.withColumn('LOCAL_REV_ACCT_ID', func.concat(func.col('gen_prod_posting_group'),
																	  func.lit('-'),
																	  func.col('invoice_type')))
		exp2aDF = exp2aDF.withColumn('LINE_NUMBER', (func.col('line_no') / 10000).cast(DecimalType(7, 0)))
		exp2aDF = exp2aDF.withColumn('LINE_DESC', func
									 .when(func.col('document_type') == 'MX',
										   func.concat(func.lit('MEMO: '),
													   func.col('description')))
									 .otherwise(func.col('description')))
		exp2aDF = exp2aDF.withColumn('QUALITY_CLASS', func
									 .when(func.col('invoice_type') == 'PQ', func.lit('REJECT'))
									 .otherwise(func.lit('GOOD')))
		exp2aDF = exp2aDF.withColumn('SHIP_CUSTOMER_ID', func.concat(func.col('sell_to_customer_no'),
																	 func.lit('-'),
																	 func.col('company_code')))
		exp2aDF = exp2aDF.withColumn('SHIP_MATERIAL_ID', func
									 .when(func.col('type') != 'ITEM', func.lit(''))
									 .when(func.coalesce(func.col('core'), func.lit('')) == '',
										   func.concat(func.col('no'),
													   func.lit('-'),
													   func.col('company_code'),
													   func.lit('-S')))
									 .otherwise(func.concat(func.col('no'),
															func.lit('-'),
															func.col('company_code'),
															func.lit('-R'))))
		exp2aDF = exp2aDF.withColumn('LOCAL_SHIP_LOCATION_ID', func
									 .when(func.col('type') != 'ITEM', func.lit(''))
									 .otherwise(func.concat(func.col('company_code'),
															func.lit('-'),
															func.col('gen_prod_posting_group'))))
		exp2aDF = exp2aDF.withColumn('LOCAL_MFG_LOCATION_ID',
									 func.concat(func.col('company_code'),
												 func.lit('-'),
												 func.col('gen_prod_posting_group')))
		exp2aDF = exp2aDF.withColumn('INVOICE_LINE_QTY', func
									 .when(func.coalesce(func.col('unit_of_measure_code'), func.lit('')) == '',
										   func.lit(0))
									 .when(func.col('invoice_type').isin('BO', 'PCAD', 'PCCOM', 'PQ', 'PS', 'PU', 'PX'),
										   func.lit(0))
									 .when((func.col('document_type') == 'MX') &
										   (func.col('gen_prod_posting_group') == 'CHEM'), func.col('quantity') * -1)
									 .when(func.col('gen_prod_posting_group') == 'CHEM', func.col('quantity'))
									 .when(func.coalesce(func.col('grammatur'), func.lit(0)) == 0, func.lit(0))
									 .when(func.col('document_type') == 'MX',
										   (func.col('quantity') / func.col('grammatur') * -1000))
									 .otherwise((func.col('quantity') / func.col('grammatur') * 1000))
									 .cast(DecimalType(20, 6)))
		exp2aDF = exp2aDF.withColumn('LOCAL_INVOICE_UOM_ID', func
									 .when(func.col('gen_prod_posting_group') == 'CHEM',
										   func.col('unit_of_measure_code'))
									 .otherwise(func.lit('M2')))
		exp2aDF = exp2aDF.withColumn('SHIP_WEIGHT_QTY', func
									 .when(func.coalesce(func.col('unit_of_measure_code'), func.lit('')) == '',
										   func.lit(0))
									 .when(func.col('gen_prod_posting_group') == 'CHEM', func.lit(0))
									 .when(func.col('invoice_type').isin('BO', 'PCAD', 'PCCOM', 'PQ', 'PS', 'PU', 'PX'),
										   func.lit(0))
									 .when(func.col('document_type') == 'MX', (func.col('quantity') * -1))
									 .otherwise(func.col('quantity'))
									 .cast(DecimalType(20, 6)))
		exp2aDF = exp2aDF.withColumn('LOCAL_SHIP_WEIGHT_UOM_ID', func
									 .when(func.col('gen_prod_posting_group') == 'CHEM', func.lit(''))
									 .when(func.col('invoice_type').isin('BO', 'PCAD', 'PCCOM', 'PQ', 'PS', 'PU', 'PX'),
										   func.lit(None))
									 .otherwise(func.col('unit_of_measure_code')))
		exp2aDF = exp2aDF.withColumn('INVOICE_LINE_VALUE', func
									 .when((func.col('document_type') == 'MX'), (func.col('amount') * -1))
									 .otherwise(func.col('amount')).cast(DecimalType(20, 4)))
		exp2aDF = exp2aDF.withColumn('LOCAL_CURRENCY_ID', func
									 .when(func.coalesce(func.col('currency_code'), func.lit('')) == '',
										   func.lit('EUR'))
									 .otherwise(func.col('currency_code')))
		exp2aDF = exp2aDF.withColumn('INVOICE_DATE', isdate_udf(func.col('posting_date_header'), func.lit('-')))
		exp2aDF = exp2aDF.withColumn('SOURCE_TYPE', func.lit(sysName))
		print("SUCCESS: Applied expression transformations to join2aDF dataframe \n")
		
		############################################################################################
		# Step 5. Create an ETL dataframe from the exp1aDF dataframe and append new columns.       #
		############################################################################################
		etl1DF = exp2aDF.withColumn('local_invoice_id', func.col('LOCAL_INVOICE_ID').cast(StringType())) \
			.withColumn('system_id', func.col('SYSTEM_ID').cast(StringType())) \
			.withColumn('local_rev_acct_id', func.col('LOCAL_REV_ACCT_ID').cast(StringType())) \
			.withColumn('rev_acct_id', func.lit(None).cast(StringType())) \
			.withColumn('line_number', func.col('LINE_NUMBER').cast(DecimalType(12, 4))) \
			.withColumn('line_desc', func.col('LINE_DESC').cast(StringType())) \
			.withColumn('ship_customer_id', func.col('SHIP_CUSTOMER_ID').cast(StringType())) \
			.withColumn('distribution_channel', func.lit(None).cast(StringType())) \
			.withColumn('end_use_class', func.lit(None).cast(StringType())) \
			.withColumn('quality_class', func.col('QUALITY_CLASS').cast(StringType())) \
			.withColumn('ship_material_id', func.col('SHIP_MATERIAL_ID').cast(StringType())) \
			.withColumn('local_ship_location_id', func.col('LOCAL_SHIP_LOCATION_ID').cast(StringType())) \
			.withColumn('ship_location_id', func.lit(None).cast(StringType())) \
			.withColumn('local_mfg_location_id', func.col('LOCAL_MFG_LOCATION_ID').cast(StringType())) \
			.withColumn('mfg_location_id', func.lit(None).cast(StringType())) \
			.withColumn('invoice_line_qty', func.col('INVOICE_LINE_QTY').cast(DecimalType(20, 4))) \
			.withColumn('local_invoice_uom_id', func.col('LOCAL_INVOICE_UOM_ID').cast(StringType())) \
			.withColumn('invoice_uom_id', func.lit(None).cast(StringType())) \
			.withColumn('ship_weight_qty', func.col('SHIP_WEIGHT_QTY').cast(DecimalType(20, 4))) \
			.withColumn('local_ship_weight_uom_id', func.col('LOCAL_SHIP_WEIGHT_UOM_ID').cast(StringType())) \
			.withColumn('ship_weight_uom_id', func.lit(None).cast(StringType())) \
			.withColumn('invoice_line_value', func.col('INVOICE_LINE_VALUE').cast(DecimalType(20, 4))) \
			.withColumn('local_currency_id', func.col('LOCAL_CURRENCY_ID').cast(StringType())) \
			.withColumn('currency_id', func.lit(None).cast(StringType())) \
			.withColumn('invoice_date', func.col('INVOICE_DATE').cast(TimestampType())) \
			.withColumn('source_type', func.col('SOURCE_TYPE').cast(StringType())) \
			.withColumn('datestamp', func.lit(format_current_dt()).cast(TimestampType())) \
			.withColumn('processing_flag', func.lit(None).cast(StringType()))
		print("SUCCESS: Processed exp1aDF and added columns to match final table layout \n")
		etl1DF.show(20)
		
		############################################################################################
		# Step 6. Write the staging dataframe to the staging parquet file				           #
		############################################################################################
		etl1DF.select('local_invoice_id', 'system_id', 'local_rev_acct_id', 'rev_acct_id', 'line_number', 'line_desc',
					  'ship_customer_id', 'distribution_channel', 'end_use_class', 'quality_class', 'ship_material_id',
					  'local_ship_location_id', 'ship_location_id', 'local_mfg_location_id', 'mfg_location_id',
					  'invoice_line_qty', 'local_invoice_uom_id', 'invoice_uom_id', 'ship_weight_qty',
					  'local_ship_weight_uom_id', 'ship_weight_uom_id', 'invoice_line_value', 'local_currency_id',
					  'currency_id', 'invoice_date', 'source_type', 'datestamp', 'processing_flag') \
			.write.parquet('s3://' + dataLake_s3Bucket + '/' + transformedfolder + stagingfile1 + '/',
						   mode='append',
						   compression='snappy')
		print("SUCCESS: Wrote to the staging parquet file \n")
		
		############################################################################################
		# Step 7. Apply expression transformations to join2aDF.           					       #
		############################################################################################
		exp3aDF = join2aDF.withColumn('LOCAL_INVOICE_ID', func.concat(func.col('document_no'),
																	  func.lit('-'),
																	  func.col('company_code')))
		exp3aDF = exp3aDF.withColumn('SYSTEM_ID', func.lit(sysID))
		exp3aDF = exp3aDF.withColumn('LOCAL_REV_ACCT_ID', func.concat(func.col('gen_prod_posting_group'),
																	  func.lit('-'),
																	  func.col('invoice_type')))
		exp3aDF = exp3aDF.withColumn('LINE_NUMBER', (func.col('line_no') / 10000).cast(DecimalType(12, 0)))
		exp3aDF = exp3aDF.withColumn('LOCAL_EXT_ATTRIBUTE_TYPE1', func.lit('WIDTH'))
		exp3aDF = exp3aDF.withColumn('LOCAL_EXT_ATTRIBUTE_TYPE2', func.lit('DIAMETER'))
		exp3aDF = exp3aDF.withColumn('LOCAL_EXT_ATTRIBUTE_TYPE3', func.lit('PRICE UOM'))
		exp3aDF = exp3aDF.withColumn('EXT_ATTRIBUTE_VALUE1', func.regexp_replace(func.col('width'), ',', '')
									 .cast(DecimalType(30, 6)))
		exp3aDF = exp3aDF.withColumn('EXT_ATTRIBUTE_VALUE2', func.regexp_replace(func.col('length_mm_m'), ',', '')
									 .cast(DecimalType(30, 6)))
		exp3aDF = exp3aDF.withColumn('EXT_ATTRIBUTE_VALUE3', func.lit(1)
									 .cast(DecimalType(30, 6)))
		exp3aDF = exp3aDF.withColumn('LOCAL_UOM_ID1', func.lit('MM'))
		exp3aDF = exp3aDF.withColumn('LOCAL_UOM_ID2', func.lit('MM'))
		exp3aDF = exp3aDF.withColumn('LOCAL_UOM_ID3', func.lit('KG'))
		exp3aDF = exp3aDF.withColumn('SOURCE_TYPE', func.lit(sysName))
		print("SUCCESS: Applied expression transformations to join2aDF dataframe \n")
		
		############################################################################################
		# Step 8. Apply normalizer transformations to exp3aDF.           					       #
		############################################################################################
		exp3aDF.createOrReplaceTempView('temp_normalize_tbl')
		norm3aDF = spark.sql("""
								SELECT	LOCAL_INVOICE_ID,
										SYSTEM_ID,
										LOCAL_REV_ACCT_ID,
										LINE_NUMBER,
										LOCAL_EXT_ATTRIBUTE_TYPE1 as LOCAL_EXT_ATTRIBUTE_TYPE,
										EXT_ATTRIBUTE_VALUE1 as EXT_ATTRIBUTE_VALUE,
										LOCAL_UOM_ID1 as LOCAL_UOM_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl
								union all
								SELECT	LOCAL_INVOICE_ID,
										SYSTEM_ID,
										LOCAL_REV_ACCT_ID,
										LINE_NUMBER,
										LOCAL_EXT_ATTRIBUTE_TYPE2 as LOCAL_EXT_ATTRIBUTE_TYPE,
										EXT_ATTRIBUTE_VALUE2 as EXT_ATTRIBUTE_VALUE,
										LOCAL_UOM_ID2 as LOCAL_UOM_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl
								union all
								SELECT	LOCAL_INVOICE_ID,
										SYSTEM_ID,
										LOCAL_REV_ACCT_ID,
										LINE_NUMBER,
										LOCAL_EXT_ATTRIBUTE_TYPE3 as LOCAL_EXT_ATTRIBUTE_TYPE,
										EXT_ATTRIBUTE_VALUE3 as EXT_ATTRIBUTE_VALUE,
										LOCAL_UOM_ID3 as LOCAL_UOM_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl""")
		print("SUCCESS: Applied normalizer transformations to exp3aDF dataframe \n")
		
		############################################################################################
		# Step 9. Create an ETL dataframe from the norm3aDF dataframe and append new columns.      #
		############################################################################################
		etl2DF = norm3aDF.withColumn('local_invoice_id', func.col('LOCAL_INVOICE_ID').cast(StringType())) \
			.withColumn('system_id', func.col('SYSTEM_ID').cast(StringType())) \
			.withColumn('local_rev_acct_id', func.col('LOCAL_REV_ACCT_ID').cast(StringType())) \
			.withColumn('rev_acct_id', func.lit(None).cast(StringType())) \
			.withColumn('line_number', func.col('LINE_NUMBER').cast(DecimalType(12, 4))) \
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
		print("SUCCESS: Processed norm3aDF and added columns to match final table layout \n")
		etl2DF.show(20)
		
		############################################################################################
		# Step 10. Write the staging dataframe to the staging parquet file				           #
		############################################################################################
		etl2DF.select('local_invoice_id', 'system_id', 'local_rev_acct_id', 'rev_acct_id', 'line_number',
					  'local_ext_attribute_type', 'ext_attribute_type', 'ext_attribute_value', 'local_uom_id', 'uom_id',
					  'local_currency_id', 'currency_id', 'source_type', 'datestamp', 'processing_flag') \
			.write.parquet('s3://' + dataLake_s3Bucket + '/' + transformedfolder + stagingfile2 + '/',
						   mode='append',
						   compression='snappy')
		print("SUCCESS: Wrote to the staging parquet file \n")
		
		############################################################################################
		# Step 11. Write out the log details.                                                      #
		############################################################################################
		filename_count = str(filename1_count) + ',' + str(filename2_count)
		write_log_file(workFlowName, sysID, filename_list, filename_count, env, startTime, '', 'Success',
					   system_s3Bucket,
					   spark)
		print("SUCCESS: Write log dataframe to parquet file\n")
		print("SUCCESS: End of job \n")
		system_cleanup(spark)
	
	except Exception as e:
		filename_count = str(filename1_count) + ',' + str(filename2_count)
		write_log_file(workFlowName, sysID, filename_list, filename_count, env, startTime, e, 'Error', system_s3Bucket,
					   spark)
		print("ERROR: Workflow processing failed. See message below for details \n")
		print(e)
		system_cleanup(spark)

############################################################################################
# End of script                                                                            #
############################################################################################
