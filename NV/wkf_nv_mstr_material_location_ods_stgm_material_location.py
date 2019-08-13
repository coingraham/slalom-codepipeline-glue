####################################################################################
# File Name: wkf_nv_mstr_material_location_ods_stgm_material_location.py           #
# Author: Slalom LLC, Roald Gomes                      		                       #
# Version: 1.0                                                                     #
# Update Date: 06/24/2019				                     	                   #
#                                                                                  #
# Description: The purpose of this process is to consume the raw extract file from #
#              the S3 landing zone and perform the ETL process to load the records #
#              into the parquet file: stgm_material_location.     				   #
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
app_name = 'Python Spark Session - NV MATERIAL_LOCATION - Staging'
env = 'Staging'
filename1 = 'NV_MSTR_MATERIAL.TXT'
filename2 = 'NV_TRAN_ITEMS_CA.TXT'
landingfolder1 = 'Landing/Received/NV/MSTR_MATERIAL'
landingfolder2 = 'Landing/Received/NV/TRAN_ITEMS_CA'
filename1_count = 0
filename2_count = 0
filename_list = filename1 + ',' + filename2
filename_count = ' '
transformedfolder = 'Transformed/'
now = datetime.datetime.now()
stagingfile1 = 'stgm_material_location'
stagingfile2 = 'stge_material_location'
startTime = datetime.datetime.now()
sysID = 'NV'
sysName = 'FMS'
systemMessage = ''
timeStamp = now.isoformat()
workFlowName = 'wkf_nv_mstr_material_ods_stgm_material_location'

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
			inputFile1DF.value.substr(981, 47).alias('gen_prod_posting_group'),
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
		inputFile2 = 's3://' + ingest_s3Bucket + '/' + landingfolder2 + '/' + filename2
		inputFile2DF = spark.read.text(inputFile2)
		input2DF = inputFile2DF.select(
			inputFile2DF.value.substr(1, 20).alias('item_no'),
			inputFile2DF.value.substr(32, 20).alias('ca_ref_base_code'),
			inputFile2DF.value.substr(52, 16).alias('effort'),
			inputFile2DF.value.substr(68, 11).alias('company_code')
		)
		
		# Informatica transformations
		input2DF = input2DF.transform(trim_and_upper).transform(convert_empty_to_null)
		
		# Get number of records
		filename2_count = input2DF.count()
		print("SUCCESS: Created the dataframe for the 2nd input file \n")
		
		############################################################################################
		# Step 3. Apply expression transformations to input2DF.           						   #
		############################################################################################
		exp2aDF = input2DF.withColumn('EFFORT', func.regexp_replace(func.col('effort'), ',', '')
									  .cast(DecimalType(30, 15)))
		print("SUCCESS: Applied expression transformations to input2DF dataframe \n")
		
		############################################################################################
		# Step 4. Join the input1DF dataframe with the exp2aDF dataframe       				       #
		############################################################################################
		join2aDF = input1DF.join(exp2aDF,
								 (input1DF.no == exp2aDF.item_no) & (input1DF.company_code == exp2aDF.company_code),
								 how='left').select(input1DF.no,
													input1DF.company_code,
													input1DF.gen_prod_posting_group,
													exp2aDF.ca_ref_base_code,
													exp2aDF.EFFORT)
		print("SUCCESS: Joined the input1DF dataframe with the exp2aDF dataframe \n")
		
		############################################################################################
		# Step 5. Apply expression transformations to join2aDF.           						   #
		############################################################################################
		exp2bDF = join2aDF.withColumn('EFFORT_EXT_ATT_VALUE1_5', func
									  .when((func.col('ca_ref_base_code').isin('COGS MIL F',
																			   'COGS MIL V',
																			   'COGS FOREI',
																			   'COGS MV',
																			   'COGS OCEAN')) |
											(func.col('ca_ref_base_code').isNull()),
											func.col('EFFORT'))
									  .otherwise(func.lit(0))
									  .cast(DecimalType(30, 15)))
		exp2bDF = exp2bDF.withColumn('EFFORT_EXT_ATT_VALUE2_6', func
									 .when((func.col('ca_ref_base_code').isin('COGS EXT F',
																			  'COGS EXT V')) |
										   (func.col('ca_ref_base_code').isNull()),
										   func.col('EFFORT'))
									 .otherwise(func.lit(0))
									 .cast(DecimalType(30, 15)))
		exp2bDF = exp2bDF.withColumn('EFFORT_EXT_ATT_VALUE3', func
									 .when((func.col('ca_ref_base_code').isin('WASTE ST',
																			  'B U F',
																			  'COGS OTH P',
																			  'COGS OTH F',
																			  'COGS OTH M')) |
										   (func.col('ca_ref_base_code').isNull()),
										   func.col('EFFORT'))
									 .otherwise(func.lit(0))
									 .cast(DecimalType(30, 15)))
		exp2bDF = exp2bDF.withColumn('EFFORT_EXT_ATT_VALUE7', func
									 .when((func.col('ca_ref_base_code').isin('COGS SHEET',
																			  'WASTE ST',
																			  'B U F',
																			  'COGS OTH P',
																			  'COGS OTH F',
																			  'COGS OTH M')) |
										   (func.col('ca_ref_base_code').isNull()),
										   func.col('EFFORT'))
									 .otherwise(func.lit(0))
									 .cast(DecimalType(30, 15)))
		exp2bDF = exp2bDF.withColumn('EFFORT_STD_COST1', func
									 .when((func.col('ca_ref_base_code').isin('COGS EXT F',
																			  'COGS EXT V',
																			  'COGS FOREI',
																			  'COGS MIL F',
																			  'COGS MIL V',
																			  'COGS MV',
																			  'WASTE ST',
																			  'B U F',
																			  'COGS OTH P',
																			  'COGS OTH F',
																			  'COGS OTH M',
																			  'COGS OCEAN')) |
										   (func.col('ca_ref_base_code').isNull()),
										   func.col('EFFORT'))
									 .otherwise(func.lit(0))
									 .cast(DecimalType(30, 15)))
		exp2bDF = exp2bDF.withColumn('EFFORT_STD_COST2', func
									 .when((func.col('ca_ref_base_code').isin('COGS EXT F',
																			  'COGS EXT V',
																			  'COGS FOREI',
																			  'COGS MIL F',
																			  'COGS MIL V',
																			  'COGS MV',
																			  'COGS OCEAN',
																			  'WASTE ST',
																			  'B U F',
																			  'COGS OTH P',
																			  'COGS OTH F',
																			  'COGS OTH M',
																			  'COGS SHEET')) |
										   (func.col('ca_ref_base_code').isNull()),
										   func.col('EFFORT'))
									 .otherwise(func.lit(0))
									 .cast(DecimalType(30, 15)))
		exp2bDF = exp2bDF.withColumn('EFFORT_SALES_QTY', func
									 .when((func.col('ca_ref_base_code') == 'SALES QTY') |
										   (func.col('ca_ref_base_code').isNull()),
										   func.col('EFFORT'))
									 .otherwise(func.lit(0))
									 .cast(DecimalType(30, 15)))
		print("SUCCESS: Applied expression transformations to join2aDF dataframe \n")
		
		############################################################################################
		# Step 6. Perform aggregation agg2aDF on exp2bDF dataframe  							   #
		############################################################################################
		agg2aDF = exp2bDF.groupBy('no', 'company_code') \
			.agg(func.max('gen_prod_posting_group').alias('gen_prod_posting_group'),
				 func.max('ca_ref_base_code').alias('ca_ref_base_code'),
				 func.sum('EFFORT_STD_COST1').alias('SUM_EFFORT_STD_COST1'),
				 func.sum('EFFORT_EXT_ATT_VALUE1_5').alias('SUM_EFFORT_EXT_ATT_VALUE1_5')) \
			.orderBy(exp2bDF.no, exp2bDF.company_code)
		print("SUCCESS: Performed aggregation agg2aDF on exp2bDF dataframe \n")
		
		############################################################################################
		# Step 7. Perform aggregation agg3aDF on exp2bDF dataframe  							   #
		############################################################################################
		agg3aDF = exp2bDF.groupBy('no', 'company_code') \
			.agg(func.max('ca_ref_base_code').alias('ca_ref_base_code'),
				 func.sum('EFFORT_SALES_QTY').alias('SUM_EFFORT_SALES_QTY')) \
			.orderBy(exp2bDF.no, exp2bDF.company_code)
		print("SUCCESS: Performed aggregation agg3aDF on exp2bDF dataframe \n")
		
		############################################################################################
		# Step 8. Join the agg3aDF dataframe with the agg2aDF dataframe       				       #
		############################################################################################
		join3aDF = agg3aDF.join(agg2aDF,
								(agg3aDF.no == agg2aDF.no) & (agg3aDF.company_code == agg2aDF.company_code),
								how='inner').select(agg2aDF.no,
													agg2aDF.company_code,
													agg2aDF.gen_prod_posting_group,
													agg2aDF.ca_ref_base_code,
													agg2aDF.SUM_EFFORT_STD_COST1,
													agg2aDF.SUM_EFFORT_EXT_ATT_VALUE1_5,
													agg3aDF.SUM_EFFORT_SALES_QTY)
		print("SUCCESS: Joined the agg3aDF dataframe with the agg2aDF dataframe \n")
		
		############################################################################################
		# Step 9. Perform aggregation agg4aDF on exp2bDF dataframe  							   #
		############################################################################################
		agg4aDF = exp2bDF.groupBy('no', 'company_code') \
			.agg(func.max('ca_ref_base_code').alias('ca_ref_base_code'),
				 func.sum('EFFORT_STD_COST2').alias('SUM_EFFORT_STD_COST2'),
				 func.sum('EFFORT_EXT_ATT_VALUE2_6').alias('SUM_EFFORT_EXT_ATT_VALUE2_6')) \
			.orderBy(exp2bDF.no, exp2bDF.company_code)
		print("SUCCESS: Performed aggregation agg4aDF on exp2bDF dataframe \n")
		
		############################################################################################
		# Step 10. Join the agg4aDF dataframe with the join3aDF dataframe       				   #
		############################################################################################
		join4aDF = agg4aDF.join(join3aDF,
								(agg4aDF.no == join3aDF.no) & (agg4aDF.company_code == join3aDF.company_code),
								how='inner').select(join3aDF.no,
													join3aDF.company_code,
													join3aDF.gen_prod_posting_group,
													join3aDF.ca_ref_base_code,
													join3aDF.SUM_EFFORT_STD_COST1,
													join3aDF.SUM_EFFORT_SALES_QTY,
													join3aDF.SUM_EFFORT_EXT_ATT_VALUE1_5,
													agg4aDF.SUM_EFFORT_STD_COST2,
													agg4aDF.SUM_EFFORT_EXT_ATT_VALUE2_6, )
		print("SUCCESS: Joined the agg4aDF dataframe with the join3aDF dataframe \n")
		
		############################################################################################
		# Step 11. Apply expression transformation exp4aDF to join4aDF.           				   #
		############################################################################################
		exp4aDF = join4aDF.withColumn('v_MATERIAL_ID', func.concat(func.col('no'),
																   func.lit('-'),
																   func.col('company_code')))
		exp4aDF = exp4aDF.withColumn('MATERIAL_ID1', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-R')))
		exp4aDF = exp4aDF.withColumn('MATERIAL_ID2', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-S')))
		exp4aDF = exp4aDF.withColumn('SYSTEM_ID', func.lit(sysID))
		exp4aDF = exp4aDF.withColumn('LOCAL_LOCATION_ID', func.concat(func.col('company_code'),
																	  func.lit('-'),
																	  func.col('gen_prod_posting_group')))
		exp4aDF = exp4aDF.withColumn('EFFORT_STD_COST1', (func.col('SUM_EFFORT_STD_COST1') * func.lit(-1))
									 .cast(DecimalType(30, 15)))
		exp4aDF = exp4aDF.withColumn('EFFORT_STD_COST2', (func.col('SUM_EFFORT_STD_COST2') * func.lit(-1))
									 .cast(DecimalType(30, 15)))
		exp4aDF = exp4aDF.withColumn('EFFORT_SALES_QTY', func.col('SUM_EFFORT_SALES_QTY')
									 .cast(DecimalType(30, 15)))
		exp4aDF = exp4aDF.withColumn('STD_COST1', func
									 .when(func.col('EFFORT_SALES_QTY') != 0,
										   (func.col('EFFORT_STD_COST1') / func.col('EFFORT_SALES_QTY')))
									 .otherwise(func.lit(0))
									 .cast(DecimalType(30, 15)))
		exp4aDF = exp4aDF.withColumn('STD_COST2', func
									 .when(func.col('EFFORT_SALES_QTY') != 0,
										   (func.col('EFFORT_STD_COST2') / func.col('EFFORT_SALES_QTY')))
									 .otherwise(func.lit(0))
									 .cast(DecimalType(30, 15)))
		exp4aDF = exp4aDF.withColumn('LOCAL_CURRENCY_ID', func.lit('EUR'))
		exp4aDF = exp4aDF.withColumn('LOCAL_INVENTORY_TYPE', func.lit('FIN'))
		exp4aDF = exp4aDF.withColumn('SOURCE_TYPE', func.lit(sysName))
		print("SUCCESS: Applied expression transformation exp4aDF to join4aDF dataframe \n")
		
		############################################################################################
		# Step 12. Apply normalizer norm4aDF to exp4aDF.           								   #
		############################################################################################
		exp4aDF.createOrReplaceTempView('temp_normalize_tbl')
		norm4aDF = spark.sql("""
								SELECT	MATERIAL_ID1 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_LOCATION_ID,
										STD_COST1 as STD_COST,
										LOCAL_CURRENCY_ID,
										LOCAL_INVENTORY_TYPE,
										SOURCE_TYPE
								FROM temp_normalize_tbl
								union all
								SELECT	MATERIAL_ID2 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_LOCATION_ID,
										STD_COST2 as STD_COST,
										LOCAL_CURRENCY_ID,
										LOCAL_INVENTORY_TYPE,
										SOURCE_TYPE
								FROM temp_normalize_tbl""")
		print("SUCCESS: Applied normalizer norm4aDF to exp4aDF dataframe \n")
		
		############################################################################################
		# Step 13. Create an ETL dataframe from norm4aDF and append new columns.  				   #
		############################################################################################
		etl1DF = norm4aDF.withColumn('material_id', func.col('MATERIAL_ID').cast(StringType()))
		etl1DF = etl1DF.withColumn('system_id', func.col('SYSTEM_ID').cast(StringType()))
		etl1DF = etl1DF.withColumn('local_location_id', func.col('LOCAL_LOCATION_ID').cast(StringType()))
		etl1DF = etl1DF.withColumn('location_id', func.lit(None).cast(StringType()))
		etl1DF = etl1DF.withColumn('std_cost', func.col('STD_COST').cast(DecimalType(30, 15)))
		etl1DF = etl1DF.withColumn('local_currency_id', func.col('LOCAL_CURRENCY_ID').cast(StringType()))
		etl1DF = etl1DF.withColumn('currency_id', func.lit(None).cast(StringType()))
		etl1DF = etl1DF.withColumn('local_profit_center_id', func.lit(None).cast(StringType()))
		etl1DF = etl1DF.withColumn('profit_center_id', func.lit(None).cast(StringType()))
		etl1DF = etl1DF.withColumn('local_inventory_type', func.col('LOCAL_INVENTORY_TYPE').cast(StringType()))
		etl1DF = etl1DF.withColumn('inventory_type', func.lit(None).cast(StringType()))
		etl1DF = etl1DF.withColumn('material_class', func.lit(None).cast(StringType()))
		etl1DF = etl1DF.withColumn('source_type', func.col('SOURCE_TYPE').cast(StringType()))
		etl1DF = etl1DF.withColumn('datestamp', func.lit(format_current_dt()).cast(TimestampType()))
		etl1DF = etl1DF.withColumn('processing_flag', func.lit(None).cast(StringType()))
		print("SUCCESS: Processed norm4aDF and added columns to match final table layout \n")
		etl1DF.show(20)
		
		############################################################################################
		# Step 14. Write the staging dataframe to the staging parquet file				           #
		############################################################################################
		etl1DF.select('material_id', 'system_id', 'local_location_id', 'location_id', 'std_cost', 'local_currency_id',
					  'currency_id', 'local_profit_center_id', 'profit_center_id', 'local_inventory_type',
					  'inventory_type', 'material_class', 'source_type', 'datestamp', 'processing_flag') \
			.write.parquet('s3://' + dataLake_s3Bucket + '/' + transformedfolder + stagingfile1 + '/',
						   mode='append',
						   compression='snappy')
		print("SUCCESS: Wrote to the staging parquet file \n")
		
		############################################################################################
		# Step 15. Perform aggregation agg5aDF on exp2bDF dataframe  							   #
		############################################################################################
		agg5aDF = exp2bDF.groupBy('no', 'company_code') \
			.agg(func.max('ca_ref_base_code').alias('ca_ref_base_code'),
				 func.sum('EFFORT_EXT_ATT_VALUE3').alias('SUM_EFFORT_EXT_ATT_VALUE3')) \
			.orderBy(exp2bDF.no, exp2bDF.company_code)
		print("SUCCESS: Performed aggregation agg5aDF on exp2bDF dataframe \n")
		
		############################################################################################
		# Step 16. Join the join4aDF dataframe with the agg5aDF dataframe       				   #
		############################################################################################
		join5aDF = join4aDF.join(agg5aDF,
								 (join4aDF.no == agg5aDF.no) & (join4aDF.company_code == agg5aDF.company_code),
								 how='inner').select(join4aDF.no,
													 join4aDF.company_code,
													 join4aDF.gen_prod_posting_group,
													 join4aDF.ca_ref_base_code,
													 join4aDF.SUM_EFFORT_EXT_ATT_VALUE1_5,
													 join4aDF.SUM_EFFORT_EXT_ATT_VALUE2_6,
													 join4aDF.SUM_EFFORT_SALES_QTY,
													 agg5aDF.SUM_EFFORT_EXT_ATT_VALUE3, )
		print("SUCCESS: Joined the join4aDF dataframe with the agg5aDF dataframe \n")
		
		############################################################################################
		# Step 17. Perform aggregation agg6aDF on exp2bDF dataframe  							   #
		############################################################################################
		agg6aDF = exp2bDF.groupBy('no', 'company_code') \
			.agg(func.max('ca_ref_base_code').alias('ca_ref_base_code'),
				 func.sum('EFFORT_EXT_ATT_VALUE7').alias('SUM_EFFORT_EXT_ATT_VALUE7')) \
			.orderBy(exp2bDF.no, exp2bDF.company_code)
		print("SUCCESS: Performed aggregation agg6aDF on exp2bDF dataframe \n")
		
		############################################################################################
		# Step 18. Join the join5aDF dataframe with the agg6aDF dataframe       				   #
		############################################################################################
		join6aDF = join5aDF.join(agg6aDF,
								 (join5aDF.no == agg6aDF.no) & (join5aDF.company_code == agg6aDF.company_code),
								 how='inner').select(join5aDF.no,
													 join5aDF.company_code,
													 join5aDF.gen_prod_posting_group,
													 join5aDF.ca_ref_base_code,
													 join5aDF.SUM_EFFORT_EXT_ATT_VALUE1_5,
													 join5aDF.SUM_EFFORT_EXT_ATT_VALUE2_6,
													 join5aDF.SUM_EFFORT_SALES_QTY,
													 join5aDF.SUM_EFFORT_EXT_ATT_VALUE3,
													 agg6aDF.SUM_EFFORT_EXT_ATT_VALUE7, )
		print("SUCCESS: Joined the join5aDF dataframe with the agg6aDF dataframe \n")
		
		############################################################################################
		# Step 19. Apply expression transformation exp6aDF to join6aDF.           				   #
		############################################################################################
		exp6aDF = join6aDF.withColumn('v_MATERIAL_ID', func.concat(func.col('no'),
																   func.lit('-'),
																   func.col('company_code')))
		exp6aDF = exp6aDF.withColumn('MATERIAL_ID1', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-R')))
		exp6aDF = exp6aDF.withColumn('MATERIAL_ID2', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-R')))
		exp6aDF = exp6aDF.withColumn('MATERIAL_ID3', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-R')))
		exp6aDF = exp6aDF.withColumn('MATERIAL_ID4', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-R')))
		exp6aDF = exp6aDF.withColumn('MATERIAL_ID5', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-S')))
		exp6aDF = exp6aDF.withColumn('MATERIAL_ID6', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-S')))
		exp6aDF = exp6aDF.withColumn('MATERIAL_ID7', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-S')))
		exp6aDF = exp6aDF.withColumn('MATERIAL_ID8', func.concat(func.col('v_MATERIAL_ID'),
																 func.lit('-S')))
		exp6aDF = exp6aDF.withColumn('SYSTEM_ID', func.lit(sysID))
		exp6aDF = exp6aDF.withColumn('LOCAL_LOCATION_ID', func.concat(func.col('company_code'),
																	  func.lit('-'),
																	  func.col('gen_prod_posting_group')))
		exp6aDF = exp6aDF.withColumn('LOCAL_EXT_ATTRIBUTE_TYPE1', func.lit('MILL'))
		exp6aDF = exp6aDF.withColumn('LOCAL_EXT_ATTRIBUTE_TYPE2', func.lit('EXTRUDING'))
		exp6aDF = exp6aDF.withColumn('LOCAL_EXT_ATTRIBUTE_TYPE3', func.lit('CONVERTING'))
		exp6aDF = exp6aDF.withColumn('LOCAL_EXT_ATTRIBUTE_TYPE4', func.lit('CARTONIZING'))
		exp6aDF = exp6aDF.withColumn('LOCAL_EXT_ATTRIBUTE_TYPE5', func.lit('MILL'))
		exp6aDF = exp6aDF.withColumn('LOCAL_EXT_ATTRIBUTE_TYPE6', func.lit('EXTRUDING'))
		exp6aDF = exp6aDF.withColumn('LOCAL_EXT_ATTRIBUTE_TYPE7', func.lit('CONVERTING'))
		exp6aDF = exp6aDF.withColumn('LOCAL_EXT_ATTRIBUTE_TYPE8', func.lit('CARTONIZING'))
		exp6aDF = exp6aDF.withColumn('EFFORT_EXT_ATT_VALUE1_5', func.col('SUM_EFFORT_EXT_ATT_VALUE1_5')
									 .cast(DecimalType(30, 15)))
		exp6aDF = exp6aDF.withColumn('EFFORT_EXT_ATT_VALUE2_6', func.col('SUM_EFFORT_EXT_ATT_VALUE2_6')
									 .cast(DecimalType(30, 15)))
		exp6aDF = exp6aDF.withColumn('EFFORT_SALES_QTY', func.col('SUM_EFFORT_SALES_QTY')
									 .cast(DecimalType(30, 15)))
		exp6aDF = exp6aDF.withColumn('EFFORT_EXT_ATT_VALUE3', func.col('SUM_EFFORT_EXT_ATT_VALUE3')
									 .cast(DecimalType(30, 15)))
		exp6aDF = exp6aDF.withColumn('EFFORT_EXT_ATT_VALUE7', func.col('SUM_EFFORT_EXT_ATT_VALUE7')
									 .cast(DecimalType(30, 15)))
		exp6aDF = exp6aDF.withColumn('EXT_ATTRIBUTE_VALUE1', func
									 .when(func.col('EFFORT_SALES_QTY') != 0,
										   (func.col('EFFORT_EXT_ATT_VALUE1_5') / func.col('EFFORT_SALES_QTY')))
									 .otherwise(func.lit(0))
									 .cast(DecimalType(30, 15)))
		exp6aDF = exp6aDF.withColumn('EXT_ATTRIBUTE_VALUE2', func
									 .when(func.col('EFFORT_SALES_QTY') != 0,
										   (func.col('EFFORT_EXT_ATT_VALUE2_6') / func.col('EFFORT_SALES_QTY')))
									 .otherwise(func.lit(0))
									 .cast(DecimalType(30, 15)))
		exp6aDF = exp6aDF.withColumn('EXT_ATTRIBUTE_VALUE3', func
									 .when(func.col('EFFORT_SALES_QTY') != 0,
										   (func.col('EFFORT_EXT_ATT_VALUE3') / func.col('EFFORT_SALES_QTY')))
									 .otherwise(func.lit(0))
									 .cast(DecimalType(30, 15)))
		exp6aDF = exp6aDF.withColumn('EXT_ATTRIBUTE_VALUE4', func.lit(0)
									 .cast(DecimalType(30, 15)))
		exp6aDF = exp6aDF.withColumn('EXT_ATTRIBUTE_VALUE5', func
									 .when(func.col('EFFORT_SALES_QTY') != 0,
										   (func.col('EFFORT_EXT_ATT_VALUE1_5') / func.col('EFFORT_SALES_QTY')))
									 .otherwise(func.lit(0))
									 .cast(DecimalType(30, 15)))
		exp6aDF = exp6aDF.withColumn('EXT_ATTRIBUTE_VALUE6', func
									 .when(func.col('EFFORT_SALES_QTY') != 0,
										   (func.col('EFFORT_EXT_ATT_VALUE2_6') / func.col('EFFORT_SALES_QTY')))
									 .otherwise(func.lit(0))
									 .cast(DecimalType(30, 15)))
		exp6aDF = exp6aDF.withColumn('EXT_ATTRIBUTE_VALUE7', func
									 .when(func.col('EFFORT_SALES_QTY') != 0,
										   (func.col('EFFORT_EXT_ATT_VALUE7') / func.col('EFFORT_SALES_QTY')))
									 .otherwise(func.lit(0))
									 .cast(DecimalType(30, 15)))
		exp6aDF = exp6aDF.withColumn('EXT_ATTRIBUTE_VALUE8', func.lit(0)
									 .cast(DecimalType(30, 15)))
		exp6aDF = exp6aDF.withColumn('LOCAL_CURRENCY_ID', func.lit('EUR'))
		exp6aDF = exp6aDF.withColumn('SOURCE_TYPE', func.lit(sysName))
		print("SUCCESS: Applied expression transformation exp6aDF to join6aDF dataframe \n")
		
		############################################################################################
		# Step 20. Apply normalizer norm6aDF to exp6aDF.           								   #
		############################################################################################
		exp6aDF.createOrReplaceTempView('temp_normalize_tbl')
		norm6aDF = spark.sql("""
								SELECT	MATERIAL_ID1 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_LOCATION_ID,
										LOCAL_EXT_ATTRIBUTE_TYPE1 as LOCAL_EXT_ATTRIBUTE_TYPE,
										EXT_ATTRIBUTE_VALUE1 as EXT_ATTRIBUTE_VALUE,
										LOCAL_CURRENCY_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl
								union all
								SELECT	MATERIAL_ID2 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_LOCATION_ID,
										LOCAL_EXT_ATTRIBUTE_TYPE2 as LOCAL_EXT_ATTRIBUTE_TYPE,
										EXT_ATTRIBUTE_VALUE2 as EXT_ATTRIBUTE_VALUE,
										LOCAL_CURRENCY_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl
								union all
								SELECT	MATERIAL_ID3 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_LOCATION_ID,
										LOCAL_EXT_ATTRIBUTE_TYPE3 as LOCAL_EXT_ATTRIBUTE_TYPE,
										EXT_ATTRIBUTE_VALUE3 as EXT_ATTRIBUTE_VALUE,
										LOCAL_CURRENCY_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl
								union all
								SELECT	MATERIAL_ID4 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_LOCATION_ID,
										LOCAL_EXT_ATTRIBUTE_TYPE4 as LOCAL_EXT_ATTRIBUTE_TYPE,
										EXT_ATTRIBUTE_VALUE4 as EXT_ATTRIBUTE_VALUE,
										LOCAL_CURRENCY_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl
								union all
								SELECT	MATERIAL_ID5 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_LOCATION_ID,
										LOCAL_EXT_ATTRIBUTE_TYPE5 as LOCAL_EXT_ATTRIBUTE_TYPE,
										EXT_ATTRIBUTE_VALUE5 as EXT_ATTRIBUTE_VALUE,
										LOCAL_CURRENCY_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl
								union all
								SELECT	MATERIAL_ID6 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_LOCATION_ID,
										LOCAL_EXT_ATTRIBUTE_TYPE6 as LOCAL_EXT_ATTRIBUTE_TYPE,
										EXT_ATTRIBUTE_VALUE6 as EXT_ATTRIBUTE_VALUE,
										LOCAL_CURRENCY_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl
								union all
								SELECT	MATERIAL_ID7 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_LOCATION_ID,
										LOCAL_EXT_ATTRIBUTE_TYPE7 as LOCAL_EXT_ATTRIBUTE_TYPE,
										EXT_ATTRIBUTE_VALUE7 as EXT_ATTRIBUTE_VALUE,
										LOCAL_CURRENCY_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl
								union all
								SELECT	MATERIAL_ID8 as MATERIAL_ID,
										SYSTEM_ID,
										LOCAL_LOCATION_ID,
										LOCAL_EXT_ATTRIBUTE_TYPE8 as LOCAL_EXT_ATTRIBUTE_TYPE,
										EXT_ATTRIBUTE_VALUE8 as EXT_ATTRIBUTE_VALUE,
										LOCAL_CURRENCY_ID,
										SOURCE_TYPE
								FROM temp_normalize_tbl""")
		print("SUCCESS: Applied normalizer norm6aDF to exp6aDF dataframe \n")
		
		############################################################################################
		# Step 21. Create an ETL dataframe from norm6aDF and append new columns.  				   #
		############################################################################################
		etl2DF = norm6aDF.withColumn('material_id', func.col('MATERIAL_ID').cast(StringType()))
		etl2DF = etl2DF.withColumn('system_id', func.col('SYSTEM_ID').cast(StringType()))
		etl2DF = etl2DF.withColumn('local_location_id', func.col('LOCAL_LOCATION_ID').cast(StringType()))
		etl2DF = etl2DF.withColumn('location_id', func.lit(None).cast(StringType()))
		etl2DF = etl2DF.withColumn('local_ext_attribute_type', func.col('LOCAL_EXT_ATTRIBUTE_TYPE').cast(StringType()))
		etl2DF = etl2DF.withColumn('ext_attribute_type', func.lit(None).cast(StringType()))
		etl2DF = etl2DF.withColumn('ext_attribute_value', func.format_number(func.col('EXT_ATTRIBUTE_VALUE'), 15))
		etl2DF = etl2DF.withColumn('local_uom_id', func.lit(None).cast(StringType()))
		etl2DF = etl2DF.withColumn('uom_id', func.lit(None).cast(StringType()))
		etl2DF = etl2DF.withColumn('local_currency_id', func.col('LOCAL_CURRENCY_ID').cast(StringType()))
		etl2DF = etl2DF.withColumn('currency_id', func.lit(None).cast(StringType()))
		etl2DF = etl2DF.withColumn('source_type', func.col('SOURCE_TYPE').cast(StringType()))
		etl2DF = etl2DF.withColumn('datestamp', func.lit(format_current_dt()).cast(TimestampType()))
		etl2DF = etl2DF.withColumn('processing_flag', func.lit(None).cast(StringType()))
		print("SUCCESS: Processed norm6aDF and added columns to match final table layout \n")
		etl2DF.show(20)
		
		############################################################################################
		# Step 22. Write the staging dataframe to the staging parquet file				           #
		############################################################################################
		etl2DF.select('material_id', 'system_id', 'local_location_id', 'location_id', 'local_ext_attribute_type',
					  'ext_attribute_type', 'ext_attribute_value', 'local_uom_id', 'uom_id', 'local_currency_id',
					  'currency_id', 'source_type', 'datestamp', 'processing_flag') \
			.write.parquet('s3://' + dataLake_s3Bucket + '/' + transformedfolder + stagingfile2 + '/',
						   mode='append',
						   compression='snappy')
		print("SUCCESS: Wrote to the staging parquet file \n")
		
		############################################################################################
		# Step 23. Write out the log details.                                                      #
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
