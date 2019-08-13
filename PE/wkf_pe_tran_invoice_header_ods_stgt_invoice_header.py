#
#########################################################################################
# File Name: wkf_pe_tran_invoice_header_ods_stgt_invoice_header.py                      #
# Author: Slalom LLC, Gee Tam                                                           #
# Version: 1.0                                                                          #
# Update Date: 03/15/2019                                                               #
#                                                                                       #
# Description: The purpose of this process is to consume the raw extract file from      #
#              the S3 landing zone and perform the ETL process to load the records      #
#              into the parquet file: stgt_invoice_header.                              #
#########################################################################################
import sys
import time
import datetime
import pandas
import gc
import boto3
#
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
#from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as func
from wrk_ppm_general import *
#
#-------------------------------------------------------------------------------
#  Defined Function area.                                                      -
#-------------------------------------------------------------------------------
#
################################################################################
# This area will have customized values specific for each ETL process.         #
################################################################################
#
appName = 'Python Spark Session - PE TRAN INVOICE HEADER - Stage'
attrCurrencyID = 'USD'
env = 'Stage'
fileName1 = 'PE_TRAN_INVOICE_HEADER.TXT'
fileName2 = ''
fileName3 = ''
fileNameList = fileName1
folderName = 'PE/TRAN_INVOICE_HEADER'
inputFileCnt1 = 0
inputFileCnt2 = 0
inputFileCnt3 = 0
inputFileCntList = ' '
now = datetime.datetime.now()
rryear_todate_udf = func.udf(rryear_todate) # Define the UDF
stagingFile = 'stgt_invoice_header'
startTime = datetime.datetime.now()
sysID = 'PE'
sysName = 'FMS'
systemMessage = ''
timeStamp = now.isoformat()
workFlowName = 'wkf_pe_tran_invoice_header_ods_stgt_invoice_header'
#
####################################################################################
# Define the input file schema(s).                                                 #
####################################################################################
#
inputFile1Schema = StructType([
        StructField('co', StringType(), True),
        StructField('order_xxxxxx_billed_from_p_l', StringType(), True),
        StructField('docu_no', DecimalType(15,4), True),
        StructField('credit_y_n', StringType(), True),
        StructField('inv_date', StringType(), True),
        StructField('inv_qty', StringType(), True),
        StructField('inv_total', StringType(), True),
        StructField('goods', StringType(), True),
        StructField('tax_code_1', StringType(), True),
        StructField('tax_amt_1', StringType(), True),
        StructField('no_lines', StringType(), True),
        StructField('ledgers_upd', StringType(), True),
        StructField('copies', StringType(), True),
        StructField('inv_addr_no_a_r', StringType(), True),
        StructField('del_addr_no_a_r', StringType(), True),
        StructField('temp_del_addrr_no', StringType(), True),
        StructField('txtno', StringType(), True),
        StructField('multi_deliv_addr', StringType(), True),
        StructField('printed', StringType(), True),
        StructField('extra_only', StringType(), True),
        StructField('user', StringType(), True),
        StructField('date', StringType(), True),
        StructField('time', StringType(), True),
        StructField('link', StringType(), True),
        StructField('disc_type', StringType(), True),
        StructField('percentage', StringType(), True),
        StructField('discount', StringType(), True),
        StructField('date_due', StringType(), True),
        StructField('inv_addr_no', StringType(), True),
        StructField('customer_no', StringType(), True),
        StructField('del_addr_no', StringType(), True),
        StructField('reason', StringType(), True),
        StructField('p_l_inv_no', StringType(), True),
        StructField('p_l_del_no', StringType(), True),
        StructField('shipment_produced', StringType(), True),
        StructField('proforma_n_a', StringType(), True),
        StructField('cash_rec_d', StringType(), True),
        StructField('ship_from_plant_no', StringType(), True),
        StructField('invoices_via_edi', StringType(), True),
        StructField('last_shipm_n_a', StringType(), True),
        StructField('inv_type', StringType(), True),
        StructField('cons_inv_n_a', StringType(), True),
        StructField('multi_invoices', StringType(), True),
        StructField('celo_due_date_1_n_a', StringType(), True),
        StructField('celo_value_1_n_a', StringType(), True),
        StructField('ship_text_no_1', StringType(), True),
        StructField('amendment_type', StringType(), True),
        StructField('price_adj', StringType(), True),
        StructField('cis_inv', StringType(), True),
        StructField('tt_n_a', StringType(), True),
        StructField('ext_to_fis_n_a', StringType(), True),
        StructField('gl_per_n_a', StringType(), True),
        StructField('gl_yr_n_a', StringType(), True),
        StructField('inv_wght_n_a', StringType(), True),
        StructField('payment_method', StringType(), True),
        StructField('alt_print', StringType(), True),
        StructField('currency_no', StringType(), True),
        StructField('terms_code', StringType(), True)
])
#
print('START: Begin ETL process... \n')
print("START: Starting a spark session \n")
#
spark = SparkSession \
.builder \
.appName(appName) \
.enableHiveSupport() \
.config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
###################################################################################################
# Pocessing Starts                                                                                #
###################################################################################################
if __name__ == "__main__":
    ###############################################################################################
    # Check to ensure that the bucket name parameters are passes into the program.                #
    ###############################################################################################
    if len(sys.argv) != 4:
        print('ERROR: Missing parameters for S3 bucket names and directories \n', file=sys.stderr)
        exit(-1)
    else:
        DbucketName = sys.argv[1]    # DbucketName = 'wrk-datalake-ppm-dev' 
        IbucketName = sys.argv[2]    # IbucketName = 'wrk-ingest-ppm-dev'
        SbucketName = sys.argv[3]    # SbucketName = 'wrk-system-ppm-dev'
    #
    try:
        #------------------------------------------------------------------------------------------
        # Start custom development.                                                               -
        #------------------------------------------------------------------------------------------
        #
        ############################################################################################
        # Step 1. Read the input file and select only the required fields.                         #
        ############################################################################################
        print('START: Reading input file(s) from the S3 landing/received bucket \n')
        inputFile1 = 's3://' + IbucketName + '/Landing/Received/' + folderName + '/' + fileName1
        inputFile1DF = spark.read.csv(inputFile1, schema=inputFile1Schema, sep = ';', header=True, nullValue='null')
        input1DF = inputFile1DF.select ('co', 'order_xxxxxx_billed_from_p_l', 'docu_no', 'credit_y_n', 'inv_date', 
                                        'inv_total', 'ledgers_upd', 'inv_addr_no', 'customer_no', 'del_addr_no','ship_from_plant_no')
        input1DF = trim_and_upper(input1DF)
        inputFileCnt1 = input1DF.count()
        #
        print ('SUCCESS: Created the ETL dataframe - PE TRAN INVOICE HEADER \n')
        #
        ############################################################################################
        # Step 2. Filter the records based on the busineass rulea.                                 #
        ############################################################################################
        etl1DF = input1DF.filter(func.col('ledgers_upd') == 'YES')
        etl1DF.show(20) # remove after testing
        print ('SUCCESS: Filtered the dataframe based on the business rules \n')
        #
        ############################################################################################
        # Step 3. Create a new Spark dataframe by adding new columns to the dataframe created in   #
        #         step above.                                                                      #
        ############################################################################################
        etl1DF =  input1DF.withColumn('local_invoice_id', func.floor(func.col('docu_no')).cast(StringType())) \
                        .withColumn('local_business_unit_id', func.when(func.col('co') == '', func.lit(None).cast(StringType())) \
                                                             .otherwise(func.col('co'))) \
                        .withColumn('business_unit_id', func.lit(None).cast(StringType())) \
                        .withColumn('sold_customer_id_temp', func.concat(func.col('customer_no'), func.col('inv_addr_no'))) \
                        .withColumn('sold_customer_id', func.when(func.col('sold_customer_id_temp') == '', func.lit(None).cast(StringType())) \
                                                             .otherwise(func.col('sold_customer_id_temp'))) \
                        .withColumn('local_invoice_type', func.when(func.col('credit_y_n') == '', func.lit(None).cast(StringType())) \
                                                             .otherwise(func.col('credit_y_n'))) \
                        .withColumn('invoice_type', func.lit(None).cast(StringType())) \
                        .withColumn('invoice_value_temp', func.regexp_replace(func.col('inv_total'), ',', '')) \
                        .withColumn('invoice_value', func.when(func.col('invoice_value_temp') == '', func.lit(None).cast(DecimalType(20,4))) \
                                                             .otherwise(func.col('invoice_value_temp')).cast(DecimalType(20,4))) \
                        .withColumn('local_currency_id', func.lit(attrCurrencyID)) \
                        .withColumn('currency_id', func.lit(None).cast(StringType())) \
                        .withColumn('invoice_date', rryear_todate_udf(func.col('inv_date'), func.lit('1901-01-01')).cast(TimestampType())) \
                        .withColumn('post_date', rryear_todate_udf(func.col('inv_date'), func.lit('1901-01-01')).cast(TimestampType())) \
                        .withColumn('system_id', func.lit(sysID)) \
                        .withColumn('source_type', func.lit(sysName)) \
                        .withColumn('datestamp', func.lit(format_current_dt ()).cast("timestamp")) \
                        .withColumn('processing_flag', func.lit(None).cast(StringType()))
        etl1DF = etl1DF.select ('local_invoice_id', 'system_id','sold_customer_id', 'local_invoice_type', 'invoice_type', 'local_business_unit_id', 'business_unit_id', 
                                'invoice_value', 'local_currency_id', 'currency_id', 'invoice_date', 'post_date', 'source_type', 'datestamp', 'processing_flag')
        #
        print ('SUCCESS: Created the ETL1 dataframe with the new columns \n')
        #
        ############################################################################################
        # Step 4. Write the staging dataframe to its staging parquet file.                         #
        ############################################################################################
        etl1DF.write.parquet('s3://' + DbucketName + '/Transformed/' + stagingFile + '/', mode='append', compression='snappy')
        print('SUCCESS: Wrote dataframe to staging parquet file \n')
        #
        ############################################################################################
        # Step 5. Write out the log details.                                                       #
        ############################################################################################
        inputFileCntList = str(inputFileCnt1)         # concatenate the list of all of the file counts
        write_log_file(workFlowName, sysID, fileNameList, inputFileCntList, env, startTime, '', 'Success', SbucketName, spark)
        print('SUCCESS: Wrote log dataframe to parquet file \n') 
        print('SUCCESS: End of job \n')
        system_cleanup(spark)
        #
    except Exception as e:
        inputFileCntList = str(inputFileCnt1)         # concatenate the list of all of the file counts
        write_log_file(workFlowName, sysID, fileNameList, inputFileCntList, env, startTime, e, 'Error', SbucketName, spark)
        print('ERROR: Workflow processing failed. See message below for details \n')
        print(e)
        system_cleanup(spark)
#
############################################################################################
# End of script.                                                                           #
############################################################################################
