#
#########################################################################################
# File Name: wkf_pe_mstr_material_ods_stgm_material.py                                  #
# Author: Slalom LLC, Gee Tam                                                           #
# Version: 1.0                                                                          #
# Update Date: 03/18/2019                                                               #
#                                                                                       #
# Description: The purpose of this process is to consume the raw extract file from      #
#              the S3 landing zone and perform the ETL process to load the records      #
#              into the parquet file: stgm_material.                                    #
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
appName = 'Python Spark Session - PE MSTR MATERIAL - Stage'
attrValue1 = 'EA'
env = 'Stage'
fileName1 = 'PE_MSTR_MATERIAL.TXT'
fileName2 = ''
fileName3 = ''
fileNameList = fileName1
folderName = 'PE/MATERIAL'
inputFileCnt1 = 0
inputFileCnt2 = 0
inputFileCnt3 = 0
inputFileCntList = ' '
now = datetime.datetime.now()
ppm_isdate_udf = func.udf(ppm_isdate) # Define the UDF
stagingFile = 'stgm_material'
startTime = datetime.datetime.now()
sysID = 'PE'
sysName = 'FMS'
systemMessage = ''
timeStamp = now.isoformat()
workFlowName = 'wkf_pe_mstr_material_ods_stgm_material'
#
####################################################################################
# Define the input file schema(s).                                                 #
####################################################################################
#
inputFile1Schema = StructType([
        StructField('kco', StringType(), True),
        StructField('kvseqnum', StringType(), True),
        StructField('kanal_code', StringType(), True),
        StructField('kcusno', StringType(), True),
        StructField('kclass', StringType(), True),
        StructField('kvar_code', StringType(), True),
        StructField('vardesc', StringType(), True),
        StructField('full_desc_1', StringType(), True),
        StructField('full_desc_2', StringType(), True),
        StructField('full_desc_3', StringType(), True),
        StructField('full_desc_4', StringType(), True),
        StructField('full_desc_5', StringType(), True),
        StructField('gl_code', StringType(), True),
        StructField('special_code_1', StringType(), True),
        StructField('special_code_2', StringType(), True),
        StructField('special_code_3', StringType(), True),
        StructField('kuser', StringType(), True),
        StructField('kDate', StringType(), True),
        StructField('kTime', StringType(), True),
        StructField('klink', StringType(), True),
        StructField('kCust_Item', StringType(), True),
        StructField('kProduct_cat', StringType(), True),
        StructField('Quote_group', StringType(), True),
        StructField('Code_number', StringType(), True),
        StructField('Price_by', StringType(), True),
        StructField('Other_ref', StringType(), True),
        StructField('Obsolete', StringType(), True),
        StructField('Cols_f', StringType(), True),
        StructField('Cols_b', StringType(), True),
        StructField('flat_Depth', StringType(), True),
        StructField('flat_Width', StringType(), True),
        StructField('int_Depth', StringType(), True),
        StructField('int_Width', StringType(), True),
        StructField('int_Height', StringType(), True),
        StructField('int_X', StringType(), True),
        StructField('Mat_group', StringType(), True),
        StructField('Mat_type', StringType(), True),
        StructField('Mat_gsm', StringType(), True),
        StructField('Mat_caliper', StringType(), True),
        StructField('Mat_comments', StringType(), True),
        StructField('Item_info_1', StringType(), True),
        StructField('Prodn_info_1', StringType(), True),
        StructField('QA_artRec', StringType(), True),
        StructField('Grade', StringType(), True),
        StructField('kCarton', StringType(), True),
        StructField('Ink_Typ', StringType(), True),
        StructField('Vars_f', StringType(), True),
        StructField('Vars_b', StringType(), True),
        StructField('Comm_code', StringType(), True),
        StructField('weight_per', StringType(), True),
        StructField('Weight_unit', StringType(), True),
        StructField('Var_Type', StringType(), True),
        StructField('kProdgrp', StringType(), True),
        StructField('Min_Stock', StringType(), True),
        StructField('Max_Stock', StringType(), True),
        StructField('Qty_per_box', StringType(), True),
        StructField('kgs_per_box', StringType(), True),
        StructField('boxes_per_pallet', StringType(), True),
        StructField('Stocked', StringType(), True),
        StructField('Ink_Text_f_1', StringType(), True),
        StructField('Ink_Text_f_2', StringType(), True),
        StructField('Ink_Text_f_3', StringType(), True),
        StructField('Ink_Text_f_4', StringType(), True),
        StructField('Ink_Text_f_5', StringType(), True),
        StructField('Ink_Text_f_6', StringType(), True),
        StructField('Ink_Text_f_7', StringType(), True),
        StructField('Ink_Text_f_8', StringType(), True),
        StructField('Ink_Text_f_9', StringType(), True),
        StructField('Ink_Text_b_1', StringType(), True),
        StructField('Ink_Text_b_2', StringType(), True),
        StructField('Ink_Text_b_3', StringType(), True),
        StructField('Ink_Text_b_4', StringType(), True),
        StructField('Ink_Text_b_5', StringType(), True),
        StructField('Ink_Text_b_6', StringType(), True),
        StructField('Ink_Text_b_7', StringType(), True),
        StructField('Ink_Text_b_8', StringType(), True),
        StructField('Ink_Text_b_9', StringType(), True),
        StructField('Varnish_Sep_Pass', StringType(), True),
        StructField('Var_Coater_Req', StringType(), True),
        StructField('Weight_1000', StringType(), True),
        StructField('Win_caliper', StringType(), True),
        StructField('Win_comments', StringType(), True),
        StructField('Win_group', StringType(), True),
        StructField('Win_gsm', StringType(), True),
        StructField('Win_type', StringType(), True),
        StructField('Win_Dim_1', StringType(), True),
        StructField('Win_dim_2', StringType(), True),
        StructField('Win_Req', StringType(), True),
        StructField('Item_Text_2', StringType(), True),
        StructField('Item_Text_3', StringType(), True),
        StructField('Item_Text_4', StringType(), True),
        StructField('Item_Text_6', StringType(), True),
        StructField('Item_Text_7', StringType(), True),
        StructField('Item_Text_8', StringType(), True),
        StructField('Item_Text_9', StringType(), True),
        StructField('Item_Text_10', StringType(), True),
        StructField('Item_Text_11', StringType(), True),
        StructField('Item_Text_13', StringType(), True),
        StructField('Item_Text_14', StringType(), True),
        StructField('Item_Text_15', StringType(), True),
        StructField('kDie_type', StringType(), True),
        StructField('kDie_no', StringType(), True),
        StructField('kdesign_lab', StringType(), True),
        StructField('kitemkey_v_1', StringType(), True),
        StructField('kitemkey_v_2', StringType(), True),
        StructField('kitemkey_v_3', StringType(), True),
        StructField('kitemkey_v_4', StringType(), True),
        StructField('kitemkey_v_5', StringType(), True),
        StructField('coverage_v_1', StringType(), True),
        StructField('coverage_v_2', StringType(), True),
        StructField('coverage_v_3', StringType(), True),
        StructField('coverage_v_4', StringType(), True),
        StructField('coverage_v_5', StringType(), True),
        StructField('Plastic_edge', StringType(), True),
        StructField('plastic_kitemkey', StringType(), True),
        StructField('Process_Press', StringType(), True),
        StructField('Base_Mat', StringType(), True),
        StructField('Bar_Code_no', StringType(), True),
        StructField('No_units', StringType(), True),
        StructField('Varnish_Ref', StringType(), True),
        StructField('Cylinder_Circ', StringType(), True),
        StructField('Foil_Blocking', StringType(), True),
        StructField('Foil_kitemkey', StringType(), True),
        StructField('Serial_Numbers', StringType(), True),
        StructField('Win_method', StringType(), True),
        StructField('Win_kitemkey', StringType(), True),
        StructField('Brand', StringType(), True),
        StructField('Product', StringType(), True),
        StructField('ink_covarage', StringType(), True),
        StructField('Paper_Board', StringType(), True),
        StructField('Prod_info_1', StringType(), True),
        StructField('Prod_info2', StringType(), True),
        StructField('Prod_info3', StringType(), True),
        StructField('Prod_info4', StringType(), True),
        StructField('Prod_info5', StringType(), True),
        StructField('Prod_info6', StringType(), True),
        StructField('Prod_info7', StringType(), True),
        StructField('Prod_info8', StringType(), True),
        StructField('Prod_info9', StringType(), True),
        StructField('Prod_info10', StringType(), True),
        StructField('no_Emboss_Passes', StringType(), True),
        StructField('Packing_Out_1', StringType(), True),
        StructField('Packing_Out_2', StringType(), True),
        StructField('Packing_Out_3', StringType(), True),
        StructField('sQty_per_box', StringType(), True),
        StructField('Case_Code', StringType(), True),
        StructField('no_Foil_Blk_Passes', StringType(), True),
        StructField('Gulf_Glued_Code', StringType(), True),
        StructField('Varnish_No', StringType(), True),
        StructField('bOuters', StringType(), True),
        StructField('bPallet_Type', StringType(), True),
        StructField('FIELD152', StringType(), True),
        StructField('FIELD153', StringType(), True),
        StructField('FIELD154', StringType(), True),
        StructField('FIELD155', StringType(), True),
        StructField('FIELD156', StringType(), True),
        StructField('FIELD157', StringType(), True),
        StructField('FIELD158', StringType(), True),
        StructField('FIELD159', StringType(), True),
        StructField('FIELD160', StringType(), True),
        StructField('FIELD161', StringType(), True),
        StructField('FIELD162', StringType(), True),
        StructField('FIELD163', StringType(), True),
        StructField('FIELD164', StringType(), True),
        StructField('FIELD165', StringType(), True),
        StructField('FIELD166', StringType(), True),
        StructField('FIELD167', StringType(), True),
        StructField('FIELD168', StringType(), True),
        StructField('FIELD169', StringType(), True),
        StructField('FIELD170', StringType(), True),
        StructField('FIELD171', StringType(), True),
        StructField('FIELD172', StringType(), True),
        StructField('FIELD173', StringType(), True),
        StructField('FIELD174', StringType(), True),
        StructField('FIELD175', StringType(), True),
        StructField('FIELD176', StringType(), True),
        StructField('FIELD177', StringType(), True),
        StructField('FIELD178', StringType(), True),
        StructField('FIELD179', StringType(), True),
        StructField('FIELD180', StringType(), True),
        StructField('FIELD181', StringType(), True),
        StructField('FIELD182', StringType(), True),
        StructField('FIELD183', StringType(), True),
        StructField('FIELD184', StringType(), True),
        StructField('FIELD185', StringType(), True),
        StructField('FIELD186', StringType(), True),
        StructField('FIELD187', StringType(), True),
        StructField('FIELD188', StringType(), True),
        StructField('FIELD189', StringType(), True),
        StructField('FIELD190', StringType(), True),
        StructField('FIELD191', StringType(), True),
        StructField('FIELD192', StringType(), True),
        StructField('FIELD193', StringType(), True),
        StructField('FIELD194', StringType(), True),
        StructField('FIELD195', StringType(), True),
        StructField('FIELD196', StringType(), True),
        StructField('FIELD197', StringType(), True),
        StructField('FIELD198', StringType(), True),
        StructField('FIELD199', StringType(), True),
        StructField('FIELD200', StringType(), True),
        StructField('FIELD201', StringType(), True),
        StructField('FIELD202', StringType(), True),
        StructField('FIELD203', StringType(), True),
        StructField('FIELD204', StringType(), True),
        StructField('FIELD205', StringType(), True),
        StructField('FIELD206', StringType(), True),
        StructField('FIELD207', StringType(), True),
        StructField('FIELD208', StringType(), True),
        StructField('FIELD209', StringType(), True),
        StructField('FIELD210', StringType(), True),
        StructField('FIELD211', StringType(), True),
        StructField('FIELD212', StringType(), True),
        StructField('FIELD213', StringType(), True),
        StructField('FIELD214', StringType(), True),
        StructField('FIELD215', StringType(), True)
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
        input1DF = inputFile1DF.select ('vardesc', 'kDate', 'kvseqnum', 'special_code_1', 'special_code_2', 'special_code_3')
        input1DF = trim_and_upper(input1DF)
        inputFileCnt1 = input1DF.count()
        #
        print ('SUCCESS: Created the ETL dataframe - PE MSTR MATERIAL \n')
        #
        ############################################################################################
        # Step 2. Create a new Spark dataframe by adding new columns to the dataframe created in   #
        #         step above.                                                                      #
        ############################################################################################
        etl1DF =  input1DF.withColumn('v_kvseqnum', func.regexp_replace(func.col('kvseqnum'), ',', '')) \
                          .withColumn('v_kvseqnum_temp1', func.substring(func.lpad(func.col('v_kvseqnum'),5,'0'),1,2)) \
                          .withColumn('v_kvseqnum_temp2', func.substring(func.lpad(func.col('v_kvseqnum'),5,'0'),-3,3)) \
                          .withColumn('special_code_1_temp', func.regexp_replace(func.col('special_code_1'), '^0*', '')) \
                          .withColumn('material_id', func.concat(func.col('v_kvseqnum_temp1'), func.lit(','), func.col('v_kvseqnum_temp2'))) \
                          .withColumn('system_id',  func.lit(sysID)) \
                          .withColumn('material_desc', func.when(func.col('vardesc') == '', func.lit(None).cast(StringType())) \
                                                             .otherwise(func.col('vardesc'))) \
                          .withColumn('local_base_uom_id', func.lit(attrValue1)) \
                          .withColumn('base_uom_id', func.lit(None).cast(StringType())) \
                          .withColumn('first_ship_date',ppm_isdate_udf(func.col('kDate')).cast(TimestampType())) \
                          .withColumn('local_kpg_id', func.concat(func.col('special_code_1_temp'), func.lit('-'), func.col('special_code_3'),func.lit('-'), func.col('special_code_2'))) \
                          .withColumn('kpg_id', func.lit(None).cast(StringType())) \
                          .withColumn('local_pfl_id',  func.lit(None).cast(StringType())) \
                          .withColumn('pfl_id', func.lit(None).cast(StringType())) \
                          .withColumn('local_spl_id',  func.lit(None).cast(StringType())) \
                          .withColumn('spl_id', func.lit(None).cast(StringType())) \
                          .withColumn('source_type', func.lit(sysName)) \
                          .withColumn('datestamp', func.lit(format_current_dt ()).cast("timestamp")) \
                          .withColumn('processing_flag', func.lit(None).cast(StringType()))
        etl1DF = etl1DF.select ('material_id', 'system_id', 'material_desc',  'local_base_uom_id', 'base_uom_id', 'first_ship_date', 
                                'local_kpg_id', 'kpg_id', 'local_pfl_id', 'pfl_id', 'local_spl_id', 'spl_id', 'source_type', 'datestamp', 'processing_flag')
        #
        print ('SUCCESS: Created the ETL1 dataframe with the new columns \n')
        #
        ############################################################################################
        # Step 3. Write the staging dataframe to its staging parquet file.                         #
        ############################################################################################
        etl1DF.write.parquet('s3://' + DbucketName + '/Transformed/' + stagingFile + '/', mode='append', compression='snappy')
        print('SUCCESS: Wrote dataframe to staging parquet file \n')
        #
        ############################################################################################
        # Step 4. Write out the log details.                                                       #
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
