#
#########################################################################################
# File Name: wkf_je_tran_gl_acct_ods_stgm_cost_center.py                                #
# Author: Slalom LLC, Gee Tam                                                           #
# Version: 1.0                                                                          #
# Update Date: 03/20/2019                                                               #
#                                                                                       #
# Description: The purpose of this process is to consume the raw extract file from      #
#              the S3 landing zone and perform the ETL process to load the records      #
#              into the parquet file: stgm_cost_center.                                 #
#########################################################################################
import sys
import datetime
from pyspark.sql import SparkSession
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
appName = 'Python Spark Session - JE TRAN COST CENTER - Stage'
attrValue1 = '99'
env = 'Stage'
fileName1 = 'JE_TRAN_GL_ACCT.TXT'
fileName2 = ''
fileName3 = ''
fileNameList = fileName1
folderName = 'JE/TRAN_GL_ACCT'
inputFileCnt1 = 0
inputFileCnt2 = 0
inputFileCnt3 = 0
inputFileCntList = ' '
#lookupTable1 = 'xref_location'
now = datetime.datetime.now()
stagingFile = 'stgm_cost_center'
startTime = datetime.datetime.now()
sysID = 'JE'
sysName = 'FMS'
systemMessage = ''
timeStamp = now.isoformat()
workFlowName = 'wkf_je_tran_gl_acct_ods_stgm_cost_center'
#
####################################################################################
# Define the input file schema(s).                                                 #
####################################################################################
#
inputFile1Schema = StructType([
        StructField('GBCO', StringType(), True),
        StructField('GBMCU', StringType(), True),
        StructField('GBOBJ', StringType(), True),
        StructField('GBSUB', StringType(), True),
        StructField('GBSBL', StringType(), True),
        StructField('GBSBLT', StringType(), True),
        StructField('GBCRCD', StringType(), True),
        StructField('GBFY', StringType(), True),
        StructField('CCPNC', StringType(), True),
        StructField('GBAPYC', StringType(), True),
        StructField('net_posting', StringType(), True),
        StructField('GBAPYN', StringType(), True),
        StructField('GBAWTD', StringType(), True)
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
        inputFile1DF = spark.read.csv(inputFile1, schema=inputFile1Schema, sep = '\t', header=True, nullValue='null')
        input1DF = inputFile1DF.select ('GBMCU', 'GBOBJ', 'GBSUB', 'GBCRCD', 'net_posting')
        input1DF = trim_and_upper(input1DF)
        inputFileCnt1 = input1DF.count()
        #
        print ('SUCCESS: Created the ETL dataframe - JE TRAN GT ACCT \n')
        #
        ############################################################################################
        # Step 2. Create the lookup dataframe from the lookup table.                               #
        ############################################################################################
        try:
            lookup1DFCntSQL = "select count(*) from ppmods.xref_location"
            lookup1DFCnt = spark.sql(lookup1DFCntSQL).collect()[0][0]
            if lookup1DFCnt >= 0:
                lookup1SQL = "select local_location_id, system_id as system_id_lkup from ppmods.xref_location where system_id = 'JE' "
                lookup1DF = spark.sql(lookup1SQL)
            print ('SUCCESS: Created the lookup table dataframe - XREF LOCATION \n')
        except Exception as e:
            inputFileCntList = str(inputFileCnt1) + ',' + str(inputFileCnt2) 
            write_log_file(workFlowName, sysID, fileNameList, inputFileCntList, env, startTime, e, 'Error', SbucketName, spark)
            sys.exit("ERROR: The lookup table does not exists in Athena \n")
        #
        ############################################################################################
        # Step 3. Join the input dataframe with the lookup dataframe.                              #
        ############################################################################################
        join1DF = input1DF.join(lookup1DF, (input1DF.GBMCU == lookup1DF.local_location_id), 'inner')
        print ('SUCCESS: Created the joined dataframe \n') 
        #
        ############################################################################################
        # Step 4. Create a new Spark dataframe by adding new columns to the dataframe created in   #
        #         step above.                                                                      #
        ############################################################################################
        etl1DF = join1DF.withColumn('cost_center_id', func.col('GBMCU').cast(StringType())) \
                        .withColumn('system_id', func.lit(sysID)) \
                        .withColumn('cost_center_desc', func.col('GBMCU').cast(StringType())) \
                        .withColumn('local_cost_center_type', func.lit(attrValue1)) \
                        .withColumn('cost_center_type', func.lit(None).cast(StringType())) \
                        .withColumn('local_functional_area_id', func.lit(attrValue1)) \
                        .withColumn('functional_area_id', func.lit(None).cast(StringType())) \
                        .withColumn('source_type', func.lit(sysName)) \
                        .withColumn('datestamp', func.lit(format_current_dt ()).cast("timestamp")) \
                        .withColumn('processing_flag', func.lit(None).cast(StringType()))
        etl1DF = etl1DF.select ('cost_center_id', 'system_id', 'cost_center_desc', 'local_cost_center_type', 'cost_center_type',
                                'local_functional_area_id', 'functional_area_id', 'source_type', 'datestamp', 'processing_flag')
        #
        print ('SUCCESS: Created the ETL1 dataframe with the new columns \n')
        #
        ############################################################################################
        # Step 5. Write the staging dataframe to its staging parquet file.                         #
        ############################################################################################
        etl1DF.write.parquet('s3://' + DbucketName + '/Transformed/' + stagingFile + '/', mode='append', compression='snappy')
        print('SUCCESS: Wrote dataframe to staging parquet file \n')
        #
        ############################################################################################
        # Step 6. Write out the log details.                                                       #
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