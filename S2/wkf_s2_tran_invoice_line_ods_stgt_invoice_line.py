#########################################################################################
# File Name: wkf_s2_tran_invoice_line_ods_stgt_invoice_line.py                      #
# Author: Slalom LLC                                                                    #
# Version: 1.0                                                                          #
# Update Date: 05/30/2019                                                               #
#                                                                                       #
# Description: The purpose of this process is to consume the raw extract file from      #
#              the S3 landing zone and perform the ETL process to load the records      #
#              into the parquet file: stgt_invoice_line.                                #
#########################################################################################

import sys
import datetime

import pyspark.sql.functions as func
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *

from wrk_ppm_general import *


def main():
    try:
        ################################################################################
        # This area will have customized values specific for each ETL process.         #
        ################################################################################

        # Creating function for only trimming columns (no upper or remove_non_print)
        def trim_all_cols(df):
            for col in df.columns:
                df = df.withColumn(col, func.ltrim(func.rtrim(df[col])))
            return df

        def is_float(str_col):
            try:
                float(str_col)
                return True
            except ValueError:
                return False
            except TypeError:
                return False
        is_float_udf = udf(is_float, returnType=BooleanType())

        def is_date(date_str):
            try:
                formatted_date = datetime.datetime.strptime(date_str, '%Y%m%d').strftime('%Y%m%d')
            except:
                formatted_date = datetime.datetime.strptime('19000101', '%Y%m%d').strftime('%Y%m%d')
            return formatted_date

        is_date_udf = udf(is_date, returnType=StringType())

        app_name = 'PySpark - S2 Tran Invoice Line - Stgt'
        env = 'Stage'
        file_name_1 = 'S2_TRAN_INVOICE_LINE_COND.TXT'
        file_name_2 = 'S2_TRAN_INVOICE_HEADER.TXT'
        file_name_3 = 'S2_TRAN_INVOICE_LINE.TXT'
        filenames = [file_name_1, file_name_2, file_name_3]
        file_name_list = ",".join(filenames)
        folder_name_1 = 'S2/TRAN_INVOICE_LINE_COND'
        folder_name_2 = 'S2/TRAN_INVOICE_HEADER'
        folder_name_3 = 'S2/TRAN_INVOICE_LINE'
        source_type = 'FMS'
        staging_file = 'stgt_invoice_line'
        start_time = datetime.datetime.now()
        sys_id = 'S2'
        workflow_name = 'wkf_s2_tran_invoice_line_ods_stgt_invoice_line'

        # Check if S3 bucket names were passed to script
        if len(sys.argv) != 4:
            raise ValueError
        else:
            datalake_bucket = sys.argv[1]  # DbucketName = 'wrk-datalake-ppm-dev'
            ingest_bucket = sys.argv[2]  # IbucketName = 'wrk-ingest-ppm-dev'
            system_bucket = sys.argv[3]  # SbucketName = 'wrk-system-ppm-dev'

        spark = create_spark_session(app_name)
        spark.sparkContext.setLogLevel('INFO')

        logger = create_logger(__name__, spark)

        ####################################################################################
        # Step 1:                                                                          #
        #  - Read the input files                                                          #
        #  - Select required fields                                                        #
        #  - Apply rtrim, ltrim, and upper functions to all cols                           #
        ####################################################################################

        """ Source Definitions """

        # S2_TRAN_INVOICE_LINE_COND flat file
        input_file_1 = "s3://" + ingest_bucket + "/Landing/Received/" + folder_name_1 + "/" + file_name_1
        input_file_1_df = spark.read.text(input_file_1)
        input_1_df = input_file_1_df\
            .select(
                input_file_1_df.value.substr(1, 10).alias("KNUMV"),
                input_file_1_df.value.substr(11, 6).alias("KPOSN"),
                input_file_1_df.value.substr(67, 5).alias("WAERS"),
                input_file_1_df.value.substr(129, 10).alias("SAKN1"),
                input_file_1_df.value.substr(183, 15).alias("KWERT"),
                input_file_1_df.value.substr(200, 1).alias("KOAID")
            )

        input_1_cnt = input_1_df.count()
        logger.info("Created df for input file: {} with trimmed and upper cols, df has {} rows"
                    .format(file_name_1, str(input_1_cnt)))

        # S2_TRAN_INVOICE_HEADER flat file
        input_file_2 = "s3://" + ingest_bucket + "/Landing/Received/" + folder_name_2 + "/" + file_name_2
        input_file_2_df = spark.read.text(input_file_2)
        input_2_df = input_file_2_df\
            .select(
                input_file_2_df.value.substr(1, 10).alias("VBELN"),
                func.trim(func.upper(input_file_2_df.value.substr(11, 4))).alias("FKART"),
                input_file_2_df.value.substr(17, 5).alias("WAERK"),
                input_file_2_df.value.substr(22, 4).alias("VKORG"),
                input_file_2_df.value.substr(34, 10).alias("KNUMV1"),
                input_file_2_df.value.substr(46, 8).alias("FKDAT"),
                input_file_2_df.value.substr(117, 14).alias("KURRF"),
                input_file_2_df.value.substr(175, 17).alias("NETWR"),
                input_file_2_df.value.substr(274, 10).alias("KUNAG"),
                input_file_2_df.value.substr(698, 10).alias("KUNWE")
            )

        input_2_cnt = input_2_df.count()
        logger.info("Created df for input file: {} with trimmed and upper cols, df has {} rows"
                    .format(file_name_2, str(input_2_cnt)))

        # S2_TRAN_INVOICE_LINE flat file
        input_file_3 = "s3://" + ingest_bucket + "/Landing/Received/" + folder_name_3 + "/" + file_name_3
        input_file_3_df = spark.read.text(input_file_3)
        input_3_df = input_file_3_df\
            .select(
                input_file_3_df.value.substr(1, 10).alias("VBELN1"),
                input_file_3_df.value.substr(11, 6).alias("POSNR"),
                input_file_3_df.value.substr(23, 16).alias("FKIMG"),
                input_file_3_df.value.substr(39, 3).alias("VRKME"),
                input_file_3_df.value.substr(103, 18).alias("NTGEW"),
                input_file_3_df.value.substr(139, 3).alias("GEWEI"),
                input_file_3_df.value.substr(175, 8).alias("FBUDA"),
                input_file_3_df.value.substr(264, 18).alias("MATNR"),
                input_file_3_df.value.substr(282, 40).alias("ARKTX"),
                input_file_3_df.value.substr(382, 4).alias("VSTEL"),
                input_file_3_df.value.substr(395, 4).alias("WERKS"),
                input_file_3_df.value.substr(479, 1).alias("SHKZG")
            )

        input_3_cnt = input_3_df.count()
        logger.info("Created df for input file: {} with trimmed and upper cols, df has {} rows"
                    .format(file_name_3, str(input_3_cnt)))
        ####################################################################################
        # Step 2: Join source files and perform final transformations                      #
        ####################################################################################

        # Filters
        # FL_LOCAL_INVOICE_TYPE
        input_2_df = input_2_df.where(func.trim(func.upper(col("FKART"))) != 'ZFD3')
        # FL_LOCAL_SHIP_LOCATION
        input_3_df = input_3_df.where(~func.trim(func.upper(col("VSTEL"))).isin('BP01', 'BP02'))

        # Joins
        # JNR_invoice_header_line_cond
        joined_1_df = input_1_df.join(input_2_df,
                                      input_1_df["KNUMV"] == input_2_df["KNUMV1"],
                                      how='left_outer')
        # JNR_hdr_line_cond
        joined_2_df = joined_1_df.join(input_3_df,
                                       (joined_1_df["VBELN"] == input_3_df["VBELN1"]) &
                                       (joined_1_df["KPOSN"] == input_3_df["POSNR"]),
                                       how='left_outer')

        # FIL_invoice_line_cond
        """
        is_spaces(SAKN1)= false
        and
        isnull(SAKN1) = false
        and 
        ltrim(rtrim(upper(VKORG)))  != 'BP01'
        and 
        ltrim(rtrim(upper(VKORG)))  != 'BP02'
        """
        transformed_df = joined_2_df\
            .where(~func.trim(func.upper(col("VKORG"))).isin(['BP01', 'BP02']))\
            .where(~((col("SAKN1").isNull()) | (func.length(func.trim(col("SAKN1"))) == 0)))

        transformed_df = transformed_df\
            .withColumn("v_invoice_line_value", func.when(func.instr(col("KWERT"), '-') == 0, func.rtrim(col("KWERT").cast(DoubleType())))
                        .otherwise(func.split(col("KWERT"), '-')[0].cast(DoubleType()) * -1.0))\
            .select(
                func.trim(func.upper(col("VBELN"))).alias("LOCAL_INVOICE_ID"),
                func.lit(sys_id).cast(StringType()).alias("SYSTEM_ID"),
                func.trim(func.upper(col("SAKN1"))).alias("LOCAL_REV_ACCT_ID"),
                func.lit(None).cast(StringType()).alias("REV_ACCT_ID"),
                func.when(is_float_udf(col("KPOSN")), col("KPOSN").cast(DoubleType()))
                    .otherwise(func.lit(0).cast(DoubleType()))
                    .cast(DecimalType(12, 4))
                    .alias("LINE_NUMBER"),
                func.trim(func.upper(col("ARKTX"))).alias("LINE_DESC"),
                func.trim(func.upper(col("KUNWE"))).alias("SHIP_CUSTOMER_ID"),
                func.lit(None).cast(StringType()).alias("DISTRIBUTION_CHANNEL"),
                func.lit(None).cast(StringType()).alias("END_USE_CLASS"),
                func.lit(None).cast(StringType()).alias("QUALITY_CLASS"),
                func.trim(func.upper(col("MATNR"))).alias("SHIP_MATERIAL_ID"),
                func.trim(func.upper(col("VSTEL"))).alias("LOCAL_SHIP_LOCATION_ID"),
                func.lit(None).cast(StringType()).alias("SHIP_LOCATION_ID"),
                func.trim(func.upper(col("WERKS"))).alias("LOCAL_MFG_LOCATION_ID"),
                func.lit(None).cast(StringType()).alias("MFG_LOCATION_ID"),
                func.when((func.trim(func.upper(col("KOAID"))) == 'B') & (func.trim(func.upper(col("SHKZG"))) == 'X'), (-1.0 * col("FKIMG").cast(DoubleType())))
                    .otherwise(func.when(func.trim(func.upper(col("KOAID"))) == 'B', col("FKIMG").cast(DoubleType())).otherwise(func.lit(0).cast(DoubleType())))
                    .cast(DecimalType(20, 4))
                    .alias("INVOICE_LINE_QTY"),
                func.trim(func.upper(col("VRKME"))).alias("LOCAL_INVOICE_UOM_ID"),
                func.lit(None).cast(StringType()).alias("INVOICE_UOM_ID"),
                func.when((func.trim(func.upper(col("KOAID"))) == 'B') & (func.trim(func.upper(col("SHKZG"))) == 'X'), (-1.0 * col("NTGEW").cast(DoubleType())))
                    .otherwise(func.when(func.trim(func.upper(col("KOAID"))) == 'B', col("NTGEW").cast(DoubleType())).otherwise(func.lit(0).cast(DoubleType())))
                    .cast(DecimalType(20, 4))
                    .alias("SHIP_WEIGHT_QTY"),
                func.trim(func.upper(col("GEWEI"))).alias("LOCAL_SHIP_WEIGHT_UOM_ID"),
                func.lit(None).cast(StringType()).alias("SHIP_WEIGHT_UOM_ID"),
                func.when(func.trim(func.upper(col("SHKZG"))) == 'X', (-1.0 * col("v_invoice_line_value") * col("KURRF").cast(DoubleType())))
                    .otherwise(col("v_invoice_line_value") * col("KURRF").cast(DoubleType()))
                    .cast(DecimalType(20, 4))
                    .alias("INVOICE_LINE_VALUE"),
                func.lit('BRL').cast(StringType()).alias("LOCAL_CURRENCY_ID"),
                func.lit(None).cast(StringType()).alias("CURRENCY_ID"),
                is_date_udf(col("FBUDA")).cast(TimestampType()).alias("INVOICE_DATE"),
                func.lit(source_type).cast(StringType()).alias("SOURCE_TYPE"),
                func.lit(format_current_dt()).cast(TimestampType()).alias("DATESTAMP"),
                func.lit(None).cast(StringType()).alias("PROCESSING_FLAG")
            )
        ####################################################################################
        # Step 3: Write transformed dataframe to staging                                   #
        ####################################################################################

        target_schema = spark.sql("select * from ppmods.{}".format(staging_file)).schema
        staging_s3_path = "s3://" + datalake_bucket + "/Transformed/" + staging_file + "/"
        if validate_schema(transformed_df.schema, target_schema):
            transformed_df.write.parquet(staging_s3_path, mode='append', compression='snappy')
        else:
            raise ValueError("Transformed df does not match schema of target table: {}".format(staging_file))

        logger.info("Successfully wrote transformed df and input file")

        ####################################################################################
        # Step 4. Write out the log details.                                               #
        ####################################################################################
        input_file_cnt_list = ",".join([str(input_1_cnt)])
        write_log_file(workflow_name, sys_id, file_name_list, input_file_cnt_list, env, start_time, '', 'Success',
                       system_bucket, spark)
        system_cleanup(spark)

    except ValueError:
        logger.error("Transformed df does not match schema of target table: {}".format(staging_file))
        system_cleanup(spark)
    except Exception as e:
        # Explicitly passing blank value for input_file_cnt_list to avoid exception if file counts are not initialized
        # input_file_cnt_list = str(input_file_cnt_1)
        write_log_file(workflow_name, sys_id, file_name_list, '', env, start_time, e, 'Error',
                       system_bucket, spark)
        logger.error(e)
        system_cleanup(spark)


if __name__ == "__main__":
    main()
