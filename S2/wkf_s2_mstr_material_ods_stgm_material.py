#########################################################################################
# File Name: wkf_s2_mstr_material_ods_stgm_material.py                                  #
# Author: Slalom LLC                                                                    #
# Version: 1.0                                                                          #
# Update Date: 05/30/2019                                                               #
#                                                                                       #
# Description: The purpose of this process is to consume the raw extract file from      #
#              the S3 landing zone and perform the ETL process to load the records      #
#              into the parquet file: stgm_material_location.                           #
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
        is_float_udf = udf(is_float)

        app_name = 'PySpark - S2 Mstr Material - Stgm'
        env = 'Stage'
        file_name_1 = 'S2_MSTR_MATERIAL_TEXT.TXT'
        file_name_2 = 'S2_MSTR_MATERIAL.TXT'
        file_name_3 = 'S2_MSTR_MATERIAL_LOCATION.TXT'
        filenames = [file_name_1, file_name_2, file_name_3]
        file_name_list = ",".join(filenames)
        folder_name_1 = 'S2/MSTR_MATERIAL_TEXT'
        folder_name_2 = 'S2/MSTR_MATERIAL'
        folder_name_3 = 'S2/MSTR_MATERIAL_LOCATION'
        source_type = 'FMS'
        staging_file = 'stgm_material'
        start_time = datetime.datetime.now()
        sys_id = 'S2'
        workflow_name = 'wkf_s2_mstr_material_ods_stgm_material'

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
        #  - Initialize lookup procedures
        #  - Read the input files                                                          #
        #  - Select required fields                                                        #
        #  - Apply rtrim, ltrim, and upper functions to all cols                           #
        ####################################################################################

        """ Source Definitions """

        # S2_MSTR_MATERIAL_TEXT flat file
        input_file_1 = "s3://" + ingest_bucket + "/Landing/Received/" + folder_name_1 + "/" + file_name_1
        input_file_1_df = spark.read.text(input_file_1)
        input_1_df = input_file_1_df\
            .select(
                input_file_1_df.value.substr(1, 18).alias("MATNR1"),
                input_file_1_df.value.substr(20, 40).alias("MAKTX")
            )

        input_1_cnt = input_1_df.count()

        logger.info("Created df for input file: {} with trimmed and upper cols, df has {} rows"
                    .format(file_name_1, str(input_1_cnt)))

        # S2_MSTR_MATERIAL flat file
        input_file_2 = "s3://" + ingest_bucket + "/Landing/Received/" + folder_name_2 + "/" + file_name_2
        input_file_2_df = spark.read.text(input_file_2)
        input_2_df = input_file_2_df.select(
                func.trim(func.upper(input_file_2_df.value.substr(1, 18))).alias("MATNR2"),
                input_file_2_df.value.substr(19, 8).alias("ERSDA"),
                func.trim(func.upper(input_file_2_df.value.substr(28, 4))).alias("MTART"),
                input_file_2_df.value.substr(33, 9).alias("MATKL"),
                input_file_2_df.value.substr(60, 3).alias("MEINS"),
                input_file_2_df.value.substr(95, 18).alias("PRDHA")
            )

        input_2_cnt = input_2_df.count()

        logger.info("Created df for input file: {} with trimmed and upper cols, df has {} rows"
                    .format(file_name_2, str(input_2_cnt)))

        # S2_MSTR_MATERIAL_LOCATION flat file
        input_file_3 = "s3://" + ingest_bucket + "/Landing/Received/" + folder_name_3 + "/" + file_name_3
        input_file_3_df = spark.read.text(input_file_3)
        input_3_df = input_file_3_df\
            .select(
                func.trim(func.upper(input_file_3_df.value.substr(1, 18))).alias("MATNR3"),
                input_file_3_df.value.substr(19, 4).alias("WERKS")
            )

        input_3_cnt = input_3_df.count()

        logger.info("Created df for input file: {} with trimmed and upper cols, df has {} rows"
                    .format(file_name_3, str(input_3_cnt)))

        ####################################################################################
        # Step 2: Join source files and perform final transformations                      #
        ####################################################################################
        # JNR_mstr_material
        joined_df = input_2_df.join(input_1_df,
                                    input_2_df["MATNR2"] == input_1_df["MATNR1"],
                                    "left_outer")

        # JNRTRANS
        joined_2_df = joined_df.join(input_3_df,
                                     joined_df["MATNR2"] == input_3_df["MATNR3"],
                                     "left_outer")

        transformed_df = joined_2_df.select(
            func.trim(func.upper(col("MATNR2"))).alias("MATERIAL_ID"),
            func.lit(sys_id).cast(StringType()).alias("SYSTEM_ID"),
            func.trim(func.upper(col("MAKTX"))).alias("MATERIAL_DESC"),
            func.trim(func.upper(col("MEINS"))).alias("LOCAL_BASE_UOM_ID"),
            func.lit(None).cast(StringType()).alias("BASE_UOM_ID"),
            func.date_format(col("ERSDA"), "yyyymmdd").cast(TimestampType()).alias("FIRST_SHIP_DATE"),
            func.when(col("WERKS").isin("BP01", "BP02") & col("WERKS").isin("ZWPA", "ZHAP"), func.trim(func.upper(col("MATNR2"))))
                .otherwise(func.concat(func.trim(func.upper(func.substring(col("PRDHA"), 1, 4))), func.lit("-"), func.trim(func.upper(col("MATKL")))))
                .alias("LOCAL_KPG_ID"),
            func.lit(None).cast(StringType()).alias("KPG_ID"),
            func.lit(None).cast(StringType()).alias("LOCAL_PFL_ID"),
            func.lit(None).cast(StringType()).alias("PFL_ID"),
            func.lit(None).cast(StringType()).alias("LOCAL_SPL_ID"),
            func.lit(None).cast(StringType()).alias("SPL_ID"),
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
            transformed_df.printSchema()
            raise ValueError("Transformed df does not match schema of target table: {}".format(staging_file))

        logger.info("Successfully wrote transformed df and input file")

        ####################################################################################
        # Step 4. Write out the log details.                                               #
        ####################################################################################
        input_file_cnt_list = str(input_1_cnt) + ',' + str(input_2_cnt)
        write_log_file(workflow_name, sys_id, file_name_list, input_file_cnt_list, env, start_time, '', 'Success',
                       system_bucket, spark)
        system_cleanup(spark)

    except ValueError as value_error:
        logger.error(value_error)
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
