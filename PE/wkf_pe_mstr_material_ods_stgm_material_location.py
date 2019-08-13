#
#########################################################################################
# File Name: wkf_pe_mstr_material_ods_stgm_material_location.py                         #
# Author: Slalom LLC, Gee Tam                                                           #
# Version: 1.0                                                                          #
# Update Date: 03/27/2019                                                               #
#                                                                                       #
# Description: The purpose of this process is to consume the raw extract file(s) from   #
#              the S3 landing zone and perform the ETL process to load the records      #
#              into the parquet file: stgm_material_location.                           #
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
from pyspark.sql.window import Window
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
appName = 'Python Spark Session - PE MSTR MATERIAL LOCATION - Stage'
attrCurrencyID = 'USD'
attrInvLocType1 = 'FIN'
attrInvLocType2 = 'RAW'
attrLocID1 = '02'
attrLocID2 = '04'
attrLocID3 = '50'
attrLocID4 = '20'
attrLocID5 = '10'
attrLocID6 = '99'
attrLocID7 = '95'
attrLocID8 = '92'
attrLocID9 = '97'
attrLocID10 = '98'
env = 'Stage'
fileName1 = 'PE_JOB.TXT'
fileName2 = 'PE_MSTR_MATERIAL.TXT'
fileName3 = 'PE_MSTR_RAW_MATERIAL.TXT'
fileNameList = fileName1 + ',' + fileName2 + ',' + fileName3
folderName1 = 'PE/JOB'
folderName2 = 'PE/MATERIAL'
folderName3 = 'PE/MATERIAL'
inputFileCnt1 = 0
inputFileCnt2 = 0
inputFileCnt3 = 0
inputFileCntList = ' '
now = datetime.datetime.now()
sourceType = 'FMS'
stagingFile = 'stgm_material_location'
startTime = datetime.datetime.now()
sysID = 'PE'
sysName = 'FMS'
systemMessage = ''
timeStamp = now.isoformat()
workFlowName = 'wkf_pe_mstr_material_ods_stgm_material_location'
#
####################################################################################
# Define the input file schema(s).                                                 #
####################################################################################
inputFile1Schema = StructType([
    StructField('Plant', StringType(), True),
    StructField('Customer', StringType(), True),
    StructField('Customer_Name', StringType(), True),
    StructField('Product_Group_Code', StringType(), True),
    StructField('Product_Group_Descp', StringType(), True),
    StructField('End_Use_Code', StringType(), True),
    StructField('End_Use_Descrip', StringType(), True),
    StructField('Item_no', StringType(), True),
    StructField('Item_Description', StringType(), True),
    StructField('ORDER_no', StringType(), True),
    StructField('Line_no', StringType(), True),
    StructField('Invoiced_qty', StringType(), True),
    StructField('Order_Date', StringType(), True),
    StructField('H/R_Date', StringType(), True),
    StructField('Closed_Date', StringType(), True),
    StructField('Act_Substrate_Amt', StringType(), True),
    StructField('Est_Substrate_Amt', StringType(), True),
    StructField('Act_Oth_Mat_Amt', StringType(), True),
    StructField('Est_Oth_Mat_Amt', StringType(), True),
    StructField('Act_Labor_Amt', StringType(), True),
    StructField('Est_Labor_Amt', StringType(), True),
    StructField('Act_Overhead_Amt', StringType(), True),
    StructField('Est_Overhead_Amt', StringType(), True),
    StructField('Act_Revenue', StringType(), True)
])
#
inputFile2Schema = StructType([
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
inputFile3Schema = StructType([
        StructField('kitemkey', StringType(), True),
        StructField('kmatgrp', StringType(), True),
        StructField('kmatcode', StringType(), True),
        StructField('mattype', StringType(), True),
        StructField('scbtype1', StringType(), True),
        StructField('scbtype2', StringType(), True),
        StructField('descrip', StringType(), True),
        StructField('grade', StringType(), True),
        StructField('brand', StringType(), True),
        StructField('mill', StringType(), True),
        StructField('lngth', StringType(), True),
        StructField('wdth', StringType(), True),
        StructField('dpth', StringType(), True),
        StructField('caliper', StringType(), True),
        StructField('gsm', StringType(), True),
        StructField('grain', StringType(), True),
        StructField('colour', StringType(), True),
        StructField('core_diam', StringType(), True),
        StructField('core_wt', StringType(), True),
        StructField('stocked', StringType(), True),
        StructField('traced', StringType(), True),
        StructField('allocated', StringType(), True),
        StructField('catalogued', StringType(), True),
        StructField('batched', StringType(), True),
        StructField('binned', StringType(), True),
        StructField('rep_price_1', StringType(), True),
        StructField('rep_price_2', StringType(), True),
        StructField('rep_price_3', StringType(), True),
        StructField('rep_date_1', StringType(), True),
        StructField('rep_date_2', StringType(), True),
        StructField('rep_date_3', StringType(), True),
        StructField('price_unit', StringType(), True),
        StructField('price_1', StringType(), True),
        StructField('price_2', StringType(), True),
        StructField('price_3', StringType(), True),
        StructField('price_4', StringType(), True),
        StructField('price_5', StringType(), True),
        StructField('price_6', StringType(), True),
        StructField('price_7', StringType(), True),
        StructField('price_8', StringType(), True),
        StructField('price_9', StringType(), True),
        StructField('price_10', StringType(), True),
        StructField('cost_unit', StringType(), True),
        StructField('ord_unit', StringType(), True),
        StructField('stock_unit', StringType(), True),
        StructField('actiongrp', StringType(), True),
        StructField('disp_seq_1', StringType(), True),
        StructField('disp_seq_2', StringType(), True),
        StructField('disp_seq_3', StringType(), True),
        StructField('unit_wt', StringType(), True),
        StructField('note_1', StringType(), True),
        StructField('note_2', StringType(), True),
        StructField('note_3', StringType(), True),
        StructField('note_4', StringType(), True),
        StructField('round_qty', StringType(), True),
        StructField('coverage', StringType(), True),
        StructField('coverage2', StringType(), True),
        StructField('coverage3', StringType(), True),
        StructField('gl_code_1', StringType(), True),
        StructField('gl_code_2', StringType(), True),
        StructField('gl_code_3', StringType(), True),
        StructField('gl_code_4', StringType(), True),
        StructField('nom_code', StringType(), True),
        StructField('ave_lngth', StringType(), True),
        StructField('stk_checked', StringType(), True),
        StructField('shelf_life', StringType(), True),
        StructField('vr_supp_1', StringType(), True),
        StructField('vr_recpri_1', StringType(), True),
        StructField('amtype', StringType(), True),
        StructField('kgeneric', StringType(), True),
        StructField('kaudit_flag', StringType(), True),
        StructField('kdate', StringType(), True),
        StructField('ktime', StringType(), True),
        StructField('kuser', StringType(), True),
        StructField('del_status', StringType(), True),
        StructField('del_statdate', StringType(), True),
        StructField('del_statuser', StringType(), True),
        StructField('stk_status', StringType(), True),
        StructField('stk_date', StringType(), True),
        StructField('stk_user', StringType(), True),
        StructField('contract_stk', StringType(), True),
        StructField('iss_to_job', StringType(), True),
        StructField('stk_val_tol', StringType(), True),
        StructField('stk_perc_tol', StringType(), True),
        StructField('stk_global', StringType(), True),
        StructField('stk_val_req', StringType(), True),
        StructField('stk_perc_req', StringType(), True),
        StructField('density', StringType(), True),
        StructField('yield', StringType(), True),
        StructField('table_length_1', StringType(), True),
        StructField('table_price_1', StringType(), True),
        StructField('table_width_1', StringType(), True),
        StructField('core_inside_1', StringType(), True),
        StructField('core_outside_1', StringType(), True),
        StructField('core_width_1', StringType(), True),
        StructField('ext_diam_1', StringType(), True),
        StructField('def_stkdays', StringType(), True),
        StructField('diff_1_speed', StringType(), True),
        StructField('diff_1_waste', StringType(), True),
        StructField('diff_2_speed', StringType(), True),
        StructField('diff_2_waste', StringType(), True),
        StructField('def_poqtywt', StringType(), True),
        StructField('sup_prod_type', StringType(), True),
        StructField('per100kg', StringType(), True),
        StructField('vr_desc_1', StringType(), True),
        StructField('vr_code_1', StringType(), True),
        StructField('vr_lead_1', StringType(), True),
        StructField('auto_gen', StringType(), True),
        StructField('auto_inv', StringType(), True),
        StructField('std_price', StringType(), True),
        StructField('std_charge', StringType(), True),
        StructField('qty_limit', StringType(), True),
        StructField('add_charge', StringType(), True),
        StructField('margin', StringType(), True),
        StructField('vat_code', StringType(), True),
        StructField('kco', StringType(), True),
        StructField('critical_path', StringType(), True),
        StructField('klink', StringType(), True),
        StructField('min_mat_qty', StringType(), True),
        StructField('imp_wght', StringType(), True),
        StructField('gulf_weight', StringType(), True),
        StructField('ref', StringType(), True),
        StructField('elect_image', StringType(), True),
        StructField('recycle_cont', StringType(), True),
        StructField('finish_code', StringType(), True),
        StructField('special_effect', StringType(), True),
        StructField('customer_stk', StringType(), True),
        StructField('kmfr_ref', StringType(), True),
        StructField('auto_desc', StringType(), True),
        StructField('mat_desc2', StringType(), True),
        StructField('mat_status', StringType(), True),
        StructField('def_klocsn', StringType(), True),
        StructField('def_kbinloc', StringType(), True),
        StructField('prev_matcode', StringType(), True),
        StructField('mat_std_stock', StringType(), True),
        StructField('mat_master_price', StringType(), True),
        StructField('mat_link_price', StringType(), True),
        StructField('mat_link_kitemkey', StringType(), True),
        StructField('mat_req_type', StringType(), True),
        StructField('mat_sub_type', StringType(), True),
        StructField('mat_solids', StringType(), True),
        StructField('lbpergallon', StringType(), True),
        StructField('kgperlitre', StringType(), True),
        StructField('master_roll', StringType(), True),
        StructField('mast_roll_width', StringType(), True),
        StructField('wght', StringType(), True),
        StructField('core_diam_int', StringType(), True),
        StructField('wght_unit', StringType(), True),
        StructField('core_diam_ext', StringType(), True),
        StructField('min_mat_charge_unit', StringType(), True),
        StructField('min_mat_charge_qty', StringType(), True),
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
        StructField('FIELD215', StringType(), True),
        StructField('FIELD216', StringType(), True),
        StructField('FIELD217', StringType(), True),
        StructField('FIELD218', StringType(), True),
        StructField('FIELD219', StringType(), True),
        StructField('FIELD220', StringType(), True),
        StructField('FIELD221', StringType(), True),
        StructField('FIELD222', StringType(), True),
        StructField('FIELD223', StringType(), True),
        StructField('FIELD224', StringType(), True),
        StructField('FIELD225', StringType(), True),
        StructField('FIELD226', StringType(), True),
        StructField('FIELD227', StringType(), True),
        StructField('FIELD228', StringType(), True),
        StructField('FIELD229', StringType(), True),
        StructField('FIELD230', StringType(), True),
        StructField('FIELD231', StringType(), True),
        StructField('FIELD232', StringType(), True),
        StructField('FIELD233', StringType(), True),
        StructField('FIELD234', StringType(), True),
        StructField('FIELD235', StringType(), True),
        StructField('FIELD236', StringType(), True),
        StructField('FIELD237', StringType(), True),
        StructField('FIELD238', StringType(), True),
        StructField('FIELD239', StringType(), True),
        StructField('FIELD240', StringType(), True),
        StructField('FIELD241', StringType(), True),
        StructField('FIELD242', StringType(), True),
        StructField('FIELD243', StringType(), True),
        StructField('FIELD244', StringType(), True),
        StructField('FIELD245', StringType(), True),
        StructField('FIELD246', StringType(), True),
        StructField('FIELD247', StringType(), True),
        StructField('FIELD248', StringType(), True),
        StructField('FIELD249', StringType(), True),
        StructField('FIELD250', StringType(), True),
        StructField('FIELD251', StringType(), True),
        StructField('FIELD252', StringType(), True),
        StructField('FIELD253', StringType(), True),
        StructField('FIELD254', StringType(), True),
        StructField('FIELD255', StringType(), True),
        StructField('FIELD256', StringType(), True),
        StructField('FIELD257', StringType(), True),
        StructField('FIELD258', StringType(), True),
        StructField('FIELD259', StringType(), True),
        StructField('FIELD260', StringType(), True),
        StructField('FIELD261', StringType(), True),
        StructField('FIELD262', StringType(), True),
        StructField('FIELD263', StringType(), True),
        StructField('FIELD264', StringType(), True),
        StructField('FIELD265', StringType(), True),
        StructField('FIELD266', StringType(), True),
        StructField('FIELD267', StringType(), True),
        StructField('FIELD268', StringType(), True),
        StructField('FIELD269', StringType(), True),
        StructField('FIELD270', StringType(), True),
        StructField('FIELD271', StringType(), True),
        StructField('FIELD272', StringType(), True),
        StructField('FIELD273', StringType(), True),
        StructField('FIELD274', StringType(), True),
        StructField('FIELD275', StringType(), True),
        StructField('FIELD276', StringType(), True),
        StructField('FIELD277', StringType(), True),
        StructField('FIELD278', StringType(), True),
        StructField('FIELD279', StringType(), True),
        StructField('FIELD280', StringType(), True),
        StructField('FIELD281', StringType(), True),
        StructField('FIELD282', StringType(), True),
        StructField('FIELD283', StringType(), True),
        StructField('FIELD284', StringType(), True),
        StructField('FIELD285', StringType(), True),
        StructField('FIELD286', StringType(), True),
        StructField('FIELD287', StringType(), True),
        StructField('FIELD288', StringType(), True),
        StructField('FIELD289', StringType(), True),
        StructField('FIELD290', StringType(), True),
        StructField('FIELD291', StringType(), True),
        StructField('FIELD292', StringType(), True),
        StructField('FIELD293', StringType(), True),
        StructField('FIELD294', StringType(), True),
        StructField('FIELD295', StringType(), True),
        StructField('FIELD296', StringType(), True),
        StructField('FIELD297', StringType(), True),
        StructField('FIELD298', StringType(), True),
        StructField('FIELD299', StringType(), True),
        StructField('FIELD300', StringType(), True),
        StructField('FIELD301', StringType(), True),
        StructField('FIELD302', StringType(), True)
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
#
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
        # Step 1. Read the 1st input file and format only the required fields - JOB.               #
        ############################################################################################
        print('START: Reading input file(s) from the S3 landing/received bucket \n')
        inputFile1 = 's3://' + IbucketName + '/Landing/Received/' + folderName1 + '/' + fileName1
        inputFile1DF = spark.read.csv(inputFile1, schema=inputFile1Schema, sep = ';', header=True, nullValue='null')
        input1DF = inputFile1DF.select ('Plant', 'Item_no', 'Invoiced_qty', 'Act_Substrate_Amt', 'Act_Oth_Mat_Amt', 'Act_Labor_Amt', 'Act_Overhead_Amt')
        inputFileCnt1 = input1DF.count()
        input1DF = trim_and_upper(input1DF)
		#
        print ('SUCCESS: Created the dataframe for input file - PE JOB \n')
        #
        ############################################################################################
        # Step 2. Read the 2nd input file and format only the required fields - MSTR MATERIAL.     #
        ############################################################################################
        inputFile2 = 's3://' + IbucketName + '/Landing/Received/' + folderName2 + '/' + fileName2
        inputFile2DF = spark.read.csv(inputFile2, schema=inputFile2Schema, sep = ';', header=True, nullValue='null')
        input2DF = inputFile2DF.select ('kvseqnum', 'kvar_code')
        inputFileCnt2 = input2DF.count()
        input2DF = trim_and_upper(input2DF)
		#
        print ('SUCCESS: Created the dataframe for input file - PE MSTR MATERIAL \n')
        #
        ############################################################################################
        # Step 3. Read the 3rd input file and format only the required fields - MSTR RAW MATERIAL. #
        ############################################################################################
        inputFile3 = 's3://' + IbucketName + '/Landing/Received/' + folderName3 + '/' + fileName3
        inputFile3DF = spark.read.csv(inputFile3, schema=inputFile3Schema, sep = ';', header=True, nullValue='null')
        input3DF = inputFile3DF.select ('kmatcode', 'kitemkey')
        inputFileCnt3 = input3DF.count()
        input3DF = trim_and_upper(input3DF)
        #
        print ('SUCCESS: Created the dataframe for input file - PE MSTR RAW MATERIAL \n')
        #
        ############################################################################################
        # Step 4. Create an aggregate datframe by performing a groupby to aggregate sum 5 columns. #
        ############################################################################################
        temp1DF = input1DF.withColumn('Invoiced_qty_dec', func.regexp_replace(func.col('Invoiced_qty'), ',', '').cast(DecimalType(10,4))) \
                    .withColumn('Act_Substrate_Amt_dec', func.regexp_replace(func.col('Act_Substrate_Amt'), ',', '').cast(DecimalType(31,4))) \
                    .withColumn('Act_Oth_Mat_Amt_dec', func.regexp_replace(func.col('Act_Oth_Mat_Amt'), ',', '').cast(DecimalType(31,4))) \
                    .withColumn('Act_Labor_Amt_dec', func.regexp_replace(func.col('Act_Labor_Amt'), ',', '').cast(DecimalType(31,4))) \
                    .withColumn('Act_Overhead_Amt_dec', func.regexp_replace(func.col('Act_Overhead_Amt'), ',', '').cast(DecimalType(31,4)))
        #
        window1 = (Window().partitionBy(temp1DF.Plant, temp1DF.Item_no).rowsBetween(-sys.maxsize, sys.maxsize))
        agg1DF = temp1DF.withColumn('Invoiced_qty1', func.sum(temp1DF.Invoiced_qty_dec).over(window1)) \
                        .withColumn('o_Act_Substrate_Amt', func.sum(temp1DF.Act_Substrate_Amt_dec).over(window1)) \
                        .withColumn('o_Act_Oth_Mat_Amt', func.sum(temp1DF.Act_Oth_Mat_Amt_dec).over(window1)) \
                        .withColumn('o_Act_Labor_Amt', func.sum(temp1DF.Act_Labor_Amt_dec).over(window1)) \
                        .withColumn('o_Act_Overhead_Amt', func.sum(temp1DF.Act_Overhead_Amt_dec).over(window1))
        #
        print ('SUCCESS: Created the aggregated dataframe \n')
        #
        ############################################################################################
        # Step 5. Add new columns to the aggregated dataframe.                                     #
        ############################################################################################
        etl1DF = agg1DF.withColumn('v_invoiced_qty', func.when(func.col('Invoiced_qty1') == 0, func.lit(1).cast(DecimalType(10,4))) \
                                                     .otherwise(func.col('Invoiced_qty1').cast(DecimalType(10,4)))) \
                         .withColumn('Item_no_temp1', func.substring(func.lpad(func.col('Item_no'),5,'0'),1,2)) \
                         .withColumn('Item_no_temp2', func.substring(func.lpad(func.col('Item_no'),5,'0'),-3,3)) \
                         .withColumn('Item_no_1', func.concat(func.col('Item_no_temp1'), func.lit(','), func.col('Item_no_temp2'))) \
                         .withColumn('avg_cost', ((func.col('o_Act_Substrate_Amt') + func.col('o_Act_Oth_Mat_Amt') + func.col('o_Act_Labor_Amt') + func.col('o_Act_Overhead_Amt'))/func.col('v_invoiced_qty')).cast(DecimalType(31,4)))
        #
        print ('SUCCESS: Created the 1st dataframe with the new columns \n')
        #
        ############################################################################################
        # Step 6. Add new columns to the 2nd input file dataframe.                                 #
        ############################################################################################
        etl2DF =  input2DF.withColumn('v_kvseqnum', func.regexp_replace(func.col('kvseqnum'), ',', '')) \
                          .withColumn('v_kvseqnum_temp1', func.substring(func.lpad(func.col('v_kvseqnum'),5,'0'),1,2)) \
                          .withColumn('v_kvseqnum_temp2', func.substring(func.lpad(func.col('v_kvseqnum'),5,'0'),-3,3)) \
                          .withColumn('material_id', func.concat(func.col('v_kvseqnum_temp1'), func.lit(','), func.col('v_kvseqnum_temp2')))
        #
        print ('SUCCESS: Created the 2nd dataframe with the new columns \n')
        #
        ############################################################################################
        # Step 7. Join the 1st ETL dataframe to the 2nd ETL dataframe.                             #
        ############################################################################################
        join1DF = etl1DF.join(etl2DF, (etl1DF.Item_no_1 == etl2DF.material_id), 'right_outer')
        join1DF = join1DF.dropDuplicates(['material_id']) # drop after testing
        print ('SUCCESS: Created the joined dataframe \n')
        #
        ############################################################################################
        # Step 8. Add columnbs to the joined dataframe.                                            #
        ############################################################################################
        etl3DF = join1DF.withColumn('o_local_location_id1', func.lit(attrLocID1)) \
                        .withColumn('o_local_location_id2', func.lit(attrLocID2)) \
                        .withColumn('o_local_location_id3', func.lit(attrLocID3)) \
                        .withColumn('o_local_location_id4', func.lit(attrLocID4)) \
                        .withColumn('o_local_location_id5', func.lit(attrLocID5)) \
                        .withColumn('o_local_location_id6', func.lit(attrLocID6)) \
                        .withColumn('o_local_location_id7', func.lit(attrLocID7)) \
                        .withColumn('o_local_location_id8', func.lit(attrLocID8)) \
                        .withColumn('o_local_location_id9', func.lit(attrLocID9)) \
                        .withColumn('o_local_location_id10', func.lit(attrLocID10)) \
                        .withColumn('plant_3', func.substring(func.col('Plant'),1,3)) \
                        .withColumn('plant_4', func.substring(func.col('Plant'),1,4)) \
                        .withColumn('std_cost_1', func.when(func.col('plant_3') == 'LOU', func.col('avg_cost').cast(DecimalType(30,15))) \
                                                       .otherwise(func.lit(0).cast(DecimalType(30,15)))) \
                        .withColumn('std_cost_2', func.when(func.col('plant_3') == 'WAR', func.col('avg_cost').cast(DecimalType(30,15))) \
                                                       .otherwise(func.lit(0).cast(DecimalType(30,15)))) \
                        .withColumn('std_cost_3', func.when(func.col('plant_3') == 'MEB', func.col('avg_cost').cast(DecimalType(30,15))) \
                                                       .otherwise(func.lit(0).cast(DecimalType(30,15)))) \
                        .withColumn('std_cost_4', func.lit(0).cast(DecimalType(30,15))) \
                        .withColumn('std_cost_5', func.when(func.col('plant_3') == 'CAG', func.col('avg_cost').cast(DecimalType(30,15))) \
                                                       .otherwise(func.lit(0).cast(DecimalType(30,15)))) \
                        .withColumn('std_cost_6', func.when(func.col('plant_4') == 'OSHC', func.col('avg_cost').cast(DecimalType(30,15))) \
                                                       .otherwise(func.lit(0).cast(DecimalType(30,15)))) \
                        .withColumn('std_cost_7', func.when(func.col('plant_4') == 'OSPC', func.col('avg_cost').cast(DecimalType(30,15))) \
                                                       .otherwise(func.lit(0).cast(DecimalType(30,15)))) \
                        .withColumn('std_cost_8', func.when(func.col('plant_3') == 'TOB', func.col('avg_cost').cast(DecimalType(30,15))) \
                                                       .otherwise(func.lit(0).cast(DecimalType(30,15)))) \
                        .withColumn('std_cost_9', func.when(func.col('plant_4') == 'OSNL', func.col('avg_cost').cast(DecimalType(30,15))) \
                                                       .otherwise(func.lit(0).cast(DecimalType(30,15)))) \
                        .withColumn('std_cost_10', func.when(func.col('plant_4') == 'PRG', func.col('avg_cost').cast(DecimalType(30,15)))
                                                       .otherwise(func.lit(0).cast(DecimalType(30,15))))
        print('SUCCESS: Added local location ids and standard costs fields \n')
        #
        ############################################################################################
        # Step 9. Normalizer step.                                                                 #
        ############################################################################################
        etl3DF.createOrReplaceTempView('temp_normalize_tbl')
        etl3DF = spark.sql("""
                    SELECT material_id, o_local_location_id1 as local_location_id, std_cost_1 as std_cost_temp FROM temp_normalize_tbl
                    union all
                    SELECT material_id, o_local_location_id2 as local_location_id, std_cost_2 as std_cost_temp FROM temp_normalize_tbl
                    union all
                    SELECT material_id, o_local_location_id3 as local_location_id, std_cost_3 as std_cost_temp FROM temp_normalize_tbl
                    union all
                    SELECT material_id, o_local_location_id4 as local_location_id, std_cost_4 as std_cost_temp FROM temp_normalize_tbl
                    union all
                    SELECT material_id, o_local_location_id5 as local_location_id, std_cost_5 as std_cost_temp FROM temp_normalize_tbl
                    union all
                    SELECT material_id, o_local_location_id6 as local_location_id, std_cost_6 as std_cost_temp FROM temp_normalize_tbl
                    union all
                    SELECT material_id, o_local_location_id7 as local_location_id, std_cost_7 as std_cost_temp FROM temp_normalize_tbl
                    union all
                    SELECT material_id, o_local_location_id8 as local_location_id, std_cost_8 as std_cost_temp FROM temp_normalize_tbl
                    union all
                    SELECT material_id, o_local_location_id9 as local_location_id, std_cost_9 as std_cost_temp FROM temp_normalize_tbl
                    union all
                    SELECT material_id, o_local_location_id10 as local_location_id, std_cost_10 as std_cost_temp FROM temp_normalize_tbl
        """)
        print ('SUCCESS: Normalized the joined dataframe to create new rows \n')
        #
        ############################################################################################
        # Step 10. Create the dataframe that will be used as input to the staging parquet          #
        #          file / table. The columns will be selected in the order to match the column     #
        #          order in the Oracle table.                                               # 
        ############################################################################################
        etl3DF = etl3DF.withColumn('system_id', func.lit(sysID)) \
                        .withColumn('location_id', func.lit(None).cast(StringType())) \
                        .withColumn('std_cost', func.col('std_cost_temp').cast(DecimalType(30,15))) \
                        .withColumn('local_currency_id', func.lit(attrCurrencyID)) \
                        .withColumn('currency_id', func.lit(None).cast(StringType())) \
                        .withColumn('local_profit_center_id', func.lit(None).cast(StringType())) \
                        .withColumn('profit_center_id', func.lit(None).cast(StringType())) \
                        .withColumn('local_inventory_type', func.lit(attrInvLocType1)) \
                        .withColumn('inventory_type', func.lit(None).cast(StringType())) \
                        .withColumn('material_class', func.lit(None).cast(StringType())) \
                        .withColumn('source_type',  func.lit(sysName)) \
                        .withColumn('datestamp', func.lit(format_current_dt ()).cast("timestamp")) \
                        .withColumn('processing_flag', func.lit(None).cast(StringType()))
        #
        etl3DF = etl3DF.select('material_id','system_id','local_location_id','location_id','std_cost',
                              'local_currency_id','currency_id', 'local_profit_center_id', 'profit_center_id', 'local_inventory_type', 
                              'inventory_type', 'material_class', 'source_type', 'datestamp', 'processing_flag')
        print ('SUCCESS: Added remaining columns to the JOB/MSTR MATERIAL dataframe \n')
        #
        ############################################################################################
        # Step 11. Create the dataframe for the MSTR RAW file that will be used to append to the   #
        #          dataframe created from the material workflow.                                   #
        ############################################################################################
        etl4DF = input3DF.withColumn('o_local_location_id1', func.lit(attrLocID1)) \
                        .withColumn('o_local_location_id2', func.lit(attrLocID2)) \
                        .withColumn('o_local_location_id3', func.lit(attrLocID3)) \
                        .withColumn('o_local_location_id4', func.lit(attrLocID4)) \
                        .withColumn('o_local_location_id5', func.lit(attrLocID5)) \
                        .withColumn('o_local_location_id6', func.lit(attrLocID6)) \
                        .withColumn('o_local_location_id7', func.lit(attrLocID7)) \
                        .withColumn('o_local_location_id8', func.lit(attrLocID8)) \
                        .withColumn('o_local_location_id9', func.lit(attrLocID9)) \
                        .withColumn('o_local_location_id10', func.lit(attrLocID10))
        print ('SUCCESS: Added new local location id columns to the MSTR RAW input dataframe \n')
        #
        ############################################################################################
        # Step 12. Normalizer step.                                                                #
        ############################################################################################
        etl4DF.createOrReplaceTempView('temp_normalize_tbl')
        etl4DF = spark.sql("""
                    SELECT kitemkey, o_local_location_id1 as local_location_id FROM temp_normalize_tbl
                    union all
                    SELECT kitemkey, o_local_location_id2 as local_location_id FROM temp_normalize_tbl
                    union all
                    SELECT kitemkey, o_local_location_id3 as local_location_id FROM temp_normalize_tbl
                    union all
                    SELECT kitemkey, o_local_location_id4 as local_location_id FROM temp_normalize_tbl
                    union all
                    SELECT kitemkey, o_local_location_id5 as local_location_id FROM temp_normalize_tbl
                    union all
                    SELECT kitemkey, o_local_location_id6 as local_location_id FROM temp_normalize_tbl
                    union all
                    SELECT kitemkey, o_local_location_id7 as local_location_id FROM temp_normalize_tbl
                    union all
                    SELECT kitemkey, o_local_location_id8 as local_location_id FROM temp_normalize_tbl
                    union all
                    SELECT kitemkey, o_local_location_id9 as local_location_id FROM temp_normalize_tbl
                    union all
                    SELECT kitemkey, o_local_location_id10 as local_location_id FROM temp_normalize_tbl
        """)
        print ('SUCCESS: Normalized the MSTR RAW input file dataframe to create new rows \n')
        #
        ############################################################################################
        # Step 13. Create the final dataframe for the MSTR RAW file that will be used to append to #
        #          the dataframe created from the material workflow.                               #
        ############################################################################################
        etl5DF = etl4DF.withColumn('material_id', func.col('kitemkey')) \
                        .withColumn('system_id', func.lit(sysID)) \
                        .withColumn('location_id', func.lit(None).cast(StringType())) \
                        .withColumn('std_cost', func.lit(0).cast(DecimalType(30,15))) \
                        .withColumn('local_currency_id', func.lit(attrCurrencyID)) \
                        .withColumn('currency_id', func.lit(None).cast(StringType())) \
                        .withColumn('local_profit_center_id', func.lit(None).cast(StringType())) \
                        .withColumn('profit_center_id', func.lit(None).cast(StringType())) \
                        .withColumn('local_inventory_type', func.lit(attrInvLocType2)) \
                        .withColumn('inventory_type', func.lit(None).cast(StringType())) \
                        .withColumn('material_class', func.lit(None).cast(StringType())) \
                        .withColumn('source_type',  func.lit(sysName)) \
                        .withColumn('datestamp', func.lit(format_current_dt ()).cast("timestamp")) \
                        .withColumn('processing_flag', func.lit(None).cast(StringType()))
        #
        etl5DF = etl5DF.select('material_id','system_id','local_location_id','location_id','std_cost',
                              'local_currency_id','currency_id', 'local_profit_center_id', 'profit_center_id', 'local_inventory_type', 
                              'inventory_type', 'material_class', 'source_type', 'datestamp', 'processing_flag')
        print ('SUCCESS: Added remaining columns to the MSTR RAW dataframe \n')
        #
        ############################################################################################
        # Step 14. Create a new Spark dataframe by appending the 2nd dataframe to the 1st one.     #
        ############################################################################################
        etl6DF =  etl3DF.union(etl5DF)
        print ('SUCCESS: Created the final dataframe by unioning the 3rd and 5th dataframes \n')
        #
        ############################################################################################
        # Step 15. Write the staging dataframe to the staging parquet file.                        #
        ############################################################################################
        etl6DF.write.parquet('s3://' + DbucketName + '/Transformed/' + stagingFile + '/', mode='append', compression='snappy')
        print('SUCCESS: Wrote to Staging file \n')
        #
        ############################################################################################
        # Step 16. Write out the log details.                                                      #
        ############################################################################################
        inputFileCntList = str(inputFileCnt1) + ',' + str(inputFileCnt2) + ',' + str(inputFileCnt3)   # concatenate the list of all of the file counts
        #
        write_log_file(workFlowName, sysID, fileNameList, inputFileCntList, env, startTime, '', 'Success', SbucketName, spark)
        print('SUCCESS: Wrote to log dataframe and parquet file \n')
        print('SUCCESS: End of job \n')
        system_cleanup(spark)
        #
    except Exception as e:
        inputFileCntList = str(inputFileCnt1) + ',' + str(inputFileCnt2) + ',' + str(inputFileCnt3)   # concatenate the list of all of the file counts
        write_log_file(workFlowName, sysID, fileNameList, inputFileCntList, env, startTime, e, 'Error', SbucketName, spark)
        print('ERROR: Workflow processing failed. See message below for details \n')
        print(e)
        system_cleanup(spark)
#
############################################################################################
# End of script                                                                            #
############################################################################################

