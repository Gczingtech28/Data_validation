
import json
import csv
import pandas as pd
import numpy as np
import hashlib
from datetime import datetime
import configparser
import asyncio
from Validations.CsvParser import getDFfromCsv, getDFfromXlsxMerge, getDFfromXls, check_dtype, check_ruleValidation,getDFfromXlsx
from Validations.JsonParser import GetAllValueByKey, GetRules
from Validations.Utility import getUniqueValueList, list_contains
from Validations.SQLDriver import GetSQLDF,GetMSSQLDF,ExecuteSQLQuery
from multiprocessing import Pool

import nest_asyncio






# def process_chunk(chunk, sourcedf, destinationdf):
#     source_col = chunk.iloc[:, 0]  # Select the first column of the chunk (source)
    
#     # Assuming the destination_chunk is based on the same chunk index as the source chunk
#     destination_chunk = destinationdf.loc[chunk.index]
    
#     # Select the first column of the destination chunk
#     destination_col = destination_chunk.iloc[:, 0]
    
#     # Compare the source column with the corresponding destination chunk column
#     comparison_result = source_col == destination_col
    
#     return comparison_result



# def combine_results(results):
#     return pd.concat(results)

def compare_chunks(chunk1, chunk2):
            unmatched_values = pd.concat([chunk1, chunk2]).drop_duplicates(keep=False)
            return unmatched_values

        # Function to process chunks in parallel
def process_chunks_parallel(chunk_pairs, num_processes):
            with Pool(num_processes) as pool:
                results = pool.starmap(compare_chunks, chunk_pairs)
            return results


def main(configFilePath):

 async def DoubleDataValidation():
        #configFilePath="configuration.ini" 
        parser = configparser.ConfigParser()
        parser.read(configFilePath)
        SOURCE_TYPE = parser.get('APP', 'source_type')
        DESTINATION_TYPE = parser.get('APP', 'dest_type')
        
        subject = 'Data_Validation'
        date = datetime.now().strftime("%Y%m%d_%I%M%S")        
        output_file_path = parser.get('APP', 'output_file')
        reportOutputDir = output_file_path + '/report/'
        errorOutputDir = output_file_path + '/error/'
        
        sourcedf = pd.DataFrame()

        ############ Read Source DataFile #######

        ### CSV ####

        if SOURCE_TYPE == 'CSV':
            source_data_file_path = parser.get('SOURCE', 'source_data_file_path')
            skip_rows = parser.get('SOURCE', 'SKIP_ROWS')
            sourcedf = getDFfromCsv(source_data_file_path,skip_rows) 
            source_row, no_of_columns = sourcedf.shape  

        ### XLSX ####

        if SOURCE_TYPE == 'XLSX':
            source_data_file_path = parser.get('SOURCE', 'source_data_file_path')
            sheet_name = parser.get('SOURCE', 'sheet_name')
            skip_rows = parser.get('SOURCE', 'SKIP_ROWS')
            sourcedf = getDFfromXlsx(source_data_file_path, sheet_name, "", "", skip_rows)
            source_row, no_of_columns = sourcedf.shape

        ### MSSql ####
        if SOURCE_TYPE == 'MSSQL':
            server = parser.get('SOURCE', 'source_server')
            User = parser.get('SOURCE', 'source_user')
            Password = parser.get('SOURCE', 'source_password')
            Database = parser.get('SOURCE', 'source_database')
            SOURCE_SCHEMA_NAME = parser.get('vTurbineMasterData_Source', 'schema_name_source')
            SOURCE_TABLE_NAME = parser.get('vTurbineMasterData_Source' , 'source_query_filter')
            sourcedf = GetSQLDF(server, SOURCE_TABLE_NAME, User, Password, Database)
            source_row, no_of_columns = sourcedf.shape
            print("Source Rows =",source_row)

        ### MySql ####
        if SOURCE_TYPE == 'MYSQL':
            User = parser.get('SOURCE', 'source_user')
            Password = parser.get('SOURCE', 'source_password')
            Database = parser.get('SOURCE', 'source_database')
            SOURCE_SCHEMA_NAME = parser.get('vTurbineMasterData_Source' , 'schema_name_source')
            SOURCE_TABLE_NAME = parser.get('vTurbineMasterData_Source' , 'source_query_filter')
            task1 = asyncio.create_task(GetSQLDF(SOURCE_TABLE_NAME, User, Password, Database))
            sourcedf = await task1
            source_row, no_of_columns = sourcedf.shape
            # group = ['g' + str(i) for i in range(1, len(sourcedf) + 1)]
            # sourcedf['group'] = group
            print("No_of_rows_MYSQL SourceDF => ", source_row)


        ############## Read Destination DataFile ####

        ### CSV ####

        destinationdf = pd.DataFrame()

        if DESTINATION_TYPE == 'CSV':
            destination_data_file_path = parser.get('DEST', 'dest_data_file_path')
            skip_rows = parser.get('DEST', 'SKIP_ROWS')
            destinationdf = getDFfromCsv(destination_data_file_path, skip_rows) 
            destination_rows, no_of_columns = destinationdf.shape  

        ### XLSX ####

        if DESTINATION_TYPE == 'XLSX':
            destination_data_file_path = parser.get('DEST', 'dest_data_file_path')
            sheet_name = parser.get('DEST', 'sheet_name')
            skip_rows = parser.get('DEST', 'SKIP_ROWS')
            destinationdf = getDFfromXlsx(destination_data_file_path, sheet_name, "", "", skip_rows)
            destination_rows, no_of_columns = sourcedf.shape

        ### MSSQLDF ####

        if DESTINATION_TYPE == 'MSSQL':
            User = parser.get('DEST', 'dest_user') 
            Password = parser.get('DEST', 'dest_password')
            Database = parser.get('DEST', 'dest_database')
            DESTINATION_SCHEMA_NAME = parser.get('vTurbineMasterData_Dest', 'schema_name_dest')
            DESTINATION_TABLE_NAME = parser.get('vTurbineMasterData_Dest' , 'destination_query_filter')
            task2 = asyncio.create_task(GetMSSQLDF(DESTINATION_TABLE_NAME, User, Password, Database))
            destinationdf = await task2
            # group = ['g' + str(i) for i in range(1, len(destinationdf) + 1)]
            # destinationdf['group'] = group
            destination_rows, no_of_columns = destinationdf.shape
            print("No_of_rows_MSSQL Destination => ", destination_rows)

            

        ### MYSQL####

        if DESTINATION_TYPE == 'MYSQL':
            User = parser.get('DEST', 'dest_user')
            Password = parser.get('DEST', 'dest_password')
            Database = parser.get('DEST', 'dest_database')
            DESTINATION_SCHEMA_NAME = parser.get('vTurbineMasterData_Dest', 'schema_name_dest')
            DESTINATION_TABLE_NAME = parser.get('vTurbineMasterData_Dest' , 'destination_query_filter')
            # destinationdf = GetSQLDF(DESTINATION_TABLE_NAME, User, Password, Database)
            task2 = asyncio.create_task(GetSQLDF(DESTINATION_TABLE_NAME, User, Password, Database))
            destinationdf = await task2
            destination_rows, no_of_columns = destinationdf.shape
            print("No_of_rows_MSSQL Destination => ", destination_rows)


        # ***************** Validation **********************
    
         
            # def process_chunk(chunk):
                        
            #  return chunk
            
            
        if source_row == destination_rows:
            print("Validations Process is Running!!!")



            
            chunk_size = 10000  # Adjust as needed
            num_processes = 4   # Adjust based on available CPU cores
            df1=sourcedf
            df2=destinationdf
                # Generate chunk pairs for comparison
            chunk_pairs = [(df1.iloc[i:i+chunk_size], df2.iloc[i:i+chunk_size]) 
                            for i in range(0, len(df1), chunk_size)]

                # Perform parallel processing
            unmatched_results = process_chunks_parallel(chunk_pairs, num_processes)

                # Combine the results from different processes
            unmatched_values = pd.concat(unmatched_results)
            print("Sourcedf ",sourcedf)
            print("DestinationDF ",destinationdf)

            print("Unmatched values:", unmatched_values)



            # Assuming the code for reading source and destination data is present here

        
                
            get_index = sourcedf.index[sourcedf.ne(destinationdf).any(axis=1)]

            source_error_df = sourcedf[sourcedf.index.isin(get_index)].sort_index() 
            source_error_df = source_error_df.loc[:, source_error_df.columns != 'hash']

            destination_error_df = destinationdf[destinationdf.index.isin(get_index)].sort_index() 
            destination_error_df = destination_error_df.loc[:, destination_error_df.columns != 'hash']
                
            print("Error Source Dataframe : \n", source_error_df)
            print("Error Destination Dataframe : \n", destination_error_df)

            sample_df_source = sourcedf.sample(frac=0.1).sort_index()
            sample_index = sample_df_source.index 
            sample_df_destination = destinationdf[destinationdf.index.isin(sample_index)].sort_index()

            # print("Source Random Record : \n", sample_df_source) 
            # print("Destination Random Record : \n", sample_df_destination)

            sample_df_source_row, no_of_columns = sample_df_source.shape
            sample_df_destination_row, no_of_columns = sample_df_destination.shape

            if sample_df_source_row == sample_df_destination_row:
                    sample_df_source = sample_df_source.loc[:, sample_df_source.columns != 'hash']
                    sample_df_destination = sample_df_destination.loc[:, sample_df_destination.columns != 'hash']
                    compare = sample_df_source.compare(sample_df_destination, keep_shape=True, keep_equal=False)
                    print("Compare of Random Data Result : \n", compare)
            else:
                    print("COUNT NOT MATCHED")
                    print("COUNT OF SOURCE DATAFRAME : ", sample_df_source_row)
                    print("COUNT OF DESTINATION DATAFRAME : ", sample_df_source_row)

            try:
                    col_name_min = parser.get('SOURCE', 'col_name_min')
                    if sourcedf[col_name_min].min() == destinationdf[col_name_min].min():
                        min = "MIN VALUE MATCH"
                        print(min)
                    else:
                        min = "MIN VALUE NOT MATCH"
                        print(min)
            except:  
                    min = "Column Name Not Match For Min Condition"
                    print(min)      

            try:
                    col_name_max = parser.get('SOURCE', 'col_name_max')
                    if sourcedf[col_name_max].max() == destinationdf[col_name_max].max():
                        max = "MAX VALUE MATCH"
                        print(max)
                    else:
                        max = "MAX VALUE NOT MATCH"
                        print(max)
            except: 
                    max = "Column Name Not Match For Max Condition"
                    print(max)

            try:
                    col_name_sum = parser.get('SOURCE', 'col_name_sum')  
                    if sourcedf[col_name_sum].astype(int).sum() == destinationdf[col_name_sum].astype(int).sum():
                        sum = "SUM VALUE MATCH"
                        print(sum)
                    else:
                        sum = "SUM VALUE NOT MATCH"
                        print(sum)
            except: 
                    sum = "Column Name Not Match For Sum Condition"
                    print(sum)

            try:        
                    col_name_avg = parser.get('SOURCE', 'col_name_avg')  
                    if sourcedf[col_name_avg].astype(int).mean() == destinationdf[col_name_avg].astype(int).mean():
                        avg = "Average VALUE MATCH"
                        print(avg)
                    else:
                        avg = "Average VALUE NOT MATCH"
                        print(avg)
            except: 
                    avg = "Column Name Not Match For Average Condition"        
                    print(avg)

            fileName = "Report_" + subject + "_" + date + ".csv"
            unmatched_values.to_csv(reportOutputDir + fileName, index=False)
            #destination_error_df.to_csv(reportOutputDir + fileName, index=False)

            fileName = "Report_" + subject + "_" + date + ".html" 
            html1 = source_error_df.to_html()
            html2 = destination_error_df.to_html()
            html3 = unmatched_values.to_html()
            html = f"<html><body><h4>Source DF Value Not Match : </h4><br><table>{html1}</table><br><h4>Destination DF Value Not Match : </h4><br><table>{html2}</table><br><h4>Compare of Random Data(10%) Result : </h4><br><table>{html3}</table><br><h4>Validations : </h4><h4>1 - {min}</h4><h4>2 - {max}</h4><h4>3 - {sum}</h4><h4>4 - {avg}</h4></body></html>"
            filePath = reportOutputDir
            text_file = open(filePath + fileName, "w")
            text_file.write(html)
            text_file.close()
        else:
         print("DataFrame Row Count Not Match")
         print("COUNT OF SOURCE DATAFRAME : ", source_row)
         print("COUNT OF DESTINATION DATAFRAME : ", destination_rows)
 nest_asyncio.apply()  # Enable nested asyncio

 asyncio.run(DoubleDataValidation())
#main()























































































































































































































































































































































































































































































































