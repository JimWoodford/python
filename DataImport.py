#Standard Imports
from datetime import datetime

import win32api
import win32con
import win32security

import logging
import os
import sys
import pyodbc
import shutil

#Third Party Imports
import pandas as pd
import numpy as np
import uuid
import xlrd

#Special Imports
sys.path.append(r'C:\Dev\Utilities\SMTP')
from SMTP_Notification import Email

#Local Imports
from local_settings import sourcePath,pyodbcDriver,pyodbcServer,pyodbcDatabase,pyodbcUser


#Logging details
logging.basicConfig(filename=r'dataImport.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s', level=30)


def generateCnxnObject():
    try:
        cnxnString ='DRIVER='+pyodbcDriver+';SERVER='+pyodbcServer+';DATABASE='+pyodbcDatabase+';UID='+pyodbcUser+';PWD='+ os.getenv('SQL_SQL02_DataImporter')
        cnxn = pyodbc.connect(cnxnString.strip('\n'))
        return cnxn
    except:
        logging.error(str(datetime.now())+' Connection to database failed')
        move_file("Rejected")
        exit()

def clear_bom(file_to_de_bom):
    for encoding in "utf-8-sig","utf-16":
        try:
            bom_file = open(file_to_de_bom, mode='r', encoding="utf-8-sig").read()
            open(file_to_de_bom, mode='w', encoding='utf-8').write(bom_file)
        except UnicodeDecodeError:
            continue

def get_data_provider(file):
    DATAPROVIDER = {}
    searchString = file.lower()

    if searchString.find("cognism") > 0:
        DATAPROVIDER = 1
    elif searchString.find("leadiro") > 0:
        DATAPROVIDER = 2    
    elif searchString.find("merit") > 0:
        DATAPROVIDER = 3
    elif searchString.find("zoominfo") > 0:
        DATAPROVIDER = 4
    elif searchString.find("permissionpass") > 0:
        DATAPROVIDER = 5
    elif searchString.find("Internal") > 0:
        DATAPROVIDER = 6
    elif searchString.find("Velosio CRM") > 0:
        DATAPROVIDER = 7
    elif searchString.find("118/Market Location") > 0:
        DATAPROVIDER = 8
    elif searchString.find("TeleM") > 0:
        DATAPROVIDER = 9
    elif searchString.find("BYTHON") > 0:
        DATAPROVIDER = 10
    elif searchString.find("PERFPITCH") > 0:
        DATAPROVIDER = 11
    elif searchString.find("ASKOSCA") > 0:
        DATAPROVIDER = 12
    elif searchString.find("IBX") > 0:
        DATAPROVIDER = 13
        #elif file.find("") > 0:
        #    DATAPROVIDER = 14
        #elif file.find("") > 0:
        #    DATAPROVIDER = 15
    else:
        logging.error(str(datetime.now())+' Data Provider not found in - ' + file)
        move_file("Rejected")
        sys.exit()
    #print("File matched to provider")

    return DATAPROVIDER

def get_mapping_column(prov_column):
    cnxn = generateCnxnObject()
    cursor = cnxn.cursor()
    try:
        cursor.execute(
            """
            SELECT MappingId, StagingColumn
            FROM DATAIMPORT.CLEANSING.StagingProviderColumnMapping
            WHERE ProviderColumn  =? AND DataLicenceProvider =?"""
            ,prov_column,DATAPROVIDER
            )
        mapping_row = cursor.fetchone()

        mapping_column = mapping_row[1]

    except:
        mapping_column = None
        #logging.error(str(datetime.now())+' Column mappings not found')
        #exit()

    return mapping_column

def clean_provider_headers(dirty_headers):
    column_number = 0
    try: 
        for column_name in dirty_headers:
            mapping_column = get_mapping_column(column_name)
            dirty_headers[column_number] = mapping_column
            column_number = column_number + 1
        #message = "Cleaning columns succeeded"
    except:
        logging.error(str(datetime.now())+' Column mapping failed')
        move_file("Rejected")   
        exit()
    return dirty_headers

def build_insert_statement(staging_headers,batchid,rowid,row):
    try: 
        import_field_list = [i for i in staging_headers if i]
        param_values = ""
        for i in import_field_list: 
            field_value = str(row[i])
            if i != import_field_list[0]:
                param_values = param_values + ","
            if pd.isnull(row[str(i)]):
                param_values = param_values + "' '"
            else:        
                #param_values = param_values + "'" + (row[str(i)].replace("'","")) +"'"
                param_values = param_values + "'" + (field_value.replace("'","")) +"'"
        #Build sql string fro each row
        sql = "INSERT INTO DATAIMPORT.Cleansing.StagingData (Batchid ,RowId, "+ " ,".join(import_field_list) +""
        sql = sql + ",SubscriberExists,Anonymised,IsEmailOptedOut,IsBlockHashed,ProjectAzorian,IsTelephoneOptedOut,Processed,"
        sql = sql + "PermissionPassRequired,PermissionPassQueued,PermissionPassCompleted)"
        sql = sql + "VALUES (""'"+str(batchid)+"', '"+str(rowid) +"'," + param_values +",0,0,0,0,0,0,0,0,0,0)"
        #print(sql)

    except:
        logging.error(str(datetime.now())+' Sql row build failed at row '+ row[i])
        move_file("Rejected")
        exit()
    
    return sql

def insert_file_into_table(datafile,staging_headers,batchid):
    #print(datafile)
    cnxn = generateCnxnObject()
    cursor = cnxn.cursor()
    
    try:
        for index, row in datafile.iterrows():
            rowid = uuid.uuid4()
            sql = build_insert_statement(staging_headers,batchid,rowid,row) 
            cursor.execute(sql)
            cnxn.commit()
        cursor.close()
    
        message = str(index) + 'row imported'
    except:
        logging.error(str(datetime.now())+' Sql import failed at row '+ row)
        move_file("Rejected")
        exit()
    return message

def process_data_file(file):
    provider_headers = {}
    try:
        data = pd.read_csv(os.path.join(FILELOCATION,file),engine="python")
        
        datafile = pd.DataFrame(data)      
        provider_headers = (list(datafile.columns.values))
        staging_headers = clean_provider_headers(provider_headers)
        #print(staging_headers)
        datafile.columns = staging_headers       

        message = insert_file_into_table(datafile,staging_headers,BATCHID)
        message = "File Imported"
        print(message)
    except:
        logging.error(str(datetime.now())+' Import Failed')
        move_file("Rejected")
        exit()

    return message

def get_data_provider_name(DataProvider):
    cnxn = generateCnxnObject()
    cursor = cnxn.cursor()
    try:
        cursor.execute(
            """
            SELECT TOP 1 Name
            FROM InboxDev.Subscriber.DataLicenceProvider
            WHERE Id  =?""" 
            ,DataProvider
            )
        Name = cursor.fetchall()

        DataProviderName = (Name[0].Name)
    except:
        logging.error(str(datetime.now())+' Data Provider Not Found in DataLicenceProvider Table' )
        move_file("Rejected")
        exit()

    return DataProviderName

def create_header(BatchId, FileName, FilePath, FileExtension,DataProvider):
    #EXEC	[Cleansing].[CreateHeader]
	#@BatchId = N'', #@FileName = N'',  #@FilePath = N'\\192.168.1.20\Share\Data Import\Leadiro\Bespoke',  #@FileExtension = N'.csv', #@DataProvider = N'Leadiro'
    cnxn = generateCnxnObject()
    cursor = cnxn.cursor()
    try:
        #DATAPROVIDERNAME = get_data_provider_name(DataProvider)
        print(BatchId,",",FileName,",",FilePath,",",FileExtension,",", DATAPROVIDERNAME)
        sql = "EXEC [DataImport].[Cleansing].[CreateHeader2] @BatchId = N'" + str(BatchId) + "'," + " @FileName = N'" + FileName + "'," + " @FilePath = N'" + FilePath + "'," + " @FileExtension = N'" + FileExtension + "'," +" @DataProvider = N'" + DATAPROVIDERNAME +"'"
        
        cursor.execute(sql)
        cnxn.commit()
        message = 'Header inserted'
    
    except:
        logging.error(str(datetime.now())+' Failed to create Header in Header Table')
        move_file("Rejected")
        concludeImport("Failure",FILELOCATION,"Rejected")
        exit()

    return message

def clean_data_in_staging_table():
    try:
        cnxn = generateCnxnObject()
        sql = "EXEC [DataImport].[Cleansing].[CleaningDataInStagingTable]"
        cnxn.execute(sql)
        cnxn.commit()

    except:
        logging.error(str(datetime.now())+' Failed to clean DataStaging Table')
        move_file("Rejected")
        concludeImport("Failure",FILELOCATION,"Rejected")
        exit()

    return    

def update_inbox_subscribers():
    try:
        cnxn = generateCnxnObject()
        sqlimport = "EXEC [DataImport].[Cleansing].[ImportSubscribers4]"
        cnxn.execute(sqlimport)
        cnxn.commit()

    except:
        logging.error(str(datetime.now())+' Failed to import/update InboxSubscribers')
        move_file("Rejected")
        exit()

    return

def assignPublications(ImportName,Publication):
    try:
        cnxn = generateCnxnObject()
        sqlimport = "EXEC [DataImport].[Cleansing].[AssignPublications] @ImportName=?, @Publication=?"
        params =  (ImportName, Publication)
        cnxn.execute(sqlimport,params)
        cnxn.commit()

    except:
        logging.error(str(datetime.now())+' Failed to assign Publications')
        move_file("Rejected")
        exit()

    return

def move_file (path):
    print(FILENAME,os.path.join(FILELOCATION,FILENAME))

    filename, file_extension = os.path.splitext(FILENAME)
    datetimeAppendedPath = "{}_{}{}".format(filename, DATETIMESTAMP, file_extension)
    importedPath = os.path.join(sourcePath,path,datetimeAppendedPath)
    print(os.path.join(FILELOCATION,FILENAME))
    
    try:
        shutil.move(os.path.join(FILELOCATION,FILENAME), importedPath)
    except:
        logging.error(str(datetime.now())+' Failed to move file to archive')
        exit()

def getEmployeeEmail(file):
    try:
        sd = win32security.GetFileSecurity (file, win32security.OWNER_SECURITY_INFORMATION)
        owner_sid = sd.GetSecurityDescriptorOwner()
        name, domain, type = win32security.LookupAccountSid(None, owner_sid)
        if domain != "INBOX":
            email = "sqlmonitoring@inboxinsight.com"
        else:
            email = name+'@inboxinsight.com'

        print(domain, '  ', name, email)
        return email
    except:
        logging.error(str(datetime.now())+' Employee Cannot be mapped')
        move_file("Rejected")
        return "sqlmonitoring@inboxinsight.com"

def concludeImport(outcome,notificationMessage,path, defaultReceivers=""):
    filename, file_extension = os.path.splitext(FILENAME)
    datetimeAppendedPath = "{}_{}{}".format(filename, DATETIMESTAMP, file_extension)
    importedPath = os.path.join(sourcePath,path,datetimeAppendedPath)

    print("Queueing {} email. {}".format(outcome,notificationMessage))

    receivers = []

    if not defaultReceivers:
        receivers.append(EMPLOYEE)
    else:
        receivers.append(defaultReceivers)

    emailNotification = Email() 

    print(receivers)

    emailNotification.send(
            subject='Import {}'.format(outcome), 
            message="{} \n\n\nFile Location:\n\n{}".format(notificationMessage,importedPath), 
            receivers=receivers,
            sender="dataImport@InboxInsight.com"
        ) 

def getCoverSheetDict(file):
    returnDict = {}
    try:
        print(os.path.join(sourcePath,'queue',file))    
        coverSheet = pd.read_excel(os.path.join(sourcePath,'queue',file),sheet_name="CoverSheet")  
        returnDict['dataProvider'] = (coverSheet['Data Licence Provider'][0])
        returnDict['importType'] = (coverSheet['Import Type'][0])
        returnDict['category'] = (coverSheet['Category'][0])
        returnDict['campaign'] = (coverSheet['Campaign'][0])
        returnDict['clientName'] = (coverSheet['Client Name'][0])
        returnDict['submitter'] = (coverSheet['Notification Email'][0])
        
    except:
        concludeImport("Failure","Can't open file - coversheet","Rejected")         
        exit()

    if returnDict['importType'] == "regular":
        returnDict['importType'] = 1
    elif returnDict['importType'] == "IFP":
        returnDict['importType'] = 2

    if returnDict['dataProvider'] == "Cognism":
        returnDict['dataProvider'] = 1
    elif returnDict['dataProvider'] == "Leadiro":
        returnDict['dataProvider'] = 2    
    elif returnDict['dataProvider'] == "Merit":
        returnDict['dataProvider'] = 3
    elif returnDict['dataProvider'] == "Zoominfo":
        returnDict['dataProvider'] = 4
    
    return returnDict

def processDataSheet(file):
    try:
        datafile = pd.read_excel(os.path.join(sourcePath,'queue',file), header = 0, sheet_name="Data")
        #print(datafile)
        provider_headers = (list(datafile.columns.values))
        staging_headers = clean_provider_headers(provider_headers)
        datafile.columns = staging_headers
        insert_file_into_table(datafile,staging_headers,BATCHID)
        message = "File Imported"
        print(message)

    except:
        concludeImport("Failure","Can't open file - datasheet","Rejected")        
        
        exit()
    else:
        return datafile

def main():

    global FILELOCATION
    global EMPLOYEE
    global FILENAME
    global DATETIMESTAMP
    global DATAPROVIDER
    global BATCHID
    global DATAPROVIDERNAME

    FILELOCATION = sourcePath+"\Queue"

    print(FILELOCATION)
    
    for file in os.listdir(FILELOCATION):
        if file.endswith(".csv"):
            #BATCHID = uuid.uuid4()
            #DATETIMESTAMP = datetime.now().strftime(r"%Y%m%d%H%M%S")
            #FILENAME = file

            #EMPLOYEE = getEmployeeEmail(os.path.join(FILELOCATION,FILENAME))
        

            #clear_bom(os.path.join(FILELOCATION,file))
            #DATAPROVIDER = get_data_provider(file)
            #DATAPROVIDERNAME = get_data_provider_name(DATAPROVIDER)
            
            #Create Header
            #create_header(BATCHID,os.path.splitext(FILENAME)[0],os.path.join(FILELOCATION,file),'.csv',DATAPROVIDER)

            #Import File ow by row
            #process_data_file(file)
            #print(DATAPROVIDERNAME,' - ', file,' - file imported BATCHID ', BATCHID)
            ##logging.info(DATAPROVIDERNAME,' - ', file,' - file imported BATCHID ', BATCHID, DATETIMESTAMP)

            #Post process file (replaces SSIS - General.dtsx)
            #clean_data_in_staging_table()

            #Update Inbox_Subscribers
            #update_inbox_subscribers()

            #Move file to archive
            move_file("Rejected")

            #email results
            concludeImport("rejected",FILELOCATION,"Processed")
        elif file.endswith(".xlsx"):
            
            BATCHID = uuid.uuid4()
            DATETIMESTAMP = datetime.now().strftime(r"%Y%m%d%H%M%S")
            FILENAME = file
            coverSheet = getCoverSheetDict(file)
            EMPLOYEE = (coverSheet['submitter'])
            DATAPROVIDER = (coverSheet['dataProvider'])
            DATAPROVIDERNAME = get_data_provider_name(DATAPROVIDER)
            headerName = ((DATAPROVIDERNAME+'-'+os.path.splitext(FILENAME)[0]))
            create_header(BATCHID,headerName,os.path.join(FILELOCATION,file),'.xlsx',DATAPROVIDER)
            processDataSheet(file)
            logging.info(DATAPROVIDERNAME,' - ', file,' - file imported BATCHID ', BATCHID, DATETIMESTAMP)
            clean_data_in_staging_table()
            update_inbox_subscribers()
            assignPublications(headerName,coverSheet['category'])
            move_file("Succeeded")
            concludeImport("Succeeded",FILELOCATION,"ProcSucceeded")
          
if __name__ == "__main__":
    main()
