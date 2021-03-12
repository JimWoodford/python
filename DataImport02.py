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

#Local Imports
from local_settings02 import sourcePath,pyodbcDriver,pyodbcServer,pyodbcDatabase,pyodbcUser

#Special Imports
sys.path.append(r'C:\Dev\Utilities\SMTP')

from SMTP_Notification import Email


#Logging details
logging.basicConfig(filename=r'dataImport.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s', level=30)


def generateCnxnObject():
    try:
        cnxnString ='DRIVER='+pyodbcDriver+';SERVER='+pyodbcServer+';DATABASE='+pyodbcDatabase+';UID='+pyodbcUser+';PWD='+ os.getenv('SQL_SQL02_DataImporter')
        cnxn = pyodbc.connect(cnxnString.strip('\n'))
        return cnxn
    except:
        logging.error(str(datetime.now())+' Connection to database failed')
        concludeImport("Failure","can't connect to server")  
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
        #elif file.find("") > 0:
        #    DATAPROVIDER = 14
        #elif file.find("") > 0:
        #    DATAPROVIDER = 15
    else:
        logging.error(str(datetime.now())+' Data Provider not found in - ' + file)
        concludeImport("Failure","Unknown data provider name","Rejected")  
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
        move_file("Rejected")
        concludeImport("Failure","Column Headers not found","Rejected")  
        #logging.error(str(datetime.now())+' Column mappings not found')
        exit()

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
        concludeImport("Failure","Column Mapping failed","Rejected")  
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
        concludeImport("Failure","SQL build failed","Rejected")  
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
        concludeImport("Failure","sql import failed","Rejected")  
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
        concludeImport("Failure","Import failed","Rejected")
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
        concludeImport("Failure","DataLicenceProvider Not Found","Rejected")
        move_file("Rejected")
        exit()

    return DataProviderName

def createHeader(BatchId, FileName, FilePath, FileExtension,DataProvider):
    #EXEC	[Cleansing].[CreateHeader]
	#@BatchId = N'', #@FileName = N'',  #@FilePath = N'\\192.168.1.20\Share\Data Import\Leadiro\Bespoke',  #@FileExtension = N'.csv', #@DataProvider = N'Leadiro'
    cnxn = generateCnxnObject()
    cursor = cnxn.cursor()
    try:
        #DATAPROVIDERNAME = get_data_provider_name(DataProvider)
        print(BatchId,",",FileName,",",FilePath,",",FileExtension,",", DATAPROVIDERNAME)
        sql = "EXEC [DataImport].[Cleansing].[CreateHeader] @BatchId = N'" + str(BatchId) + "'," + " @FileName = N'" + FileName + "'," + " @FilePath = N'" + FilePath + "'," + " @FileExtension = N'" + FileExtension + "'," +" @DataProvider = N'" + DATAPROVIDERNAME +"'"
        
        cursor.execute(sql)
        cnxn.commit()
        message = 'Header inserted'
    
    except:
        logging.error(str(datetime.now())+' Failed to create Header in Header Table')
        move_file("Rejected")
        concludeImport("Failure","Failed to create Header in Header Table","Rejected")
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

def get_duplicate_Subscribers():
    try:
        cnxn = generateCnxnObject()
        cursor = cnxn.cursor()
        cursor.execute(
            """
                SELECT COUNT(DISTINCT STG.Email) 
                FROM DataImport.Cleansing.StagingData STG
                INNER JOIN Cleansing.Header H ON H.BatchId = STG.BatchId 
                INNER JOIN InboxDev.dbo.inbox_Subscriber S ON S.Email = STG.Email 
                WHERE H.Processed = 0
                AND STG.PermissionPassRequired = 0
                AND STG.Processed = 0"""
            
            )
        duplicate_row = cursor.fetchone()
        duplicates = duplicate_row[0]
        #print(duplicate_row[0])

    except:
        logging.error(str(datetime.now())+' Failed to run duplicate procedure')
        move_file("Rejected")
        concludeImport("Failure",FILELOCATION,"Rejected")
        exit()

    return duplicates

def update_inbox_subscribers():
    try:
        cnxn = generateCnxnObject()
        sqlimport = "EXEC [DataImport].[Cleansing].[ImportSubscribers_Batched]"
        cnxn.execute(sqlimport)
        cnxn.commit()

    except:
        logging.error(str(datetime.now())+' Failed to import/update InboxSubscribers')
        move_file("Rejected")
        concludeImport("Failure","Failed to import/update InboxSubscribers","Rejected")
        exit()

    return

def assignPublications(ImportName,Publication,ImportType):
    try:
        cnxn = generateCnxnObject()
        sqlimport = "EXEC [DataImport].[Cleansing].[AssignPublications] @ImportName=?, @Publication=?, @ImportType=?"
        params =  (ImportName, Publication, ImportType)
        cnxn.execute(sqlimport,params)
        cnxn.commit()

    except:
        logging.error(str(datetime.now())+' Failed to assign Publications')
        concludeImport("Failure","Failed  To Assign Publications","Rejected")
        move_file("Rejected")
        exit()

    return

def updateHeaderLogs(Batch,Submitter,Import,Publication):
    try:
        cnxn = generateCnxnObject()
        sqlupdate = "EXEC [DataImport].[Cleansing].[UpdateHeaderLogs] @BatchId = N'" + str(Batch) + "', @Submitter = N'" + Submitter + "', @ImportType = N'" + str(Import) + "', @Publication = N'" + Publication + "'"
        cnxn.execute(sqlupdate)
        cnxn.commit()

    except:
        logging.error(str(datetime.now())+' Failed to UpdateHeaders')
        concludeImport("Failure","Failed to Update Headers","Rejected")
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
        concludeImport("Failure","No email address","Rejected","sqlmonitoring@inboxinsight.com")
        exit()

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
        returnDict['category'] = (coverSheet['Regular Import Category'][0])
        returnDict['submitter'] = (coverSheet['Notification Email'][0])
        returnDict['publication'] = (coverSheet['Custom Import Publication'][0])
        
    except:
        move_file("Rejected")
        concludeImport("Failure","Can't open file - coversheet","Rejected")         
        exit()

    if returnDict['importType'] == "Regular":
        returnDict['importType'] = 1
        returnDict['publication'] = returnDict['category']
    elif returnDict['importType'] == "Custom":
        returnDict['importType'] = 2
        returnDict['publication'] = returnDict['publication'] #redundant but included to show workings

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
        print(datafile)
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

def Check_Country(Country):
    cnxn = generateCnxnObject()
    cursor = cnxn.cursor()
    try:
        cursor.execute(
            """
            SELECT StgCountry
            FROM DATAIMPORT.CLEANSING.CountryDataSet
            WHERE StgCountry  =? """
            ,Country
            )
        mapped_country = cursor.fetchone()       

    except:
        mapped_country = 'None'
        

    return mapped_country      

def FileCountryCheck(file):
    Invalid_Countries = 0
    returnDict = {}
    try:
        print(os.path.join(sourcePath,'queue',file)) 
        datafile = pd.read_excel(os.path.join(sourcePath,'queue',file), header = 0, sheet_name="Data")
        returnDict['Country'] = (datafile['Country'])

        CountrySet = set(returnDict['Country'])
        for Country in CountrySet:
            mapped_country = Check_Country(Country)
            print(Country, mapped_country)
            if (mapped_country == 'None') + (not mapped_country):
                Invalid_Countries = Invalid_Countries + 1
        print(Invalid_Countries)

        ##any invald countries then reject with email and logging
        if Invalid_Countries > 0:
            move_file("Rejected")
            concludeImport("Failure",str(Invalid_Countries) + " Countries with no matching datasets in file","Rejected")  
            logging.error(str(datetime.now())+ ' ' +str(Invalid_Countries + ' Invalid Countries found'))
            exit()
        #otherwise carry on
    except:
        move_file("Rejected")
        concludeImport("Failure","Failed country validation - unknown reason" ,"Rejected")  
        logging.error(str(datetime.now())+ ' ' + "Failed country validation - unknown reason")
        exit()
    return


def main():

    global FILELOCATION
    global EMPLOYEE
    global FILENAME
    global DATETIMESTAMP
    global DATAPROVIDER
    global BATCHID
    global DATAPROVIDERNAME
    
    FILELOCATION = sourcePath+"\Queue"
    #FILELOCATION = sourcePath+"\Rejected"
    
    for file in os.listdir(FILELOCATION):
    
        if file.endswith(".xlsx"):
            BATCHID = uuid.uuid4()
            DATETIMESTAMP = datetime.now().strftime(r"%Y%m%d%H%M%S")
            FILENAME = file
            coverSheet = getCoverSheetDict(file)
            print(coverSheet)
            EMPLOYEE = (coverSheet['submitter'])
            FileCountryCheck(file)
            print(EMPLOYEE)
            DATAPROVIDER = (coverSheet['dataProvider'])
            DATAPROVIDERNAME = get_data_provider_name(DATAPROVIDER)
            headerName = (os.path.splitext(FILENAME)[0])
            createHeader(BATCHID,headerName,os.path.join(FILELOCATION,file),'.xlsx',DATAPROVIDER)
            processDataSheet(file)
            logging.info(DATAPROVIDERNAME,' - ', file,' - file imported BATCHID ', BATCHID, DATETIMESTAMP)
            clean_data_in_staging_table()
            duplicateSubscribers = get_duplicate_Subscribers()
            update_inbox_subscribers()
            assignPublications(headerName,coverSheet['publication'],coverSheet['importType'])
            updateHeaderLogs(BATCHID,EMPLOYEE,coverSheet['importType'],coverSheet['publication'])
            move_file("Succeeded")
            concludeImport("Succeeded", "\n\nDuplicate subscribers found = " + str(duplicateSubscribers) + "\nFilename : " + FILENAME + "\nDataSource : " +(DATAPROVIDERNAME+'-'+os.path.splitext(FILENAME)[0]) +"\nPublication : " + coverSheet['publication'],"Succeeded")

          
if __name__ == "__main__":
    main()
