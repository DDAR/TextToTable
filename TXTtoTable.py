""" #####################################################
    NAME: TXTtoTable.py
    Source Name:
    Version: ArcGIS 10
    Author: DD Arnett
    Usage: Parcel-Based Land Information
    Required Arguments:
    Optional Arguments:
    Description: Takes the text files generated from assessor's database and
    creates geobase tables then dbf tables.
    Date Created: Mar 1, 2012
    Updated:
##################################################### """
import arcinfo
import os
import sys
import arcpy
import time
import string
import smtplib
import subprocess
from arcpy import env
from datetime import date
from time import strftime
log = r"D:\Data\Geodatabase\Taxlots\log.txt"
now = date.today()
mail_recipient = ("donna.arnett@co.yakima.wa.us")

def emailAlert(tableError):
    SUBJECT = "GIS-Cascade reports IP Address may be Unreachable!"
    TO = mail_recipient
    FROM = "GIS-Cascade@co.yakima.wa.us"
    text = "The " + tableError + " did not process. Please check! (TXTtoTable.py)"
    BODY = string.join((
        "From: %s" % FROM,
        "To: %s" % TO,
        "Subject: %s" % SUBJECT ,
        "",
        text
        ), "\r\n")
    server = smtplib.SMTP('mailhost.co.yakima.wa.us')#('ntx7.co.yakima.wa.us')
    server.sendmail(FROM, [TO], BODY)
    server.quit()

def WriteLog(tableName):
    # Open log file
    reportfile = open(log, 'a')
    reportText = now.strftime("%Y%m%d") +  ' ' + strftime("%H:%M:%S") + ' ' + 'Processed ' + tableName + '\n'
    reportfile.write(reportText)
    reportfile.close()
    #close the log file.

def ErrorWriteLog(tableName):
    #open log file
    reportfile = open(log, 'a')
    reportText = now.strftime("%Y%m%d") +  ' ' + strftime("%H:%M:%S") + ' ' + 'Table (' + tableName + ') was not Processed' + '\n'
    reportfile.write(reportText)
    reportfile.close()
    #close the log file.

# removes existing database
def killObject( object ):
    if arcpy.Exists(object):
        arcpy.Delete_management(object)

# creates new file geodatabase
def makeFGDB ( dir, db):
    if os.path.isdir(dir) !=1:
        os.mkdir(dir)
    killObject (os.path.join(dir, db))
    arcpy.CreateFileGDB_management(dir, db)

# creates a geodatabase table
def createGdbTable( db, table, fields ):
    killObject(db + '/' + table)
    arcpy.CreateTable_management(db, table)
    for field in fields:
        if field[1] == 'TEXT':
            arcpy.AddField_management(db + '/' + table,field[0],field[1], '','',field[2])
        else:
            arcpy.AddField_management(db + '/' + table,field[0],field[1],field[2],field[3])


# deletes extra field if exists
def deleteXfield( table ):
    fld = arcpy.ListFields(table)
    fldnamelist = []
    for f in fld:
        fldnamelist.append(f.name)
    if fldnamelist[-1] == 'NoName':
        arcpy.DeleteField_management(table, "NoName")
    if fldnamelist[-1] == 'EXTRA':
        arcpy.DeleteField_management(table, "EXTRA")

def createGeoTable(geoBase, outName, txtData):
    try:
        #print txtData
        geoTable = os.path.join(geoBase, outName)
        #print geoTable
        geoTemp = os.path.join(geoBase, "temp")
        #print geoTemp
        if arcpy.Exists(geoTable):
            killObject(geoTemp)
            arcpy.Rename_management(geoTable, geoTemp)
            killObject(geoTable)
        arcpy.CopyRows_management(txtData, geoTable)
        arcpy.AddIndex_management(geoTable, "ASSESSOR_N", "TaxLots", "NON_UNIQUE", "ASCENDING")
        killObject(geoTemp)

    except arcpy.ExecuteError:
        arcpy.Rename_management(geoTemp, geoBase)
        killObject(geoTemp)
        print arcpy.GetMessages()

# creates an info table
def createInfo(inTable, outLocation, outTable):
    #print inTable, outLocation, outTable
    #outTable = outTable + '.dat'
    outTPath = os.path.join(outLocation, outTable)
    #print outTPath
    datFile = outTPath + '.dat'
    #print datFile
    if arcpy.Exists(outTPath):
        killObject(outTPath)
    if arcpy.Exists(datFile):
        killObject(datFile)
    #arcpy.TableToTable_conversion(inTable, outLocation, outTable)
    arcpy.CopyRows_management(inTable, outTPath)
    arcpy.AddIndex_management(outTPath, "ASSESSOR_N")
    arcpy.Rename_management(outTPath, datFile)

# creates an dbf table
def createDBF(inTable, outLocation, outTable):
    outTPath = os.path.join(outLocation, outTable)
    if arcpy.Exists(outTPath):
        killObject(outTPath)
    arcpy.TableToTable_conversion(inTable, outLocation, outTable)

def createtempTxtFile(inText, tempTxt, headerList):
    #print tempTxt
    if os.path.isfile(tempTxt):
        os.remove(tempTxt)
    f=open(inText)
    o=open(tempTxt, "a")
    last = headerList[-1:]
    lastitem = last[0]
    lastitemfig = lastitem[0]
    for field in headerList:
        if field[0] == lastitemfig:
            header = field[0]
            o.write(header)
        else:
            header = field[0] + "	"
            o.write(header)
    o.write("\n")
    while 1:
        line = f.readline()
        lineleng = len(line)
        #print line
        if not line: break
        if lineleng < 2: break
        if line[-2] == '|':
            line = line[:-2] + '\n'
        line = line.replace("|","	")
        tc = line[:12]
        #print line
        if tc.find("-") == 6:
            rs = line.partition("-")
            line = rs[0] + rs[2]
        o.write(line)
    o.close()

def createtempLegalFile(inText, tempTxt, headerList):
    if os.path.isfile(tempTxt):
        os.remove(tempTxt)
    f=open(inText)
    o=open(tempTxt, "a")
    last = headerList[-1:]
    lastitem = last[0]
    lastitemfig = lastitem[0]
    for field in headerList:
        if field[0] == lastitemfig:
            header = field[0]
            o.write(header)
        else:
            header = field[0] + "	"
            o.write(header)
    o.write("\n")
    while 1:
        line = f.readline()
        if not line: break
        if line[-2] == '|':
            line = line[:-2] + '\n'
        line = line.replace("|","	")
        line = line.replace('"', "*")
        tc = line[:12]
        if tc.find("-") == 6:
            rs = line.partition("-")
            line = rs[0] + rs[2]
        o.write(line)

    o.close()

def fixLegalData(inTable):
    with arcpy.da.UpdateCursor(inTable, 'LEGAL_LINE') as cursor:
        for row in cursor:
            mystring = row[0]
            #print type(mystring)
            if (type(mystring) == unicode or type(mystring) == str):
                mystring = mystring.replace('*', '"')
                row[0] = mystring
                cursor.updateRow(row)
            else:
                cursor.updateRow(row)


def createtempSalesTxtFilel(inText, tempTxt, headerList):
    if os.path.isfile(tempTxt):
        os.remove(tempTxt)
    f=open(inText)
    o=open(tempTxt, "a")
    last = headerList[-1:]
    lastitem = last[0]
    lastitemfig = lastitem[0]
    for field in headerList:
        if field[0] == lastitemfig:
            header = field[0]
            o.write(header)
        else:
            header = field[0] + "	"
            o.write(header)
    o.write("\n")
    while 1:
        line = f.readline()
        if not line: break
        if line[-1] == '|':
            line[:-1]
        line = line.replace("|","	")
        line = line.replace('"', "'")
        tc = line[:12]
        if tc.find("-") == 6:
            rs = line.partition("-")
            line = rs[0] + rs[2]
        o.write(line)
    o.close()

def createdbfTablefromTxt(filePath, outName, txtData):
    try:
        dbfTable = os.path.join(filePath, outName)
        tempgeoName = "temp" + outName
        tempgeoTable = os.path.join(filePath, tempgeoName)
        if arcpy.Exists(dbfTable):
            killObject(dbfTable)
        if arcpy.Exists(tempgeoTable):
            killObject(tempgeoTable)
        arcpy.CopyRows_management(txtData, tempgeoTable)
        arcpy.Sort_management(tempgeoTable, dbfTable, [["ASSESSOR_N", "ASCENDING"]] )
        killObject(tempgeoTable)

    except arcpy.ExecuteError:
        print arcpy.GetMessages()

def createAltInfo(inTable, outLocation, outTable):
    outTPath = os.path.join(outLocation, outTable)
    datFile = outTPath + '.dat'
    if arcpy.Exists(outTPath):
        killObject(outTPath)
    if arcpy.Exists(datFile):
        killObject(datFile)
    arcpy.TableToTable_conversion(inTable, outLocation, outTable)
    arcpy.AddIndex_management(outTPath, "ASSESSOR_N", "Tax Lots")
    arcpy.Rename_management(outTPath, datFile)

def createAltGeoTable(geoBase, outName, dbfTable):
    try:
        geoTable = os.path.join(geoBase, outName)
        tempGTable = outName + "temp"
        tempgeoTable = os.path.join(geoBase, tempGTable)
        if arcpy.Exists(geoTable):
            killObject(geoTable)
        if arcpy.Exists(tempgeoTable):
            killObject(tempgeoTable)
        arcpy.TableToTable_conversion(dbfTable, geoBase, outName)
        #arcpy.Sort_management(tempgeoTable, geoTable, [["ASSESSOR_N", "ASCENDING"]] )
        arcpy.AddIndex_management(geoTable, "ASSESSOR_N", "Tax Lots")
        killObject(tempgeoTable)

    except arcpy.ExecuteError:
        print arcpy.GetMessages()

def runTheProcess(tempTxt, geoBaseTable, orgFile, infoName, dbfName):
    fullfdatabase = r'D:\Data\Geodatabase\Taxlots\Tables.gdb'
    temp2Txt = os.path.join(temptxtPath, tempTxt + "temp.txt")
    print temp2Txt
    if os.path.isfile(temp2Txt):
        os.remove(temp2Txt)
    geobaseTable = os.path.join(fullfdatabase, geoBaseTable)
    origTxt = os.path.join(txtPath, orgFile)
    try:
        createGeoTable(fullfdatabase, geoBaseTable, tempTxt)
        createInfo(geobaseTable, infoFilesPath, infoName)
        createDBF(geobaseTable, infoFilesPath, dbfName)
        WriteLog(orgFile)
    except arcpy.ExecuteError:
        print arcpy.GetMessages()
        ErrorWriteLog(orgFile)
        emailAlert(orgFile)

def fixPrntTbl(txtfile, temptxtfile):
    try:
        fOpen = open(temptxtfile, "w")
        for line in open(txtfile):
            lines = line.replace("-", "")
            #print lines
            fOpen.write(lines)
        fOpen.close()
    except arcpy.ExecuteError:
        print arcpy.GetMessages()
        ErrorWriteLog(orgFile)
        emailAlert(orgFile)


# message with time
def message(msg):
	LocalTime = time.asctime(time.localtime(time.time()))
	mmsg = msg + LocalTime; arcpy.AddMessage(mmsg); print mmsg

#-------------------------------------------------------------------------------
#                      Parameters and Variables
#-------------------------------------------------------------------------------
# Paths and Table Names
env.overwriteOutput = 'TRUE'
fdatabase = 'Tables.gdb'
fullfdatabase = r'D:\Data\Geodatabase\Taxlots\Tables.gdb'
#, 'comm_group_info.txt', 'comm_sec_info.txt'
txtFiles = ['char_info.txt', 'chld_nfo_1.txt', 'chld_nfo_2.txt', 'legal.txt', 'permit_info.txt', 'prop_nfo.txt', 'prty_nfo.txt', 'sale_nfo.txt', 'sm_chld_nfo.txt', 'sm_prnt_nfo.txt', 'value_nfo.txt']
infoFilesPath = r'D:\Data\Geodatabase\Taxlots\informix'
txtPath = r'D:\Data\disk_2\assessor\asr_unload'
temptxtPath = r'D:\Data\Geodatabase\Taxlots\informix\Tempfiles'
# Field definitions
charFields = [['ASSESSOR_N', 'TEXT', '11'],['QUALITY', 'TEXT', '25'], ['YEAR_BLT', 'TEXT', '11'], ['EFF_YEAR', 'TEXT', '11'], ['STORIES', 'TEXT', '11'], ['MAIN_SQFT', 'TEXT', '11'], ['UPPR_SQFT', 'TEXT', '11'],['BSMT_SQFT', 'TEXT', '11'],['FN_BSMT_SQ', 'TEXT', '11'],['BSMT_GAR', 'TEXT', '11'],['BEDROOMS', 'TEXT', '11'],['FULL_BATH', 'TEXT', '11'],['QTR_BATH', 'TEXT', '11'],['HALF_BATH', 'TEXT', '11'],['ATT_GAR', 'TEXT', '11'],['BLT_GAR', 'TEXT', '11'],['CRPT_SQFT', 'TEXT', '11'],['BLD_STYLE', 'TEXT', '20'],['CONDITION', 'TEXT', '13']]
chld1Fields = [['ASSESSOR_N','TEXT', '11'], ['SEG_CHILD_', 'LONG', '6'], ['SITUS_ADDR', 'TEXT', '50'], ['SITE_CITY', 'TEXT', '40'], ['SIZE', 'DOUBLE', '9', '2'], ['MEASURE', 'TEXT', '50'], ['LAND_VALUE', 'LONG', '9'], ['IMPRVMNT_V', 'LONG', '9'], ['NEW_CONST_', 'LONG', '9'], ['TAX_YEAR', 'SHORT', '4'], ['USE_CODE', 'TEXT', '50'], ['OWNER_FNAME', 'TEXT', '30'], ['OWNER_MNAM', 'TEXT', '30'], ['OWNER_LNAM', 'TEXT', '30'], ['OWNER_ORG_', 'TEXT', '40'], ['OWNER_MAIL', 'TEXT', 50],['OWNER_CITY','TEXT','40'],['OWNER_STATE', 'TEXT', '40'], ['OWNER_ZIP', 'TEXT', '9'], ['OWNER2_FNAME', 'TEXT','30'], ['OWNER2_MNAME', 'TEXT', '30'],['OWNER2_LNAME', 'TEXT', '30'],['OWNER2_ONAME', 'TEXT', '40'], ['OWNER2_MAIL', 'TEXT', '50'], ['OWNER2_CITY', 'TEXT', '40'], ['OWNER2_STATE', 'TEXT', '40'], ['OWNER2_ZIP', 'TEXT', '9']]
chld2Fields = [['ASSESSOR_N', 'TEXT', '11'], ['OWNER_CITY', 'TEXT', '40'], ['OWNER_STAT', 'TEXT', '50'], ['OWNER_ZIP_', 'TEXT', '9'], ['TAXP_FNAME', 'TEXT', '30'], ['TAXP_MNAME', 'TEXT', '30'], ['TAXP_LNAME', 'TEXT', '30'], ['TAXP_ORG_N', 'TEXT', '40'], ['TAXP_MAILI', 'TEXT', '50'], ['TAXP_CITY', 'TEXT', '40'], ['TAXP_STATE', 'TEXT', '50'], ['TAXP_ZIP_C', 'TEXT', '9']]
legalFields = [['ASSESSOR_N', 'TEXT', '11'], ['LINE_NR', 'SHORT', '4'], ['SEG_CHILD_', 'LONG', '6'], ['LEGAL_LINE', 'TEXT', '225']]
permitFields = [['ASSESSOR_N', 'TEXT', '11'], ['PERMIT_NO', 'TEXT', '20'], ['ISSUE_DATE', 'TEXT', '10'], ['AMOUNT', 'DOUBLE', '10', '0'], ['JURISDICT', 'TEXT', '20'], ['TYPE_CODE', 'TEXT', '2'], ['DESCRIPT', 'TEXT', '25']]
propFields = [['ASSESSOR_N', 'TEXT', '11'], ['TCA', 'SHORT', '3'], ['TAX_YEAR', 'SHORT', '4'], ['USE_CODE', 'TEXT', '65'], ['LOCATED_ON', 'TEXT', '11'], ['MKT_LAND', 'LONG', '9'], ['MKT_IMPVT', 'LONG', '9'], ['NEW_CONST', 'LONG', '9'], ['CU_LAND', 'LONG', '9'], ['CU_IMPVT', 'LONG', '9'], ['SIZE', 'DOUBLE', '9', '2'], ['MEASURE', 'TEXT', '50'], ['SITUS_ADDR', 'TEXT', '50'], ['SITUS_ZIP', 'TEXT', '9'], ['SITUS_CITY', 'TEXT', '40'], ['CU_DATE', 'SHORT', '4'], ['CU_VALUE', 'LONG', '9'], ['CYCLE', 'SHORT', '2'], ['NBHD', 'TEXT', '4'], ['INS_DATE', 'DATE'], ['INS_NUM', 'SHORT', '2'], ['CUR_CYCLE', 'SHORT', '2'], ['HOUSE_NO', 'LONG', '6']]
prtyFields = [['ASSESSOR_N', 'TEXT', '11'], ['FIRST_NAME', 'TEXT', '30'], ['MIDDLE_NAM', 'TEXT', '30'], ['LAST_NAME', 'TEXT', '30'], ['ORG_NAME', 'TEXT', '50'], ['ROLE', 'TEXT', '50'], ['STATUS', 'TEXT', '50'], ['ROLE_PERCE', 'DOUBLE', '9', '3'], ['EFF_FROM_D', 'TEXT', '10'], ['MAILING_AD', 'TEXT', '50'], ['MAILING_CI', 'TEXT', '40'], ['STATE', 'TEXT', '50'], ['ZIP_CODE', 'TEXT', '9']]
saleFields = [['ASSESSOR_N', 'TEXT', '11'], ['EXCISE_NUM', 'TEXT', '10'], ['GRANTOR_NA', 'TEXT', '75'], ['GROSS_SALE', 'DOUBLE', '13', '2'], ['PORTION_IN', 'TEXT', '1'], ['DOCUMENT_D', 'TEXT', '10'], ['QUALITY', 'TEXT', '2'], ['QUALITY_DE', 'TEXT', '25']]
smchldFields = [['SEG_MERGE_', 'TEXT', '10'], ['ASSESSOR_N', 'TEXT', '11']]
smprntFields = [['SEG_MERGE_', 'TEXT', '10'], ['ASSESSOR_N', 'TEXT', '11'], ['CONT_IND', 'TEXT', '1'], ['EFF_TO_DAT', 'TEXT', '10']]
valueFields = [['ASSESSOR_N', 'TEXT', '11'], ['TCA', 'SHORT', '3'], ['TAX_YEAR', 'SHORT', '4'], ['PROP_USE', 'TEXT', '65'],['MKT_LAND', 'LONG', '9'],['MKT_IMPVT', 'LONG', '9'], ['NEW_CONST', 'LONG', '9'], ['CU_LAND', 'LONG', '9'], ['CU_IMPVT', 'LONG', '9'], ['TVR', 'LONG', '9'], ['TVE', 'LONG', '9'], ['DATE', 'SHORT', '4'], ['CROP', 'LONG', '9'], ['AVR', 'LONG', '9']]
dict = {'Char':'charFields', 'chld_nfo_1':'chld1Fields', 'chld_nfo_2':'chld2Fields', 'Legal':'legalFields', 'permit_info':'permitFields', 'Property':'propFields', 'Party':'prtyFields', 'Sales':'saleFields', 'sm_chld_nfo':'smchldFields', 'sm_prnt_nfo':'smprntFields', 'PValues':'valueFields'}
dict2 = {'char_info':'Char', 'legal':'Legal', 'prop_nfo':'Property', 'prty_nfo':'Party', 'sale_nfo':'Sales', 'value_nfo':'PValues'}

#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
# Loop through the text file and build the tables
#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

WriteLog("=============  D:\Scripts\Daily\Assessor_Taxlots\TXTtoTable.py  =============")
rt = "Start Time: "; message(rt)
#--------------char_info.txt--------------------------------------------------
try:
	origTxt = os.path.join(txtPath, "char_info.txt")
	print origTxt
	tempTxt = os.path.join(temptxtPath, "Chartemp.txt")
	print tempTxt
	createtempTxtFile(origTxt, tempTxt, charFields)
	runTheProcess(tempTxt, "Char2", "char_info.txt", "char_nfo", "char_nfo.dbf")
except arcpy.ExecuteError:
	print arcpy.GetMessages()
#-----------chld_nfo_1.txt-----------------------------------------------------
try:
	origTxt = os.path.join(txtPath, "chld_nfo_1.txt")
	tempTxt = os.path.join(temptxtPath, "chld_nfo_1temp.txt")
	createtempTxtFile(origTxt, tempTxt, chld1Fields)
	runTheProcess(tempTxt, "chld_nfo_1", "chld_nfo_1.txt", "chld_nfo_1", "chld_nfo_1.dbf")
except arcpy.ExecuteError:
	print arcpy.GetMessages()
#-----------chld_nfo_2.txt-----------------------------------------------------
try:
	origTxt = os.path.join(txtPath, "chld_nfo_2.txt")
	tempTxt = os.path.join(temptxtPath, "chld_nfo_2temp.txt")
	createtempTxtFile(origTxt, tempTxt, chld2Fields)
	runTheProcess(tempTxt, "chld_nfo_2", "chld_nfo_2.txt", "chld_nfo_2", "chld_nfo_2.dbf")
except arcpy.ExecuteError:
	print arcpy.GetMessages()
#-----------legal.txt-----------------------------------------------------
try:
	tempTxt = "Legal"
	temp2Txt = os.path.join(temptxtPath, tempTxt + "temp.txt")
	origTxt = os.path.join(txtPath, "legal.txt")
	#origTxt = r'D:\Data\disk_5\projects\county\gis\Python\outLegal.txt'
	tempTxt = os.path.join(temptxtPath, "Legaltemp.txt")
	legalTables = os.path.join(fullfdatabase, 'Legal')
	createtempLegalFile(origTxt, tempTxt, legalFields)
	createGeoTable(fullfdatabase, "Legal", tempTxt)
	deleteXfield(legalTables)
	fixLegalData(legalTables)
	createInfo(legalTables, infoFilesPath, "Legal")
	createDBF(legalTables, infoFilesPath, "Legal.dbf")
	WriteLog("legal.txt")
	#runTheProcess(tempTxt, "Legal", "legal.txt", "Legal", "Legal.dbf")
except arcpy.ExecuteError:
	print arcpy.GetMessages()
#-----------Permit_info.txt-----------------------------------------------------
try:
	origTxt = os.path.join(txtPath, "permit_info.txt")
	tempTxt = os.path.join(temptxtPath, "permit_infotemp.txt")
	createtempTxtFile(origTxt, tempTxt, permitFields)
	runTheProcess(tempTxt, "permit_info", "permit_info.txt", "permit_info", "permit_info.dbf")
except arcpy.ExecuteError:
	print arcpy.GetMessages()
#-----------prop_nfo.txt-----------------------------------------------------
try:
    origTxt = os.path.join(txtPath, "prop_nfo.txt")
    tempTxt = os.path.join(temptxtPath, "prop_nfotemp.txt")
    dbfFile = os.path.join(infoFilesPath, "prop_nfo.dbf")
    fullGeoTable = os.path.join(fullfdatabase, "Property")
    createtempTxtFile(origTxt, tempTxt, propFields)
    createdbfTablefromTxt(infoFilesPath, "prop_nfo.dbf", tempTxt)
    createAltGeoTable(fullfdatabase, "Property", dbfFile)
    createInfo(fullGeoTable, infoFilesPath, "prop_nfo")
    WriteLog("prop_nfo.txt")

except arcpy.ExecuteError:
	print arcpy.GetMessages()
	ErrorWriteLog("prop_nfo.txt")
	emailAlert("prop_nfo.txt")
#-----------prty_nfo.txt-----------------------------------------------------
try:
    origTxt = os.path.join(txtPath, "prty_nfo.txt")
    #print origTxt
    tempTxt = os.path.join(temptxtPath, "prty_nfotemp.txt")
    #print tempTxt
    dbfFile = os.path.join(infoFilesPath, "prty_nfo.dbf")
    #print dbfFile
    fullGeoTable = os.path.join(fullfdatabase, "Party")
    createtempTxtFile(origTxt, tempTxt, prtyFields)
    createdbfTablefromTxt(infoFilesPath, "prty_temp.dbf", tempTxt)
    tempdbf = os.path.join(infoFilesPath, "prty_temp.dbf")
    objid = 'Owner'
    where_clause = '"ROLE" = ' + "'" + objid + "'"
    killObject(dbfFile)
    arcpy.TableToTable_conversion(tempdbf, infoFilesPath, "prty_nfo.dbf", where_clause)
    killObject(tempdbf)
    createAltGeoTable(fullfdatabase, "Party", dbfFile)
    createInfo(fullGeoTable, infoFilesPath, "prty_nfo")
    WriteLog("prty_nfo.txt")

except arcpy.ExecuteError:
	print arcpy.GetMessages()
	ErrorWriteLog("prty_nfo.txt")
	emailAlert("prty_nfo.txt")
#-----------sale_nfo.txt-----------------------------------------------------
try:
	origTxt = os.path.join(txtPath, "sale_nfo.txt")
	tempTxt = os.path.join(temptxtPath, "Salestemp.txt")
	createtempSalesTxtFilel(origTxt, tempTxt, saleFields)
	runTheProcess(tempTxt, "Sales", "sale_nfo.txt", "sale_nfo", "sales.dbf")
except arcpy.ExecuteError:
	print arcpy.GetMessages()
#-----------sm_chld_nfo.txt-----------------------------------------------------
try:
	origTxt = os.path.join(txtPath, "sm_chld_nfo.txt")
	tempTxt = os.path.join(temptxtPath, "sm_chld_nfotemp.txt")
	tempTxt2 = os.path.join(temptxtPath, "sm_chld_nfotemp2.txt")
	geobaseTable = os.path.join(fullfdatabase, "sm_chld_nfo")
	fixPrntTbl(origTxt, tempTxt2)
	createtempTxtFile(tempTxt2, tempTxt, smchldFields)
	createGeoTable(fullfdatabase, "sm_chld_nfo", tempTxt)
	createInfo(geobaseTable, infoFilesPath, "sm_chld_nfo")
	createDBF(geobaseTable, infoFilesPath, "sm_chld_nfo.dbf")
	#runTheProcess(tempTxt, "sm_chld_nfo", "sm_chld_nfo.txt", "sm_chld_nfo", "sm_chld_nfo.dbf")
except arcpy.ExecuteError:
	print arcpy.GetMessages()
#----------sm_prnt_nfo.txt------------------------------------------------------
try:
    origTxt = os.path.join(txtPath, "sm_prnt_nfo.txt")
    tempTxt = os.path.join(temptxtPath, "sm_prnt_nfotemp.txt")
    tempTxt2 = os.path.join(temptxtPath, "sm_prnt_nfotemp2.txt")
    geobaseTable = os.path.join(fullfdatabase, "sm_prnt_nfo")
    fixPrntTbl(origTxt, tempTxt2)
    createtempTxtFile(tempTxt2, tempTxt, smprntFields)
    createGeoTable(fullfdatabase, "sm_prnt_nfo", tempTxt)
    createInfo(geobaseTable, infoFilesPath, "sm_prnt_nfo")
    createDBF(geobaseTable, infoFilesPath, "sm_prnt_nfo.dbf")
    #runTheProcess(tempTxt, "sm_prnt_nfo", "sm_prnt_nfo.txt", "sm_prnt_nfo", "sm_prnt_nfo.dbf")
except arcpy.ExecuteError:
    print arcpy.GetMessages()
#---------value_nfo.txt--------------------------------------------------------
try:
    origTxt = os.path.join(txtPath, "value_nfo.txt")
    tempTxt = os.path.join(temptxtPath, "value_nfotemp.txt")
    dbfFile = os.path.join(infoFilesPath, "value_nfo.dbf")
    fullGeoTable = os.path.join(fullfdatabase, "PValues")
    createtempTxtFile(origTxt, tempTxt, valueFields)
    createdbfTablefromTxt(infoFilesPath, "value_nfo.dbf", tempTxt)
    createAltGeoTable(fullfdatabase, "PValues", dbfFile)
    createInfo(fullGeoTable, infoFilesPath, "value_nfo")
    WriteLog("value_nfo.txt")

except arcpy.ExecuteError:
	print arcpy.GetMessages()
	ErrorWriteLog("value_nfo.txt")
	emailAlert("value_nfo.txt")
