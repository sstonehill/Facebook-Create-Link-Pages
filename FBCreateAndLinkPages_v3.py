# -*- coding: utf-8 -*-
"""
Created on Tue May 19 17:53:30 2015

@author: sstonehill
"""

# -*- coding: utf-8 -*-
import MySQLdb
import urllib
import urllib2
import requests
import re
import xlsxwriter
import xlrd
import csv
import time
import os
import sys
from pandas import *

#LocationIDs = "(2297361)"

def pullSQL():
    yextProdDB = MySQLdb.connect(host="db-slave.nj1.yext.com", user="readonly", passwd="chelsea", db="alpha")
    cursor = yextProdDB.cursor() 
    SQLQuery = []
    
    with open ("J:\SQL\Facebook Fields for Script.sql", "r") as myfile:
        for line in myfile:
            if "--" in line:
                SQLQuery.append((re.match(r'^.*?\--', line).group(0)).replace("--",""))
            elif line[0] == "(" and line[-1] == ")":
                SQLQuery.append(LocationIDs)
            elif line[0] == "(" and line[1].isdigit():
                SQLQuery.append(LocationIDs)
                next(myfile)
            else:
                SQLQuery.append(line)
                
    cursor.execute(''.join(SQLQuery))
    yextProdData = cursor.fetchall()
    yextProdData = [list(v) for v in yextProdData]
    yextProdData.insert(0,[v[0] for v in cursor.description])
    yextProdData = [[str(v) for v in w] for w in yextProdData]
    yextProdData = [[v[1:] if v.startswith("'") else v for v in w] for w in yextProdData]
    yextProdDB.close()
    
    locationCMSDB = MySQLdb.connect(host="cms-sql-slave.nj1.yext.com", user="geostore-ro", passwd="pigeonlatlng", db="alpha")
    cursor = locationCMSDB.cursor() 
    SQLQuery = []
    
    with open ("J:\SQL\Facebook Categories for Script.sql", "r") as myfile:
        for line in myfile:
            if "--" in line:
                SQLQuery.append((re.match(r'^.*?\--', line).group(0)).replace("--",""))
            elif line[0] == "(" and line[-1] == ")":
                SQLQuery.append(LocationIDs)
            elif line[0] == "(" and line[1].isdigit():
                SQLQuery.append(LocationIDs)
                next(myfile)
            else:
                SQLQuery.append(line)
    
    cursor.execute(''.join(SQLQuery))
    locationCMSData = cursor.fetchall()
    locationCMSData = [list(v) for v in locationCMSData]
    locationCMSData.insert(0,[v[0] for v in cursor.description])
    locationCMSData = [[str(v) for v in w] for w in locationCMSData]
    locationCMSData = [[v[1:] if v.startswith("'") else v for v in w] for w in locationCMSData]
    locationCMSDB.close()
    
    tokenDict = {}
    for x in xrange(1, len(yextProdData)):
        if yextProdData[x][13] == 'None' or yextProdData[x][14] == 'None':
            print yextProdData[x][18] + " does not have a Facebook account assigned or DB-Slave is behind"
        elif not tokenDict:
            tokenDict = getAllTokens(yextProdData[x][14])
            yextProdData[x][14] = tokenDict[yextProdData[x][13]]
        elif tokenDict:
            yextProdData[x][14] = tokenDict[yextProdData[x][13]]
    
        for y in xrange(1, len(locationCMSData)):
            if yextProdData[x][18] == locationCMSData[y][0]:
                yextProdData[x][11] = locationCMSData[y][1]
                yextProdData[x][12] = locationCMSData[y][2]
    
    exportXLSX(yextProdData, "FacebookInput.xlsx")


def createAndLinkPages(fileName, ignoreWarning, updateVanity): #createAndLinkPages('FacebookInput.xlsx', False, False)
    fileName = 'FacebookInput.xlsx'
    data = importXLSX(fileName)
    outputSMS = [['locationid', 'error', 'attempted request']]
    outputErrors = [['locationid', 'partnerid', 'PL Status', 'externalId', 'externalUrl']]
  
    for x in xrange(1, len(data)):
        time.sleep(1)
        if updateVanity:
            vanityURL = data[x][15]
        else:
            vanityURL = ''
        
        location = '{"city":"'+data[x][3]+'","state":"'+data[x][4]+'","country":"'+data[x][5]+'","zip":"' \
        +data[x][6]+'","street":"'+data[x][2]+'","longitude":'+data[x][9]+',"latitude":'+data[x][8]+'}'
        
        if (data[x][10] == '0' or data[x][10] == ''):
            request = 'https://graph.facebook.com/v2.3/'+data[x][13]+'/locations?access_token='+data[x][14]+ \
            '&main_page_id='+data[x][13]+'&store_number='+data[x][0]+ \
            '&store_name='+data[x][1]+'&location='+location+'&phone='+data[x][7]+'&page_username='+vanityURL+ \
            '&place_topics=['+data[x][11].replace("'","")+']&ignore_coordinate_warnings='+ str(ignoreWarning)

            response = requests.post(request)
            print data[x][18] + ' : ' + str(response.json())
            externalID = re.sub("[^0-9]", "", response.text)
            if response.status_code == 200:
                outputSMS.append([data[x][18], 559, 'Sync', externalID, 'http://facebook.com/' + externalID])
            else:
                outputErrors.append([data[x][18], str(response.json()), request])

        else:
            request = 'https://graph.facebook.com/v2.3/'+data[x][13]+'/locations?access_token='+data[x][14]+ \
            '&main_page_id='+data[x][13]+'&store_number='+data[x][0]+'&location_page_id='+data[x][10]+ \
            '&store_name='+data[x][1]+'&location='+location+'&phone='+data[x][7]+'&page_username='+vanityURL+ \
            '&place_topics=['+data[x][11].replace("'","")+']&ignore_coordinate_warnings='+ str(ignoreWarning)
            
            response = requests.post(request)
            print data[x][18] + ' : ' + str(response.json())
            if response.status_code <> 200:
                outputErrors.append([data[x][18], str(response.json()), request])

    exportXLSX2(outputSMS, outputErrors, 'FacebookOutput.xlsx')
    
    
def getAllTokens(url):
    tokenRequest = urllib2.Request(url, headers={'accept': '*/*'})
    html = urllib2.urlopen(tokenRequest).read()
            
    tokenDict = {}
    aList = re.split('"access_token":', str(html))
    aList.pop(0)
    for x in xrange(0, len(aList)):
        brandPageList = [long(v) for v in aList[x].split('"') if v.isdigit()]
        brandPageID = str(brandPageList[len(brandPageList) - 1])
        token = aList[x].split('"')[1]
        tokenDict[brandPageID] = token
    return tokenDict

def exportXLSX(listName, fileName):
    xbook = xlsxwriter.Workbook(fileName, {'strings_to_urls': False})
    xsheet = xbook.add_worksheet('FacebookInput')
    rowNum = 0
    for row in listName:
        xsheet.write_row(rowNum, 0, row)
        rowNum += 1
    xbook.close()
        
def exportXLSX2(listName, list2Name, fileName):
    xbook = xlsxwriter.Workbook(fileName, {'strings_to_urls': False})
    xsheet1 = xbook.add_worksheet('Created Pages')
    xsheet2 = xbook.add_worksheet('Errors')
    rowNum = 0
    for row in listName:
        xsheet1.write_row(rowNum, 0, row)
        rowNum += 1
    rowNum = 0
    for row in list2Name:
        xsheet2.write_row(rowNum, 0, row)
        rowNum += 1
    xbook.close()

def importXLSX(fileName):
    inputData = []
    workbook = xlrd.open_workbook(fileName)
    worksheet = workbook.sheet_by_name('FacebookInput')
    for x in xrange(0, worksheet.nrows):
        inputData.append([])
        for y in xrange(0, worksheet.ncols):
            inputData[x].append(str(worksheet.cell_value(x, y)))
    return inputData


def controlMain():
    print "\nWelcome to the Facebook Create & Link Pages tool!"
    print "\n**Before you begin, please make sure to SAVE and CLOSE any related Excel files**"
    print "\nPlease select an option below:\n\t 1. Start process from beginning \n\t 2. Create & link pages (only if you have previously created and reviewed your SQL input file) \n\t 0. Quit the program"
    option = selectOption(["1","2","0"], "Enter option #")

    if option == "0":
        sys.exit("You have selected the Quit Program option. Exiting program now.")
    elif option == "1":
        controlPullSQL()
    elif option == "2":
        controlCreateLinkPages()
        

def controlPullSQL():
    print "Please enter a comma-separated list of Location IDs below:"
    LocationIDs = raw_input("Location IDs: ")
    LocationIDs = "("+LocationIDs+")"
    print "Importing SQL to file...\n"
    path = pullSQL(LocationIDs)
    print "Please review the SQL Input file: " + path
    print "\n**Remember to SAVE and CLOSE any related files before continuing.**"
    print "\nAfter review, enter 1 to continue to create & link your pages, or enter 0 to quit."
    option = selectOption(["1","0"], "Enter option #")
    if option == "0":
        sys.exit("You have selected the Quit Program option. Exiting program now.")
    elif option == "1":
        controlCreateLinkPages()
    
def controlCreateLinkPages():
#    print "Please enter your SQL Input filepath below, or enter 0 to use the default."
#    filepath = raw_input("Enter filepath: ")
#    if filepath == "0":
#        filepath = str(os.getcwd())+"\\FacebookInput.xlsx"
#    filepath = os.path.join(filepath)
    filepath = str(os.getcwd())+"\\FacebookInput.xlsx"
    print "To create & link your pages, please enter True or False for the below questions." 
    ignoreWarning = selectOption(["True","False"],"Ignore warnings? (True/False)")
    if ignoreWarning == "True": ignoreWarning = True
    else: ignoreWarning = False

    updateVanity = selectOption(["True","False"],"Update vanity URLs? (True/False)")
    if updateVanity == "True": updateVanity = True
    else: updateVanity = False    

    print "Processing...\n"
    outfile = createAndLinkPages(filepath, ignoreWarning, updateVanity)
    print "Output file path: " + outfile

#Function that handles user input based on provided list of valid options
def selectOption(valid_list, message):
    option = raw_input(message + ": ")
    count = 0
    while(option not in valid_list and count < 3):
        if count == 2:
            sys.exit("Too many invalid options have been entered. Quitting program.")
        print "You have entered an invalid option. Please try again."
        option = raw_input(message + ": ")
        count = count + 1
    return option


controlMain()

