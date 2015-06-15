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

def pullSQL(LocationIDs):
    yextProdDB = MySQLdb.connect(host="db-slave.nj1.yext.com", user="readonly", passwd="chelsea", db="alpha")
    cursor = yextProdDB.cursor() 
    SQLQuery = []
    
    with open ("J:\SQL\Facebook - Fields for Script.sql", "r") as myfile:
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
    yextProdData = [list(i) for i in yextProdData]
    yextProdData.insert(0,[i[0] for i in cursor.description])
    yextProdDB.close()

    
    locationCMSDB = MySQLdb.connect(host="cms-sql-slave.nj1.yext.com", user="geostore-ro", passwd="pigeonlatlng", db="alpha")
    cursor = locationCMSDB.cursor() 
    SQLQuery = []
    
    with open ("J:\SQL\Facebook - Categories for Script.sql", "r") as myfile:
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
    locationCMSData = [list(i) for i in locationCMSData]
    locationCMSData.insert(0,[i[0] for i in cursor.description])
    locationCMSDB.close()
    
    tokenDict = getAllTokens(yextProdData[1][14])
    
    for x in xrange(1, len(yextProdData)):
        yextProdData[x][10] = str(yextProdData[x][10].replace("'", ""))
        
        if yextProdData[x][13] != None:
            yextProdData[x][13] = str(yextProdData[x][13].replace("'", ""))
            yextProdData[x][14] = str(tokenDict[yextProdData[x][13]])
        
        for y in xrange(1, len(locationCMSData)):
            if str(yextProdData[x][18]) == str(locationCMSData[y][0]):
                yextProdData[x][11] = str(locationCMSData[y][1].replace("'", ""))
                yextProdData[x][12] = str(locationCMSData[y][2].replace("'", ""))

    path = exportXLSX(yextProdData, "FacebookInput.xlsx")
    return path
    

def createAndLinkPages(fileName, ignoreWarning, ignoreCoordinateWarning, updateVanity): #createAndLinkPages('FacebookInput.xlsx', False, False)
    aList = []    
    aList = importXLSX(fileName)
    outputList = []
    outputList.append(['locationid', 'partnerid', 'PL Status', 'externalId', 'externalUrl'])
    errorList = []
    errorList.append(['locationid', 'error'])
    
    for x in xrange(1, len(aList)):
        time.sleep(1)
        yextID = str(int(float(aList[x][0])))
        name = str(urllib.quote_plus(aList[x][1]))
        address = str(urllib.quote_plus(aList[x][2]))
        city = str(urllib.quote_plus(aList[x][3]))
        state = str(aList[x][4])
        country = str(aList[x][5])
        zipCode = str(int(float(aList[x][6])))
        phone = str(int(float(aList[x][7])))
        latitude = str(aList[x][8])
        longitude = str(aList[x][9])
        pageID = str(aList[x][10].replace("'", ""))
        categories = str(aList[x][11].replace("'", ""))
        brandPageID = str(aList[x][13].replace("'", ""))
        accessToken = str(aList[x][14])
        pID = '559'
        ID = str(aList[x][18])
        
        if updateVanity:
            vanityURL = str(aList[x][15])
        else:
            vanityURL = ''
        
        location = '{"city":"'+city+'","state":"'+state+'","country":"'+country+'","zip":"' \
        +zipCode+'","street":"'+address+'","longitude":'+longitude+',"latitude":'+latitude+'}'
        
        if (pageID == '0' or pageID == ''):
            request='https://graph.facebook.com/v2.3/'+brandPageID+'/locations?access_token='+accessToken+ \
            '&main_page_id='+brandPageID+'&store_number='+yextID+ \
            '&store_name='+name+'&location='+location+'&phone='+phone+'&page_username='+vanityURL+ \
            '&place_topics=['+categories+']&ignore_coordinate_warnings='+ str(ignoreCoordinateWarning)+'&ignore_coordinate_warnings='+ str(ignoreWarning)

            response = requests.post(request)
            print str(float(ID)) + ' : ' + str(response.json())
            newPageID = re.sub("[^0-9]", "", response.text)
            if response.status_code == 200:
                outputList.append([str(float(ID)), pID, 'Sync', newPageID, 'http://facebook.com/' + newPageID])
            else:
                errorList.append([str(float(ID)), str(response.json()), request])

        else:
            request='https://graph.facebook.com/v2.3/'+brandPageID+'/locations?access_token='+accessToken+ \
            '&main_page_id='+brandPageID+'&store_number='+yextID+'&location_page_id='+pageID+ \
            '&store_name='+name+'&location='+location+'&phone='+phone+'&page_username='+vanityURL+ \
            '&place_topics=['+categories+']&ignore_coordinate_warnings='+ str(ignoreCoordinateWarning)+'&ignore_coordinate_warnings='+ str(ignoreWarning)
            
            response = requests.post(request)
            print ID + ' : ' + str(response.json())
            if response.status_code <> 200:
                errorList.append([str(float(ID)), str(response.json())])

#    exportCSV(outputList, 'FacebookOutput.csv')
    path = exportXLSX2(outputList, errorList, 'FacebookOutput.xlsx')
    return path
    
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
    path = str(os.getcwd())+"\\"+fileName
    return path
        
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
    path = str(os.getcwd())+"\\"+fileName
    return path

def importXLSX(fileName):
    inputData = []
    workbook = xlrd.open_workbook(fileName)
    worksheet = workbook.sheet_by_name('FacebookInput')
    for x in xrange(0, worksheet.nrows):
        inputData.append([])
        for y in xrange(0, worksheet.ncols):
            inputData[x].append(str(worksheet.cell_value(x, y)))
    return inputData

def exportCSV(listName, fileName):
    with open(fileName, 'wb') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(listName)

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

    ignoreCoordinateWarning = selectOption(["True","False"],"Ignore coordinate warnings? (True/False)")
    if ignoreCoordinateWarning == "True": ignoreCoordinateWarning = True
    else: ignoreCoordinateWarning = False  
    
    updateVanity = selectOption(["True","False"],"Update vanity URLs? (True/False)")
    if updateVanity == "True": updateVanity = True
    else: updateVanity = False    

    print "Processing...\n"
    outfile = createAndLinkPages(filepath, ignoreWarning, ignoreCoordinateWarning, updateVanity)
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