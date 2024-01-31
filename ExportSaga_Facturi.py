# Version 1.1.3

import subprocess
import re
import os
import socket
import time
import datetime
import sys
from datetime import date, timedelta
from dbfpy import dbf

# Python2 location
programdir = "/home/ubuntuserver/ExportSagaPI/"

# Export location and Python location
dbfnamestatic = "/home/ubuntuserver/ExportSagaPI/Export/Intrari/"

# Magazine array
magNume = ["P1", "P2", "P3", "P4", "P5", "Cob", "Fet", "CTp6", "CTg"]

# Location for Firebird Database
dbdir = ['"192.168.10.100:D:/FBData/SMARTCASH.FDB"', '"192.168.20.100:D:/FBData/SMARTCASH.FDB"', '"192.168.30.100:D:/IBData/SMARTCASH.FDB"', '"192.168.40.100:D:/IBData/SMARTCASH.FDB"', '"192.168.50.100:D:/IBData/SMARTCASH.FDB"', '"192.168.150.100:D:/IBData/SMARTCASH.FDB"', '"192.168.100.100:D:/IBData/SMARTCASH.FDB"', '"192.168.0.100:D:/IBData/SMARTCASH.FDB"', '"192.168.200.100:D:/FBData/SMARTCASH.FDB"']

# Static variable 'CONT'
contstatic = ["371.6.", "371.4.", "371.8.", "371.10.", "371.12.", "371.9.", "371.11.", "371.13.", "371.14."]

# Write to log current date and mark start of program
now = datetime.datetime.now()
with open(programdir + "ExportSaga_Facturi_Linux.txt", "a") as logFile:
    logFile.write("================AUTO================\n")
    logFile.write("[" + str(now) + "] : Start program" + "\n")

# Convert now to string and keep only date in dbf compatible format (DATE FROM YESTERDAY)
now = str(now)
#today = "2022-03-26"
today = datetime.date.today()
#datequery = "2022-03-25"
datequery = today - datetime.timedelta(days=1)
datequery = "'" + str(datequery) + "'"
print (datequery)
tva=0

# dbf headers in one array
headers = ["NR_NIR", "NR_INTRARE", "COD", "DATA", "SCADENT", "TIP", "COD_ART", "DEN_ART", "UM", "CANTITATE", "TVA_ART", "VALOARE", "TVA", "CONT", "PRET_VANZ", "GRUPA", "TVAI", "DEN_TIP", "GESTIUNE", "DEN_GEST"]

# Subprogram for connecting to the database and getting the set information based on 3 conditions
def isqlquery(data):
    with open(programdir + "inputfile", "w") as inputFile:
        inputFile.write("CONNECT " + dbdir[magSelectat] + ";\n")
        inputFile.write(data)
    return str(subprocess.check_output("/home/ubuntuserver/ExportSagaPI/isql -u SYSDBA -p masterke -i " + programdir + "inputfile", shell=True))
    # Raspberry PI
    #return str(subprocess.check_output("BOX86_NOBANNER=1 /usr/local/bin/box86 /home/pi/ExportSagaPI/isql -u SYSDBA -p masterke -i " + programdir + "inputfile", shell=True))

def formatstr(substring, curline, removerest):
    # Keep string after last variable
    # Find FIRST NUMBER in substring
    # Keep the string starting with first number
    # Position of first whitespace
    # Leave only substring in variable
    substring = curline[removerest.start():]
    removerest = re.search(r"\d", substring)
    substring = substring[removerest.start():]
    removerest = re.search(r"\s", substring)
    curline = substring
    substring = substring[:removerest.start()]
    return substring, curline, removerest

def isqlsub(isqloutput2):
    # Find first number in substring
    # Keep the string starting with first number
    # Position of first whitespace
    # Keep rest of string in isqloutput2
    # Keep only first number in substring and return
    substring = re.search(r"\d", isqloutput2)
    substring = isqloutput2[substring.start():]
    removerest = re.search(r"\s", substring)
    isqloutput2 = substring
    substring = substring[:removerest.start()]
    return substring

def entrytvabased(idrec, tva):
    # Query the database with the global 'tva' variable and parameter 'idrec' and save output to a list
    isqloutput2 = isqlquery("SELECT CANTFIZ, PRETVANZARE, VALOARE_ACHIZITIE, VALOARE_ACHIZITIE_TVA FROM RECITEMS WHERE IDREC = " + str(idrec) + " AND TVA = " + str(tva) + ";")
    isqloutput2 = isqloutput2[isqloutput2.find("==="):]
    isqloutput2 = isqloutput2.split("\n")
    isqloutput2 = isqloutput2[1:-2]
    # Initialize sums to 0
    pretvanzare = 0
    valoare_achizitie = 0
    valoare_achizitie_tva = 0
    # For every line in the output list
    for subcurline in isqloutput2:
        # Find first number (cantfiz) in string
        # Keep the string starting with first number
        # Position of first whitespace (after CANTFIZ)
        # Keep rest of string in subcurline
        # Keep only first number CANTFIZ

        # CHANGED ON 15 OCTOBER 2021 because there could be many lines here and one might be only whitespaces to it gives an error
        try:
            cantfiz = re.search(r"\d", subcurline)
            cantfiz = subcurline[cantfiz.start():]
            removerest = re.search(r"\s", cantfiz)
            subcurline = cantfiz
            cantfiz = cantfiz[:removerest.start()]
            # Format the SQL queries into variables
            subpretvanzare, subcurline, removerest = formatstr("CANTFIZ", subcurline, removerest)
            subvaloare_achizitie, subcurline, removerest = formatstr("VALOARE_ACHIZITIE", subcurline, removerest)
            subvaloare_achizitie_tva, subcurline, removerest = formatstr("VALOARE_ACHIZITIE_TVA", subcurline, removerest)
            # Sum them up and return them
            pretvanzare = pretvanzare + float(cantfiz) * float(subpretvanzare)
            valoare_achizitie = valoare_achizitie + float(subvaloare_achizitie)
            valoare_achizitie_tva = valoare_achizitie_tva + float(subvaloare_achizitie_tva)
        except:
            continue
    return round(pretvanzare, 2), round(valoare_achizitie, 2), round(valoare_achizitie_tva, 2)

def filldb(datequerryarray):
    # Go through the 2 arrays and add records in the database
    rec = db.newRecord()
    iterator = 0
    for currentheader in headers:
        rec[currentheader] = datequerryarray[iterator]
        iterator = iterator + 1
    rec.store()

# Go through every magazin
for magSelectat in range(0,len(magNume)):
#for magSelectat in range(1, 2):
    # Write to log current datetime and specify which MAGAZINE the query is working on
    now = datetime.datetime.now()
    with open(programdir + "ExportSaga_Facturi_Linux.txt", "a") as logFile:
        logFile.write("--\n")
        logFile.write("[" + str(now) + "] : Query " + magNume[magSelectat] + " started" + "\n")

    # Initilize boolean to indicate that the WEB(cod furnizor) exists in the database
    existaCodSmartCash = True
    # Create dbf with custom name and add fields
    datedbf = datequery[1:-1]
    datedbf = datedbf[8:] + "-" + datedbf[5:-3] + "-" + datedbf[:4]
    dbfname = dbfnamestatic + magNume[magSelectat] + "/IN_" + datedbf + "_" + datedbf + "_" + magNume[magSelectat] + ".dbf"
    print (dbfname)
    db = dbf.Dbf(dbfname, new=True)
    db.addField(
        ("NR_NIR","N", 11),
        ("NR_INTRARE","C", 17),
        ("COD","C", 9),
        ("DATA","D"),
        ("SCADENT","D"),
        ("TIP","C", 2),
        ("COD_ART","C", 17),
        ("DEN_ART","C", 61),
        ("UM","C", 6),
        ("CANTITATE","N", 15, 2),
        ("TVA_ART","N", 11),
        ("VALOARE","N", 15, 2),
        ("TVA","N", 15, 2),
        ("CONT","C", 21),
        ("PRET_VANZ","N", 15, 2),
        ("GRUPA","C", 17),
        ("TVAI","L"),
        ("DEN_TIP","C", 17),
        ("GESTIUNE","C", 3),
        ("DEN_GEST","C", 37),
    )

    # Try in case of SERVER communication errors
    try:
        # First query depending on date
        # Split into array of strings
        # Remove first 3 and last 2 lines (garbage)
        isqloutput = isqlquery("SELECT IDREC, NIR, IDFURN, NRFACT, DATAFACT FROM REC WHERE DATANIR = " + datequery + ";")
        isqloutput = isqloutput[isqloutput.find("==="):]
        isqloutput = isqloutput.split("\n")
        isqloutput = isqloutput[1:-2]

        for curline in isqloutput:
            # Only keep going if the boolean thats indicates the WEB(cod furnizor) exists in the database is 1
            if existaCodSmartCash == True:
                # CHANGED ON 26 OCTOBER 2021 because there could be many lines here and one might be only whitespaces to it gives an error
                try:
                    # Find first number (IDREC) in string
                    # Keep the string starting with first number
                    # Position of first whitespace (after PRET)
                    # Keep rest of string in curline
                    # Keep only first number IDREC
                    idrec = re.search(r"\d", curline)
                    idrec = curline[idrec.start():]
                    removerest = re.search(r"\s", idrec)
                    curline = idrec
                    idrec = idrec[:removerest.start()]

                    # Format the SQL queries into variables
                    nir, curline, removerest = formatstr("NIR", curline, removerest)
                    idfurn, curline, removerest = formatstr("IDFURN", curline, removerest)
                    nrfact, curline, removerest = formatstr("NRFACT", curline, removerest)
                    datafact, curline, removerest = formatstr("DATAFACT", curline, removerest)

                    # Add NIR to nrfact to look like "NR_INTRARE-NR_NIR" : "35601-6095"
                    nrfact = nrfact + "-" + nir

                    # Try in case there is no COD(WEB) for the FURNIZOR in SmartCash Shop
                    try:
                        # COD (ie: 00028)
                        isqloutput2 = isqlquery("SELECT WEB FROM FURNIZORI WHERE IDFURN = " + idfurn + ";")
                        isqloutput2 = isqloutput2[isqloutput2.find("==="):]
                        furnizor = isqlsub(isqloutput2)
                    except Exception as e:
                        db.close()
                        # If there is no WEB(cod furnizor) set the boolean to false
                        existaCodSmartCash = False
                        # Variables for renaming the dbf file to indicate an error
                        nowEXCEPT = datetime.datetime.now()
                        nowEXCEPT = str(nowEXCEPT.hour) + "-" + str(nowEXCEPT.minute) + "-" + str(nowEXCEPT.second)
                        datadbf = "IN_" + datedbf + "_" + datedbf + "_" + magNume[magSelectat] + ".dbf"
                        print ("\n\n\nCod furnizor inexistent in SmartCash Shop" + str(e))
                        # Write to log current datetime and specify the error
                        with open(programdir + "ExportSaga_Facturi_Linux.txt", "a") as logFile:
                            logFile.write("[" + str(now) + "] : ERROR COD FURNIZOR MAGISTER " + magNume[magSelectat] + " " + str(e) + "\n")
                        # Go to the dbf location and rename the DBF to indicate an error
                        tmp = os.system("cd {} && mv {} errorCodFurnizorMagister{}".format(dbfnamestatic + magNume[magSelectat], datadbf, datadbf + nowEXCEPT + ".dbf"))
                        time.sleep(1)
                        continue

                    # GESTIUNE (ie: 8)
                    isqloutput2 = isqlquery("SELECT IDSHOP FROM DATEMAGAZIN;")
                    isqloutput2 = isqloutput2[isqloutput2.find("==="):]
                    idshop = isqlsub(isqloutput2)

                    # DEN_GEST
                    dengest = "Pento Magazin [" + idshop + "]"

                    # Convert numeric values correctly for dbf
                    nir = int(nir)

                    # Convert Magister date format ('2021-09-24') to dbf compatible ('20210924')
                    datafact = datafact[:-6] + datafact[5:-3] + datafact[8:]

                    # Query PRET_VANZ, VALOARE, TVA for TVA = 19
                    tva = 19
                    pretvanzare, valoare_achizitie, valoare_achizitie_tva = entrytvabased(idrec, tva)
                    # DEN_ART and CONT for TVA=19
                    denart = "INTRARE COTA A"
                    cont = contstatic[magSelectat] + str(tva)

                    # If there is something (pretvanzare isn't 0) for this TVA save into the datequeryarray and write to database by calling the subprogram
                    if pretvanzare !=0:
                        datequeryarray = [nir, nrfact, furnizor, datafact, "", "", "", denart, "Buc", 1, tva, valoare_achizitie, valoare_achizitie_tva, cont, pretvanzare, "SC", "FALSE", "Nedefinit", idshop, dengest]
                        #print(datequeryarray)
                        filldb(datequeryarray)

                    # Query PRET_VANZ, VALOARE, TVA for TVA = 9
                    tva = 9
                    pretvanzare, valoare_achizitie, valoare_achizitie_tva = entrytvabased(idrec, tva)
                    # DEN_ART and CONT for TVA=9
                    denart = "INTRARE COTA B"
                    cont = contstatic[magSelectat] + str(tva)
                    # If there is something (pretvanzare isn't 0) for this TVA save into the datequeryarray and write to database by calling the subprogram
                    if pretvanzare !=0:
                        datequeryarray = [nir, nrfact, furnizor, datafact, "", "", "", denart, "Buc", 1, tva, valoare_achizitie, valoare_achizitie_tva, cont, pretvanzare, "SC", "FALSE", "Nedefinit", idshop, dengest]
                        #print(datequeryarray)
                        filldb(datequeryarray)

                    # Query PRET_VANZ, VALOARE, TVA for TVA = 5
                    tva = 5
                    pretvanzare, valoare_achizitie, valoare_achizitie_tva = entrytvabased(idrec, tva)
                    # DEN_ART and CONT for TVA=9
                    denart = "INTRARE COTA C"
                    cont = contstatic[magSelectat] + str(tva)
                    # If there is something (pretvanzare isn't 0) for this TVA save into the datequeryarray and write to database by calling the subprogram
                    if pretvanzare !=0:
                        datequeryarray = [nir, nrfact, furnizor, datafact, "", "", "", denart, "Buc", 1, tva, valoare_achizitie, valoare_achizitie_tva, cont, pretvanzare, "SC", "FALSE", "Nedefinit", idshop, dengest]
                        #print(datequeryarray)
                        filldb(datequeryarray)
                except:
                    continue
        db.close()

        # Write to log current datetime and specify which MAGAZINE the query finished
        now = datetime.datetime.now()
        with open(programdir + "ExportSaga_Facturi_Linux.txt", "a") as logFile:
            logFile.write("[" + str(now) + "] : Query " + magNume[magSelectat] + " finished\n")

    except Exception as e:
        db.close()
        # Variables for renaming the dbf file to indicate an error
        nowEXCEPT = datetime.datetime.now()
        nowEXCEPT = str(nowEXCEPT.hour) + "-" + str(nowEXCEPT.minute) + "-" + str(nowEXCEPT.second)
        datadbf = "IN_" + datedbf + "_" + datedbf + "_" + magNume[magSelectat] + ".dbf"
        print ("\n\n\nEroare comunicare SERVER" + str(e))
        # Write to log current datetime and specify the error
        with open(programdir + "ExportSaga_Facturi_Linux.txt", "a") as logFile:
            logFile.write("[" + str(now) + "] : ERROR " + magNume[magSelectat] + " " + str(e) + "\n")
        # Go to the dbf location and rename the DBF to indicate an error
        tmp = os.system("cd {} && mv {} error{}".format(dbfnamestatic + magNume[magSelectat], datadbf, datadbf + nowEXCEPT + ".dbf"))

# Write to log current datetime and specify the program finished
now = datetime.datetime.now()
with open(programdir + "ExportSaga_Facturi_Linux.txt", "a") as logFile:
    logFile.write("[" + str(now) + "] : Finished program\n")
