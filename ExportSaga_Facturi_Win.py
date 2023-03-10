# Version 1.1

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
programdir = "C:\Python27\\"

# Export location and Python location
dbfnamestatic = "D:\delAuto\SAGA_Export\Intrari\\"

# Magazine array
magNume = ["P1", "P2", "P3", "P4", "P5", "Cob", "Fet", "CTp6", "CTg"] 

# Location for Firebird Database
dbdir = ['"192.168.10.100:D:\FBData\SMARTCASH.FDB"', '"192.168.20.100:D:\IBData\SMARTCASH.FDB"', '"192.168.30.100:D:\IBData\SMARTCASH.FDB"', '"192.168.40.100:D:\IBData\SMARTCASH.FDB"', '"192.168.50.100:D:\IBData\SMARTCASH.FDB"', '"192.168.150.100:D:\IBData\SMARTCASH.FDB"', '"192.168.100.100:D:\IBData\SMARTCASH.FDB"', '"192.168.0.100:D:\IBData\SMARTCASH.FDB"', '"192.168.200.100:D:\FBData\SMARTCASH.FDB"']

# Static variable 'CONT'
contstatic = ["371.6.", "371.4.", "371.8.", "371.10.", "371.12.", "371.9.", "371.11.", "371.13.", "371.14."]

# Write to log current date and mark start of program
now = datetime.datetime.now()
tmp = os.system('echo ==================================== >> {}ExportSaga_Facturi_Win.txt'.format(programdir))
tmp = os.system('echo [{}] : Start program >> {}ExportSaga_Facturi_Win.txt'.format(now,programdir))

tva=0

# dbf headers in one array
headers = ["NR_NIR", "NR_INTRARE", "COD", "DATA", "SCADENT", "TIP", "COD_ART", "DEN_ART", "UM", "CANTITATE", "TVA_ART", "VALOARE", "TVA", "CONT", "PRET_VANZ", "GRUPA", "TVAI", "DEN_TIP", "GESTIUNE", "DEN_GEST"]

# Subprogram for getting magazin selectat
def inputMagSelectat():
    for i in range(0,len(magNume)):
        print (str(i + 1) + " : " + magNume[i])
    magSelectat = input("\nSelecteaza gestiune : ")
    if (magSelectat >= 1 and magSelectat <=9):
        magSelectat = magSelectat - 1
        print (magNume[magSelectat] + "\n")
        return magSelectat
    else:
        print ("\nSelectie invalida")
        time.sleep(5)
        os._exit(-1)

# Subprogram for getting start and end dates + delta dates
def inputdate():
    try:
        print ("Data start")
        startY, startM, startD = input("An  : "), input("Luna: "), input("Ziua: ")
        print("Data sfarsit")
        endY, endM, endD = input("An  : "), input("Luna: "), input("Ziua: ")
        
        # Calculate delta in days between the dates
        sdate = date(startY, startM, startD)
        edate = date(endY, endM, endD)
        delta = edate - sdate
        
        try:
            # Save to deltaint the number of delta days as int
            removerest = re.search(r"\s", str(delta))
            deltaint = int(str(delta)[:removerest.start()])
        except:
            # In case sdate and edate are equal set deltaint to 0
            deltaint = 0
        
        # Create array to keep the dates
        datearray = list(range(0,deltaint+1))
        for i in range (delta.days + 1):
            day = sdate + timedelta(days=i)
            day = "'" + str(day) + "'"
            datearray[i]=day
            print(day)
        return datearray
    except  Exception as e:
        # In case of error print message and exception error and go back to the beggining of inputdate to try again
        print("\n============ Interval gresit\n")
        print(e)
        time.sleep(10)
        #inputdate()

# Subprogram for connecting to the database and getting the set information based on 3 conditions
def isqlquery(condition,selection, table, column, data):
    tmp=os.system('echo CONNECT {}; > {}inputfile'.format(dbdir[magSelectat],programdir))
    if condition == 1:
        # 23 NOVEMBER 2021 REMOVED 'AND DATAREC IS NOT NULL'
        tmp=os.system('echo SELECT {} FROM {} WHERE {}={}; >> {}inputfile'.format(selection, table, column, data, programdir))
    elif condition == 2:
        tmp=os.system('echo SELECT {} FROM {} WHERE {}={}; >> {}inputfile'.format(selection, table, column, data, programdir))
    elif condition == 3:
        tmp=os.system('echo SELECT {} FROM {}; >> {}inputfile'.format(selection, table, programdir))
    elif condition == 4:
        tmp=os.system('echo SELECT {} FROM {} WHERE {}={} AND TVA={}; >> {}inputfile'.format(selection, table, column, data, tva, programdir))
    return str(subprocess.check_output("isql.lnk -u SYSDBA -p masterke -i {}inputfile".format(programdir),shell=True))

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
    isqloutput2 = isqlquery(4,"CANTFIZ,PRETVANZARE,VALOARE_ACHIZITIE,VALOARE_ACHIZITIE_TVA","RECITEMS","IDREC",idrec)
    isqloutput2 = isqloutput2.split("\n")
    isqloutput2 = isqloutput2[3:-2]
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
            subpretvanzare, subcurline, removerest = formatstr("CANTFIZ",subcurline, removerest)
            subvaloare_achizitie, subcurline, removerest = formatstr("VALOARE_ACHIZITIE",subcurline, removerest)
            subvaloare_achizitie_tva, subcurline, removerest = formatstr("VALOARE_ACHIZITIE_TVA",subcurline, removerest)
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

# Magazin gestiune
magSelectat = inputMagSelectat()

# Save into an array all dates
datearray = inputdate()

# Create dbf with custom name and add fields
datedbfstart = datearray[0][1:-1]
datedbfstart = datedbfstart[8:] + "-" + datedbfstart[5:-3] + "-" + datedbfstart[:4]
datedbfend = datearray[-1][1:-1]
datedbfend = datedbfend[8:] + "-" + datedbfend[5:-3] + "-" + datedbfend[:4]
dbfname = dbfnamestatic + magNume[magSelectat] + "\IN_" + datedbfstart + "_" + datedbfend + "_" + magNume[magSelectat] + "_MANUAL.dbf"
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

try:
    for dataquery in datearray:
        # First query depending on date
        # Split into array of strings
        # Remove first 3 and last 2 lines (garbage)
        isqloutput = isqlquery(1,"IDREC,NIR,IDFURN,NRFACT,DATAFACT","REC","DATANIR",dataquery)
        isqloutput = isqloutput.split("\n")
        isqloutput = isqloutput[3:-2]

        for curline in isqloutput:
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
                nir, curline, removerest = formatstr("NIR",curline, removerest)
                idfurn, curline, removerest = formatstr("IDFURN",curline, removerest)
                nrfact, curline, removerest = formatstr("NRFACT",curline, removerest)
                datafact, curline, removerest = formatstr("DATAFACT",curline, removerest)

                # Add NIR to nrfact to look like "NR_INTRARE-NR_NIR" : "35601-6095"
                nrfact = nrfact + "-" + nir
                
                # Try in case there is no COD(WEB) for the FURNIZOR in SmartCash Shop
                try:
                    # COD (ie: 00028)
                    isqloutput2 = isqlquery(2,"WEB","FURNIZORI","IDFURN",idfurn)
                    furnizor = isqlsub(isqloutput2)
                except Exception as e:
                    db.close()
                    # Variables for renaming the dbf file to indicate an error
                    nowEXCEPT = datetime.datetime.now()
                    nowEXCEPT = str(nowEXCEPT.hour) + "-" + str(nowEXCEPT.minute) + "-" + str(nowEXCEPT.second)
                    datadbf = "IN_" + datedbfstart + "_" + datedbfend + "_" + magNume[magSelectat] + ".dbf"
                    print (str(e) + "Cod furnizor inexistent in SmartCash Shop")
                    # Write to log current datetime and specify the error
                    tmp = os.system('echo [{}] : ERROR COD FURNIZOR MAGISTER {} {} >> {}ExportSaga_Facturi_Win.txt'.format(now, magNume[magSelectat], e, programdir))
                    # Go to the dbf location and rename the DBF to indicate an error
                    tmp = os.system("D: && cd {} && rename {} errorCodFurnizorMagister{}".format(dbfnamestatic + magNume[magSelectat], datadbf, datadbf + nowEXCEPT + ".dbf"))
                    time.sleep(5)
                    os._exit(-1)

                # GESTIUNE (ie: 8)
                isqloutput2 = isqlquery(2,"IDSHOP","DATEMAGAZIN",0,0)
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

    # Write to log current date and mark end of program
    now = datetime.datetime.now()
    tmp = os.system('echo [{}] : Finished program {} >> {}ExportSaga_Facturi_Win.txt'.format(now, magNume[magSelectat], programdir))

    print ("\n\nSucces!")
    time.sleep(30)
except Exception as e:
    db.close()
    # Variables for renaming the dbf file to indicate an error
    nowEXCEPT = datetime.datetime.now()
    nowEXCEPT = str(nowEXCEPT.hour) + "-" + str(nowEXCEPT.minute) + "-" + str(nowEXCEPT.second)
    datadbf = "IN_" + datedbfstart + "_" + datedbfend + "_" + magNume[magSelectat] + ".dbf"
    print (str(e) + "Eroare comunicare SERVER")
    # Write to log current datetime and specify the error
    tmp = os.system('echo [{}] : ERROR {} {} >> {}ExportSaga_Facturi_Win.txt'.format(now, magNume[magSelectat], e, programdir))
    # Go to the dbf location and rename the DBF to indicate an error
    tmp = os.system("D: && cd {} && rename {} error{}".format(dbfnamestatic + magNume[magSelectat], datadbf, datadbf + nowEXCEPT + ".dbf"))
