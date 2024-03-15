# Version 1.2.3

# also for fdb  sudo apt install libfbclient2
import fdb # https://fdb.readthedocs.io/en/v2.0/getting-started.html
import os
import time
import datetime
import sys
import re
from datetime import date, timedelta
from dbfpy3 import dbf

# Python3 location
programdir = "/home/ubuntuserver/ExportSagaPI/"
# programdir = "D:/delAuto/ExportSagaPI/"

# Export location and Python location
dbfFileNamestatic = "/home/ubuntuserver/ExportSagaPI/Export/Intrari/"
# dbfFileNamestatic = "D:/delAuto/SAGA_Export/Intrari/"

# Magazine array
magNume = ["P1", "P2", "P3", "P4", "P5", "Cob", "Fet", "CTp6", "CTg"]

# Location for Firebird Database
dbdir = ["192.168.10.100:D:/FBData/04/SMARTCASH.FDB", "192.168.20.100:D:/FBData/SMARTCASH.FDB", "192.168.30.100:D:/IBData/SMARTCASH.FDB", "192.168.40.100:D:/IBData/SMARTCASH.FDB", "192.168.50.100:D:/IBData/SMARTCASH.FDB", "192.168.150.100:D:/IBData/SMARTCASH.FDB", "192.168.100.100:D:/IBData/SMARTCASH.FDB", "192.168.0.100:D:/IBData/SMARTCASH.FDB", "192.168.200.100:D:/FBData/SMARTCASH.FDB"]

# Static variable 'CONT'
contstatic = ["6", "4", "8", "10", "12", "9", "11", "13", "14"]

asociereCotaTVA = {19: "INTRARE COTA A", 
                    9: "INTRARE COTA B", 
                    5: "INTRARE COTA C", 
                    0: "INTRARE COTA E"}

def inputMagSelectat():
    for i in range(0, len(magNume)):
        print (str(i + 1) + " : " + magNume[i])
    magSelectat = int(input("\nSelecteaza gestiune : "))
    if (magSelectat >= 1 and magSelectat <= 9):
        magSelectat = magSelectat - 1
        print (magNume[magSelectat] + "\n")
        return magSelectat
    else:
        print ("\nSelectie invalida")
        time.sleep(5)
        os._exit(-1)

def inputdate():
    try:
        print("Data")
        dataInput = [int(x) for x in (input("An  : "), input("Luna: "), input("Ziua: "))]
        
        return date(dataInput[0], dataInput[1], dataInput[2])
    except  Exception as e:
        print("\n============ Data gresita ============\n")
        print(e)
        time.sleep(5)
        os._exit(-1)

if sys.argv[1].strip().lower() == 'manual':
    datequeryObject = inputdate()
    magSelectatManual = inputMagSelectat()
    manualStringFileNameAppend = '_MANUAL'
else:
    #today = "2022-03-26"
    #datequery = "2022-03-25"
    today = datetime.date.today()
    datequeryObject = today - datetime.timedelta(days = 1)
    manualStringFileNameAppend = ''

datequery = f"\'{datequeryObject}\'"
print(datequery)
datequeryDBF = datequeryObject.strftime('%d-%m-%Y')

# Write to log current date and mark start of program
with open(programdir + "ExportSaga_Facturi.txt", "a") as logFile:
    logFile.write(f"\n[{datetime.datetime.now()}] : Start program\n")

# Go through every magazin
for magSelectat in range(0, len(magNume)):
# for magSelectat in range(4, 5):
    if sys.argv[1].strip().lower() == 'manual' and magSelectatManual != magSelectat:
        continue

    # Write to log current datetime and specify which MAGAZINE the query is working on
    with open(programdir + "ExportSaga_Facturi.txt", "a") as logFile:
        logFile.write(f"[{datetime.datetime.now()}] : Query {magNume[magSelectat]} started\n")

    dbfFileName = f"{dbfFileNamestatic}{magNume[magSelectat]}/IN_{datequeryDBF}_{datequeryDBF}_{magNume[magSelectat]}{manualStringFileNameAppend}.dbf"

    # Try in case of SERVER communication errors
    try:
        fdbConnection = fdb.connect(dsn = dbdir[magSelectat], user = "sysdba", password = "masterkey") # Firebird
        fdbCursor1 = fdbConnection.cursor()
        fdbCursor2 = fdbConnection.cursor()

        fdbCursor1.execute(f"SELECT idrec, nir, (SELECT web FROM furnizori WHERE idfurn = rec.idfurn), nrfact, datafact FROM rec WHERE codop = 1 AND nir IS NOT NULL AND datanir = {datequery}")
        listaCursor1 = fdbCursor1.fetchall()

        # Daca un furnizor nu are WEB asociat (cod Saga)
        if '' in [x[2] for x in listaCursor1]:
            dbfFileName = dbfFileName.replace('IN_', 'errorCodFurnizorMagister_IN_')
            with open(programdir + "ExportSaga_Facturi.txt", "a") as logFile:
                logFile.write(f"[{datetime.datetime.now()}] : ERROR COD FURNIZOR MAGISTER {magNume[magSelectat]}\n")
            with open(dbfFileName, "w") as dbfFileTmp:
                dbfFileTmp.write(f"nu exista WEB")
            continue

        print(dbfFileName)
        db = dbf.Dbf(dbfFileName, new = True)
        db.add_field(
            ("C", "NR_NIR", 16),
            ("C", "NR_INTRARE", 16),
            ("C", "GESTIUNE", 4),
            ("C", "DEN_GEST", 36),
            ("C", "COD", 8),
            ("D", "DATA"),
            ("D", "SCADENT"),
            ("C", "TIP", 1),
            ("N", "TVAI", 1),
            ("C", "COD_ART", 16),
            ("C", "DEN_ART", 60),
            ("C", "UM", 5),
            ("N", "CANTITATE", 14, 3),
            ("C", "DEN_TIP", 36),
            ("N", "TVA_ART", 2),
            ("N", "VALOARE", 15, 2),
            ("N", "TVA", 15, 2),
            ("C", "CONT", 20),
            ("N", "PRET_VANZ", 15, 2),
            ("C", "GRUPA", 16),
        )

        for (idrec, nir, furnizor, nrfact, datafact) in listaCursor1:
            # Add NIR to nrfact to look like "NR_INTRARE-NR_NIR" : "35601-6095"
            nrfact = f"{nrfact}-{nir}"

            fdbCursor2.execute(f"SELECT idshop FROM datemagazin")
            listaCursor2 = fdbCursor2.fetchall()
            idshop = listaCursor2[0][0]

            # DEN_GEST
            dengest = f"Pento Magazin [{idshop}]"

            # Convert date format ('2021-09-24') to dbf compatible ('20210924')
            datafact = str(datafact).replace('-', '')

            fdbCursor2.execute(f"SELECT tva, SUM(cantfiz * pretvanzare), SUM(valoare_achizitie), SUM(valoare_achizitie_tva) FROM recitems WHERE idrec = {idrec} GROUP BY tva")
            listaCursor2 = fdbCursor2.fetchall()
            for (tva, pretvanzare, valoare_achizitie, valoare_achizitie_tva) in listaCursor2:
                try:
                    pretvanzare = round(pretvanzare, 2)
                    valoare_achizitie = round(valoare_achizitie, 2)
                    valoare_achizitie_tva = round(valoare_achizitie_tva, 2)

                    denart = asociereCotaTVA[tva] # INTRARE COTA A
                    if tva == 0:
                        cont = f"461.SGR.{contstatic[magSelectat]}"
                        pretvanzare = 0.0
                    else:
                        cont = f"371.{contstatic[magSelectat]}.{tva}"

                    rec = db.new()
                    rec['NR_NIR'] = nir
                    rec['NR_INTRARE'] = nrfact
                    rec['COD'] = furnizor
                    rec['DATA'] = datafact
                    rec['SCADENT'] = ''
                    rec['TIP'] = ''
                    rec['COD_ART'] = ''
                    rec['DEN_ART'] = denart
                    rec['UM'] = 'Buc'
                    rec['CANTITATE'] = 1
                    rec['TVA_ART'] = tva
                    rec['VALOARE'] = valoare_achizitie
                    rec['TVA'] = valoare_achizitie_tva
                    rec['CONT'] = cont
                    rec['PRET_VANZ'] = pretvanzare
                    rec['GRUPA'] = 'SC'
                    rec['TVAI'] = 0
                    rec['DEN_TIP'] = 'Nedefinit'
                    rec['GESTIUNE'] = idshop
                    rec['DEN_GEST'] = dengest
                    db.write(rec)
                except Exception as e:
                    print(e)
                    with open(programdir + "ExportSaga_Facturi.txt", "a") as logFile:
                        logFile.write(f"[{datetime.datetime.now()}] : ERROR {magNume[magSelectat]} idrec = {idrec} {e}\n")

        db.close()
        
    except Exception as e:
        dbfFileName = dbfFileName.replace('IN_', 'error_IN_')
        with open(programdir + "ExportSaga_Facturi.txt", "a") as logFile:
            logFile.write(f"[{datetime.datetime.now()}] : ERROR {magNume[magSelectat]} {e}\n")
        with open(dbfFileName, "w") as dbfFileTmp:
            dbfFileTmp.write(f"{e}")



    # AVIZE

    dbfFileName = f"{dbfFileNamestatic}{magNume[magSelectat]}/IN_{datequeryDBF}_{datequeryDBF}_{magNume[magSelectat]}_aviz{manualStringFileNameAppend}.dbf"

    # Try in case of SERVER communication errors
    try:
        fdbConnection = fdb.connect(dsn = dbdir[magSelectat], user = "sysdba", password = "masterkey") # Firebird
        fdbCursor1 = fdbConnection.cursor()
        fdbCursor2 = fdbConnection.cursor()

        fdbCursor1.execute(f"SELECT idout, serie_document_fiscal, (SELECT web FROM furnizori WHERE idfurn = out.idfurn), nr_document_intern, valretfurn, data_event FROM out WHERE codop = 4 AND data_event = {datequery}")
        listaCursor1 = fdbCursor1.fetchall()

        # Daca un furnizor nu are WEB asociat (cod Saga)
        if '' in [x[2] for x in listaCursor1]:
            dbfFileName = dbfFileName.replace('IN_', 'errorCodFurnizorMagister_IN_')
            with open(programdir + "ExportSaga_Facturi.txt", "a") as logFile:
                logFile.write(f"[{datetime.datetime.now()}] : ERROR COD FURNIZOR MAGISTER {magNume[magSelectat]}\n")
            with open(dbfFileName, "w") as dbfFileTmp:
                dbfFileTmp.write(f"nu exista WEB")
            continue

        print(dbfFileName)
        db = dbf.Dbf(dbfFileName, new = True)
        db.add_field(
            ("C", "NR_NIR", 16),
            ("C", "NR_INTRARE", 16),
            ("C", "GESTIUNE", 4),
            ("C", "DEN_GEST", 36),
            ("C", "COD", 8),
            ("D", "DATA"),
            ("D", "SCADENT"),
            ("C", "TIP", 1),
            ("N", "TVAI", 1),
            ("C", "COD_ART", 16),
            ("C", "DEN_ART", 60),
            ("C", "UM", 5),
            ("N", "CANTITATE", 14, 3),
            ("C", "DEN_TIP", 36),
            ("N", "TVA_ART", 2),
            ("N", "VALOARE", 15, 2),
            ("N", "TVA", 15, 2),
            ("C", "CONT", 20),
            ("N", "PRET_VANZ", 15, 2),
            ("C", "GRUPA", 16),
        )

        for (idout, serieDocumentFiscal, furnizor, nrDocumentIntern, valFact, datafact) in listaCursor1:
            # Add NIR to nrfact to look like "NR_INTRARE-NR_NIR" : "35601-6095"
            nrfact = f"{serieDocumentFiscal}-{nrDocumentIntern}" # nu stiu cum sa pun aici ????

            fdbCursor2.execute(f"SELECT idshop FROM datemagazin")
            listaCursor2 = fdbCursor2.fetchall()
            idshop = listaCursor2[0][0]

            # DEN_GEST
            dengest = f"Pento Magazin [{idshop}]"

            # Convert date format ('2021-09-24') to dbf compatible ('20210924')
            datafact = str(datafact).replace('-', '')
            datafact = datafact.split()[0]
            print(datafact)

            fdbCursor2.execute(f"SELECT tva, SUM(cantitate * pret_achizitie_estimat) FROM outitems WHERE idout = {idout} GROUP BY tva")
            listaCursor2 = fdbCursor2.fetchall()
            for (tva, valoare_achizitie) in listaCursor2:
                try:
                    valoare_achizitie = (-1) * round(valoare_achizitie, 2)
                    valoare_achizitie_tva = round(valoare_achizitie * tva / 100, 2)
                    valoare_achizitie_cu_tva = round(valoare_achizitie + valoare_achizitie_tva, 2)

                    denart = asociereCotaTVA[tva] # INTRARE COTA A
                    if tva == 0:
                        cont = f"461.SGR.{contstatic[magSelectat]}"
                    else:
                        cont = f"371.{contstatic[magSelectat]}.{tva}"

                    rec = db.new()
                    rec['NR_NIR'] = nrDocumentIntern
                    rec['NR_INTRARE'] = nrfact
                    rec['COD'] = furnizor
                    rec['DATA'] = datafact
                    rec['SCADENT'] = ''
                    rec['TIP'] = ''
                    rec['COD_ART'] = ''
                    rec['DEN_ART'] = denart
                    rec['UM'] = 'Buc'
                    rec['CANTITATE'] = 1
                    rec['TVA_ART'] = tva
                    rec['VALOARE'] = valoare_achizitie
                    rec['TVA'] = valoare_achizitie_tva
                    rec['CONT'] = cont
                    rec['PRET_VANZ'] = valFact
                    rec['GRUPA'] = 'SC'
                    rec['TVAI'] = 0
                    rec['DEN_TIP'] = 'Nedefinit'
                    rec['GESTIUNE'] = idshop
                    rec['DEN_GEST'] = dengest
                    db.write(rec)
                except Exception as e:
                    print(e)
                    with open(programdir + "ExportSaga_Facturi.txt", "a") as logFile:
                        logFile.write(f"[{datetime.datetime.now()}] : ERROR {magNume[magSelectat]} idrec = {idrec} {e}\n")

        db.close()







        # Write to log current datetime and specify which MAGAZINE the query finished
        with open(programdir + "ExportSaga_Facturi.txt", "a") as logFile:
            logFile.write(f"[{datetime.datetime.now()}] : Query {magNume[magSelectat]} finished\n")

    except Exception as e:
        dbfFileName = dbfFileName.replace('IN_', 'error_IN_')
        with open(programdir + "ExportSaga_Facturi.txt", "a") as logFile:
            logFile.write(f"[{datetime.datetime.now()}] : ERROR {magNume[magSelectat]} {e}\n")
        with open(dbfFileName, "w") as dbfFileTmp:
            dbfFileTmp.write(f"{e}")

print ("\n\nSucces!")
fdbConnection.close()

# Write to log current datetime and specify the program finished
with open(programdir + "ExportSaga_Facturi.txt", "a") as logFile:
    logFile.write(f"[{datetime.datetime.now()}] : Finished program\n")

time.sleep(30)
