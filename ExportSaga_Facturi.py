# Version 1.3

# also for fdb  sudo apt install libfbclient2
import fdb # https://fdb.readthedocs.io/en/v2.0/getting-started.html
import time
import datetime
import sys
from datetime import date
from dbfpy3 import dbf

from config import folderProgram, dbfFileNamestatic, magNume, dbdir, contstatic, asociereCotaTVA

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
        sys.exit()

def inputdate():
    try:
        print("Data")
        dataInput = [int(x) for x in (input("An  : "), input("Luna: "), input("Ziua: "))]
        
        return date(dataInput[0], dataInput[1], dataInput[2])
    except  Exception as e:
        print("\n============ Data gresita ============\n")
        print(e)
        time.sleep(5)
        sys.exit()

if len(sys.argv) <= 1:
    print('specify manual/automat')
    sys.exit()

if sys.argv[1].strip().lower() == 'manual':
    datequeryObject = inputdate()
    magSelectatManual = inputMagSelectat()
    nowTime = datetime.datetime.now().strftime('%H_%M_%S')
    manualStringFileNameAppend = f'_MANUAL_{nowTime}'
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
with open(folderProgram + "ExportSaga_Facturi.txt", "a") as logFile:
    logFile.write(f"\n[{datetime.datetime.now()}] : Start program\n")

# Go through every magazin
for magSelectat in range(0, len(magNume)):
# for magSelectat in range(4, 5):
    if sys.argv[1].strip().lower() == 'manual' and magSelectatManual != magSelectat:
        continue

    # Write to log current datetime and specify which MAGAZINE the query is working on
    with open(folderProgram + "ExportSaga_Facturi.txt", "a") as logFile:
        logFile.write(f"[{datetime.datetime.now()}] : Query {magNume[magSelectat]} started\n")

    dbfFileName = f"{dbfFileNamestatic}{magNume[magSelectat]}/IN_{datequeryDBF}_{datequeryDBF}_{magNume[magSelectat]}{manualStringFileNameAppend}.dbf"

    # Try in case of SERVER communication errors
    try:
        fdbConnection = fdb.connect(dsn = dbdir[magSelectat], user = "sysdba", password = "masterkey") # Firebird
        fdbCursor1 = fdbConnection.cursor()
        fdbCursor2 = fdbConnection.cursor()

        fdbCursor1.execute(f"SELECT idrec, nir, (SELECT web FROM furnizori WHERE idfurn = rec.idfurn), nrfact, datafact, (SELECT nume FROM furnizori WHERE idfurn = rec.idfurn) FROM rec WHERE stare = 1 AND codop = 1 AND nir IS NOT NULL AND datanir = {datequery}")
        listaCursor1 = fdbCursor1.fetchall()

        # Daca un furnizor nu are WEB asociat (cod Saga)
        if '' in [x[2] for x in listaCursor1]:
            print('\n######################\n# Lipsa cod furnizor #\n######################\n')
            print('\n'.join(set([x[5] for x in listaCursor1 if x[2] == '']))) # afisez lista furnizori cod lipsa
            dbfFileName = dbfFileName.replace('IN_', 'errorCodFurnizorMagister_IN_')
            with open(folderProgram + "ExportSaga_Facturi.txt", "a") as logFile:
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

        for (idrec, nir, furnizor, nrfact, datafact, _) in listaCursor1:
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
                    with open(folderProgram + "ExportSaga_Facturi.txt", "a") as logFile:
                        logFile.write(f"[{datetime.datetime.now()}] : ERROR {magNume[magSelectat]} idrec = {idrec} {e}\n")

        db.close()
        
    except Exception as e:
        print("\n\n@@@@@@@@@@@@@@@@@@@@@@@@\nEROARE COMUNICARE SERVER\n@@@@@@@@@@@@@@@@@@@@@@@@\n\n")
        dbfFileName = dbfFileName.replace('IN_', 'error_IN_')
        with open(folderProgram + "ExportSaga_Facturi.txt", "a") as logFile:
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

        fdbCursor1.execute(f"SELECT idout, observatii, (SELECT web FROM furnizori WHERE idfurn = out.idfurn), nr_document_intern, valretfurn, data_event, (SELECT nume FROM furnizori WHERE idfurn = out.idfurn) FROM out WHERE inchis = 1 AND codop = 4 AND data_event = {datequery}")
        listaCursor1 = fdbCursor1.fetchall()

        # Daca un furnizor nu are WEB asociat (cod Saga)
        if '' in [x[2] for x in listaCursor1]:
            print('\n######################\n# Lipsa cod furnizor #\n######################\n')
            print('\n'.join(set([x[6] for x in listaCursor1 if x[2] == '']))) # afisez lista furnizori cod lipsa
            dbfFileName = dbfFileName.replace('IN_', 'errorCodFurnizorMagister_IN_')
            with open(folderProgram + "ExportSaga_Facturi.txt", "a") as logFile:
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

        for (idout, observatiiNrFact, furnizor, nrDocumentIntern, valFact, datafact, _) in listaCursor1:
            # Add NIR to nrfact to look like "NR_INTRARE-NR_NIR" : "35601-6095"
            nrfact = f"{observatiiNrFact}-{nrDocumentIntern}" # nu stiu cum sa pun aici ????

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
                    pretvanzare = valFact

                    valoare_achizitie = (-1) * round(valoare_achizitie, 2)
                    valoare_achizitie_tva = round(valoare_achizitie * tva / 100, 2)
                    valoare_achizitie_cu_tva = round(valoare_achizitie + valoare_achizitie_tva, 2)

                    denart = asociereCotaTVA[tva] # INTRARE COTA A
                    if tva == 0:
                        cont = f"461.SGR.{contstatic[magSelectat]}"
                        pretvanzare = 0.0
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
                    rec['CANTITATE'] = -1
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
                    with open(folderProgram + "ExportSaga_Facturi.txt", "a") as logFile:
                        logFile.write(f"[{datetime.datetime.now()}] : ERROR {magNume[magSelectat]} idrec = {idrec} {e}\n")

        db.close()







        # Write to log current datetime and specify which MAGAZINE the query finished
        with open(folderProgram + "ExportSaga_Facturi.txt", "a") as logFile:
            logFile.write(f"[{datetime.datetime.now()}] : Query {magNume[magSelectat]} finished\n")

    except Exception as e:
        print("\n\n@@@@@@@@@@@@@@@@@@@@@@@@\nEROARE COMUNICARE SERVER\n@@@@@@@@@@@@@@@@@@@@@@@@\n\n")
        dbfFileName = dbfFileName.replace('IN_', 'error_IN_')
        with open(folderProgram + "ExportSaga_Facturi.txt", "a") as logFile:
            logFile.write(f"[{datetime.datetime.now()}] : ERROR {magNume[magSelectat]} {e}\n")
        with open(dbfFileName, "w") as dbfFileTmp:
            dbfFileTmp.write(f"{e}")

print ("\n\nSucces!")
fdbConnection.close()

# Write to log current datetime and specify the program finished
with open(folderProgram + "ExportSaga_Facturi.txt", "a") as logFile:
    logFile.write(f"[{datetime.datetime.now()}] : Finished program\n")

time.sleep(30)
