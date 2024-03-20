# Python3 location
folderProgram = "/home/ubuntuserver/ExportSagaPI/"
# folderProgram = "D:/delAuto/ExportSagaPI/"

# FACTURI / AVIZE
dbfFileNamestatic = "/home/ubuntuserver/ExportSagaPI/Export/Intrari/"
# dbfFileNamestatic = "D:/delAuto/SAGA_Export/Intrari/"

# Z uri
exportLocationStaticZ = "/home/ubuntuserver/ExportSagaPI/Export/Iesiri/"
# exportLocationStaticZ = "D:/delAuto/SAGA_Export/Iesiri/"



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

nrPOSDict = {
    'P1': [1],
    'P2': [1],
    'P3': [101],
    'P4': [101],
    'P5': [102],
    'Cob': [101, 102],
    'Fet': [101, 104, 103],
    'CTp6': [5, 4, 3],
    'CTg': [1, 2],
}