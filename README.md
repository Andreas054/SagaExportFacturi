# SagaExportFacturi
This is made to run on
- Raspberry Pi 32 bit version with **BOX86** installed;
- Ubuntu Server


[Firebird 2.5.9](https://github.com/FirebirdSQL/firebird/releases/tag/R2_5_9) (*FirebirdSS-2.5.9.27139-0.i686.tar.gz*) is required.

## Additional libraries
- ~~[dbfpy](https://pypi.org/project/dbfpy/)~~ (pip on Python2 not working anymore)
- [dbfpy](Lib/site-packages)

## Crontab for Ubuntu Server
### sudo
- @reboot ethtool -s enp3s0 wol g # enable WoL for next boot
- @reboot vbetool dpms off # turn off screen
- 0 23 * * * shutdown -h now
- 0 12 1 * * python2 /home/ubuntuserver/AnalizaSmartcashPI/AddToDatabaseSQL.py
### normal
- 30 7 * * * python2 /home/ubuntuserver/ExportSagaPI/ExportSaga_Facturi_Linux.py
- 30 10 1 * * python2 /home/ubuntuserver/AnalizaSmartcashPI/AnalizaSmartcashRPI.py

## Files/folders inside the working directory

- /opt/firebird/bin/isql
- Export/Intrari/Cob
- Export/Intrari/CTg
- Export/Intrari/CTp6
- Export/Intrari/Fet
- Export/Intrari/P1
- Export/Intrari/P2
- Export/Intrari/P3
- Export/Intrari/P4
- Export/Intrari/P5
