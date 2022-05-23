import ftplib
import os
import time
import mysql.connector as mysql
import requests
from requests.exceptions import HTTPError
from datetime import datetime, timedelta

import sys
import MySQLconnection



start_time = datetime.now()
db = mysql.connect(
    host=MySQLconnection.DB["host"],
    user=MySQLconnection.DB["user"],
    password=MySQLconnection.DB["password"],
    database=MySQLconnection.DB["database"],
    port=MySQLconnection.DB["port"]
)

cursor = db.cursor()

## defining the Query
print('Введите дату в формате год-месяц-день(2021-12-30)')
date = input()
#date = ((datetime.today() + timedelta(days=-1)).strftime('%Y-%m-%d'))
query = "SELECT * FROM cdr WHERE (calldate BETWEEN '" + date + " 00:00:00' AND '" + date + " 23:59:59') and (userfield!='')"

try:
    dir_name = "D://Download//"+date
    os.mkdir(dir_name)
except:
    print('папка уже существует')
## getting records from the table

cursor.execute(query)

## fetching all records from the 'cursor' object

records = cursor.fetchall()
ftp = ftplib.FTP(MySQLconnection.FTP["ftp"], MySQLconnection.FTP["login"], MySQLconnection.FTP["password"])
for record in records:
    if record[15] != '':
        CreateDate = int(record[00].timestamp())
        Duration = record[9]
        Kontr1 = record[2]
        Kontr2 = record[3]
        if record[15][6:].find("g",0,5) >= 0:
            newName = str(CreateDate) + "_" + str(Duration) + "_in_" + Kontr1 + "_" + Kontr2 + ".wav"
        elif record[15][6:].find("OUT",0,5) >= 0:
            newName = str(CreateDate) + "_" + str(Duration) + "_out_" + Kontr1 + "_" + Kontr2 + ".wav"
        else:
            newName = str(CreateDate) + "_" + str(Duration) + "_internal_" + Kontr1 + "_" + Kontr2 + ".wav"

        filename = os.path.join("\\", record[15][6:])
        lf = open("D://Download//"+date + "//" + newName, "wb")
        filename2 = record[15][6:]
        ftp.retrbinary("RETR " + record[15][6:], lf.write, 8*1024)

folder = os.listdir(dir_name)


for item in folder:
    PATH_TO_STORAGE_URL = "https://eu.vol1.io/storage/" + MySQLconnection.PATH_TO_STORAGE_UPLOAD + "/" + str(int((time.time()))) + "_" + item
    n = 99
    while n > 0:
        try:
            file = open(dir_name + "/" + item, 'rb')
            url = PATH_TO_STORAGE_URL
            H = {
                'X-Token': MySQLconnection.Token,
                'X-Token-Secret': MySQLconnection.TokenS,
            }
            r = requests.put(PATH_TO_STORAGE_URL, data=file, headers=H, allow_redirects=True, timeout=120)

        # если ответ успешен, исключения задействованы не будут
        except HTTPError as http_err:
            print('HTTP error occurred: %s', http_err)
        except Exception as err:
            print('Other error occurred: %s', err)
        else:
            if r.status_code == 201:
                print("OK")
                try:
                    file.close()
                    os.remove(dir_name + "//" + item)
                    print("Deleted")
                except OSError as e:  ## if failed, report it back to the user ##
                    print("Error: %s - %s." % (e.filename, e.strerror))

                n = 0
            else:
                n = n - 1
                print(r)
                print(item)


print(datetime.now() - start_time)
#sys.exit()



