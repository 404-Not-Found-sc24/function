import pandas as pd
import pymysql
import re

err_li = []

picnic_db = pymysql.connect(
    user={database_user}, 
    passwd={database_password}, 
    host={database_host}, 
    db={database_name}, 
    charset='utf8'
)

cur = picnic_db.cursor()

data = pd.read_csv('./csv_data/cityImagesOutput.csv', encoding='CP949')
print(len(data))

for i in range(len(data)):
    url = data['ObjectUrl'][i]
    id = data['Number'][i]
    name = data["ObjectName"][i].split(".")[0]
    try:
        cur.execute("INSERT INTO city VALUES (" + str(i) + ",'" + url + "','" + name + "')")
    except:
        print(i)
    
picnic_db.commit()
picnic_db.close()