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

data = pd.read_csv('./csv_data/LocationImage.csv', encoding='CP949')
print(len(data))
for i in range(100000, len(data)):
    name = str(data["ObjectName"][i])
    name = name.split("_")[0]
    cur.execute("SELECT location_id FROM location WHERE name='" + name + "'")
    result = str(cur.fetchall())
    result = re.sub('[^0-9]', '', result)
    if not result or len(result) > 5:
        err_li.append(i)
        continue
    url = str(data["ObjectUrl"][i])
    try:
        cur.execute("INSERT INTO location_image VALUES (" + str(i) + ",'" + url + "'," + result + ")")
        print(i)
    except:
        err_li.append(result)
        
picnic_db.commit()
picnic_db.close()
print(len(err_li))