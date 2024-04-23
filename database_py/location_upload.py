import pandas as pd
import pymysql
import re

total_len = 0
err_li = []

picnic_db = pymysql.connect(
    user={database_user}, 
    passwd={database_password}, 
    host={database_host}, 
    db={database_name}, 
    charset='utf8'
)
cur = picnic_db.cursor()
cities = ["서울특별시", "대전광역시", "부산광역시", "광주광역시", "울산광역시", "대구광역시", "인천광역시", "세종특별자치시"]


# Culture
data = pd.read_csv('./csv_data/Culture.csv')
for i in range(len(data)):
    # location table
    name = str(data["name"][i])
    name = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', name)
    phone = str(data["phone"][i])
    phone = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', phone)
    address = str(data["address"][i])
    address = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', address)
    if len(address) != 0 :
        city = str(address.split()[0])
        if address.split()[0] not in cities and len(address.split()) > 2:
            city += " " + str(address.split()[1])
    city = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', city)
    details = str(data["details"][i]).replace("<br />", "")
    details = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', details)
    latitude = str(data["latitude"][i])
    longitude = str(data["longitude"][i])
    try:
        cur.execute("INSERT INTO location VALUES (" + str(i) + ",'" + address + "','" + city + "','" + details + "','문화시설'," + latitude + "," + longitude + ",'" + name + "','" + phone +"')")
    except:
        err_li.append(str(i) + " error in location : \n" + "INSERT INTO location VALUES (" + str(i) + ",'" + address + "','" + city + "','" + details + "','문화시설'," + latitude + "," + longitude + ",'" + name + "','" + phone +"')")
        continue
    # culture table
    time = str(data["time"][i]).replace("<br />", "")
    time = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', time)
    offDate = str(data["offDate"][i]).replace("<br />", "")
    offDate = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', offDate)
    fee = str(data["fee"][i]).replace("<br />", "")
    fee = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', fee)
    discount = str(data["discount"][i]).replace("<br />", "")
    discount = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', discount)
    parking = str(data["parking"][i]).replace("<br />", "")
    parking = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', parking)
    babycar = str(data["babycar"][i]).replace("<br />", "")
    babycar = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', babycar)
    pet = str(data["pet"][i]).replace("<br />", "")
    pet = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', pet)
    detail = str(data["detail"][i]).replace("<br />", "")
    detail = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', detail)
    try:
        cur.execute("INSERT INTO culture VALUES (" + str(i) + ",'" + babycar + "','" + detail + "','" + discount + "','" + fee + "','" + offDate + "','" + parking + "','" + pet + "','" + time + "'," + str(i) + ")")
    except:
        err_li.append(str(i) + " error in culture : \n" + "INSERT INTO culture VALUES (" + str(i) + ",'" + babycar + "','" + detail + "','" + discount + "','" + fee + "','" + offDate + "','" + parking + "','" + pet + "','" + time + "'," + str(i) + ")")
        
total_len += len(data)
print("culture done : ", len(data), "EA")

# Accomodation
data = pd.read_csv('./csv_data/Accomodation.csv')
for i in range(len(data)):
    # location table
    name = str(data["name"][i])
    name = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', name)
    phone = str(data["phone"][i])
    phone = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', phone)
    address = str(data["address"][i])
    if len(address) != 0 :
        city = str(address.split()[0])
        if address.split()[0] not in cities and len(address.split()) > 2:
            city += " " + str(address.split()[1])
    address = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', address)
    details = str(data["details"][i]).replace("<br />", "")
    details = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', details)
    latitude = str(data["latitude"][i])
    longitude = str(data["longitude"][i])
    try:
        cur.execute("INSERT INTO location VALUES (" + str(i + total_len) + ",'" + address + "','" + city + "','" + details + "','숙박'," + latitude + "," + longitude + ",'" + name + "','" + phone +"')")
    except:
        err_li.append(str(i) + " error in location : " + "INSERT INTO location VALUES (" + str(i + total_len) + ",'" + address + "','" + city + "','" + details + "','숙박'," + latitude + "," + longitude + ",'" + name + "','" + phone +"')")
        continue
    # accommodation table
    check_in = str(data["checkIn"][i]).replace("<br />", "")
    check_in = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', check_in)
    check_out = str(data["checkOut"][i]).replace("<br />", "")
    check_out = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', check_out)
    cook = str(data["cook"][i]).replace("<br />", "")
    cook = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', cook)
    detail = str(data["detail"][i]).replace("<br />", "")
    detail = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', detail)
    parking = str(data["parking"][i]).replace("<br />", "")
    parking = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', parking)
    reservation = str(data["reservation"][i]).replace("<br />", "")
    reservation = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', reservation)
    try:
        cur.execute("INSERT INTO accommodation VALUES (" + str(i) + ",'" + check_in + "','" + check_out + "','" + cook + "','" + detail + "','" + parking + "','" + reservation + "'," + str(i + total_len) + ")")
    except:
        err_li.append(str(i) + " error in accomodation : " + "INSERT INTO accommodation VALUES (" + str(i) + ",'" + check_in + "','" + check_out + "','" + cook + "','" + detail + "','" + parking + "','" + reservation + "'," + str(i + total_len) + ")")

total_len += len(data)
print("accomodation done : ", len(data), "EA")


# Festival
data = pd.read_csv('./csv_data/Festival.csv')
for i in range(len(data)):
    # location table
    name = str(data["name"][i])
    name = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', name)
    phone = str(data["phone"][i])
    phone = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', phone)
    address = str(data["address"][i])
    if len(address) != 0 :
        city = str(address.split()[0])
        if address.split()[0] not in cities and len(address.split()) > 2:
            city += " " + str(address.split()[1])
    address = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', address)
    details = str(data["details"][i]).replace("<br />", "")
    details = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', details)
    latitude = str(data["latitude"][i])
    longitude = str(data["longitude"][i])
    try:
        cur.execute("INSERT INTO location VALUES (" + str(i + total_len) + ",'" + address + "','" + city + "','" + details + "','축제 공연 행사'," + latitude + "," + longitude + ",'" + name + "','" + phone +"')")
    except:
        err_li.append(str(i) + " error in location : " + "INSERT INTO location VALUES (" + str(i + total_len) + ",'" + address + "','" + city + "','" + details + "','축제 공연 행사'," + latitude + "," + longitude + ",'" + name + "','" + phone +"')")
        continue
    # festival table
    detail = str(data["detail"][i]).replace("<br />", "")
    detail = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', detail)
    endDate = str(data["endDate"][i]).replace("<br />", "")
    endDate = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', endDate)
    fee = str(data["fee"][i]).replace("<br />", "")
    fee = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', fee)
    startDate = str(data["startDate"][i]).replace("<br />", "")
    startDate = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', startDate)
    time = str(data["time"][i]).replace("<br />", "")
    time = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', time)
    try:
        cur.execute("INSERT INTO festival VALUES (" + str(i) + ",'" + detail + "','" + endDate + "','" + fee + "','" + startDate + "','" + time + "'," + str(i + total_len) + ")")
    except:
        err_li.append(str(i) + " error in festival : " + "INSERT INTO festival VALUES (" + str(i) + ",'" + detail + "','" + endDate + "','" + fee + "','" + startDate + "','" + time + "'," + str(i + total_len) + ")")

total_len += len(data)
print("festival done : ", len(data), "EA")


# Leisure
data = pd.read_csv('./csv_data/Leisure.csv')
for i in range(len(data)):
    # location table
    name = str(data["name"][i])
    name = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', name)
    phone = str(data["phone"][i])
    phone = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', phone)
    address = str(data["address"][i])
    if len(address) != 0 :
        city = str(address.split()[0])
        if address.split()[0] not in cities and len(address.split()) > 2:
            city += " " + str(address.split()[1])
    address = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', address)
    details = str(data["details"][i]).replace("<br />", "")
    details = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', details)
    latitude = str(data["latitude"][i])
    longitude = str(data["longitude"][i])
    try:
        cur.execute("INSERT INTO location VALUES (" + str(i + total_len) + ",'" + address + "','" + city + "','" + details + "','레포츠'," + latitude + "," + longitude + ",'" + name + "','" + phone +"')")
    except:
        err_li.append(str(i) + " error in location : " + "INSERT INTO location VALUES (" + str(i + total_len) + ",'" + address + "','" + city + "','" + details + "','레포츠'," + latitude + "," + longitude + ",'" + name + "','" + phone +"')")
        continue
    # leisure table
    babycar = str(data["babycar"][i]).replace("<br />", "")
    babycar = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', babycar)
    detail = str(data["detail"][i]).replace("<br />", "")
    detail = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', detail)
    fee = str(data["fee"][i]).replace("<br />", "")
    fee = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', fee)
    offDate = str(data["offDate"][i]).replace("<br />", "")
    offDate = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', offDate)
    openDate = str(data["openDate"][i]).replace("<br />", "")
    openDate = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', openDate)
    parking = str(data["parking"][i]).replace("<br />", "")
    parking = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', parking)
    pet = str(data["pet"][i]).replace("<br />", "")
    pet = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', pet)
    time = str(data["time"][i]).replace("<br />", "")
    time = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', time)
    try:
        cur.execute("INSERT INTO leisure VALUES (" + str(i) + ",'" + babycar + "','" + detail + "','" + fee + "','" + offDate + "','" + openDate + "','" + parking + "','" + pet + "','" + time + "'," + str(i + total_len) + ")")
    except:
        err_li.append(str(i) + " error in leisure : \n" + "INSERT INTO leisure VALUES (" + str(i) + ",'" + babycar + "','" + detail + "','" + fee + "','" + offDate + "','" + openDate + "','" + parking + "','" + pet + "','" + time + "'," + str(i + total_len) + ")")

total_len += len(data)
print("leisure done : ", len(data), "EA")


# Restaurant
data = pd.read_csv('./csv_data/Restaurant.csv')
for i in range(len(data)):
    # location table
    name = str(data["name"][i])
    name = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', name)
    phone = str(data["phone"][i])
    phone = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', phone)
    address = str(data["address"][i])
    if len(address) != 0 :
        city = str(address.split()[0])
        if address.split()[0] not in cities and len(address.split()) > 2:
            city += " " + str(address.split()[1])
    address = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', address)
    details = str(data["details"][i]).replace("<br />", "")
    details = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', details)
    latitude = str(data["latitude"][i])
    longitude = str(data["longitude"][i])
    try:
        cur.execute("INSERT INTO location VALUES (" + str(i + total_len) + ",'" + address + "','" + city + "','" + details + "','음식점'," + latitude + "," + longitude + ",'" + name + "','" + phone +"')")
    except:
        err_li.append(str(i) + " error in location : " + "INSERT INTO location VALUES (" + str(i + total_len) + ",'" + address + "','" + city + "','" + details + "','음식점'," + latitude + "," + longitude + ",'" + name + "','" + phone +"')")
        continue
    # restaurant table
    dayOff = str(data["dayOff"][i]).replace("<br />", "")
    dayOff = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', dayOff)
    mainMenu = str(data["mainMenu"][i]).replace("<br />", "")
    mainMenu = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', mainMenu)
    menu = str(data["menu"][i]).replace("<br />", "")
    menu = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', menu)
    packaging = str(data["packaging"][i]).replace("<br />", "")
    packaging = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', packaging)
    parking = str(data["parking"][i]).replace("<br />", "")
    parking = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', parking)
    try:
        cur.execute("INSERT INTO restaurant VALUES (" + str(i) + ",'" + dayOff + "','" + mainMenu + "','" + menu + "','" + packaging + "','" + parking + "'," + str(i + total_len) + ")")
    except:
        err_li.append(str(i) + " error in restaurant : \n" + "INSERT INTO restaurant VALUES (" + str(i) + ",'" + dayOff + "','" + mainMenu + "','" + menu + "','" + packaging + "','" + parking + "'," + str(i + total_len) + ")")

total_len += len(data)
print("restaurant done : ", len(data), "EA")


# Shoping
data = pd.read_csv('./csv_data/Shoping.csv')
for i in range(len(data)):
    # location table
    name = str(data["name"][i])
    name = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', name)
    phone = str(data["phone"][i])
    phone = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', phone)
    address = str(data["address"][i])
    if len(address) != 0 :
        city = str(address.split()[0])
        if address.split()[0] not in cities and len(address.split()) > 2:
            city += " " + str(address.split()[1])
    address = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', address)
    details = str(data["details"][i]).replace("<br />", "")
    details = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', details)
    latitude = str(data["latitude"][i])
    longitude = str(data["longitude"][i])
    try:
        cur.execute("INSERT INTO location VALUES (" + str(i + total_len) + ",'" + address + "','" + city + "','" + details + "','쇼핑'," + latitude + "," + longitude + ",'" + name + "','" + phone +"')")
    except:
        err_li.append(str(i) + " error in location : " + "INSERT INTO location VALUES (" + str(i + total_len) + ",'" + address + "','" + city + "','" + details + "','쇼핑'," + latitude + "," + longitude + ",'" + name + "','" + phone +"')")
        continue
    # shopping table
    babycar = str(data["babycar"][i]).replace("<br />", "")
    babycar = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', babycar)
    offDate = str(data["offdate"][i]).replace("<br />", "")
    offDate = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', offDate)
    parking = str(data["parking"][i]).replace("<br />", "")
    parking = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', parking)
    pet = str(data["pet"][i]).replace("<br />", "")
    pet = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', pet)
    time = str(data["time"][i]).replace("<br />", "")
    time = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', time)
    try:
        cur.execute("INSERT INTO shopping VALUES (" + str(i) + ",'" + babycar + "','" + offDate + "','" + parking + "','" + pet + "','" + time + "'," + str(i + total_len) + ")")
    except:
        err_li.append(str(i) + " error in shopping : \n" + "INSERT INTO shopping VALUES (" + str(i) + ",'" + babycar + "','" + offDate + "','" + parking + "','" + pet + "','" + time + "'," + str(i + total_len) + ")")

total_len += len(data)
print("shopping done : ", len(data), "EA")


# Tour
data = pd.read_csv('./csv_data/Tour.csv')
for i in range(len(data)):
    # location table
    name = str(data["name"][i])
    name = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', name)
    phone = str(data["phone"][i])
    phone = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', phone)
    address = str(data["address"][i])
    if len(address) != 0 :
        city = str(address.split()[0])
        if address.split()[0] not in cities and len(address.split()) > 2:
            city += " " + str(address.split()[1])
    address = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', address)
    details = str(data["details"][i]).replace("<br />", "")
    details = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', details)
    latitude = str(data["latitude"][i])
    longitude = str(data["longitude"][i])
    try:
        cur.execute("INSERT INTO location VALUES (" + str(i + total_len) + ",'" + address + "','" + city + "','" + details + "','관광지'," + latitude + "," + longitude + ",'" + name + "','" + phone +"')")
    except:
        err_li.append(str(i) + " error in location : " + "INSERT INTO location VALUES (" + str(i + total_len) + ",'" + address + "','" + city + "','" + details + "','관광지'," + latitude + "," + longitude + ",'" + name + "','" + phone +"')")
        continue
    # tour table
    babycar = str(data["babycar"][i]).replace("<br />", "")
    babycar = re.sub('[^0-9a-zA-Z-_.:~ㄱ-ㅣ가-힣\s]', '', babycar)
    detail = str(data["detail"][i]).replace("<br />", "")
    detail = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', detail)
    offDate = str(data["offdate"][i]).replace("<br />", "")
    offDate = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', offDate)
    parking = str(data["parking"][i]).replace("<br />", "")
    parking = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', parking)
    pet = str(data["pet"][i]).replace("<br />", "")
    pet = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', pet)
    time = str(data["time"][i]).replace("<br />", "")
    time = re.sub('[^0-9a-zA-Z-_.ㄱ-ㅣ가-힣\s]', '', time)
    try:
        cur.execute("INSERT INTO tour VALUES (" + str(i) + ",'" + babycar + "','" + detail + "','" + offDate + "','" + parking + "','" + pet + "','" + time + "'," + str(i + total_len) + ")")
    except:
        err_li.append(str(i) + " error in tour : \n" + "INSERT INTO tour VALUES (" + str(i) + ",'" + babycar + "','" + detail + "','" + offDate + "','" + parking + "','" + pet + "','" + time + "'," + str(i + total_len) + ")")

total_len += len(data)
print("tour done : ", len(data), "EA")

print("error count : ", len(err_li), " EA")
for s in err_li:
    print(s)

picnic_db.commit()
picnic_db.close()