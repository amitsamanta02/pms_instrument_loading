import requests
import mysql.connector
from mysql.connector import IntegrityError


def database_scheme_load(data, data_dict):
    meta = data_dict['meta']
    val = (data['schemeCode'], data['schemeName'], meta['fund_house'], meta['scheme_type'], meta['scheme_category'])
    try:
        curd.execute(schemesql, val)
    except IntegrityError as err:
        print(err)
        print("dropping record " + str(val))
    # for val in navdata:
    #     naval = (data['schemeCode'],decimal.Decimal(val['nav']),val['date'])
    #     print(naval)
    #      try:
    #          curd.execute(navsql,naval)
    #      except IntegrityError as err:
    #          print(err)
    #          print("dropping nav record " + str(naval))

    mydb.commit()


def process_mf_scheme(data_list):
    curd.execute(get_max_id)
    result = curd.fetchall()
    max_id = '99999999'
    for r in result:
        max_id = r[0]

    print("max_id:" + max_id)

    for data in data_list:
        parturl = str(data['schemeCode'])
        if max_id < parturl:
            url = navurl + parturl
            res = requests.get(url)
            if res.status_code == 200:
                try:
                    data_dict = res.json()
                    database_scheme_load(data, data_dict)
                except ValueError as e:
                    print(f"Exception: {e}")
                    print(str(response.content))
                    print(str(response.headers))
                    print(res.text)
        else:
            print('Bypass as record already in db')
            print(parturl)


if __name__ == '__main__':
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="asdf1234"
    )
    curd = mydb.cursor()
    schemeurl = "https://api.mfapi.in/mf"
    schemesql = ("insert into security.mf_info(mf_code,fund_name,created_userid,created_date,update_date,fund_house,"
                 "scheme_type,scheme_category) values (%s,%s,'samanami',current_timestamp, current_timestamp,%s,%s,%s)")

    navsql = "insert into security.mf_nav_detail(mf_id,nav,date) values(%s,%s,%s)"

    get_max_id = "select max(mf_code) from security.mf_info;"

    navurl = "https://api.mfapi.in/mf/"

    response = requests.get(schemeurl)

    if response.status_code == 200:
        try:
            res_dict = response.json()
            process_mf_scheme(res_dict)
        except ValueError as exc:
            print(f"Exception: {exc}")
            print(str(response.content))
            print(str(response.headers))
    else:
        print('not able consume mutual fund scheme data')
