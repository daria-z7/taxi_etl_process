from io import StringIO, BytesIO
import xml.etree.ElementTree as ET

import pandas as pd

from db.create_ftps_conn import create_ftps
from db.functions import formate_date, yesterday
from db.upload_table_data import get_pers_num, get_last_trans_id


def get_waybills_data(days_back: int):
    df = pd.DataFrame(columns=[
        'waybill_num',
        'driver_pers_num',
        'car_plate_num',
        'work_start_dt',
        'work_end_dt',
        'issue_dt'
        ])

    ftps = create_ftps()
    file_list = []
    yesterday_date = yesterday(days_back=days_back)

    ftps.retrlines('LIST /waybills', file_list.append)
    for file in file_list:
        file_data = str(file).split()
        file_date = formate_date(file_data[-4], file_data[-3]) 
        if file_date == yesterday_date:
            r = StringIO()
            ftps.retrlines(f'RETR /waybills/{file_data[-1]}', r.write)
            tree = ET.ElementTree(ET.fromstring(r.getvalue()))
            root = tree.getroot()
            for element in root.findall('waybill'):
                waybill_num = element.attrib['number']
                car_plate_num = element.find('car').text
                issue_dt = element.attrib['issuedt']
                for snode in element.findall('driver'):
                    d_name = snode.find('name').text
                    d_license = snode.find('license').text
                    pers_num = get_pers_num(str(d_license))
                for snode in element.findall('period'):
                    start_dt = snode.find('start').text
                    end_dt = snode.find('stop').text
            r.close()
            df.loc[df.shape[0]] = [waybill_num, pers_num, car_plate_num, start_dt, end_dt, issue_dt]
    ftps.quit()
    return df

def get_payment_data(days_back: int):
    df = pd.DataFrame(columns=[
        'transaction_id',
        'card_num',
        'transaction_amt',
        'transaction_dt',
        ])
    
    ftps = create_ftps()
    file_list = []
    yesterday_date = yesterday(days_back=days_back)
    ftps.retrlines('LIST /payments', file_list.append)
    for file in file_list:
        file_data = str(file).split()
        file_date = formate_date(file_data[-4], file_data[-3]) 
        if file_date == yesterday_date:
            r = BytesIO()
            try:
                ftps.retrbinary(f'RETR /payments/{file_data[-1]}', r.write)
            except EOFError as error:
                ftps.quit()
                count = get_last_trans_id()
                df['transaction_id'] = pd.RangeIndex(count, count + len(df)) + 1
                return df
            r.seek(0)
            df1 = pd.read_csv(r, sep='\t', header=None, names = ['transaction_dt', 'card_num', 'transaction_amt',])
            df = pd.concat([df, df1]) 
    ftps.quit()
    count = get_last_trans_id()
    df['transaction_id'] = pd.RangeIndex(count, count + len(df)) + 1
    df['transaction_dt'] = df['transaction_dt'].astype('datetime64[ns]')
    return df
