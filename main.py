# import pandas as pd
# import numpy as np
# import os
# import pymysql
# import struct
# from dotenv import load_dotenv

# # โหลดค่าจาก .env
# load_dotenv()

# def get_mysql_conn():
#     return pymysql.connect(
#         host=os.getenv("MYSQL_HOST"),
#         port=int(os.getenv("MYSQL_PORT")),
#         user=os.getenv("MYSQL_USER"),
#         password=os.getenv("MYSQL_PASSWORD"),
#         database=os.getenv("MYSQL_DB"),
#         charset='utf8mb4',
#         cursorclass=pymysql.cursors.Cursor
#     )

# def load_snapshot(table_name):
#     snapshot_file = f'snapshots/{table_name}_snapshot.csv'
#     if os.path.exists(snapshot_file):
#         return pd.read_csv(snapshot_file)
#     else:
#         return pd.DataFrame()

# def save_snapshot(df, table_name):
#     os.makedirs("snapshots", exist_ok=True)
#     df.to_csv(f'snapshots/{table_name}_snapshot.csv', index=False)

# def data_cleansing(csv_url, table_name):
#     print(f"\nProcessing table: {table_name}")

#     # STEP 1: Load CSV
#     df = pd.read_csv(csv_url)
#     df.columns = df.columns.str.strip()
#     df_original = df.copy()

#     # STEP 2: Compare with snapshot
#     old_df = load_snapshot(table_name)
#     if 'TimeStamp' not in df.columns:
#         raise ValueError("Missing 'TimeStamp' column in data.")
    
#     df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
#     if not old_df.empty:
#         old_df['TimeStamp'] = pd.to_datetime(old_df['TimeStamp'])
#         df = df[~df['TimeStamp'].isin(old_df['TimeStamp'])]

#     if df.empty:
#         print("No new data to upload.")
#         return pd.DataFrame()

#     # STEP 3: Clean & Transform
#     LIMITS = {
#         'อุณหภูมิ_c': (5, 55), 
#         'อุณหภูมิดิน_c': (5, 55),
#         'ความชื้น_เปอร์เซ็นต์': (0, 100), 
#         'ความชื้นดิน_เปอร์เซ็นต์': (0, 100),
#         'แรงดัน_hPa': (950, 1250), 
#         'PH': (3, 9),
#         'ความเค็ม_เปอร์เซ็นต์': (0, 20000),
#         'ไนโตรเจน_เปอร์เซ็นต์': (0, 1999),
#         'ฟอสฟอรัส_เปอร์เซ็นต์': (0, 1999),
#         'โพแทสเซียม_เปอร์เซ็นต์': (0, 1999),
#     }
#     for col, (mn, mx) in LIMITS.items():
#         df[col] = pd.to_numeric(df[col], errors='coerce')
#         df[col] = df[col].where(df[col].between(mn, mx), np.nan)

#     cols = list(LIMITS.keys())
#     for col in cols:
#         df[col] = df[col].interpolate(method='pchip', limit_direction='both')
#         df[col] = df[col].ffill().bfill()
#         df[col] = df[col].rolling(window=3, min_periods=2, center=True).mean()
#     df[cols] = df[cols].round(1)

#     # STEP 4: Upload
#     conn = get_mysql_conn()
#     cursor = conn.cursor()
#     for _, row in df.iterrows():
#         cursor.execute(f"""
#             INSERT INTO {table_name} (
#                 TimeStamp, AirTemp, SoilTemp, AirHumi, SoilHumi, Pressure,
#                 LightIntensity, FloatingBall, PH, Conductivity,
#                 Nitrogen, Phosphorus, Potassium, WindSpeed
#             ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """, tuple(row.values))
#     conn.commit()
#     cursor.close()
#     conn.close()
#     print(f"Uploaded {df.shape[0]} new rows to {table_name}")
    

#     # STEP 5: Save updated snapshot
#     updated_snapshot = pd.concat([old_df, df], ignore_index=True)
#     save_snapshot(updated_snapshot, table_name)

#     return df

# # ฟังก์ชันสำหรับแต่ละชีต
# def data_cleansing1():
#     return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vSjA_VIRX-RfDE_jbtdK1bjGv05z2XP0gSTSfzHSNjqlr537UQ4L0SvX4qPBh2oewlnbatg90kMYV-g/pub?output=csv', 'mydurian_db1')
# def data_cleansing2():
#     return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vQevKYzrZn0tJ91s6_3aJ-3_qWkuJkMO1iIRknDokLowCQ_oaET8sEVId-vfo31M4mQ22Sy7SyE1mCz/pub?output=csv', 'mydurian_db2')
# def data_cleansing3():
#     return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vT57fByqaHxnBejXuSfN8Y3PtOu88zX9hbDWHjZ_bpcxQLj5yZ5qS0ZusfkfvZ6ZnysXRQE6I9dXkjE/pub?output=csv', 'mydurian_db3')
# def data_cleansing4():
#     return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vR9QpP-p0LsaEuPoRhHg4aBXkkznMMYzzEGvAATCOCGwAIsb1Nt50W10MkrlVcEmq3Yf_3Xuoq9YUtE/pub?output=csv', 'mydurian_db4')
# def data_cleansing5():
#     return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vTK_TPcAsI0ZKEVYK4QSrowLAuPCOOAxGfoW2tKOGIkt5_h-k-DFwcyMQn2YlYKUXiCcNjK5el0Qg3z/pub?output=csv', 'mydurian_db5')
# def data_cleansing6():
#     return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vStKEFx923EuiVg46bsSkhcvnZh6Ty2bXD9dQkso03DWcODL0EcN5w7mZw1HjdcbBypjx8_EnRLoJ84/pub?output=csv', 'mydurian_db6')
# # def data_cleansing7():
# #     return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vTkvJN2nfTq60D6ZD8zk1e-QAVj-9mRDcvIBY0RCEf8KKkORZJ-nonDB-zcVcB5pcFAIdSC3lpBkR2j/pub?output=csv', 'mydurian_db7')
# def data_cleansing8():
#     return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vQ-UHLiWuqHGRWUZWAuIpIZV_hQuBYrr_Q6qgoy4gN6edTJjwR9c0AQljtr5NJPBkN_zmblPJ7wfr7y/pub?output=csv', 'mydurian_db8')
# def data_cleansing9():
#     return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vSJwIebx8s1_0Vl5GQ3ZQnU7eT38rJKDc3OPCj_dzB_cH9ZR3fERIFb-3Hf4lbPhP7foqSkcJyHz43v/pub?output=csv', 'mydurian_db9')
# def data_cleansing10():
#     return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vQ8w2gCDBakWUoxLoCGoIv1Hl3l_NprEuiR1W1Mgu0McPmvjIWoIMCtTLkOlEkl4mxGdLkJBD-ZuTaO/pub?output=csv', 'mydurian_db10')

# # เรียกใช้งาน
# if __name__ == "__main__":
#     data_cleansing1()
#     data_cleansing2()
#     data_cleansing3()
#     data_cleansing4()
#     data_cleansing5()
#     data_cleansing6()
#     # data_cleansing7()
#     data_cleansing8()
#     data_cleansing9()
#     data_cleansing10()














import pandas as pd
import numpy as np
import os
import pymysql
from dotenv import load_dotenv

# load data from .env
load_dotenv()

def get_mysql_conn():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT")),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DB"),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.Cursor
    )

def load_snapshot(table_name):
    snapshot_file = f'snapshots/{table_name}_snapshot.csv'
    if os.path.exists(snapshot_file):
        return pd.read_csv(snapshot_file)
    else:
        return pd.DataFrame()

def save_snapshot(df, table_name):
    os.makedirs("snapshots", exist_ok=True)
    df.to_csv(f'snapshots/{table_name}_snapshot.csv', index=False)

def data_cleansing(csv_url, table_name):
    print(f"\nProcessing table: {table_name}")

    # STEP 1: Load CSV
    df = pd.read_csv(csv_url)
    df.columns = df.columns.str.strip()

    # STEP 2: Compare with snapshot
    old_df = load_snapshot(table_name)
    if 'TimeStamp' not in df.columns:
        raise ValueError("Missing 'TimeStamp' column in data.")
    
    df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
    if not old_df.empty:
        old_df['TimeStamp'] = pd.to_datetime(old_df['TimeStamp'])
        df = df[~df['TimeStamp'].isin(old_df['TimeStamp'])]

    if df.empty:
        print("No new data to upload.")
        return pd.DataFrame()

    # STEP 3: Clean & Transform
    LIMITS = {
        'อุณหภูมิ_c': (5, 55), 
        'อุณหภูมิดิน_c': (5, 55),
        'ความชื้น_เปอร์เซ็นต์': (0, 100), 
        'ความชื้นดิน_เปอร์เซ็นต์': (0, 100),
        'แรงดัน_hPa': (950, 1250), 
        'PH': (3, 9),
        'ความเค็ม_เปอร์เซ็นต์': (0, 20000),
        'ไนโตรเจน_เปอร์เซ็นต์': (0, 1999),
        'ฟอสฟอรัส_เปอร์เซ็นต์': (0, 1999),
        'โพแทสเซียม_เปอร์เซ็นต์': (0, 1999),
    }
    for col, (mn, mx) in LIMITS.items():
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col] = df[col].where(df[col].between(mn, mx), np.nan)

    cols = list(LIMITS.keys())
    for col in cols:
        df[col] = df[col].interpolate(method='pchip', limit_direction='both')
        df[col] = df[col].ffill().bfill()
        df[col] = df[col].rolling(window=3, min_periods=2, center=True).mean()
    df[cols] = df[cols].round(1)

    # STEP 4: Upload
    conn = get_mysql_conn()
    cursor = conn.cursor()
    data_tuples = [tuple(row.values) for _, row in df.iterrows()]
    cursor.executemany(f"""
        INSERT INTO {table_name} (
            TimeStamp, AirTemp, SoilTemp, AirHumi, SoilHumi, Pressure,
            LightIntensity, FloatingBall, PH, Conductivity,
            Nitrogen, Phosphorus, Potassium, WindSpeed
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, data_tuples)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Uploaded {df.shape[0]} new rows to {table_name}")
    
    # STEP 5: Save updated snapshot
    updated_snapshot = pd.concat([old_df, df], ignore_index=True)
    save_snapshot(updated_snapshot, table_name)

    return df

# ฟังก์ชันสำหรับแต่ละชีต
def data_cleansing1():
    return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vSjA_VIRX-RfDE_jbtdK1bjGv05z2XP0gSTSfzHSNjqlr537UQ4L0SvX4qPBh2oewlnbatg90kMYV-g/pub?output=csv', 'mydurian_db1')
def data_cleansing2():
    return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vQevKYzrZn0tJ91s6_3aJ-3_qWkuJkMO1iIRknDokLowCQ_oaET8sEVId-vfo31M4mQ22Sy7SyE1mCz/pub?output=csv', 'mydurian_db2')
def data_cleansing3():
    return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vT57fByqaHxnBejXuSfN8Y3PtOu88zX9hbDWHjZ_bpcxQLj5yZ5qS0ZusfkfvZ6ZnysXRQE6I9dXkjE/pub?output=csv', 'mydurian_db3')
def data_cleansing4():
    return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vR9QpP-p0LsaEuPoRhHg4aBXkkznMMYzzEGvAATCOCGwAIsb1Nt50W10MkrlVcEmq3Yf_3Xuoq9YUtE/pub?output=csv', 'mydurian_db4')
def data_cleansing5():
    return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vTK_TPcAsI0ZKEVYK4QSrowLAuPCOOAxGfoW2tKOGIkt5_h-k-DFwcyMQn2YlYKUXiCcNjK5el0Qg3z/pub?output=csv', 'mydurian_db5')
def data_cleansing6():
    return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vStKEFx923EuiVg46bsSkhcvnZh6Ty2bXD9dQkso03DWcODL0EcN5w7mZw1HjdcbBypjx8_EnRLoJ84/pub?output=csv', 'mydurian_db6')
# def data_cleansing7():
#     return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vTkvJN2nfTq60D6ZD8zk1e-QAVj-9mRDcvIBY0RCEf8KKkORZJ-nonDB-zcVcB5pcFAIdSC3lpBkR2j/pub?output=csv', 'mydurian_db7')
def data_cleansing8():
    return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vQ-UHLiWuqHGRWUZWAuIpIZV_hQuBYrr_Q6qgoy4gN6edTJjwR9c0AQljtr5NJPBkN_zmblPJ7wfr7y/pub?output=csv', 'mydurian_db8')
def data_cleansing9():
    return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vSJwIebx8s1_0Vl5GQ3ZQnU7eT38rJKDc3OPCj_dzB_cH9ZR3fERIFb-3Hf4lbPhP7foqSkcJyHz43v/pub?output=csv', 'mydurian_db9')
def data_cleansing10():
    return data_cleansing('https://docs.google.com/spreadsheets/d/e/2PACX-1vQ8w2gCDBakWUoxLoCGoIv1Hl3l_NprEuiR1W1Mgu0McPmvjIWoIMCtTLkOlEkl4mxGdLkJBD-ZuTaO/pub?output=csv', 'mydurian_db10')

if __name__ == "__main__":
    funcs = [
        data_cleansing1,
        data_cleansing2,
        data_cleansing3,
        data_cleansing4,
        data_cleansing5,
        data_cleansing6,
        # data_cleansing7,  # Skipped if needed
        data_cleansing8,
        data_cleansing9,
        data_cleansing10
    ]

    for func in funcs:
        try:
            func()
        except Exception as e:
            print(f"Skipping {func.__name__} due to error: {e}")
