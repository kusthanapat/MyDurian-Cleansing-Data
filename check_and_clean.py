import os
import hashlib
import pandas as pd
from main import *  # import ฟังก์ชัน cleansing ทั้ง 16 อัน

DATA_SOURCES = [
    ('https://docs.google.com/spreadsheets/d/e/2PACX-1vSjA_VIRX-RfDE_jbtdK1bjGv05z2XP0gSTSfzHSNjqlr537UQ4L0SvX4qPBh2oewlnbatg90kMYV-g/pub?output=csv', data_cleansing1, 'Sheet1'),
    ('https://docs.google.com/spreadsheets/d/e/2PACX-1vQevKYzrZn0tJ91s6_3aJ-3_qWkuJkMO1iIRknDokLowCQ_oaET8sEVId-vfo31M4mQ22Sy7SyE1mCz/pub?output=csv', data_cleansing2, 'Sheet2'),
    ('https://docs.google.com/spreadsheets/d/e/2PACX-1vT57fByqaHxnBejXuSfN8Y3PtOu88zX9hbDWHjZ_bpcxQLj5yZ5qS0ZusfkfvZ6ZnysXRQE6I9dXkjE/pub?output=csv', data_cleansing3, 'Sheet3'),
    ('https://docs.google.com/spreadsheets/d/e/2PACX-1vR9QpP-p0LsaEuPoRhHg4aBXkkznMMYzzEGvAATCOCGwAIsb1Nt50W10MkrlVcEmq3Yf_3Xuoq9YUtE/pub?output=csv', data_cleansing4, 'Sheet4'),
    ('https://docs.google.com/spreadsheets/d/e/2PACX-1vTK_TPcAsI0ZKEVYK4QSrowLAuPCOOAxGfoW2tKOGIkt5_h-k-DFwcyMQn2YlYKUXiCcNjK5el0Qg3z/pub?output=csv', data_cleansing5, 'Sheet5'),
    ('https://docs.google.com/spreadsheets/d/e/2PACX-1vStKEFx923EuiVg46bsSkhcvnZh6Ty2bXD9dQkso03DWcODL0EcN5w7mZw1HjdcbBypjx8_EnRLoJ84/pub?output=csv', data_cleansing6, 'Sheet6'),
    ('https://docs.google.com/spreadsheets/d/e/2PACX-1vTkvJN2nfTq60D6ZD8zk1e-QAVj-9mRDcvIBY0RCEf8KKkORZJ-nonDB-zcVcB5pcFAIdSC3lpBkR2j/pub?output=csv', data_cleansing7, 'Sheet7'),
    ('https://docs.google.com/spreadsheets/d/e/2PACX-1vQ-UHLiWuqHGRWUZWAuIpIZV_hQuBYrr_Q6qgoy4gN6edTJjwR9c0AQljtr5NJPBkN_zmblPJ7wfr7y/pub?output=csv', data_cleansing8, 'Sheet8'),
    ('https://docs.google.com/spreadsheets/d/e/2PACX-1vSJwIebx8s1_0Vl5GQ3ZQnU7eT38rJKDc3OPCj_dzB_cH9ZR3fERIFb-3Hf4lbPhP7foqSkcJyHz43v/pub?output=csv', data_cleansing9, 'Sheet9'),
    ('https://docs.google.com/spreadsheets/d/e/2PACX-1vQ8w2gCDBakWUoxLoCGoIv1Hl3l_NprEuiR1W1Mgu0McPmvjIWoIMCtTLkOlEkl4mxGdLkJBD-ZuTaO/pub?output=csv', data_cleansing10, 'Sheet10'),
    ('https://docs.google.com/spreadsheets/d/e/2PACX-1vRp3MHesEttQ1Av3bzYwYt5711pmYA93U3aSdkJEUjDNzPiwDVLFRH8mhzc4ROdrKk2fTRgsXllzXOh/pub?output=csv', data_cleansing11, 'Sheet11'),
    ('https://docs.google.com/spreadsheets/d/e/2PACX-1vRgcrJC2tkafw1m8tiRZ80dE7_C2LBXIQ4Woazu5j2jlQgw8tL5oAMRdtx3uirVYg8DJaVeWayP5D5A/pub?output=csv', data_cleansing12, 'Sheet12'),
    ('https://docs.google.com/spreadsheets/d/e/2PACX-1vRjkvCUmKjYlsr33eABnIAo7kZM-anLR7r91LSVyZg9EGvsFnHXQcS3yX0m5On6rBte44YU-ql-m1y2/pub?output=csv', data_cleansing13, 'Sheet13'),
    ('https://docs.google.com/spreadsheets/d/e/2PACX-1vSLc6xq0Sf-5UC-hE3_soP2b91CuTIdCCE6pXqlim65WJaRxnvZMTIR1mj4p_icChvhtbNNZtihFxh-/pub?output=csv', data_cleansing14, 'Sheet14'),
    ('https://docs.google.com/spreadsheets/d/e/2PACX-1vRWjuq2yFNO2Mho0lMM-pewgBCFMR4JZCshtWQa_6Ll1zbkJHHWjeNOK9k_H1zH9c9-QrEiaCJmIHVJ/pub?output=csv', data_cleansing15, 'Sheet15'),
    ('https://docs.google.com/spreadsheets/d/e/2PACX-1vTuNJbCxzJvIAOdPSZ9z6LS-qUDzhuNMjVNuN323RLt0Vkrhya-y7VxTNAZ9N5qHXB6m_tiamrlde6j/pub?output=csv', data_cleansing16, 'Sheet16'),
]

CACHE_DIR = ".cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def hash_dataframe(df: pd.DataFrame) -> str:
    return hashlib.md5(pd.util.hash_pandas_object(df, index=True).values).hexdigest()

def load_cached_hash(name):
    path = os.path.join(CACHE_DIR, f"{name}.hash")
    if os.path.exists(path):
        return open(path).read().strip()
    return ""

def save_hash(name, hash_val):
    with open(os.path.join(CACHE_DIR, f"{name}.hash"), "w") as f:
        f.write(hash_val)

def main():
    for idx, (url, func, name) in enumerate(DATA_SOURCES, start=1):
        try:
            df = pd.read_csv(url)
            current_hash = hash_dataframe(df)
            old_hash = load_cached_hash(name)

            if current_hash != old_hash:
                print(f"🔁 Detected change in Sheet{idx} → Running data_cleansing{idx}()")
                func()
                save_hash(name, current_hash)
            else:
                print(f"Sheet{idx} has no change.")
        except Exception as e:
            print(f"⚠️ Failed to process Sheet{idx}: {e}")

if __name__ == "__main__":
    main()
