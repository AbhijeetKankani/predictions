import pandas as pd
from hdbcli import dbapi

# Parquet file and relevant columns for retrieval
PARQUET_FILE = "df_identification_20240814_sample.parquet"
STP_RESULT_COLUMNS = [
    "EKP", "kalknr", "Quelle fuer Bezug Sendungsstruktur", "Bezugs-Menge", "Mengenprognose",
    "Bezugs-Raummass", "Ist-Raummass", "Abholart Zusammenfassung", "soll_menge_est",
    "prognose_methode_pm", "prognose_menge_pm", "prognose_check_pm", "prognose_methode_akt",
    "prognose_check_akt", "soll_raummass_est", "prognose_raummass_pm", "prognose_methode_raummass_pm",
    "prognose_check_raummass_pm", "prognose_raummass_akt", "prognose_methode_raummass_akt",
    "prognose_check_raummass_akt", "pm_kt_flag", "pm_vemo_flag", "signal_mnt", "signal_mnt_est",
    "signal_vol", "signal_vol_est", "cal_kdg2warning_flag", "cal_gesamt_sh2pr_flag", "cal_neukunden_flag",
    "cal_abholkosten_korrektur", "cal_volumenueberschreitung_check"
]

# Function to access and filter data from the Parquet file
def access_identification(ekp: int, kalknr: str = None) -> pd.DataFrame:
    print("Attempting to read data from Parquet file...")
    try:
        df_id_parquet = pd.read_parquet(PARQUET_FILE)
        print("Successfully read Parquet file.")
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        return pd.DataFrame()

    print("Filtering data based on EKP and kalknr...")
    df_result = df_id_parquet[df_id_parquet['EKP'] == ekp][STP_RESULT_COLUMNS]
    
    if kalknr:
        if 'kalknr' in df_result.columns:
            df_result = df_result[df_result['kalknr'] == kalknr]
        else:
            print("Warning: 'kalknr' column not found in data. Skipping kalknr filter.")

    # Truncate KALKNR values to 4 characters if necessary
    if 'kalknr' in df_result.columns:
        df_result['kalknr'] = df_result['kalknr'].apply(lambda x: x[:4] if isinstance(x, str) else x)

    print(f"Number of records found: {len(df_result)}")

    if not df_result.empty:
        # Print the complete record and data types
        print("\nComplete Record:")
        print(df_result.iloc[0])
        print("\nData Types:")
        print(df_result.dtypes)

        # Validate and handle NaN values
        df_result = df_result.fillna(0)
    
    return df_result

# Function to insert data into HANA Cloud
def insert_data_to_hana(df: pd.DataFrame):
    print("Starting the connection to HANA Cloud...")
    
    try:
        conn = dbapi.connect(
            address='47ea62e3-34fc-490d-9f29-1395a0a7d833.hna0.prod-eu20.hanacloud.ondemand.com',
            port=443,
            user='DBADMIN',
            password='sbxHANA1007'
        )
        print("Connected to HANA Cloud!")
    except Exception as e:
        print(f"Failed to connect to HANA Cloud: {e}")
        return

    cursor = conn.cursor()

    insert_statement = """
    INSERT INTO "PRIMA_AI"."PRIMA_PRICE_DELTA_PYTHON_POC" 
    (EKP, KALKNR, QUELLE, BEZUGS_MENGE, MENGENPROGNOSE, BEZUGS_RAUMMASS, IST_RAUMMASS,
    ABHOLART_ZUSAMMENFASSUNG, SOLL_MENGE_EST, PROGNOSE_METHODE_PM, PROGNOSE_MENGE_PM,
    PROGNOSE_CHECK_PM, PROGNOSE_METHODE_AKT, PROGNOSE_CHECK_AKT, SOLL_RAUMMASS_EST,
    PROGNOSE_RAUMMASS_PM, PROGNOSE_METHODE_RAUMMASS_PM, PROGNOSE_CHECK_RAUMMASS_PM,
    PROGNOSE_RAUMMASS_AKT, PROGNOSE_METHODE_RAUMMASS_AKT, PROGNOSE_CHECK_RAUMMASS_AKT,
    PM_KT_FLAG, PM_VEMO_FLAG, SIGNAL_MNT, SIGNAL_MNT_EST, SIGNAL_VOL, SIGNAL_VOL_EST,
    CAL_KDG2WARNING_FLAG, CAL_GESAMT_SH2PR_FLAG, CAL_NEUKUNDEN_FLAG, CAL_ABHOLKOSTEN_KORREKTUR,
    CAL_VOLUMENUEBERSCHREITUNG_CHECK)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    data_to_insert = [tuple(x) for x in df.to_numpy()]
    print(f"Preparing to insert {len(data_to_insert)} records into HANA Cloud.")

    try:
        if data_to_insert:
            cursor.executemany(insert_statement, data_to_insert)
            conn.commit()
            print("Records inserted successfully into HANA Cloud")
        else:
            print("No records to insert into HANA Cloud.")
    except dbapi.Error as e:
        print(f"Error during data insertion: {e}")
    finally:
        cursor.close()
        conn.close()
        print("Connection to HANA Cloud closed.")

# Main function to run the entire process
def run_ship_to_profile(ekp: int, kalknr: str = None):
    print(f"Starting process for EKP: {ekp}, kalknr: {kalknr}")
    df_res = access_identification(ekp=ekp, kalknr=kalknr)  # Retrieve data
    
    if not df_res.empty:
        print("Data found, proceeding to insert into HANA Cloud.")
        insert_data_to_hana(df_res)
    else:
        print("No records found for the specified EKP and kalknr.")

# Example executions
run_ship_to_profile(5000959631)
#run_ship_to_profile(5003243537, "A027")
#run_ship_to_profile(5003243537, "PA022")
