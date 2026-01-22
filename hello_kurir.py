# %%
print('hello world')
import geopandas as gpd
import numpy as np

# Baca file shapefile (.shp)
lokasi_data=r'/home/usr_rda/verifikasi/usecase/Batas_Wilayah_KelurahanDesa_10K_AR.shp'
#lokasi_data=r'C:\Users\User\Downloads\latihan_python2\Batas_Wilayah_KelurahanDesa_10K_AR.shp'
gdf = gpd.read_file(lokasi_data)


# %%
gdf=gdf[['KDCPUM','geometry']]

# %%
gdf[gdf.select_dtypes(include='object').columns]=gdf.select_dtypes(include='object').apply(lambda x:x.str.upper())

# %%
# Pastikan shapefile juga di EPSG:4326, kalau beda Ã¢â€ â€™ konversi
gdf = gdf.to_crs("EPSG:4326")

# %% [markdown]
# cek tanggal 

# %%
import geopandas as gpd
import numpy as np
from sqlalchemy import create_engine,text
import pandas as pd
from datetime import datetime,timedelta

server_etl = 'bansosreport-db.cmfru4yoszrg.ap-southeast-3.rds.amazonaws.com'
database_etl = 'POSPAY_DB'
username_etl = 'admin'
password_etl = 'B4ns05dB'
tablename_etl='kurir_posting_rekap'

# %%
engine_etl= create_engine(f'mssql+pyodbc://{username_etl}:{password_etl}@{server_etl}/{database_etl}?driver=ODBC+Driver+17+for+SQL+Server')

# %%
query=f"""
SELECT MAX(entry_time)entry_time
FROM {tablename_etl}
"""

# %%
df=pd.read_sql(query,engine_etl)

# %%
# %%
tanggal_awal=df['entry_time'][0].date()
# tanggal_awal=pd.Timestamp(2023,1,1).date()

# %%
tanggal_awal+=timedelta(days=1)

# %%
# %%
print('hello world')


# %%
tanggal_awal

# %%
import pandas as pd
from sqlalchemy import create_engine
import urllib.parse

# Konfigurasi koneksi
username = "P05payDB"       # ganti dengan username MySQL kamu
password = "P05PayDb123!@" # ganti dengan password MySQL kamu
password = urllib.parse.quote_plus(password)
host = "repdb-sbufintech.cmfru4yoszrg.ap-southeast-3.rds.amazonaws.com"      # atau IP server MySQL
port = 3306             # default port MySQL
database = "pospayDB"

# Buat engine koneksi
engine_sumber = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}")

from datetime import datetime,timedelta
tanggal_akhir=datetime.today().date()
print(tanggal_awal,tanggal_akhir)
while tanggal_awal<=tanggal_akhir:
# Contoh query
    query = f"""
    SELECT DISTINCT
    entry_time,
    posting_id,
    account_no,
    payload->>'$.serviceid' product_code, 
    jenis_layanan product_name,
    CAST(payload->'$.latitude' AS DECIMAL(18,10))  longitude,
    CAST(payload->'$.longitude' AS DECIMAL(18,10))  latitude,
    kurir_posting.total_fee amount,
    CAST(payload->'$.fee' AS DECIMAL(18,2)) AS fee_amount
    FROM pospayDB.kurir_posting
    WHERE entry_time>='{tanggal_awal}'
    AND entry_time<'{tanggal_awal+timedelta(days=1)}'
    """

    # Eksekusi query langsung ke DataFrame Pandass
    df = pd.read_sql(query, engine_sumber)

    # %%
    import json
    if len(df)==0:
        tanggal_awal+=timedelta(days=1)
        continue
        
    # df_parsed = pd.json_normalize(df['payload'].apply(json.loads))
    # df_parsed=df_parsed[['latitude','longitude']]
    # df_parsed.rename(columns={
    #     'latitude': 'longitude',
    #     'longitude': 'latitude'
    # }, inplace=True)

    # gabungkan dengan tabel lama
    
    # df = pd.concat([df.drop(columns=['payload']), df_parsed], axis=1)
    # %%


    # %%
    df

    # %% [markdown]
    # joinkeun gdf

    # %%
    

    # %% [markdown]
    # maser desa dan kantor

    # %%
    import pandas as pd
    from sqlalchemy import create_engine

    # Konfigurasi koneksi
    username = "admbpnt"
    password = "BPNTp05indo"
    host = "bpnt.cmfru4yoszrg.ap-southeast-3.rds.amazonaws.com"
    port = 5432
    database = "postgres"

    # Buat engine koneksi
    engine_wilayah = create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}")

    # Query data
    query = """
    SELECT DISTINCT kd_kec,kd_kprk 
    FROM master_desa"""

    # Baca ke Pandas DataFrame
    df_master_desa = pd.read_sql(query, engine_wilayah)

    df_master_desa.drop_duplicates(subset=['kd_kec'], keep='first', inplace=True)



    # %% [markdown]
    # master kantor

    # %%
    # sebelum cleansing
    import pandas as pd
    from sqlalchemy import create_engine,text
    import urllib.parse

    # Koneksi ke SQL Server
    server = '10.24.20.11'
    database = 'DB_REFERENSI'
    username = 'sa'
    password = 'P05juara1'

    # Buat koneksi engine
    engine_ref= create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server')

    query_ref_kantor=f'''
    SELECT DISTINCT NOPEND_DIRIAN,REGIONAL,KC,KCU,JENIS
    FROM ref_kcu_kc_2023
    '''

    df_ref_kantor=pd.read_sql(query_ref_kantor,engine_ref)
    df_ref_kantor.drop_duplicates(subset=['NOPEND_DIRIAN'], keep='first', inplace=True)
    df_ref_kantor[df_ref_kantor.select_dtypes(include='object').columns]=df_ref_kantor.select_dtypes(include='object').apply(lambda x:x.str.upper())

    # %% [markdown]
    # joinekeun

    # %%
    from shapely.geometry import Point
    # df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce').fillna(0)
    # df['latitude']  = pd.to_numeric(df['latitude'], errors='coerce').fillna(0)

    # %%
    gdf_points = gpd.GeoDataFrame(
        df,
        geometry=[Point(xy) for xy in zip(df["longitude"], df["latitude"])],
        crs="EPSG:4326"  # WGS84 (longitude/latitude)
    )

    # %%
    # %%
    # Join titik dengan polygon
    gdf_joined = gpd.sjoin(gdf_points, gdf, how="left", predicate="within")

    # %%
    gdf_joined['KDCPUM']=gdf_joined['KDCPUM'].str.replace('.','')



    # %%
    gdf_joined=pd.merge(gdf_joined, df_master_desa, left_on='KDCPUM', right_on='kd_kec', how='left')



    # %%
    gdf_joined=pd.merge(gdf_joined, df_ref_kantor, left_on='kd_kprk', right_on='NOPEND_DIRIAN', how='left')



    # %%
    gdf_joined.drop(columns=['latitude','longitude','index_right','kd_kprk','kd_kec','NOPEND_DIRIAN','geometry'], inplace=True)



    # %%
    gdf_joined.loc[gdf_joined['KDCPUM'].isna(),['KDCPUM','REGIONAL','KC','KCU','JENIS']]='TIDAK DITEMUKAN'

    # %%
    gdf_joined['entry_time']=pd.to_datetime(gdf_joined['entry_time']).dt.date

    # %%
    gdf_joined.columns=gdf_joined.columns.str.lower()

    # %%
    gdf_points

    # %%
    gdf_joined.head()

    # %%
    gdf_joined['entry_time']=pd.to_datetime(gdf_joined['entry_time'])
    # gdf_joined[['posting_id','account_no','amount','fee_amount']] = (gdf_joined[['posting_id','account_no','amount','fee_amount']].fillna(0).astype(int))
    # gdf_joined['type'] = np.where(gdf_joined['type'].isna(), 'TIDAK DITEMUKAN', gdf_joined['type'])

    # %%
    gdf_joined=gdf_joined.groupby(['entry_time','product_code','product_name','regional','kc','kcu','jenis','account_no']).agg(
    {
        'posting_id':'count',
        'amount':'sum',
        'fee_amount':'sum'
    }).reset_index()

    # %%
    print('seleai')

    # %%
    gdf_joined

    # %%
    server_etl = 'bansosreport-db.cmfru4yoszrg.ap-southeast-3.rds.amazonaws.com'
    database_etl = 'POSPAY_DB'
    username_etl = 'admin'
    password_etl = 'B4ns05dB'
    tablename_etl='kurir_posting_rekap'

    # %%
    engine_etl= create_engine(f'mssql+pyodbc://{username_etl}:{password_etl}@{server_etl}/{database_etl}?driver=ODBC+Driver+17+for+SQL+Server')

    gdf_joined.to_sql(f'{tablename_etl}', con=engine_etl, if_exists='append', index=False,chunksize=1000)
    print(f'berhasil buat table {tablename_etl}')
    del gdf_joined,df
    tanggal_awal+=timedelta(days=1)
