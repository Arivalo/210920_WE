import streamlit as st
import requests
import pandas as pd
import datetime as dt
import base64
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
from PIL import Image
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator
import preset_loader as pre_load

from diagnostyka_czujnikow import czujnik
from diagnostyka_czujnikow import system


def init():

    presets = pre_load.load_presets()
    
    device_list = {preset['nazwa']:{"id":preset['id'], "lp":i} for i, preset in enumerate(presets)}
    
    return device_list, presets
    
    
def get_table_download_link(df, nazwa_pliku):
    """Generates a link allowing the data in a given panda dataframe to be downloaded
    in:  dataframe
    out: href string
    """
    csv = df.to_csv()
    b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
    href = f'<a href="data:file/csv;base64,{b64}" download="{nazwa_pliku}.csv">Download stats table</a>'
    
    return href
    
def download_data(url, haslo=st.secrets['password'], login=st.secrets['username'], retry=5):

    i = 0

    while i < retry:
        r = requests.get(url,auth=(login, haslo))

        try:
            j = r.json()
            break
        except:
            i += 1
            print(f"Try no.{i} failed")

    if i == retry:
        print(f"Failed to fetch data for: {url}")
        return pd.DataFrame()
        
    df = pd.DataFrame.from_dict(j['entities'])
    if not df.empty:
        try:
            df['longtitude'] = [x['coordinates']['x'] for x in df['_meta']]
            df['latitude'] = [y['coordinates']['y'] for y in df['_meta']]
            df.pop('_meta')   
            
        except KeyError:
            print(f'Url error: {url}')
            
        df.ffill(inplace=True)
        df['updatedAt'] = pd.to_datetime(df['updatedAt']).dt.tz_localize(None)         
            
    return df
    
def utworz_url(data_od, data_do, id):
    str_base = st.secrets['url']
    data_do_parted = str(data_do).split("-")
    str_out = f"{str_base}?from={data_od}T08:00:00Z&to={data_do}T15:00:00Z&monitoredId={id}&limit=10000000"
    return str_out
    
def service_available(num_retry=5):
    for i in range(num_retry):
        r = requests.get(st.secrets['url'][:23])
        status = (r.status_code != 503) or status
        
        if status:
            break
    
    return status
    
    
class CzujnikWentylatora(czujnik.Czujnik):
    
    def __init__(self, CAN_series, value_series=None, zakres_CAN=(0, 32768), zakres_przeliczone=(-40, 0),
                 max_delta=20, nazwa="", lin_mul=1, offset=0, dt_series=None, vel_series=None,
                 cs_przebieg_min=10, cs_procent_min=5, measured="", desc="", unit=""):

        self.linear_multiplier = lin_mul
        self.offset = offset
        self.measured = measured
        self.description = desc
        self.unit = unit

        # ------------------------------------------------------ #
        czujnik.Czujnik.__init__(self, CAN_series, value_series=value_series, zakres_CAN=zakres_CAN,
                         zakres_przeliczone=zakres_przeliczone, max_delta=max_delta, nazwa=nazwa, dt_series=dt_series,
                         vel_series=vel_series, cs_przebieg_min=cs_przebieg_min, cs_procent_min=cs_procent_min)
    
    def przelicz_czujnik(self):
        return self.CAN_series*self.linear_multiplier-self.offset
        
        
def prepare_data(date, config, xt_to_V=200):
    
    data = download_data(utworz_url(date, date, config['id']))
    
    diagnostyka = system.SystemDiagnozy()
    
    date_time = pd.to_datetime(data["updatedAt"]).dt.tz_localize(None)
    
    for sensor in config["config"]:
    
        cfg = config["config"][sensor]
        
        try:
            diagnostyka.dodaj_czujnik(CzujnikWentylatora(data[sensor]/200, nazwa=sensor, lin_mul=cfg["read_to_unit"], offset=cfg["offset"],
                                                    measured=cfg['measured'], desc=cfg['description'], unit=cfg["unit"], dt_series=date_time))
            
        except KeyError:
            diagnostyka.dodaj_czujnik(CzujnikWentylatora(None, nazwa=sensor, lin_mul=cfg["read_to_unit"], offset=cfg["offset"],
                                                    measured=cfg['measured'], desc=cfg['description'], unit=cfg["unit"], dt_series=date_time))
                                                    
    return diagnostyka
    

def mean_table(diagnostics):

    table = {
        "typ pomiaru":[],
        "średnia":[]
    }
    
    for sensor in diagnostics.lista_czujnikow:
        table["typ pomiaru"].append(sensor.measured)
        table["średnia"].append(sensor.value_series.mean())
        
    return pd.DataFrame(table).set_index("typ pomiaru")
    
    
def wykres(czujnik):
    
    fig, ax = plt.subplots(figsize=(8,5))
    
    xfmt = mdates.DateFormatter('%H:%M')
    
    ax.plot(czujnik.dt_series, czujnik.value_series)
    
    ax.set_xlabel("Czas")
    ax.set_ylabel(f"{czujnik.measured} [{czujnik.unit}]")
    
    ax.set_title(czujnik.nazwa)
    
    ax.xaxis.set_major_formatter(xfmt)
    
    return fig
        
    
  
## MAIN ##
    

device_list, presets = init()

st.set_page_config(layout="wide", page_title='test')
    
c1,c2,c3 = st.columns((1,2,2))

device = c1.selectbox("Wybierz urządzenie", device_list, help="Wybierz urządzenie którego dane chcesz wyświetlić")

data = c1.date_input("Wybierz datę", value=dt.date(2021,8,3), min_value=dt.date(2021,7,1), max_value=dt.date.today(), help="Wybierz dzień który chcesz wyświetlić")

device_config = presets[device_list[device]['lp']]

system_diagnostyki = prepare_data(data, device_config)

c2.table(mean_table(system_diagnostyki))

c3.table(pd.DataFrame({"i":["TBC" for x  in range(7)], "wskaźniki eksploatacyjne":["-" for x in range(7)]}).set_index("i"))


## WYKRESY

cols = st.columns((1,1,1))

for i, sensor in enumerate(system_diagnostyki.lista_czujnikow):
    temp_fig = wykres(sensor)
    
    cols[i%3].write(temp_fig)


    


