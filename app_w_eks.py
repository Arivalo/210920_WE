import streamlit as st
import requests
import pandas as pd
import numpy as np
import datetime as dt
import base64, io
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
from PIL import Image
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator
import preset_loader as pre_load

from scipy.signal import savgol_filter

from diagnostyka_czujnikow import czujnik
from diagnostyka_czujnikow import system


def init():

    presets = pre_load.load_presets()
    
    device_list = {preset['nazwa']:{"id":preset['id'], "lp":i} for i, preset in enumerate(presets)}
    
    device_info = pd.read_csv("lista_urzadzen.csv", index_col=1)
    
    return device_list, presets, device_info
    
    
def get_table_download_link(df, nazwa_pliku):
    """Generates a link allowing the data in a given panda dataframe to be downloaded
    in:  dataframe
    out: href string
    """
    csv = df.to_csv()
    b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
    href = f'<a href="data:file/csv;base64,{b64}" download="{nazwa_pliku}.csv">Download stats table</a>'
    
    return href
    
def get_table_download_link_excel(df, nazwa_pliku, dev_conf):

    rename_dict = {xt:f"{xt[-5:]} {dev_conf['config'][xt]['measured']} [{dev_conf['config'][xt]['unit']}]" for xt in dev_conf["config"]}
    
    df = df.rename(columns=rename_dict)
    
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, sheet_name='Sheet1', index=False)
    workbook=writer.book
    worksheet = writer.sheets['Sheet1']
    
    format = workbook.add_format({'text_wrap': True})
    
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, value, format)
    
    writer.save()
    excel_data = output.getvalue()
    b64 = base64.b64encode(excel_data)
    payload = b64.decode()
    html = f'<a href="data:text/xlsx;base64,{payload}" download="{nazwa_pliku}.xlsx">Pobierz dane z dnia</a>'
    
    return html
    
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
        try:
            df['updatedAt'] = pd.to_datetime(df['updatedAt']).dt.tz_localize(None)
        except KeyError:
            return pd.DataFrame()
            
    return df
    
def utworz_url(data_od, data_do, id):
    str_base = st.secrets['url']
    data_do_parted = str(data_do).split("-")
    str_out = f"{str_base}?from={data_od}T04:00:00Z&to={data_do}T12:00:00Z&monitoredId={id}&limit=10000000"
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
    
    try:
        date_time = pd.to_datetime(data["updatedAt"]).dt.tz_localize(None)
    except KeyError:
        date_time = None
    
    for sensor in config["config"]:
    
        cfg = config["config"][sensor]
        
        try:
            diagnostyka.dodaj_czujnik(CzujnikWentylatora(data[sensor]/200, nazwa=sensor, lin_mul=cfg["read_to_unit"], offset=cfg["offset"],
                                                    measured=cfg['measured'], desc=cfg['description'], unit=cfg["unit"], dt_series=date_time))
            
        except KeyError:
            diagnostyka.dodaj_czujnik(CzujnikWentylatora(None, nazwa=sensor, lin_mul=cfg["read_to_unit"], offset=cfg["offset"],
                                                    measured=cfg['measured'], desc=cfg['description'], unit=cfg["unit"], dt_series=date_time))
                                                    
    return diagnostyka, date_time
    

def mean_table(diagnostics, sigA, sigB):

    table = {
        "typ pomiaru":[],
        "średnia":[]
    }
    
    for sensor in diagnostics.lista_czujnikow[:-1]:
        table["typ pomiaru"].append(f"{sensor.measured} [{sensor.unit}]")
        table["średnia"].append(sensor.value_series.mean())
    
    if sigA is not None and sigB is not None:
        table["typ pomiaru"].append(f"Ciśnienie tłoczenia [Pa]")
        table["typ pomiaru"].append(f"Wilgotność [%]")
        table["średnia"].append(pd.Series(sigA).mean())
        table["średnia"].append(pd.Series(sigB).mean())
    
    return pd.DataFrame(table).set_index("typ pomiaru").round(1).astype(str)


def tabela_info(dev_info, dev):
    #info
    dev=int(dev)
    
    table = {
        "typ pomiaru":[],
        dev:[]
    }
    
    table["typ pomiaru"].append("Lokalizacja")
    table[dev].append(dev_info.loc[dev]["lokalizacja"])
    
    table["typ pomiaru"].append("Klient")
    table[dev].append(dev_info.loc[dev]["klient"])
    
    return pd.DataFrame(table).set_index("typ pomiaru")
    
    
    
def separate_signals(signal, dt_series=None, window=15):
    '''
    Zwraca 2 sygnaly na podstawie jednego sygnalu na zasadzie wyznaczania max i min wartosci z okna o szerokosci "window":
    - sygnal A - minimum wartosc z okna
    - sygnal B - maksymalna wartosc z okna
    
    jesli podane sa dane sygnalu czasowego (dt_series) dodatkowo zwraca skojarzone z sygnalami A i B probki z tych danych
    
    signal <list type>
    dane sygnalu z którego mają być wydzielone sygnaly max i min
    
    dt_series <list type> (default: None)
    skojarzone z sygnalem "signal" dane
    
    window <number> (default:15)
    szerokosc okien z których bedzie wyciagany max i min
    '''
    
    sig_len = len(signal)
    #print(sig_len)
    
    sig_A, sig_B = [], []
    
    if dt_series is not None:
        sig_A_dt, sig_B_dt = [], []
    
    for i in range(0, sig_len, window):
        
        sig_temp = signal[i:min(sig_len-1, i+window)]
        
        sig_A.append(min(sig_temp))
        sig_B.append(max(sig_temp))
        
        if dt_series is not None:
            sig_A_dt.append(dt_series[np.argmin(sig_temp)+i])
            sig_B_dt.append(dt_series[np.argmax(sig_temp)+i])
    
    if dt_series is None:
        return sig_A, sig_B
        
    else:
        return (sig_A, sig_A_dt), (sig_B, sig_B_dt)
    
    
def wykres(czujnik, filtruj=False):
    
    fig, ax = plt.subplots(figsize=(8,5))
    
    xfmt = mdates.DateFormatter('%H:%M')
    
    try:
        data_series = savgol_filter(czujnik.value_series, 101, 1)
        if filtruj:
            ax.plot(czujnik.dt_series, czujnik.value_series, c='gray', alpha=0.6)
        ax.plot(czujnik.dt_series, data_series)
    except ValueError:
        pass
        
    # poszerzenie limitu na y
    ylim = plt.ylim()
    ydelta = (ylim[1] - ylim[0])/2
    
    plt.ylim((ylim[0]-ydelta, ylim[-1]+ydelta))
    
    ax.set_xlabel("Czas")
    ax.set_ylabel(f"{czujnik.measured} [{czujnik.unit}]")
    
    ax.set_title(czujnik.nazwa)
    
    ax.xaxis.set_major_formatter(xfmt)
    
    return fig


  
## MAIN ##
    

device_list, presets, device_info = init()

st.set_page_config(layout="wide", page_title='Dashboard eksploatacyjny')
    
c1,c2,c3 = st.columns((1,2,2))

device = c1.selectbox("Wybierz urządzenie", device_list, help="Wybierz urządzenie którego dane chcesz wyświetlić")

c1.table(tabela_info(device_info, device))

data = c1.date_input("Wybierz datę", value=dt.date.today(), min_value=dt.date(2021,7,1), max_value=dt.date.today(), help="Wybierz dzień który chcesz wyświetlić")

device_config = presets[device_list[device]['lp']]
#st.write(device_config)

system_diagnostyki, dt_series = prepare_data(data, device_config)

if system_diagnostyki.lista_czujnikow[-1].value_series is not None:
    try:
        (sig_A, sig_A_dt), (sig_B, sig_B_dt) = separate_signals(system_diagnostyki.lista_czujnikow[-1].value_series.values, dt_series=system_diagnostyki.lista_czujnikow[-1].dt_series.values, window=128)
    except AttributeError:
        (sig_A, sig_A_dt), (sig_B, sig_B_dt) = (None, None), (None, None)
else:
    (sig_A, sig_A_dt), (sig_B, sig_B_dt) = (None, None), (None, None)

plot_real = c1.checkbox("Rysuj niefiltrowane dane", help="Tymczasowa opcja wyboru w celu pokazania stopnia filtrowania oryginalnych danych")

download_filter = c1.checkbox("Pobierz filtrowane dane", help="Jeśli zaznaczone dane będą filtorwane tak jak do wykresów przed pobraniem")

c2.table(mean_table(system_diagnostyki, sig_A, sig_B))

df_out = pd.DataFrame()
for i, sensor in enumerate(system_diagnostyki.lista_czujnikow[:]):
    if i == 0 and dt_series is not None:
        df_out["Data"] = dt_series.dt.date
        df_out["Czas"] = dt_series.dt.time
    if download_filter and sensor is not system_diagnostyki.lista_czujnikow[-1]:
        df_out[sensor.nazwa] = savgol_filter(sensor.value_series, 101, 1)
        if i==3:
            df_out[sensor.nazwa] = df_out[sensor.nazwa].round(2)
        else:
            df_out[sensor.nazwa] = df_out[sensor.nazwa].round(1)
    else:
        if i==3:
            df_out[sensor.nazwa] = df_out[sensor.nazwa].round(2)
        else:
            df_out[sensor.nazwa] = df_out[sensor.nazwa].round(1)

if sig_A_dt is not None:
    df_temp_A = pd.DataFrame({"Ciśnienie tłoczenia [Pa]":sig_A, "Czas":pd.to_datetime(pd.Series(sig_A_dt)).dt.time})
    df_temp_B = pd.DataFrame({"Wilgotność [%]":sig_B, "Czas":pd.to_datetime(pd.Series(sig_B_dt)).dt.time})

    df_out = df_out.merge(df_temp_A, how="left", on="Czas")
    df_out = df_out.merge(df_temp_B, how="left", on="Czas")

    df_out = df_out.ffill().bfill()
    

c1.markdown(get_table_download_link_excel(df_out, f'{device}_{data}', device_config), unsafe_allow_html=True)

c3.table(pd.DataFrame({"i":["TBC" for x  in range(8)], "wskaźniki eksploatacyjne":["-" for x in range(8)]}).set_index("i"))


## WYKRESY

cols = st.columns((1,1,1))

for i, sensor in enumerate(system_diagnostyki.lista_czujnikow[:-1]):
    temp_fig = wykres(sensor, filtruj=plot_real)
    
    cols[i%3].write(temp_fig)
    
# IN06

fig, ax = plt.subplots(figsize=(8,5))
    
xfmt = mdates.DateFormatter('%H:%M')

if plot_real:
    ax.plot(system_diagnostyki.lista_czujnikow[-1].dt_series.values, system_diagnostyki.lista_czujnikow[-1].value_series.values, label='oryginalny sygnał', c='gray', alpha=0.6)

ax.plot(sig_A_dt, sig_A, label='sygnał A')
ax.plot(sig_B_dt, sig_B, label='signał B')

plt.title("XT_UAIN_06")
ax.set_xlabel("Czas")
ax.set_ylabel(f"wilgotność/ciśnienie tłoczenia [%/Pa]")

ax.xaxis.set_major_formatter(xfmt)

plt.legend()

# poszerzenie limitu na y
ylim = plt.ylim()
ydelta = (ylim[1] - ylim[0])/2
plt.ylim((ylim[0]-ydelta, ylim[-1]+ydelta))

cols[0].write(fig)
    


