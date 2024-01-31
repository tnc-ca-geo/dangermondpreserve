import pandas as pd
import numpy as np
import os
from datetime import timedelta

#Define helper fn
def find_data_rows(lines):
    i = 0 
    while i < len(lines):
        if lines[i] == "[Data]\r\n":
            return i 
        i += 1

def find_discrete_ts(df): #Finds contiguous segments of time series data
    df['tdelta'] = df['timestamp_utc'].diff()
    gap_finder = df.loc[df['tdelta'] > timedelta(days=7)]
    tseries = []

    indices = np.append([0], gap_finder.index.values)
    indices = np.append(indices, df.index.values[-1]+1)

    indexr = [(indices[i-1], indices[i]-1) for i in range(1, len(indices))]

#     For each gap, section our the data
    for _indexr in indexr:
        tseries.append(df[_indexr[0]:_indexr[1]])

    return tseries

def read_levs_to_csvs(LEV_dir_list, dendra_dir):
    WellDevices = pd.DataFrame()
    WellData = {}
    #Parse each LEV to extract data and metadata
    for lev in LEV_dir_list:
        if "Compensated" in lev:
            continue
        with open(lev, newline="\n") as lev_text:
            lev_lines = lev_text.readlines()
            _dataStart = find_data_rows(lev_lines) + 2
            
            #Pull out metadata from datafile
            _metadata = {}
            for _mdx in lev_lines[10:_dataStart]:
                _spltKeyDat = _mdx.replace(" ", "").strip().split("=")
                if len(_spltKeyDat) > 1:
                    if _spltKeyDat[0] == "Unit" and "LevelUnit" in _metadata.keys():
                        _spltKeyDat[0] = "TemperatureUnit"
                    elif _spltKeyDat[0] == "Unit":
                        _spltKeyDat[0] = "LevelUnit"
                    _metadata[_spltKeyDat[0]] = _spltKeyDat[1]

            #Which well?
            location = (_metadata.get("Location") or "Unknown").replace("/", "").replace("#", "").replace(" ","").lower()
            instrumentType = _metadata.get('Instrumenttype' or "Unknown")
            
            ## Find Data Pointer
            _df = pd.read_fwf(lev, skiprows=_dataStart, names=["date", "time", "level", "temperature"], encoding='iso-8859-1')
            _df = _df.iloc[:-1]
            _df = _df.set_index(_df.agg(('{0[date]}{0[time]}' + f"{location}{instrumentType}").format, axis=1).apply(lambda x: hash(x)))
            _df['date'] = pd.to_datetime(_df['date'])
            
            #Update Metadata
            _metadata['Data_start_date'] = min(_df['date'])
            _metadata['Data_end_date'] = max(_df['date'])
            _metadata['TemperatureUnit'] = _metadata['TemperatureUnit'][-1]
            _metadata["Location"] = location
            _metadata['Data_source'] = "lev"
            
            #Save data_units to different columns
            _df = _df.dropna(subset='temperature')
            if _metadata['TemperatureUnit'] == "C":
                _df = _df.rename(columns = {'temperature': 'temperature_C'})
                _df['temperature_F'] = np.NaN
            elif _metadata['TemperatureUnit'] == "F":
                _df = _df.rename(columns = {'temperature': 'temperature_F'})
                _df['temperature_C'] = np.NaN   
            
            _df = _df.dropna(subset='level')
            if _metadata['LevelUnit'] == "ft":
                _df = _df.rename(columns = {'level': 'level_ft'})
                _df['level_m'] = np.NaN
            elif _metadata['LevelUnit'] == "m":
                _df = _df.rename(columns = {'level': 'level_m'})
                _df['level_ft'] = np.NaN 
                
            _df['TIMESTAMP'] = pd.to_datetime(_df['date'].astype(str) + " " + _df['time'].astype(str))
            _df = _df.drop(['date', 'time'], axis=1)
            _df = _df.dropna(subset='TIMESTAMP')
            
            #Save data to dataframes
            if location in WellData.keys():
                if 'xle_lev' in WellData[location].keys():
                    WellData[location]['xle_lev'] = pd.concat([WellData[location]['xle_lev'], _df], axis=0)
                else:
                    WellData[location]['xle_lev'] = _df
            else:
                WellData[location] = {}
                WellData[location]['xle_lev'] = _df

            WellDevices = pd.concat([WellDevices, pd.DataFrame(_metadata, index=[instrumentType+location])], ignore_index = True)

    if not os.path.exists(f"{dendra_dir}/Data"):
        os.mkdir(f"{dendra_dir}/Data")

    for well in WellData.keys():
        if 'xle_lev' in WellData[well]:
            WellData[well]['xle_lev'].index.names = ["seq_id"]
            WellData[well]['xle_lev'] = WellData[well]['xle_lev'].drop_duplicates(keep="first")
            WellData[well]['xle_lev'].to_csv(f"{dendra_dir}/Data/{well}_dendra_xle_lev.csv")

    print(f"LEVs from {WellData.keys()} were parsed, and csvs were written to DendraJSONs/Data ready for upload to Dendra")

    return WellData, WellDevices


