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

def lev_to_csv(well_meta, lev, out_path):
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
        location = well_meta['slug'].replace("Dangermond", "").replace(" ","").lower()
        
        ## Find Data Pointer
        _df = pd.read_fwf(lev, skiprows=_dataStart, names=["date", "time", "level", "temperature"], encoding='iso-8859-1')
        _df = _df.iloc[:-1]
        _df['date'] = pd.to_datetime(_df['date'])
        _df['TIMESTAMP'] = pd.to_datetime(_df['date'].astype(str) + " " + _df['time'].astype(str))
        _df = _df.dropna(subset=['TIMESTAMP'])
        _df = _df.set_index("TIMESTAMP")

        #Update Metadata
        _metadata['Data_start_date'] = min(_df['date'])
        _metadata['Data_end_date'] = max(_df['date'])
        _metadata['TemperatureUnit'] = _metadata['TemperatureUnit'][-1]
        _metadata["Location"] = location
        _metadata['Data_source'] = "lev"
            
        #Save data_units to different columns
        _df = _df.dropna(subset=['temperature'])
        if _metadata['TemperatureUnit'] == "C":
            _df = _df.rename(columns = {'temperature': 'temperature_C'})
            _df['temperature_F'] = np.NaN
        elif _metadata['TemperatureUnit'] == "F":
            _df = _df.rename(columns = {'temperature': 'temperature_F'})
            _df['temperature_C'] = np.NaN   
            
        _df = _df.dropna(subset=['level'])
        if _metadata['LevelUnit'] == "ft":
            _df = _df.rename(columns = {'level': 'level_ft'})
            _df['level_m'] = np.NaN
        elif _metadata['LevelUnit'] == "m":
            _df = _df.rename(columns = {'level': 'level_m'})
            _df['level_ft'] = np.NaN 

        _df = _df.drop(['date', 'time'], axis=1)
        _df.to_csv(out_path)
        
        return True