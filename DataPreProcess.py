#! /user/bin/env Python3.6
# -*- coding: utf-8 -*-
# File: DataPreProcess.py
# Date: 2019-07-24
# Author: Wud

import pandas as pd
#import numpy as np
#from datetime import datetime
import json
import requests
from configparser import ConfigParser
import logging
from datetime import datetime

import sys
sys.path.append('/root/fuelSaving/fuelPredAnaService/FuelPrediction')
from ParametersInit import *
from set_logger import setup_logger

local_log_filename = local_log_path + eval(date_info) + '.log'
local_logger = setup_logger('PreProcess_log', local_log_filename)

#global local_log_path, date_info
#date_info = eval(date_info)
#local_log_filename = local_log_path + date_info + '.log'
#logging.basicConfig(level=logging.INFO, filename = local_log_filename, format="%(asctime)s %(name)s:%(levelname)s:%(message)s", datefmt="%d-%M-%Y %H:%M:%S")
#logger = logging.getLogger("local_log")

class DataPreProcess(object):
    def __init__(self, start_date, end_date, new_data_exist):
        global airline_his_data_path, airport_info_path, config_file_path, cols_seq
        _airline_his_data = pd.read_csv(airline_his_data_path, low_memory = False)
        if 'Unnamed: 0' in _airline_his_data.columns:
            _airline_his_data.drop(['Unnamed: 0'], axis = 1, inplace = True)
        _airline_his_data = _airline_his_data[cols_seq]
        
        if new_data_exist:
            _airline_new_data = self._readDataInterface(config_file_path, start_date, end_date)
            local_logger.info("Read Interface Data Successfully...")
            _cols = _airline_new_data.columns.tolist()
            _sub_cols = []
            for i in _cols:
                for j in i:
                    if j.isupper():
                        i = i.replace(j, '_'+j)
                _sub_cols.append(i)
            _sub_cols = [i.lower() for i in _sub_cols]
            _airline_new_data.columns = _sub_cols

            _airline_new_data = _airline_new_data[_airline_his_data.columns.tolist()]
            _airline_new_data = _airline_new_data.loc[~_airline_new_data.loc[:,'leg_id'].isin(_airline_his_data.loc[:,'leg_id'])]
            _airline_data = pd.concat([_airline_his_data, _airline_new_data], axis = 0)
        else:
            _airline_data = _airline_his_data
        _airline_data.to_csv(airline_his_data_path)
        local_logger.info("Update Airline History Data Successfully...")
        self.airline_data = _airline_data
        self.columns = self.airline_data.columns.tolist()       
        _airport_info = pd.read_csv(airport_info_path)
        self.airport_info = _airport_info
        
    def _readDataInterface(self, config_file_path, start_date, end_date):
        cp = ConfigParser()
        cp.read(config_file_path)
        #read interface
        project_code = cp.get("interface", "project_code")
        code = cp.get("interface", "code")
        url = cp.get("interface", "url")
        username = cp.get("interface", "username")
        password = cp.get("interface", "password")
        # field = cp.get("interface", "field")
        # field_list = field.split(",")
        para_list = [{"parameterCode": "startDate", "parameterType": "string", "parameterValue": start_date},
                     {"parameterCode": "endDate", "parameterType": "string", "parameterValue": end_date}]
        para = {"interfaceCode": code, "username": username, "password": password, "projectCode": project_code, "parameterList": para_list}
        
        data_request = requests.post(url, data = json.dumps(para), headers = {"content-type": "application/json"})
        result = json.loads(data_request.text)
        data = pd.DataFrame(result['dataList'])
        return data
    
    def _dataTypeArrange(self, airline_data):
        _data = airline_data.astype(str)
        _data = _data.applymap(lambda x:x.replace(',', ''))
        
        _not_numeric_cols = ['flight_date', 'routes', 'ac_type']
        _numeric_cols = _data.columns[~_data.columns.isin(_not_numeric_cols)]
        _data[_numeric_cols] = _data[_numeric_cols].astype(float)
        
        if 'leg_id' in self.columns:
            _data.leg_id = _data.leg_id.astype(int)
        if 'flight_date' in self.columns:
            _data.flight_date = pd.to_datetime(_data.flight_date)
        if 'ac_type' in self.columns:
            ac_type = _data['ac_type'].values
            ac_type = pd.get_dummies(ac_type)
            _data = pd.concat([_data, ac_type], axis = 1)
            _data.rename(columns = {'A320-214S': 'S', 'A320-214W': 'W', 'A320-251N': 'N'}, inplace = True)
            _data.drop(['ac_type'], axis = 1, inplace = True)
#             ac_type = OneHotEncoder(categories = 'auto').fit_transform(ac_type.reshape(-1, 1)).toarray().astype(int)
#             ac_type_columns = [x[-1] for x in list(_data['ac_type'].unique())]
#             _data = pd.concat([_data, pd.DataFrame(ac_type, columns = ac_type_columns)], axis = 1)
#             _data.drop(['ac_type'], axis = 1, inplace = True)
#             _cols_seq = list(_data.columns.values[~_data.columns.isin(ac_type_columns)]) + ['S', 'W', 'N']
#             _data = _data[_cols_seq]
        return _data
    
    def _fillAcWeightNA(self, gwc, payload, fuel):
        return gwc - payload - fuel
    
    def _getAirportInfo(self, airport_info_df):
        _data = airport_info_df.loc[:, ['airport_code4', 'airport_longitude', 'airport_latitude']]
        _data.iloc[:, 1:3] = round(_data.iloc[:, 1:3], 1)
        _data.dropna(inplace = True)
        _data.drop_duplicates(inplace = True)
        _data = _data.loc[(_data['airport_latitude'] != 0) & (_data['airport_latitude'] != 0)]
        
        _freq_df = pd.DataFrame(_data['airport_code4'].value_counts())
        _freq_df['airport'] = _freq_df.index.values
        _freq_df = _freq_df.reset_index(drop = True)
        _freq_df.rename(columns = {'airport_code4': 'freq'}, inplace = True)
        
        _data_new = _data.loc[_data['airport_code4'].isin(_freq_df.loc[_freq_df.freq == 1, 'airport'])]
        for i in _freq_df.loc[_freq_df.freq >1, 'airport']:
            _sub_df = _data.loc[_data.airport_code4 == i, :]
            _data_new = _data_new.append(_sub_df.iloc[0,:])
        _data_new = _data_new.reset_index(drop = True)
        return _data_new
    
    def _get_cols_to_del(self, str_useless, str_all):
        _cols_to_del_ix = []
        for _index, _value in enumerate(str_all):
            #if str in value, return index, else return -1
            if _value.find(str_useless) != -1:
                _cols_to_del_ix.append(_index)
        _cols_to_del = list(str_all[_cols_to_del_ix])
        return _cols_to_del
    
    def _combine_airport_info(self, airline_data, airport_info):
        _data = airline_data.copy()
        _airport_info = self._getAirportInfo(self.airport_info)
        _data['dept_airport'] = _data.routes.apply(lambda x:x[:4])
        _data['arr_airport'] = _data.routes.apply(lambda x:x[5:])
        _data = pd.merge(_data, _airport_info, how = 'left', left_on = 'dept_airport', right_on = 'airport_code4')
        _data.rename(columns = {'airport_longitude': 'dept_lon', 'airport_latitude': 'dept_lat'}, inplace = True)
        _data = pd.merge(_data, _airport_info, how = 'left', left_on = 'arr_airport', right_on = 'airport_code4')
        _cols_to_del = self._get_cols_to_del('airport_code4', _data.columns.values)
        _data.drop(columns = _cols_to_del, inplace = True)
        _data.rename(columns = {'airport_longitude': 'arr_lon', 'airport_latitude': 'arr_lat'}, inplace = True)
        _data.drop(['dept_airport', 'arr_airport'], axis = 1, inplace = True)
        return _data
        
    @property
    def processed_airline_data(self):
        _data = self._dataTypeArrange(self.airline_data)
        _data['dry_opt_weight'].fillna(self._fillAcWeightNA(_data['take_off_gwc'], _data['actual_payload'], _data['fixed_take_off_fuel']), inplace = True)
        _airport_info = self._getAirportInfo(self.airport_info)
        _data = self._combine_airport_info(_data, _airport_info)
        
        _sub_data = _data.loc[:,['airborne_fuel', 'airborne_fuel_h', 'actual_altitude']]
        _sub_data.dropna(inplace = True)
        _not_null_rows = _sub_data.index.values.tolist()
        _data = _data.loc[_not_null_rows]
        return _data

if __name__ == '__main__':
    print('hello world!')
