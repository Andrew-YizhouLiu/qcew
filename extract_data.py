# -*- coding: utf-8 -*-

__author__ = 'Andrew Liu'

#%%
import urllib.request
import urllib
from numba import jit
import numpy as np
import pandas as pd
#%%

def qcewCreateDataRows(csv):
    dataRows = []
    try: dataLines = csv.decode().split('\r\n')
    except er: dataLines = csv.split('\r\n');
    for row in dataLines:
        dataRows.append(row.split(','))
    return dataRows

@jit
def qcewGetAreaData(year,qtr,area):
    urlPath = "http://data.bls.gov/cew/data/api/[YEAR]/[QTR]/area/[AREA].csv"
    urlPath = urlPath.replace("[YEAR]",year)
    urlPath = urlPath.replace("[QTR]",qtr.lower())
    urlPath = urlPath.replace("[AREA]",area.upper())
    httpStream = urllib.request.urlopen(urlPath)
    csv = httpStream.read()
    httpStream.close()
    return qcewCreateDataRows(csv)

@jit
def convert_dataframe(csv):
    # This function convert the csv file into dataframe and select only the relevant columns
    df = pd.DataFrame(data = csv[1:],columns = csv[0])
    df = df[['"area_fips"','"own_code"','"industry_code"','"year"','"month1_emplvl"','"month2_emplvl"','"month3_emplvl"']]

    return df

@jit
def select_private(df):
    # This function keeps only private sector numbers
    df = df.drop(df[df['"own_code"']!='"5"'].index)
    
    return df

@jit
def select_variable(df):
    # This function selects relevant variables, namely manufacturing employment,
    # retail empployment, and total employment
    manu = df[df['"industry_code"']=='"1013"'].index # manufacturing
    retail = df[df['"industry_code"']=='"44-45"'].index # retail trade
    total = df[df['"industry_code"'] == '"10"'].index
    select = manu.append([retail,total])
    df = df.loc[select]
    df = df[['"month1_emplvl"','"month2_emplvl"','"month3_emplvl"']]
    df = df.T
    
    return df

@jit
def add_index(df,year,qtr):
    # This function put dates as index and change the column names for the dataframe
    assert len(df.index)==3
    start_month = (int(qtr)-1)*3+1
    if start_month < 10:
        df.index = pd.date_range(year+'0'+str(start_month)+'01',periods=3,freq='M')
    else:
        df.index = pd.date_range(year+str(start_month)+'01',periods=3,freq='M')        
    df = df.rename({df.columns[0]:'manu_emp',df.columns[1]:'re_emp',df.columns[2]:'t_emp'},axis='columns')
    
    return df
    
@jit
def add_state_column(df,area):
    # This function add a column 'state'
    df['state'] = pd.Series(int(int(area)/1000),index=df.index)
    
    return df

#%%
@jit
def extract_state(area):
    # Extract the data in one state
    years = ["2013","2014","2015","2016","2017"]
    qtrs = ["1","2","3","4"]
    df = pd.DataFrame(columns=['manu_emp','re_emp','t_emp','state'])
    for year in years:
        for qtr in qtrs:
            csv = qcewGetAreaData(year,qtr,area)
            print("Year "+year+" quarter "+qtr+" start")
            csv = convert_dataframe(csv)
            csv = select_private(csv)
            csv = select_variable(csv)
            csv = add_index(csv,year,qtr)
            csv = add_state_column(csv,area)
            print("Year "+year+" quarter "+qtr+" complete")
            df = df.append(csv)
    
    return df
            
@jit
def extract(states):
    # Extract all states
    df = pd.DataFrame(columns=['manu_emp','re_emp','t_emp','state'])
    for state in states:
        print("State "+state+" start")
        temp = extract_state(state)
        df = df.append(temp)
        print("State "+state+" complete")
        
    return df
        
@jit
def create_state_list():
    # Create the list of states
    
    states = ['01000',
              '02000',
              '04000',
              '05000',
              '06000',
              '08000',
              '09000',
              '10000',
              '11000',
              '12000',
              '13000',
              '15000',
              '16000',
              '17000',
              '18000',
              '19000',
              '20000',
              '21000',
              '22000',
              '23000',
              '24000',
              '25000',
              '26000',
              '27000',
              '28000',
              '29000',
              '30000',
              '31000',
              '32000',
              '33000',
              '34000',
              '35000',
              '36000',
              '37000',
              '38000',
              '39000',
              '40000',
              '41000',
              '42000',
              '44000',
              '45000',
              '46000',
              '47000',
              '48000',
              '49000',
              '50000',
              '51000',
              '53000',
              '54000',
              '55000',
              '56000']
    
    return states

@jit
def extract_all():
    states = create_state_list()
    df = extract(states)
    
    return df
