# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
"""

@author: Andre
"""

__author__ = 'Andrew Liu'

#%%
from numba import jit
#import numpy as np
import pandas as pd
#%%
@jit
def read_csv(year,area):
    if area != '11000 District of Columbia':
        path = r'E:\Data\QCEW\Raw'+'\\'+year+'.q1-q4.by_area'+'\\'+year+'.q1-q4 '+area+' -- Statewide.csv'
        data = pd.read_csv(path)
    else:
        path = r'E:\Data\QCEW\Raw'+'\\'+year+'.q1-q4.by_area'+'\\'+year+'.q1-q4 '+area+'.csv'
        data = pd.read_csv(path)    
    return data

@jit
def convert_dataframe(df):
    # This function select the relevant columns of the pd dataframe
    df = df[['area_fips','own_code','industry_code','year','qtr','month1_emplvl','month2_emplvl','month3_emplvl']]

    return df

@jit
def select_private(df):
    # This function keeps only private sector numbers
    df = df.drop(df[df['own_code']!=5].index)
    
    return df

@jit
def select_quarter(df,qtr):
    # This function selects the chosen quarter
    quarter = df[df['qtr']==qtr].index
    df=df.loc[quarter]
    
    return df
#%%
@jit
def select_variable(df):
    # This function selects relevant variables, namely manufacturing employment,
    # retail empployment, and total employment
    manu = df[df['industry_code']=='1013'].index # manufacturing
    retail = df[df['industry_code']=='44-45'].index # retail trade
    total = df[df['industry_code'] == '10'].index
    select = manu.append([retail,total])
    df = df.loc[select]
    df = df[['month1_emplvl','month2_emplvl','month3_emplvl']]
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
    area = area[0:5]
    df['state'] = pd.Series(int(int(area)/1000),index=df.index)
    
    return df

#%%
@jit
def extract_state(area):
    # Extract the data in one state
    years = ["2004","2005","2006","2007","2008","2009","2010","2011","2012"]
    qtrs = [1,2,3,4]
    df = pd.DataFrame(columns=['manu_emp','re_emp','t_emp','state'])
    for year in years:
        csv = read_csv(year,area)
        for qtr in qtrs:
            csv1 = csv
            print("Year "+year+" quarter "+str(qtr)+" start")
            csv1 = convert_dataframe(csv1)
            csv1 = select_private(csv1)
            csv1 = select_quarter(csv1,qtr)
            csv1 = select_variable(csv1)
            csv1 = add_index(csv1,year,qtr)
            csv1 = add_state_column(csv1,area)
            print("Year "+year+" quarter "+str(qtr)+" complete")
            df = df.append(csv1)
    
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
    
    states = ['01000 Alabama',
              '02000 Alaska',
              '04000 Arizona',
              '05000 Arkansas',
              '06000 California',
              '08000 Colorado',
              '09000 Connecticut',
              '10000 Delaware',
              '11000 District of Columbia',
              '12000 Florida',
              '13000 Georgia',
              '15000 Hawaii',
              '16000 Idaho',
              '17000 Illinois',
              '18000 Indiana',
              '19000 Iowa',
              '20000 Kansas',
              '21000 Kentucky',
              '22000 Louisiana',
              '23000 Maine',
              '24000 Maryland',
              '25000 Massachusetts',
              '26000 Michigan',
              '27000 Minnesota',
              '28000 Mississippi',
              '29000 Missouri',
              '30000 Montana',
              '31000 Nebraska',
              '32000 Nevada',
              '33000 New Hampshire',
              '34000 New Jersey',
              '35000 New Mexico',
              '36000 New York',
              '37000 North Carolina',
              '38000 North Dakota',
              '39000 Ohio',
              '40000 Oklahoma',
              '41000 Oregon',
              '42000 Pennsylvania',
              '44000 Rhode Island',
              '45000 South Carolina',
              '46000 South Dakota',
              '47000 Tennessee',
              '48000 Texas',
              '49000 Utah',
              '50000 Vermont',
              '51000 Virginia',
              '53000 Washington',
              '54000 West Virginia',
              '55000 Wisconsin',
              '56000 Wyoming']
    
    return states

@jit
def extract_all():
    states = create_state_list()
    df = extract(states)
    
    return df 


#%%
df = extract_all()
#%%
path = r'C:\Users\Andre\Dropbox\Minimum wage and occupational mobility\data\Employment share\emp_share_04_12.csv'
df.to_csv(path)