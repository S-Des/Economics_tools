# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
# Risk2Life 

import pandas as pd


# Setting variables (York Project F10 used as example)

A_vul=6 #Area Vulnerability - as set by user and found in economics tool

P_vul=0.3 #People Vulnerability - as set by user and found in economics tool

D=0 #Debris Factor - as set by user and found in economics tool

v= 0.5 #Velocities for RP's 5,10,20,50,75,100,200 and 100 year

#userinput = input('give me filepath pls')
userinput = 'C:\\D\\TV_Depths.csv'

nrd_df = pd.read_csv(userinput)

shape = nrd_df.shape
totalrows = shape[0]
rowslist = range(0,9)

listheaders = list(nrd_df.columns.values)
listofreturns=listheaders[25:35]
# main loop


# use a dictionary to create a list of lists with the key being each return
fatalitiesDict ={}
# add blank list to dictonary with each return as a key
for rp in listofreturns:
    fatalitiesDict[rp] = []

for row in range(0,totalrows):
    for rp in listofreturns:
        depth=nrd_df.at[row,rp]
        if depth < -99:
            depth = 0
        hazard = depth*(v+0.5)+D
        fatalities = 2*hazard*A_vul*P_vul
        print(rp)
        print(depth)
        print(hazard)
        print(fatalities)
        fatalitiesDict[rp].append(fatalities)# add result to the correct return period list in the dictionary

# add dictonary lists to dataframe as new columns

nrdupdated_df = nrd_df # create copy of nrd_df      
for rp in listofreturns:
    newcolumnlist = fatalitiesDict[rp]#copies list from dictionary to new blank list
    newHeader = str(rp+'_fatalities')
    nrdupdated_df[newHeader] = newcolumnlist # assign new header, and data to new column
        
outputpath = input('save location pls:')
nrdupdated_df.to_csv(path_or_buf=outputpath'\\NRD\\fatalities'.csv', sep=',')

