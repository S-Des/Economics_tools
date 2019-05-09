# -*- coding: utf-8 -*-
"""
Created on Wed Mar 06 11:50:22 2019

@author: Simon.Desmet
"""

# economics point extract interrogator - this script has been written to identify discrepancies in flood depth data

import pandas as pd



import os

import datetime




# standard lists

standardreturns = ['0005','0010','0020','0025','0030','0040','0050','0075','0100','0200','1000']

standardscenarios = ['DM','DN','DS','D1','D2','D3','D4','D5','BS','SS','CC','UD','DF']

standardepochs = ['20', '50', '80']




# define input 


NRD_datapath = raw_input("file path for NRD point extract CSV:")



# read in NRD point extract data

NRD_DF = pd.read_csv(NRD_datapath, header=0)

# determine the model/appraisals, scenario, returnperiod and epochs
csvheaderlist = list(NRD_DF) # gets all headers in csv into a list

runslist = []
for head in csvheaderlist: # gets all headers with a 0 in them into a new list called runs list0
    if '0' in head:# KEY ASSUMPTION, there are no non run headers with a 0!
        runslist.append(head)
        
print runslist

#get list of models
models = []
for run in runslist:
    modelID = run[0:2]
    models.append(modelID)
modelset = set(models)
modellist = list(modelset)

print modellist


# get list of scenarios
scenarioruns = []
for run in runslist:
    scenarioID = run[2:4]
    scenarioruns.append(scenarioID)
scenarioset = set(scenarioruns)
scenariolist = list(scenarioset)

print scenariolist


# get list of returns

returnsruns = []
for run in runslist:
    returnsID = run[4:8]
    returnsruns.append(returnsID)
returnsset = set(returnsruns)
returnslist = list(returnsset)

returnslist.sort() # alphanumeric list sort to get returns in numerical order
print returnslist


#get list of epochs

epochruns = []
for run in runslist:
    epochID = run[8:10]
    epochruns.append(epochID)
epochset = set(epochruns)
epochlist = list(epochset)

epochlist.sort()# alphanumeric list sort to get epochs in numerical order
print epochlist

#define output
current_date = datetime.datetime.now().date()
current_time = str(datetime.datetime.now().time())
timestring = current_time.split('.')[0]
timestring2 = timestring.replace(':','-')
date = str(current_date)+'_'+timestring2

outputpath = os.path.dirname(os.path.abspath(NRD_datapath))+'\\'+modellist[0]+'_'+date+'_'+'NRD_checkfile.csv'
output = open(outputpath,'w')



#-- compare lists read out of data with standard lists

scenariospresent = list(set(scenariolist) & set(standardscenarios)) # what scenarios are in both our NRD and the standard
output.write(scenariospresent)
output.write("\n")
nonstandardscenarios = list(set(scenariolist)-set(standardscenarios)) # are any NRD scenarios not in the standard list
output.write(nonstandardscenarios)
output.write("\n")
scenarios_not_present = list(set(standardscenarios)-set(scenariolist)) # what scenarios are in the standard set but not the NRD
output.write(scenarios_not_present)
output.write("\n")


# estimate if any data is missing
totalruns = len(runslist)#total number of runs

expectedruns = len(modellist)*len(scenariolist)*len(returnslist)*len(epochlist)# total estimated from multiplying models by scenarios by returns by epochs

if totalruns - expectedruns !=0:
    totalrunsreport = 'missing runs? runs expected; '+str(expectedruns)+' runs in NRD data; '+str(totalruns)+','
    print totalrunsreport
    output.write(totalrunsreport)
    output.write("\n")

#print table headers for checks to file
outputheaders = 'message, objectID, Easting, Northing, mcmcode, RUN ID 1, depth, RUN ID 2, depth, test,'
output.write(outputheaders)
output.write("\n")


# main loop

for mod in modellist:
    # create list of modelscenarios
    modscenlist = []
    for sce in scenariolist:
        modscen = mod+sce
        modscenlist.append(modscen)
    print modscenlist
    #use list to select all columns and create table
    for modscen in modscenlist: # create table with all runs from a modelled scenario only
        scen_DF =  NRD_DF[['objectid','easting', 'northing', 'mcmcode']] # creates copy of NRD_DF with just object IDs, E&N and MCMcodes
        scen_headlist = [] # blank list for loading run headers to include in new table
        for sc in csvheaderlist: # for each header in NRD_DF if our desired scenario appears, add to new list.
            if modscen in sc:
                scen_headlist.append(sc)
        working_DF = NRD_DF[scen_headlist] # creates new DF with scen head list columns only
        scen_DF = scen_DF.join(working_DF, how='left') # joins results DF to object id, E&N etc
        # at this point the table we want to check is ready.
        
        # return period comparison loop
        line = 'return period comparisons,'
        output.write(line)
        output.write("\n") 
        for ep in epochlist:
            rets_headlist = []
            for scen in scen_headlist:
                if ep in scen[8:10]:
                    rets_headlist.append(scen)# create list of headers with epoch in
            rets_headlist.sort()# sort by return
            print rets_headlist
            rowtotal = len(scen_DF.index)    # get total number of rows
            rowcounter = 0
            while rowcounter != rowtotal:
                print rowcounter
                for col1 in rets_headlist:
                    col2index = rets_headlist.index(col1)+1
                    if col2index <= len(rets_headlist)-1:# if our column 2 is off the end of the column list we need to break the for loop and stop.  
                        col2 = rets_headlist[col2index]
                        if scen_DF.at[rowcounter,col1]>scen_DF.at[rowcounter,col2]: # our comparison. if A is greater than B, write output to check file else continue.
                            line = 'error,'+str(scen_DF.at[rowcounter,'objectid'])+','+str(scen_DF.at[rowcounter,'easting'])+','+str(scen_DF.at[rowcounter,'northing'])+','+str(scen_DF.at[rowcounter,'mcmcode'])+','+col1+','+str(scen_DF.at[rowcounter,col1])+','+col2+','+str(scen_DF.at[rowcounter,col2])+','+'returnperiodfail'+','     
                            print 'error!'
                            print line
                            output.write(line)
                            output.write("\n")
                        else:
                            print 'no errors found'                            
                            continue
                    else:
                        continue

                rowcounter = rowcounter+1 # add +1 to check next row
        line = 'epoch comparisons,'
        output.write(line)
        output.write("\n")       
        # epoch comparison loop
        for retn in returnslist:
            epoc_headlist = []
            for scen in scen_headlist:
                if retn in scen[4:8]:
                    epoc_headlist.append(scen) # creates header list to search including each epoch variant for a given return period
                epoc_headlist.sort()
                print epoc_headlist
            rowcounter = 0
            while rowcounter != rowtotal:
                print rowcounter
                for col1 in epoc_headlist:
                    col2index = epoc_headlist.index(col1)+1
                    if col2index <= len(epoc_headlist)-1:# if our column 2 is off the end of the column list we need to break the for loop and stop.  
                        col2 = epoc_headlist[col2index]
                        if scen_DF.at[rowcounter,col1]>scen_DF.at[rowcounter,col2]: # our comparison. if A is greater than B, write output to check file else continue.
                            line = 'error,'+str(scen_DF.at[rowcounter,'objectid'])+','+str(scen_DF.at[rowcounter,'easting'])+','+str(scen_DF.at[rowcounter,'northing'])+','+str(scen_DF.at[rowcounter,'mcmcode'])+','+col1+','+str(scen_DF.at[rowcounter,col1])+','+col2+','+str(scen_DF.at[rowcounter,col2])+','+'epochfail'+','     
                            print 'error!'
                            print line
                            output.write(line)
                            output.write("\n")
                        else:
                            print 'no errors found'                            
                            continue
                    else:
                        continue               
                rowcounter = rowcounter+1 # add +1 to check next row            
         
for mod in modellist:

#scenario comparison
            #compare by pairs
            # DN<DM
            #DS<DM
            #D1<DM etc
    # is there a DN result, if yes then do DM vs DN
    if 'DN' in scenariolist:
        DNrunslist = [] # get list of all DN runs
        for run in runslist:
            if str(mod+'DN') in run:# model e.g. FB+DN 'FBDN' in run then add it to list.
                DNrunslist.append(run)
            else:
                continue
        print DNrunslist
        #loop over rows in NRD_DF using the DN runs list and comparing with matching DM run.
        line = 'DM-DN comparisons,'
        output.write(line)
        output.write("\n")   
        #check matching DM run actually exists!
        rowtotal = len(NRD_DF.index)
        for DNrun in DNrunslist:
            rowslist = range(0,rowtotal)
            for ro in rowslist:
                print ro
                rowcounter = ro
                DMrun = DNrun.replace('DN','DM')
                if DMrun in runslist:
                    print DMrun
                    #do test
                    if NRD_DF.at[rowcounter,DMrun]>NRD_DF.at[rowcounter,DNrun]: # our comparison. if A is greater than B, write output to check file else continue.
                        line = 'error,'+str(NRD_DF.at[rowcounter,'objectid'])+','+str(NRD_DF.at[rowcounter,'easting'])+','+str(NRD_DF.at[rowcounter,'northing'])+','+str(NRD_DF.at[rowcounter,'mcmcode'])+','+DMrun+','+str(NRD_DF.at[rowcounter,DMrun])+','+DNrun+','+str(NRD_DF.at[rowcounter,DNrun])+','+'DN>DM_fail'+','     
                        print 'error! DM has a greater depth than DN'
                        print line
                        output.write(line)
                        output.write("\n")
                    else:
                        print 'DN is greater than DM'                            
                        continue
                else:
                    continue
    else:
        continue
    optionlist = scenariolist.remove('DN','DN')# leave only Do Something or test scenarios and test against DM
    if len(optionlist)>0:#ensure the list is not empty
        for DSscen in optionlist:
            #check option against DM
            DSscen_runslist = []
            for run in runslist:
                if str(mod+DSscen) in run:
                    DSscen_runslist.append(run)
                else:
                    continue
            print DSscen_runslist
            line = str(DSscen)+'-DM comparisons,'
            output.write(line)
            output.write("\n")   
            rowtotal = len(NRD_DF.index)
            for DSrun in DSscen_runslist:
                rowslist = range(0,rowtotal)
                for ro in rowslist:
                    print ro
                    rowcounter = ro
                    DMrun = DSrun.replace(DSscen,'DM')
                    if DMrun in runslist:
                        print DMrun
                    #do test
                        if NRD_DF.at[rowcounter,DMrun]<NRD_DF.at[rowcounter,DSrun]: # our comparison. if A is greater than B, write output to check file else continue.
                            line = 'error,'+str(NRD_DF.at[rowcounter,'objectid'])+','+str(NRD_DF.at[rowcounter,'easting'])+','+str(NRD_DF.at[rowcounter,'northing'])+','+str(NRD_DF.at[rowcounter,'mcmcode'])+','+DMrun+','+str(NRD_DF.at[rowcounter,DMrun])+','+DSrun+','+str(NRD_DF.at[rowcounter,DSrun])+','+'DN>DM_fail'+','     
                            print 'error! '+DSscen+' has a greater depth than DM'
                            print line
                            output.write(line)
                            output.write("\n")
                        else:
                            print 'DM is greater than '+DSscen                            
                            continue
                    else:
                        continue 
    
    else:
        continue
    

print 'End of check'
current_date = datetime.datetime.now().date()
current_time = str(datetime.datetime.now().time())
finalline = 'End of check'+'_'+current_date+'_'+current_time+','
output.write(finalline)
output.write("\n")
    
    



output.close()




















