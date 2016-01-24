from __future__ import division

import csv
import datetime
import math
import re
import MySQLdb
import os
import pandas

# Here's what we need to do:
# the algorithm gives a column of census blocks and corresponding column of hypothetical district grouping
# Using Block ID to voting district you can group the blocks up into county/district combos
# Using precinct to voting district, you can get the associated string name for each county/district combo
# the precinct level voting data has these names in it

# I think I can skip the 3rd step there - precinct level voting data appears to also have the county/district combo
# One data problem is that absentee/early voting is not submitted by precinct so you'll have to decide how to deal with these

# The ideal finished product is just all the algorithm districts with a number of R and D votes for that district

# HOW TO ACCOMPLISH THIS
# IF each block maps to exactly one district, consolidate the algorithm data up to districts (county/district combo)
#		-- test to see if this is true - unclear right now
# Match each county/district combo to the Harvard data and assign the correct voting statistics


# compile all the block map/district files
# execfile('/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/data_cleaning.py')

def block_vd_pandas():
	files=os.listdir('/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/Raw Data/Block ID to voting district/')
	stem='/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/Raw Data/Block ID to voting district/'

	master_list=[]
	files=[file for file in files if file[-3:]=='txt']

	for name in files:
		state=name[17:19]
		name=stem+name
		print name
		with open(name,'rb') as file:
			reader=csv.reader(file)
			temp=[row for row in reader]

		temp=temp[1:]
		for row in temp:
			row.append(state)
			master_list.append(row)	

	temp_pandas=pd.DataFrame(master_list,columns=['BlockID','CountyFP','District','state'])
	return temp_pandas

def load_algorithm_blocks():
	files=os.listdir('/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/Raw Data/Algorithm District Blocks/')
	stem='/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/Raw Data/Algorithm District Blocks/'

	master_list=[]
	files=[file for file in files if file[-3:]=='csv']

	for name in files:
		state=name[0:2]
		name=stem+name
		print name
		with open(name,'rb') as file:
			reader=csv.reader(file)
			temp=[row for row in reader]

		for row in temp:
			row.append(state)
			master_list.append(row)	

	temp_pandas=pd.DataFrame(master_list,columns=['BlockID','HouseDistrict','state'])
	return temp_pandas

def load_precinct_data():
	files=os.listdir('/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/Raw Data/precinct_votes/')
	stem='/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/Raw Data/precinct_votes/'

	master_list=[]
	files=[file for file in files if file[-3:]=='tab']

	for name in files:
		state=name[0:2]
		year=name[3:7]
		name=stem+name
		print name
		with open(name,'rb') as file:
			reader=csv.reader(file)
			temp=[row for row in reader]

		# it looks like precinct names are *not* consistent between years. Ugh.
		wanted_columns=['state','year','precinct_code','precinct']
		column_indices=[]

		for row in temp:
			row.append(state)
			master_list.append(row)	

	temp_pandas=pd.DataFrame(master_list,columns=['BlockID','county','HouseDistrict','state'])
	return temp_pandas	

def precinct_names():
	files=os.listdir('/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/Raw Data/Precinct to voting district/')
	stem='/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/Raw Data/Precinct to voting district/'

	master_list=[]
	files=[file for file in files if file[-3:]=='txt']

	for name in files:
		state=name[11:13]
		name=stem+name
		print name
		with open(name,'rb') as file:
			reader=csv.reader(file, delimiter='|')
			temp=[row for row in reader]

		temp=temp[1:]
		for row in temp:
			row.append(state)
			master_list.append(row)	

	# for row in master_list
	temp_pandas=pd.DataFrame(master_list,columns=['StateFP','CountyFP','District','Name','Namelsad','state_x'])
	return temp_pandas

def full_script():
	a=block_vd_pandas()
	b=load_algorithm_blocks()
	cd=pd.merge(a,b,on='BlockID')

	# need to create a column that tracks how many blocks are in each district - a count of duplicates
	# and then, when you get rid of blocks and boil the data set down to algorithm/district combos, need
	# to know how many blocks from a given district are in a given HouseDistrict

	# Count county/district/state duplicates AND county/district/state/algorithm district dupes
	cd['blocks']=cd.groupby(['state_x','CountyFP','District'])['BlockID'].transform('count')
	cd['ADblocks']=cd.groupby(['state_x','CountyFP','District','HouseDistrict'])['BlockID'].transform('count')

	# delete all county/district/state duplicates
	k=cd[['CountyFP','District','state_x','HouseDistrict']].duplicated()
	i=cd[~k]
	i['percent_district_in_AlgoHouseDist']=i['ADblocks']/i['blocks']
	del i['state_y']

	# Merge in the names of precincts
	c=precinct_names()
	cid=pd.merge(i,c,on=['state_x','CountyFP','District'])

	# Merge in the names of counties by FIPS
	fip_file='/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/Raw Data/national_county.csv'

	with open(fip_file,'rb') as file:
			reader=csv.reader(file)
			temp=[row[1:4] for row in reader]

	fips=pd.DataFrame(temp,columns=['StateFP','CountyFP','County'])
	cid=pd.merge(cid,fips,on=['StateFP','CountyFP'])

	# big re-ordering of CID just to make it a little cleaner
	cid=cid[['state_x','StateFP','County','CountyFP','District','Name','Namelsad','BlockID','HouseDistrict','blocks','ADblocks','percent_district_in_AlgoHouseDist']]

	# so now you have a nice thing: 186k row matrix of VTDs matched to algorithm districts.
	# The problem is that the Harvard file are not well-aligned with the census files, so
	# to match them, we have to do something like get all districts that are in Alabama in
	# CountyFP 001, then fuzzy match on district code, name, and namelsad. 

	votes=



	# State by state breakdown of the voting files:

	# AK: 1 district
	# AL: Name/namelsad is a very close match for harvard 'precinct' (use with county)
	# AZ: Combination of County Name + District. ex: district=54, county=Rock point, harvard='54 Rock Point - Prec #: 54.1' 
	# AR: Name is a very close match for harvard 'precinct' (use with county) - some absentee
	# CA: tricky - statefips+countyfips gives harvard county_code. The rest is completely impenetrable. I have no idea.
	# CO: 'District' matches the harvard 'vtd' field
	# CT: Harvard data is aggregated by something other than voting district. Unuseable.
	# DE: 1 district
	# FL: County is represented by first three characters in Harvard's 'county' ie Volusia becomes VOL, precinct is that prefix ('VOL') + District
	# GA: County and District match 'county' and 'precinct' in Harvard
	# HI
	# ID
	# IL
	# IN
	# IA
	# KS
	# KY
	# LA
	# ME
	# MD
	# MA
	# MI
	# MN
	# MS
	# MO
	# MT
	# NE
	# NV
	# NH
	# NJ
	# NM
	# NY
	# NC
	# ND
	# OH
	# OK
	# OR
	# PA
	# RI
	# SC
	# SD
	# TN
	# TX
	# UT
	# VT
	# VA
	# WA
	# WV
	# WI: 
	# WY: 1 district






