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

		column_indices=[]

		for row in temp:
			row.append(state)
			master_list.append(row)	

	temp_pandas=pd.DataFrame(master_list,columns=['BlockID','HouseDistrict','state'])
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

	# so now you have a nice thing: 186k row matrix of VTDs matched to algorithm districts.
	# at this point, KY, RI, and OR have been dropped because of the lack of block to VTD
	# data for them.

	# time to bring in the precinct voting data.
	votes=










