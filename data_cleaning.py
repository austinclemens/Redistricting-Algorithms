from __future__ import division

import csv
import datetime
import math
import re
import MySQLdb
import os
import pandas as pd
import difflib

pd.set_option('display.width',150)

realstate=['AK','AR','CA','CO','FL','GA','IA','IL','KS','KY','LA','ME','MI','MN','MS','NC','NH','NY','OH','OR','PA','SC','SD','WI','VA','TX','WV']

states = {
        'AK': 'Alaska',
        'AL': 'Alabama',
        'AR': 'Arkansas',
        'AZ': 'Arizona',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'HI': 'Hawaii',
        'IA': 'Iowa',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'MA': 'Massachusetts',
        'MD': 'Maryland',
        'ME': 'Maine',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MO': 'Missouri',
        'MS': 'Mississippi',
        'MT': 'Montana',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'NE': 'Nebraska',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NV': 'Nevada',
        'NY': 'New York',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VA': 'Virginia',
        'VT': 'Vermont',
        'WA': 'Washington',
        'WI': 'Wisconsin',
        'WV': 'West Virginia',
        'WY': 'Wyoming'
}

# 2014 exit polls
#			Dem		Rep		Other
# White M 	33%		64%		3%
# White F 	42%		56%		2%
# Black M 	86%		13%		1%
# Black F 	91%		8%		1%
# Latino M 	57%		41%		2%
# Latino F 	66%		32%		2%
# Other		49%		48%		3%

#			Dem		Rep		Other
# White		38%		60%		2%
# Black		89%		10%		1%
# Latino	62%		36%		2%
# Asian		49%		50%		1%
# Other		49%		47%		4%

#				Dem		Rep		Other
# White 18-29	43%		54%		3%
# White 30-44	40%		58%		2%
# White 45-64	36%		62%		2%
# White 65+		36%		62%		2%
# Black 18-29	88%		11%		1%
# Black 30-44	86%		12%		2%
# Black 45-64	90%		9%		1%
# Black 65+		92%		7%		1%
# Latino 18-29	68%		28%		4%
# Latino 30-44	56%		42%		2%
# Latino 45-64	62%		37%		2%
# Latino 65+	64%		34%		2%
# Other			49%		49%		2%

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

# notes for assembling block demographics:
# in the algeo2010.pl style file, you get the block number by:
# filtering for row[0][8:11]=='750'
#		row[27:32]+row[54:60]+row[61:65]
# In the 2nd data file, 
# 	hispanic over 18: row[77]
#	white over 18: row[80]
#	black over 18: row[81]
#	asian over 18: row[83]
#	other: row[76] - row[77] - row[80] - row[81] - row[83]

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


def load_demographics():
	directories=os.listdir('/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/Raw Data/census_redistricting_data/')
	directories=[d for d in directories if d[-2:]=='pl']
	master=pd.DataFrame([['',0,0,0,0,0]],columns=['BlockID','hispanic','white','black','asian','other'])

	for direct in directories:
		path='/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/Raw Data/census_redistricting_data/'+direct
		state=direct[0:2]
		print 'demographics: ',state

		with open(path+'/'+state+'geo2010.pl','rU') as csvfile:
			reader=csv.reader(csvfile)
			temp=[row for row in reader]

		geos=[[row[0][18:25],row[0][8:11],row[0][27:32]+row[0][54:60]+row[0][61:65]] for row in temp]
		geos=pd.DataFrame(geos,columns=['logical','type','BlockID'])

		with open(path+'/'+state+'000022010.pl','rU') as csvfile:
			reader=csv.reader(csvfile)
			temp=[row for row in reader]

		data=[[row[4],int(row[77]),int(row[80]),int(row[81]),int(row[83]),int(row[76])-int(row[77])-int(row[80])-int(row[81])-int(row[83])] for row in temp]
		data=pd.DataFrame(data,columns=['logical','hispanic','white','black','asian','other'])

		full=pd.merge(geos,data,on='logical')
		full=full[full['type']=='750']
		print len(full)
		full=full[['BlockID','hispanic','white','black','asian','other']]

		master=pd.concat([master,full])

	return master


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

def existing_districts():
	with open('/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/Raw Data/National_CD114.txt','rU') as cfile:
		reader=csv.reader(cfile)
		temp=[row for row in reader]

	existingd=pd.DataFrame(temp,columns=['BlockID','real_district'])
	return existingd

def state_diagnostics():
	for key in states.keys():
		try:
			calculate_districts(key,cd)
		except:
			print 'ERROR ',key

def state_diagnostics2():
	for key in realstate:
		try:
			calculate_districts(key,cd)
		except:
			print 'ERROR ',key

def full_script():
	a=block_vd_pandas()
	total=load_demographics()
	c=pd.merge(a,total,on='BlockID')
	existingd=existing_districts()
	c=pd.merge(c,existingd,on='BlockID')
	b=load_algorithm_blocks()
	b.columns=['BlockID2','HouseDistrict','state']
	cd=pd.merge(c,b,left_on='BlockID',right_on='BlockID2',how='outer')

	return cd

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
	cid=cid[['state_x','StateFP','County','CountyFP','District','Name','Namelsad','BlockID','HouseDistrict','real_district','blocks','ADblocks','percent_district_in_AlgoHouseDist']]

	return cid

	# so now you have a nice thing: 186k row matrix of VTDs matched to algorithm districts.
	# The problem is that the Harvard file are not well-aligned with the census files, so
	# to match them, we have to do something like get all districts that are in Alabama in
	# CountyFP 001, then fuzzy match on district code, name, and namelsad. 
	# following is sa look at how each state might be matched

	# State by state breakdown of the voting files FOR 2010:

	# AK: 1 district
	# AL: remove leading 0s from countyFP (ie 1,2,3), match to fips_cnty then fuzzy match namelsad to precinct. vars: g2010_USH_dv, g2010_USH_rv, g2010_USH_dv2, g2010_USH_rv2
	# AZ: none
	# AR: County is a perfect match for County, there are absentee breakouts in each county (sometimes>1). name is a close match for precinct. g2010_USH_dv	g2010_USH_rv
	# CA: IDGI. it's like a slightly shorter block id and it doesn't seem to match anything I have.
	# CO: It seems like vtd is an exact match for District. g2010_USH_rv	g2010_USH_dv
	# CT: This is based on townships or something? I don't see how it can be done.
	# DE: 1 district
	# FL: have to pull apart the precinct column, which looks like: 'ALA0001' - this matches to County: 'Alachua County' and District: '0001'. it's not entirely clear how that first letter thing works. g2010_USH_dv	g2010_USH_rv
	# GA: none
	# HI: something is up here and I don't know what... the census stuff I downloaded is circa 2011 yet the 2010 file has totally different names for districts of matching numbers. I think this one is best left alone.
	# ID: can't be done. Lots of aggregation in the Harvard files.
	# IL: none
	# IN: none
	# IA: none
	# KS: easy - match County to county and then vtd to District. g2010_USH_dv	g2010_USH_rv
	# KY - investigate - KY is not in cid why?
	# LA: no way to match
	# ME: no way to match
	# MD: no way to match
	# MA: no way to match
	# MI: no way to match 
	# MN: match county to int(County) and then Name to precinct. g2010_USH_dv	g2010_USH_rv
	# MS: match County to county and then Name to precinct. g2010_USH_dv	g2010_USH_rv	g2010_USH_dv2	g2010_USH_rv2
	# MO: match county to County and then... precinct should match name but the match is *very* inexact. I'm not sure traditional word distance kinda thing will work. g2010_USH_dv	g2010_USH_rv g2010_USH_dv2	g2010_USH_rv2 g2010_USH_dv3	g2010_USH_rv3
	# MT: 1 district
	# NE: none
	# NV: this is basically perfect - County to county_name, then precinct to Name - the 'PRECINCT 16' part of name, not 'CHURCHILL PREINCT 16', g2010_USH_dv	g2010_USH_rv
	# NH: County to county and town to Name g2012_USH_rv	g2012_USH_dv
	# NJ: none
	# NM: none
	# NY: match County to county_name and vtd08 to District. g2010_USH_dv	g2010_USH_rv
	# NC: County to county and District to vtd. g2010_USH_dv	g2010_USH_rv g2010_USH_dv2	g2010_USH_rv2
	# ND: 1 district
	# OH: County to county and Name to precinct_code. dv dv2 dv3
	# OK: look at precinct column, which looks like this: "ALFALFA CO. PCT 020110" - the first bit matches County, the number matches up to District but only the last 3 digits (110), dv and dv2
	# OR: I don't think these can be matched but check cid - for some reason most of OR dropped
	# PA: match CountyFP to fips and then District to vtd (make sure you int em) just dv
	# RI: no RI rows in CID
	# SC: county to County and Name to precinct should fuzzy match well, dv and dv2
	# SD: 1 district
	# TN: county to County and precinct to Name - not close on some matches but should fuzzy match fine. dv and dv2
	# TX: County to county and District to vtd, dv only
	# UT: none
	# VT: 1 district
	# VA: County to county and then fuzzy Name to precinct, dv only
	# WA: County to county and Name to precinct, dv only
	# WV: at first blush these match up but some counties with unusual schemes do not sync between the datasets, I would avoid
	# WI: match mcd to Name, dv only
	# WY: 1 district

	# State by state breakdown of the voting files FOR 2012:

	# AK: 1 district
	# AL: fuzzy match County to county then fuzzy match namelsad to precinct. vars: g2012_USH_dv, g2012_USH_rv, g2012_USH_dv2, g2012_USH_rv2
	# AZ: remove 'County' from County and then match to county, take the first 2 (3?) digits of precinct and match to District. There is an election total row but that wil be ignored with this procedure g2012_USH_dv	g2012_USH_rv
	# AR: County is a perfect match for County, there are absentee breakouts in each county (sometimes>1). name is a close match for precinct. g2012_USH_dv	g2012_USH_rv
	# CA: none
	# CO: none
	# CT: This is based on townships or something? I don't see how it can be done.
	# DE: 1 district
	# FL: none
	# GA: county is a match with County (although county has no 'county suffix') and precinct is an exact match for district. g2012_USH_dv, g2012_USH_rv, g2012_USH_dv2, g2012_USH_rv2, g2012_USH_dv3, g2012_USH_rv3, g2012_USH_dv4, g2012_USH_rv4
	# HI: something is up here and I don't know what... the census stuff I downloaded is circa 2011 yet the 2010 file has totally different names for districts of matching numbers. I think this one is best left alone.
	# ID: Can't be done.
	# IL: Match County to county, match precinct to Name. precinct looks like: 'Ellington PCT 3' vs name: ELLINGTON 3 - so maybe just remove pct g2012_USH_dv g2012_USH_rv g2012_USH_dv2 g2012_USH_rv2 g2012_USH_dv3 g2012_USH_rv3
	# IN: none
	# IA: none
	# KS: easy - match County to county and then vtd to District. g2012_USH_dv	g2012_USH_rv
	# KY - investigate - KY is not in cid why?
	# LA: no way to match
	# ME: no way to match
	# MD: no way to match
	# MA: no way to match
	# MI: none
	# MN: match county to int(County) and then int(district) to precinct. g2010_USH_dv	g2010_USH_rv
	# MS: match County to county and then Name to precinct. g2012_USH_dv	g2012_USH_rv	g2012_USH_dv2	g2012_USH_rv2
	# MO: none
	# MT: 1 district
	# NE: match county to County and Name to precinct g2012_USH_dv	g2012_USH_rv
	# NV: none
	# NH: County to county and precinct to Name g2012_USH_rv	g2012_USH_dv
	# NJ: none
	# NM: county to County and District to precinct. g2012_USH_rv	g2012_USH_tv
	# NY: none
	# NC: County to county and District to vtd. g2010_USH_dv	g2010_USH_rv g2010_USH_dv2	g2010_USH_rv2
	# ND: 1 district
	# OH: none
	# OK: match County to county and then District to the last 3 (4?) digits of precinct, dv and dv2
	# OR: I don't think these can be matched but check cid - for some reason most of OR dropped
	# PA: match County to county and District to precinct_code, but there is no house race here?
	# RI: no RI rows in CID
	# SC: county to County and Name to precinct should fuzzy match well, dv and dv2
	# SD: 1 district
	# TN: county to County and precinct to Name - not close on some matches but should fuzzy match fine. dv and dv2
	# TX: County to county and District to vtd, dv only
	# UT: none
	# VT: 1 district
	# VA: County to county and then fuzzy Name to precinct, dv only
	# WA: County to county and Name to precinct_name, dv only
	# WV: none
	# WI: none
	# WY: 1 district

	# states with missing CID rows: RI, OR, KY
	# this is due to missing fips codes for counties - doesn't seem like having them would help but for matching more states but come back to this

def merge_exitpolls(cid):
	with open('/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/exitpoll_altered_final.csv','rU') as cfile:
		reader=csv.reader(cfile)
		exits=[row for row in reader]

	exits=exits[1:]

	for i,row in enumerate(exits):
		for j,entry in enumerate(row):

			try:
				exits[i][j]=float(exits[i][j])
			except:
				pass

			if entry=='':
				exits[i][j]=.5

	exits=pd.DataFrame(exits,columns=['state_x', 'exit_type', 'whitervote', 'whitedvote', 'blackrvote', 'blackdvote', 'hispanicrvote', 'hispanicdvote', 'asianrvote', 'asiandvote', 'otherrvote', 'otherdvote', 'whiteturnout', 'blackturnout', 'hispanicturnout', 'asianturnout', 'otherturnout'])
	cid=pd.merge(cid,exits,on='state_x')
	return cid

def calculate_districts(state,cd):

	state2=states[state].upper()

	with open('/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/rawvote.csv','rU') as cfile:
		reader=csv.reader(cfile)
		exits=[row for row in reader if row[0]==state2]

	rdists=0
	ddists=0
	totalvotes=0
	totalrvotes=0
	rcompete=0
	for row in exits:
		if int(row[2])+int(row[3])>0:
			if int(row[2])>int(row[3]):
				rdists=rdists+1
			else:
				ddists=ddists+1

			totalvotes=totalvotes+int(row[2])+int(row[3])
			totalrvotes=totalrvotes+int(row[2])

			rper=int(row[2])/(int(row[2])+int(row[3]))
			dper=int(row[3])/(int(row[2])+int(row[3]))

			# print rper,dper
			if abs(rper-dper)<.05:
				rcompete=rcompete+1


	temp=cd[(cd['state_x']==state) & (cd['real_district']!='ZZ')]
	real_districts=list(set(temp['real_district']))
	algo_districts=list(set(temp['HouseDistrict']))

	temp['rvotes']=temp['white']*temp['whitervote']*temp['whiteturnout']+temp['black']*temp['blackrvote']*temp['blackturnout']+temp['hispanic']*temp['hispanicrvote']*temp['hispanicturnout']+temp['asian']*temp['asianrvote']*temp['asianturnout']+temp['other']*temp['otherrvote']*temp['otherturnout']
	temp['dvotes']=temp['white']*temp['whitedvote']*temp['whiteturnout']+temp['black']*temp['blackdvote']*temp['blackturnout']+temp['hispanic']*temp['hispanicdvote']*temp['hispanicturnout']+temp['asian']*temp['asiandvote']*temp['asianturnout']+temp['other']*temp['otherdvote']*temp['otherturnout']

	correct_dists=0
	real40white=0

	errors=[]

	for dist in real_districts:
		dist_temp=temp[temp['real_district']==dist]
		if dist=='00':
			real_compare=[row for row in exits if int(row[1])==1]
		else:
			real_compare=[row for row in exits if int(row[1])==int(dist)]
		rc=real_compare[0]

		if int(rc[2])+int(rc[3])>0:

			rper=int(rc[2])/(int(rc[2])+int(rc[3]))

			tempr=dist_temp['rvotes'].sum()
			tempd=dist_temp['dvotes'].sum()

			if tempr/(tempr+tempd)>.5 and rper>.5:
				correct_dists=correct_dists+1
			if tempr/(tempr+tempd)<.5 and rper<.5:
				correct_dists=correct_dists+1

			errors.append(abs(tempr/(tempr+tempd)-rper))

		whites=dist_temp['white'].sum()
		others=dist_temp['black'].sum()+dist_temp['hispanic'].sum()+dist_temp['asian'].sum()+dist_temp['other'].sum()

		if whites/(whites+others)<.5:
			real40white=real40white+1

	algo_compete=0
	algo_rdists=0
	totalvotesa=0
	totalrvotesa=0
	algo40white=0

	for dist in algo_districts:
		if math.isnan(float(dist)):
			dist_temp=temp[temp['HouseDistrict'].isnull()]
		else:
			dist_temp=temp[temp['HouseDistrict']==dist]

		tempr=dist_temp['rvotes'].sum()
		tempd=dist_temp['dvotes'].sum()

		if tempr>tempd:
			algo_rdists=algo_rdists+1

		if abs((tempr/(tempr+tempd))-(tempd/(tempr+tempd)))<.05:
			algo_compete=algo_compete+1

		whites=dist_temp['white'].sum()
		others=dist_temp['black'].sum()+dist_temp['hispanic'].sum()+dist_temp['asian'].sum()+dist_temp['other'].sum()

		if whites/(whites+others)<.5:
			algo40white=algo40white+1

	# print 'Districts,real correct,avg error,r popvote %,% real r,% algo r,real competitive,algo competitive,real minority districts, algo minority districts'
	print state,len(real_districts),correct_dists,sum(errors)/len(errors),(totalrvotes/totalvotes),(rdists/(rdists+ddists)),algo_rdists/(rdists+ddists),rcompete,algo_compete,real40white,algo40white


def arrange_rawvote():
	# this is a one-time thing to clean up the rawvote file (which comes from http://psephos.adam-carr.net/countries/u/usa/congress/house2014.txt)
	states=['ALABAMA', 'ALASKA', 'ARIZONA', 'ARKANSAS', 'CALIFORNIA', 'COLORADO', 'CONNECTICUT', 'DELAWARE', 'FLORIDA', 'GEORGIA', 'HAWAII', 'IDAHO', 'ILLINOIS', 'INDIANA', 'IOWA', 'KANSAS', 'KENTUCKY', 'LOUISIANA', 'MAINE', 'MARYLAND', 'MASSACHUSETTS', 'MICHIGAN', 'MINNESOTA', 'MISSISSIPPI', 'MISSOURI', 'MONTANA', 'NEBRASKA', 'NEVADA', 'NEW HAMPSHIRE', 'NEW JERSEY', 'NEW MEXICO', 'NEW YORK', 'NORTH CAROLINA', 'NORTH DAKOTA', 'OHIO', 'OKLAHOMA', 'OREGON', 'PENNSYLVANIA', 'RHODE ISLAND', 'SOUTH CAROLINA', 'SOUTH DAKOTA', 'TENNESSEE', 'TEXAS', 'UTAH', 'VERMONT', 'VIRGINIA', 'WASHINGTON', 'WEST VIRGINIA', 'WISCONSIN', 'WYOMING']
	first=1
	master=[]
	current_state=''

	with open('/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/Raw Data/rawvote.txt','rU') as cfile:
		rawvote=[row for row in cfile]

	for row in rawvote[6:]:
		print row
		row=row.strip()
		if row in states:
			previous_state=current_state
			current_state=row

		if 'DISTRICT' in row:
			temp=int(row[9:11])
			if first==0:
				if temp==1:
					master.append([previous_state,district,rvote,dvote])
					rvote=0
					dvote=0
				if temp!=1:
					master.append([current_state,district,rvote,dvote])
					rvote=0
					dvote=0
			if first==1:
				first=0
			district=int(row[9:11])

		if 'AT-LARGE' in row.upper():
			temp=1
			if first==0:
				if temp==1:
					master.append([previous_state,district,rvote,dvote])
					rvote=0
					dvote=0
				if temp!=1:
					master.append([current_state,district,rvote,dvote])
					rvote=0
					dvote=0
			district=1

		if 'Republican' in row:
			if 'Unopposed' not in row:
				row=row.split('  ')
				vote=row[-2].strip().replace(',','').replace('.','')
				rvote=int(vote)
			if 'Unopposed' in row:
				rvote=0

		if 'Democratic' in row:
			if 'Unopposed' not in row:
				row=row.split('  ')
				vote=row[-2].strip().replace(',','').replace('.','')
				dvote=int(vote)
			if 'Unopposed' in row:
				rvote=0

	return master

def turnout_rawvote(cd):
	# take the rawvote data, which has race by composition of electorate, and change it to turnout %age for each race
	# pass in the block pandas datafile to compare
	with open('/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/exitpoll_noblanks.csv','rU') as cfile:
		reader=csv.reader(cfile)
		exits=[row for row in reader]

	# open up the election results file to get total votes in each state
	with open('/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/rawvote.csv','rU') as cfile:
		reader=csv.reader(cfile)
		rawvote=[row for row in reader]

	rawvote=pd.DataFrame([[row[0],row[1],int(row[2]),int(row[3])] for row in rawvote[1:]],columns=['state','district','republican','democrat'])

	for row in exits[1:]:
		state=states[row[0]].upper()
		temp=rawvote[rawvote['state']==state]
		totalvotes=temp['democrat'].sum()+temp['republican'].sum()

		print state,len(temp),totalvotes

		totalwhite=cd[cd['state_x']==row[0]]['white'].sum()
		totalblack=cd[cd['state_x']==row[0]]['black'].sum()
		totalhispanic=cd[cd['state_x']==row[0]]['hispanic'].sum()
		totalasian=cd[cd['state_x']==row[0]]['asian'].sum()
		totalother=cd[cd['state_x']==row[0]]['other'].sum()

		whiteturnout=float(row[12])*totalvotes/totalwhite
		blackturnout=float(row[13])*totalvotes/totalblack
		hispanicturnout=float(row[14])*totalvotes/totalhispanic
		asianturnout=float(row[15])*totalvotes/totalasian
		otherturnout=float(row[16])*totalvotes/totalother

		row.append(whiteturnout)
		row.append(blackturnout)
		row.append(hispanicturnout)
		row.append(asianturnout)
		row.append(otherturnout)

	with open('/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/rawvote_altered.csv','wb') as cfile:
		writer=csv.writer(cfile)
		for row in exits:
			writer.writerow(row)



def merge_harvard(cid):
	prec_votes_folder='/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/Raw Data/precinct_votes'
	state_files=os.listdir(prec_votes_folder)
	state_files=[file for file in state_files if file[-3:]=='tab']

	cid['g2012_USH_dv']=0
	cid['g2012_USH_rv']=0
	cid['g2012_USH_dv2']=0
	cid['g2012_USH_rv2']=0
	cid['g2012_USH_dv3']=0
	cid['g2012_USH_rv3']=0
	cid['g2012_USH_dv4']=0
	cid['g2012_USH_rv4']=0

	# load all state files into the data_dict
	data_dict={}

	for dfile in state_files:
		state=dfile[0:2]
		year=dfile[3:7]
		dfile=prec_votes_folder+'/'+dfile

		with open(dfile,'rU') as csvfile:
			reader=csv.reader(csvfile,delimiter='\t')
			temp=[row for row in reader]
		
		temp=pd.DataFrame(temp[1:],columns=temp[0])

		if state not in data_dict.keys():
			data_dict[state]={}

		data_dict[state][year]=temp

	# cid columns: u'state_x', u'StateFP', u'County', u'CountyFP', u'District', u'Name'
	# merge in each state file following the rules above.
	for state in data_dict.keys():
		for year in data_dict[state].keys():

			temp=data_dict[state][year]
			# all the merge rules go here
			if state=='AL':
				if int(year)==2010:
					temp['fips_cnty']=temp['fips_cnty'].astype(int)
					temp['new'] = temp.apply(lambda x: fuzzy_matcher(x['precinct'], cid[(cid['state_x']==state) & (cid['CountyFP'].astype(int)==x['fips_cnty'])]['Namelsad']),axis=1)
					temp.apply(lambda x: x['precinct'])

				elif int(year)==2012:
					pass

def fuzzy_matcher(string,comparison):
	# function for fuzzy matching. Cutoff is an important variable - make it bigger and it will prevent false matches
	# but may miss true matches, and if there is a match for every string it won't matter so I err on the side of making
	# it smaller.
	try:
		res=difflib.get_close_matches(string.lower(),[c.lower() for c in comparison],cutoff=.4)[0]
	except:
		res=''
	return res

