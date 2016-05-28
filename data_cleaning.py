from __future__ import division

import csv
import datetime
import math
import re
import MySQLdb
import os
import pandas as pd
import difflib
import numpy as np

pd.set_option('display.width',150)

realstate=['AR','CA','CO','FL','GA','IA','IL','KS','KY','LA','ME','MI','MN','MS','NC','NH','NY','OH','OR','PA','SC','WI','VA','TX','WV']

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
	with open('/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/Raw Data/National_CD2010.txt','rU') as cfile:
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
	# total=load_demographics()
	total=a
	c=pd.merge(a,total,on='BlockID')
	existingd=existing_districts()
	c=pd.merge(c,existingd,on='BlockID')
	b=load_algorithm_blocks()
	b.columns=['BlockID2','HouseDistrict','state']
	cd=pd.merge(c,b,left_on='BlockID',right_on='BlockID2',how='outer')

	# need to create a column that tracks how many blocks are in each district - a count of duplicates
	# and then, when you get rid of blocks and boil the data set down to algorithm/district combos, need
	# to know how many blocks from a given district are in a given HouseDistrict

	# Count county/district/state duplicates AND county/district/state/algorithm district dupes
	cd=cd[pd.notnull(cd['HouseDistrict'])]
	cd['CountyFP']=cd['CountyFP_x']
	cd['District']=cd['District_x']
	del cd['CountyFP_x']
	del cd['CountyFP_y']
	del cd['District_x']
	del cd['District_y']
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

	# lowercasing stuff is useful later
	cid['County'] = cid['County'].str.lower()
	cid['Name'] = cid['Name'].str.lower()
	cid['Namelsad'] = cid['Namelsad'].str.lower()
	cid['County']=cid.apply(lambda x: x['County'].replace('county','').strip().lower(),axis=1)
	cid['County']=cid.apply(lambda x: x['County'].replace('.',''),axis=1)
	cid['County']=cid.apply(lambda x: x['County'].replace(' ',''),axis=1)

	return cid

	# so now you have a nice thing: 186k row matrix of VTDs matched to algorithm districts.
	# The problem is that the Harvard file are not well-aligned with the census files, so
	# to match them, we have to do something like get all districts that are in Alabama in
	# CountyFP 001, then fuzzy match on district code, name, and namelsad. 

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

	if len(algo_districts)>70:
		algo_districts=['00']

	# print state2
	# print real_districts
	# print algo_districts

	temp['rvotes']=temp['white']*temp['whitervote']*temp['whiteturnout']+temp['black']*temp['blackrvote']*temp['blackturnout']+temp['hispanic']*temp['hispanicrvote']*temp['hispanicturnout']+temp['asian']*temp['asianrvote']*temp['asianturnout']+temp['other']*temp['otherrvote']*temp['otherturnout']
	temp['dvotes']=temp['white']*temp['whitedvote']*temp['whiteturnout']+temp['black']*temp['blackdvote']*temp['blackturnout']+temp['hispanic']*temp['hispanicdvote']*temp['hispanicturnout']+temp['asian']*temp['asiandvote']*temp['asianturnout']+temp['other']*temp['otherdvote']*temp['otherturnout']

	correct_dists=0
	real40white=0
	simulated_rdists=0

	errors=[]

	# print 'REAL DISTRICTS'

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

		if tempr/(tempr+tempd)>.5:
			simulated_rdists=simulated_rdists+1

		# print whites,others,tempr,tempd,tempr/(tempr+tempd)

	algo_compete=0
	algo_rdists=0
	totalvotesa=0
	totalrvotesa=0
	algo40white=0

	# print 'ALGORITHM DISTRICTS'

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

		totalvotesa=totalvotesa+tempr+tempd
		totalrvotesa=totalrvotesa+tempr

		# print whites,others,tempr,tempd,tempr/(tempr+tempd)

	# print 'Districts,real correct,avg error,r popvote %,% real r,% algo r,% simulated r,real competitive,algo competitive,real minority districts, algo minority districts'
	# print totalrvotesa/totalvotesa
	print state,len(real_districts),correct_dists,sum(errors)/len(errors),(totalrvotes/totalvotes),(rdists/(rdists+ddists)),algo_rdists/(rdists+ddists),simulated_rdists/(rdists+ddists),rcompete,algo_compete,real40white,algo40white


def arrange_rawvote(rv='/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/Raw Data/rawvote.txt'):
	# this is a one-time thing to clean up the rawvote file (which comes from http://psephos.adam-carr.net/countries/u/usa/congress/house2014.txt)
	states=['ALABAMA', 'ALASKA', 'ARIZONA', 'ARKANSAS', 'CALIFORNIA', 'COLORADO', 'CONNECTICUT', 'DELAWARE', 'FLORIDA', 'GEORGIA', 'HAWAII', 'IDAHO', 'ILLINOIS', 'INDIANA', 'IOWA', 'KANSAS', 'KENTUCKY', 'LOUISIANA', 'MAINE', 'MARYLAND', 'MASSACHUSETTS', 'MICHIGAN', 'MINNESOTA', 'MISSISSIPPI', 'MISSOURI', 'MONTANA', 'NEBRASKA', 'NEVADA', 'NEW HAMPSHIRE', 'NEW JERSEY', 'NEW MEXICO', 'NEW YORK', 'NORTH CAROLINA', 'NORTH DAKOTA', 'OHIO', 'OKLAHOMA', 'OREGON', 'PENNSYLVANIA', 'RHODE ISLAND', 'SOUTH CAROLINA', 'SOUTH DAKOTA', 'TENNESSEE', 'TEXAS', 'UTAH', 'VERMONT', 'VIRGINIA', 'WASHINGTON', 'WEST VIRGINIA', 'WISCONSIN', 'WYOMING']
	first=1
	master=[]
	current_state=''

	with open(rv,'rU') as cfile:
		rawvote=[row for row in cfile]

	dvote=0
	rvote=0

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

	# return data_dict

	# cid columns: u'state_x', u'StateFP', u'County', u'CountyFP', u'District', u'Name'
	# merge in each state file following the rules above.


	# State by state breakdown of the voting files FOR 2010:

	# NONE AK: 1 district
	# DONE AL: remove leading 0s from countyFP (ie 1,2,3), match to fips_cnty then fuzzy match namelsad to precinct. vars: g2010_USH_dv, g2010_USH_rv, g2010_USH_dv2, g2010_USH_rv2
	# NONE AZ: none
	# DONE AR: County is a perfect match for County, there are absentee breakouts in each county (sometimes>1). name is a close match for precinct. g2010_USH_dv	g2010_USH_rv
	# NONE CA: IDGI. it's like a slightly shorter block id and it doesn't seem to match anything I have.
	# DONE CO: It seems like vtd is an exact match for District. g2010_USH_rv	g2010_USH_dv
	# NONE CT: This is based on townships or something? I don't see how it can be done.
	# NONE DE: 1 district
	# FL: have to pull apart the precinct column, which looks like: 'ALA0001' - this matches to County: 'Alachua County' and District: '0001'. it's not entirely clear how that first letter thing works. g2010_USH_dv	g2010_USH_rv
	# NONE GA: none
	# NONE HI: something is up here and I don't know what... the census stuff I downloaded is circa 2011 yet the 2010 file has totally different names for districts of matching numbers. I think this one is best left alone.
	# NONE ID: can't be done. Lots of aggregation in the Harvard files.
	# NONE IL: none
	# NONE IN: none
	# NONE IA: none
	# DONE KS: easy - match County to county and then vtd to District. g2010_USH_dv	g2010_USH_rv
	# NONE KY - investigate - KY is not in cid why?
	# NONE LA: no way to match
	# NONE ME: no way to match
	# NONE MD: no way to match
	# NONE MA: no way to match
	# NONE MI: no way to match 
	# DONE MN: match county to int(County) and then Name to precinct. g2010_USH_dv	g2010_USH_rv
	# DONE MS: match County to county and then Name to precinct. g2010_USH_dv	g2010_USH_rv	g2010_USH_dv2	g2010_USH_rv2
	# DONE MO: match county to County and then... precinct should match name but the match is *very* inexact. I'm not sure traditional word distance kinda thing will work. g2010_USH_dv	g2010_USH_rv g2010_USH_dv2	g2010_USH_rv2 g2010_USH_dv3	g2010_USH_rv3
	# NONE MT: 1 district
	# NONE NE: none
	# DONE NV: this is basically perfect - County to county_name, then precinct to Name - the 'PRECINCT 16' part of name, not 'CHURCHILL PREINCT 16', g2010_USH_dv	g2010_USH_rv
	# DONE NH: County to county and town to Name g2012_USH_rv	g2012_USH_dv
	# NONE NJ: none
	# NONE NM: none
	# NONE NY: match County to county_name and vtd08 to District. g2010_USH_dv	g2010_USH_rv - I can't figure this out. It seems like it should be perfect but it doesn't match actual district votes at all
	# DONE NC: County to county and District to vtd. g2010_USH_dv	g2010_USH_rv g2010_USH_dv2	g2010_USH_rv2 this match is not amazing, probably because of the dv2 and rv2 variables
	# NONE ND: 1 district
	# DONE OH: County to county and Name to precinct_code. dv dv2 dv3, because of the 3 dvs some of these estimates are not amazing but all but one is correct - remember you could account for multi districts but the idea is just are you matching right, and these results make it look like you def are. A good sample of votes in each district
	# OK: look at precinct column, which looks like this: "ALFALFA CO. PCT 020110" - the first bit matches County, the number matches up to District but only the last 3 digits (110), dv and dv2
	# NONE OR: I don't think these can be matched but check cid - for some reason most of OR dropped
	# NONE PA: match CountyFP to fips and then District to vtd (make sure you int em) just dv - turns out the county fips don't match up at all - both have 67 but the cid ones run to 133 while harvard is just 1-67
	# NONE RI: no RI rows in CID
	# DONE SC: county to County and Name to precinct should fuzzy match well, dv and dv2 - harvard dataset misspells like half the county names. Great stuff.
	# NONE SD: 1 district
	# DONE TN: county to County and precinct to Name - not close on some matches but should fuzzy match fine. dv and dv2
	# DONE TX: County to county and District to vtd, dv only
	# NONE UT: none
	# NONE VT: 1 district
	# VA: County to county and then fuzzy Name to precinct, dv only
	# WA: County to county and Name to precinct, dv only
	# WV: at first blush these match up but some counties with unusual schemes do not sync between the datasets, I would avoid
	# WI: match mcd to Name, dv only
	# NONE WY: 1 district


	# State by state breakdown of the voting files FOR 2012:

	# NONE AK: 1 district
	# DONE AL: fuzzy match County to county then fuzzy match namelsad to precinct. vars: g2012_USH_dv, g2012_USH_rv, g2012_USH_dv2, g2012_USH_rv2
	# DONE AZ: remove 'County' from County and then match to county, take the first 2 (3?) digits of precinct and match to District. There is an election total row but that wil be ignored with this procedure g2012_USH_dv	g2012_USH_rv
	# DONE AR: County is a perfect match for County, there are absentee breakouts in each county (sometimes>1). name is a close match for precinct. g2012_USH_dv	g2012_USH_rv
	# NONE CA: none
	# NONE CO: none
	# NONE CT: This is based on townships or something? I don't see how it can be done.
	# NONE DE: 1 district
	# NONE FL: none
	# GA: county is a match with County (although county has no 'county suffix') and precinct is an exact match for district. g2012_USH_dv, g2012_USH_rv, g2012_USH_dv2, g2012_USH_rv2, g2012_USH_dv3, g2012_USH_rv3, g2012_USH_dv4, g2012_USH_rv4
	# NONE HI: something is up here and I don't know what... the census stuff I downloaded is circa 2011 yet the 2010 file has totally different names for districts of matching numbers. I think this one is best left alone.
	# NONE ID: Can't be done.
	# IL: Match County to county, match precinct to Name. precinct looks like: 'Ellington PCT 3' vs name: ELLINGTON 3 - so maybe just remove pct g2012_USH_dv g2012_USH_rv g2012_USH_dv2 g2012_USH_rv2 g2012_USH_dv3 g2012_USH_rv3
	# NONE IN: none
	# NONE IA: none
	# DONE KS: easy - match County to county and then vtd to District. g2012_USH_dv	g2012_USH_rv
	# NONE KY - investigate - KY is not in cid why?
	# NONE LA: no way to match
	# NONE ME: no way to match
	# NONE MD: no way to match
	# NONE MA: no way to match
	# NONE MI: none
	# DONE MN: match county to int(County) and then int(district) to precinct. g2010_USH_dv	g2010_USH_rv
	# DONE MS: match County to county and then Name to precinct. g2012_USH_dv	g2012_USH_rv	g2012_USH_dv2	g2012_USH_rv2
	# NONE MO: none
	# NONE MT: 1 district
	# NE: match county to County and Name to precinct g2012_USH_dv	g2012_USH_rv
	# NONE NV: none
	# NH: County to county and precinct to Name g2012_USH_rv	g2012_USH_dv
	# NONE NJ: none
	# NM: county to County and District to precinct. g2012_USH_rv	g2012_USH_tv
	# NONE NY: none
	# NC: County to county and District to vtd. g2010_USH_dv	g2010_USH_rv g2010_USH_dv2	g2010_USH_rv2
	# NONE ND: 1 district
	# NONE OH: none
	# OK: match County to county and then District to the last 3 (4?) digits of precinct, dv and dv2
	# NONE OR: I don't think these can be matched but check cid - for some reason most of OR dropped
	# PA: match County to county and District to precinct_code, but there is no house race here?
	# NONE RI: no RI rows in CID
	# SC: county to County and Name to precinct should fuzzy match well, dv and dv2
	# NONE SD: 1 district
	# TN: county to County and precinct to Name - not close on some matches but should fuzzy match fine. dv and dv2
	# TX: County to county and District to vtd, dv only
	# NONE UT: none
	# NONE VT: 1 district
	# VA: County to county and then fuzzy Name to precinct, dv only
	# WA: County to county and Name to precinct_name, dv only
	# NONE WV: none
	# NONE WI: none
	# NONE WY: 1 district

	# load up actual votes
	with open('/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/2010_actual_vote.csv','rU') as cfile:
		reader=csv.reader(cfile)
		actual2010=[row for row in reader]

	with open('/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/2012_actual_vote.csv','rU') as cfile:
		reader=csv.reader(cfile)
		actual2012=[row for row in reader]

# DOUBLE CHECK LIST - States you should double check
# MINNESOTA - this one might have mistakes based on the merge, which is non-standard
# MISSOURI - this is a standard merge but the name match between Name and precinct is very tricky, so I think this is just bad matching, probably incurable
	for state in [stated for stated in data_dict.keys() if stated=='TX']:
		print state
		for year in data_dict[state].keys():

			temp=data_dict[state][year]
			# all the merge rules go here

			if state=='MO' or state=='SC' or state=='TN':
				if int(year)==2010:

					if(state=='SC'):
						# fix all the county names that are misspelled
						temp.ix[temp.county=='Malrboro','county']='marlboro'
						temp.ix[temp.county=='Sumpter','county']='sumter'
						temp.ix[temp.county=='Chsterfield','county']='chesterfield'
						temp.ix[temp.county=='Dillion','county']='dillon'
						temp.ix[temp.county=='Bearfort','county']='beaufort'
						temp.ix[temp.county=='Oragneburg','county']='orangeburg'

					# print temp[temp['county']=='dillon']

					tempcid=cid[cid['state_x']==state]
					field='precinct'

					a=standard_state(field,temp,state,year,tempcid)

					cid2=pd.merge(tempcid,a,on=['Name','County'],how='outer')
					temp2=temp[['precinct','g2010_USH_dv','g2010_USH_rv']]
					cid2=pd.merge(cid2,temp2,on=['precinct'],how='outer')

					cid2['g2010_USH_rv']=cid2['g2010_USH_rv'].replace('', np.nan)
					cid2['g2010_USH_dv']=cid2['g2010_USH_dv'].replace('', np.nan)

					# calculate % r for each real district and each algo district
					rdists=list(set(cid2['real_district']))
					adists=list(set(cid2['HouseDistrict']))
					rdists=[r for r in rdists if type(r)==type('a')]
					if state=='SC':
						rdists=['02', '03', '01', '06', '04', '05']
					adists=[a for a in adists if type(a)==type('a')]

					# print rdists,adists

					for i,dist in enumerate(rdists):
						actual_vote=[row for row in actual2010 if states[state].lower()==row[0].lower() and int(row[1])==int(dist)][0]

						# print actual_vote
						cid3=cid2.drop_duplicates(['state_x','County','Name'])
						rdist_per=cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()/(cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()+cid3[cid3['real_district']==dist]['g2010_USH_dv'].astype(float).sum())
						cid2['rvote']=cid2['g2010_USH_rv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						cid2['dvote']=cid2['g2010_USH_dv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						adist_per=cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()/(cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()+cid2[cid2['HouseDistrict']==adists[i]]['dvote'].astype(float).sum())
						rrvotes=cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()
						rdvotes=cid3[cid3['real_district']==dist]['g2010_USH_dv'].astype(float).sum()
						print state,year,dist,rdist_per,adist_per,rrvotes,rdvotes,actual_vote[2],actual_vote[3],int(actual_vote[2])/(int(actual_vote[2])+int(actual_vote[3]))

			# if state=='NY':
			# 	if int(year)==2010:
			# 		tempcid=cid[cid['state_x']==state]
			# 		field='precinct'

			# 		if state=='NY':
			# 			temp['precinct']=temp['vtd08']
			# 			temp['county']=temp['county_name']
			# 		a=standard_state(field,temp,state,year,tempcid,field2='District')
			# 		return a

			# 		cid2=pd.merge(tempcid,a,on=['District','County'],how='outer')
			# 		temp2=temp[['precinct','g2010_USH_dv','g2010_USH_rv']]
			# 		cid2=pd.merge(cid2,temp2,on=['precinct'],how='outer')

			# 		cid2['g2010_USH_rv']=cid2['g2010_USH_rv'].replace('', np.nan)
			# 		cid2['g2010_USH_dv']=cid2['g2010_USH_dv'].replace('', np.nan)

			# 		# calculate % r for each real district and each algo district
			# 		rdists=list(set(cid2['real_district']))
			# 		adists=list(set(cid2['HouseDistrict']))
			# 		rdists=[r for r in rdists if type(r)==type('a')]
			# 		adists=[a for a in adists if type(a)==type('a')]

			# 		for i,dist in enumerate(rdists):
			# 			actual_vote=[row for row in actual2010 if states[state].lower()==row[0].lower() and int(row[1])==int(dist)][0]

			# 			cid3=cid2.drop_duplicates(['state_x','County','Name'])
			# 			rdist_per=cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()/(cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()+cid3[cid3['real_district']==dist]['g2010_USH_dv'].astype(float).sum())
			# 			cid2['rvote']=cid2['g2010_USH_rv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
			# 			cid2['dvote']=cid2['g2010_USH_dv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
			# 			adist_per=cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()/(cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()+cid2[cid2['HouseDistrict']==adists[i]]['dvote'].astype(float).sum())
			# 			rrvotes=cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()
			# 			rdvotes=cid3[cid3['real_district']==dist]['g2010_USH_dv'].astype(float).sum()
			# 			print state,year,dist,rdist_per,adist_per,rrvotes,rdvotes,actual_vote[2],actual_vote[3],int(actual_vote[2])/(int(actual_vote[2])+int(actual_vote[3]))

			if state=='OH':
				if int(year)==2010:
					tempcid=cid[cid['state_x']==state]
					field='precinct'
					temp['precinct']=temp['precinct_code']
					a=standard_state(field,temp,state,year,tempcid)

					cid2=pd.merge(tempcid,a,on=['Name','County'],how='outer')
					temp2=temp[['precinct','g2010_USH_dv','g2010_USH_rv']]
					cid2=pd.merge(cid2,temp2,on=['precinct'],how='outer')

					cid2['g2010_USH_rv']=cid2['g2010_USH_rv'].replace('', np.nan)
					cid2['g2010_USH_dv']=cid2['g2010_USH_dv'].replace('', np.nan)

					# calculate % r for each real district and each algo district
					rdists=list(set(cid2['real_district']))
					adists=list(set(cid2['HouseDistrict']))
					rdists=[r for r in rdists if type(r)==type('a')]
					adists=[a for a in adists if type(a)==type('a')]

					for i,dist in enumerate(rdists):
						actual_vote=[row for row in actual2010 if states[state].lower()==row[0].lower() and int(row[1])==int(dist)][0]

						cid3=cid2.drop_duplicates(['state_x','County','Name'])
						rdist_per=cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()/(cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()+cid3[cid3['real_district']==dist]['g2010_USH_dv'].astype(float).sum())
						cid2['rvote']=cid2['g2010_USH_rv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						cid2['dvote']=cid2['g2010_USH_dv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						adist_per=cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()/(cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()+cid2[cid2['HouseDistrict']==adists[i]]['dvote'].astype(float).sum())						
						rrvotes=cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()
						rdvotes=cid3[cid3['real_district']==dist]['g2010_USH_dv'].astype(float).sum()
						print state,year,dist,rdist_per,adist_per,rrvotes,rdvotes,actual_vote[2],actual_vote[3],int(actual_vote[2])/(int(actual_vote[2])+int(actual_vote[3]))


			if state=='NV':
				if int(year)==2010:
					tempcid=cid[cid['state_x']==state]
					field='precinct'
					temp['county']=temp['county_name']
					temp['precinct']=temp['precinct_code']
					a=standard_state(field,temp,state,year,tempcid)

					cid2=pd.merge(tempcid,a,on=['Name','County'],how='outer')
					temp2=temp[['precinct','g2010_USH_dv','g2010_USH_rv']]
					cid2=pd.merge(cid2,temp2,on=['precinct'],how='outer')

					cid2['g2010_USH_rv']=cid2['g2010_USH_rv'].replace('', np.nan)
					cid2['g2010_USH_dv']=cid2['g2010_USH_dv'].replace('', np.nan)

					# calculate % r for each real district and each algo district
					rdists=['02', '03', '01']
					adists=list(set(cid2['HouseDistrict']))
					rdists=[r for r in rdists if type(r)==type('a')]
					adists=[a for a in adists if type(a)==type('a')]

					for i,dist in enumerate(rdists):
						actual_vote=[row for row in actual2010 if states[state].lower()==row[0].lower() and int(row[1])==int(dist)][0]

						cid3=cid2.drop_duplicates(['state_x','County','Name'])
						rdist_per=cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()/(cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()+cid3[cid3['real_district']==dist]['g2010_USH_dv'].astype(float).sum())
						cid2['rvote']=cid2['g2010_USH_rv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						cid2['dvote']=cid2['g2010_USH_dv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						adist_per=cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()/(cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()+cid2[cid2['HouseDistrict']==adists[i]]['dvote'].astype(float).sum())						
						rrvotes=cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()
						rdvotes=cid3[cid3['real_district']==dist]['g2010_USH_dv'].astype(float).sum()
						print state,year,dist,rdist_per,adist_per,rrvotes,rdvotes,actual_vote[2],actual_vote[3],int(actual_vote[2])/(int(actual_vote[2])+int(actual_vote[3]))

			if state=='AL' or state=='AR' or state=='MS' or state=='NH':
				if int(year)==2010:
					tempcid=cid[cid['state_x']==state]
					field='precinct'
					if state=='NH':
						temp['precinct']=temp['town']
					a=standard_state(field,temp,state,year,tempcid)

					cid2=pd.merge(tempcid,a,on=['Name','County'],how='outer')
					temp2=temp[['precinct','g2010_USH_dv','g2010_USH_rv']]
					cid2=pd.merge(cid2,temp2,on=['precinct'],how='outer')

					cid2['g2010_USH_rv']=cid2['g2010_USH_rv'].replace('', np.nan)
					cid2['g2010_USH_dv']=cid2['g2010_USH_dv'].replace('', np.nan)

					# calculate % r for each real district and each algo district
					rdists=list(set(cid2['real_district']))
					adists=list(set(cid2['HouseDistrict']))
					rdists=[r for r in rdists if type(r)==type('a')]
					adists=[a for a in adists if type(a)==type('a')]

					# couple little notes here.
					# first, district 01 appears to have been mis-coded by the harvard people - it shows ~120k democratic votes and
					# ~20k republican votes, but these are in fact the totals for republican votes and constitution party votes
					# respectively, so this next bit hand fixes that problem. 2nd, it's important to keep in mind that if you want
					# to check real districts you need to dedupe first to get rid of the entries for algorithm districts that break
					# bounds - this should do it: cid3=cid2.drop_duplicates(['state_x','County','Name'])
					# that will work for everything.

					if state=='AL':
						cid2.ix[cid2.real_district=='01','g2010_USH_rv']=cid2[cid2['real_district']=='01']['g2010_USH_dv']
						cid2.ix[cid2.real_district=='01','g2010_USH_dv']=0

					for i,dist in enumerate(rdists):
						actual_vote=[row for row in actual2010 if states[state].lower()==row[0].lower() and int(row[1])==int(dist)][0]

						cid3=cid2.drop_duplicates(['state_x','County','Name'])
						rdist_per=cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()/(cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()+cid3[cid3['real_district']==dist]['g2010_USH_dv'].astype(float).sum())
						cid2['rvote']=cid2['g2010_USH_rv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						cid2['dvote']=cid2['g2010_USH_dv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						adist_per=cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()/(cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()+cid2[cid2['HouseDistrict']==adists[i]]['dvote'].astype(float).sum())						
						rrvotes=cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()
						rdvotes=cid3[cid3['real_district']==dist]['g2010_USH_dv'].astype(float).sum()
						print state,year,dist,rdist_per,adist_per,rrvotes,rdvotes,actual_vote[2],actual_vote[3],int(actual_vote[2])/(int(actual_vote[2])+int(actual_vote[3]))

				elif int(year)==2012:
					tempcid=cid[cid['state_x']==state]
					field='precinct'
					a=standard_state(field,temp,state,year,tempcid)

					cid2=pd.merge(tempcid,a,on=['Name','County'],how='outer')
					temp2=temp[['precinct','g2012_USH_dv','g2012_USH_rv']]
					cid2=pd.merge(cid2,temp2,on=['precinct'],how='outer')

					cid2['g2012_USH_rv']=cid2['g2012_USH_rv'].replace('', np.nan)
					cid2['g2012_USH_dv']=cid2['g2012_USH_dv'].replace('', np.nan)

					# calculate % r for each real district and each algo district
					rdists=list(set(cid2['real_district']))
					adists=list(set(cid2['HouseDistrict']))
					rdists=[r for r in rdists if type(r)==type('a')]
					adists=[a for a in adists if type(a)==type('a')]

					for i,dist in enumerate(rdists):
						actual_vote=[row for row in actual2012 if states[state].lower()==row[0].lower() and int(row[1])==int(dist)][0]

						cid3=cid2.drop_duplicates(['state_x','County','Name'])
						rdist_per=cid3[cid3['real_district']==dist]['g2012_USH_rv'].astype(float).sum()/(cid3[cid3['real_district']==dist]['g2012_USH_rv'].astype(float).sum()+cid3[cid3['real_district']==dist]['g2012_USH_dv'].astype(float).sum())
						cid2['rvote']=cid2['g2012_USH_rv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						cid2['dvote']=cid2['g2012_USH_dv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						adist_per=cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()/(cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()+cid2[cid2['HouseDistrict']==adists[i]]['dvote'].astype(float).sum())						
						rrvotes=cid3[cid3['real_district']==dist]['g2012_USH_rv'].astype(float).sum()
						rdvotes=cid3[cid3['real_district']==dist]['g2012_USH_dv'].astype(float).sum()
						print state,year,dist,rdist_per,adist_per,rrvotes,rdvotes,actual_vote[2],actual_vote[3],int(actual_vote[2])/(int(actual_vote[2])+int(actual_vote[3]))

			if state=='AZ':
				if int(year)==2012:
					tempcid=cid[cid['state_x']==state]
					field='precinct'
					a=standard_state(field,temp,state,year,tempcid)
					# return a

					cid2=pd.merge(tempcid,a,on=['Name','County'],how='outer')
					temp2=temp[['precinct','g2012_USH_dv','g2012_USH_rv']]
					cid2=pd.merge(cid2,temp2,on=['precinct'],how='outer')

					cid2['g2012_USH_rv']=cid2['g2012_USH_rv'].replace('', np.nan)
					cid2['g2012_USH_dv']=cid2['g2012_USH_dv'].replace('', np.nan)

					# calculate % r for each real district and each algo district
					rdists=list(set(cid2['real_district']))
					adists=list(set(cid2['HouseDistrict']))
					rdists=[r for r in rdists if type(r)==type('a')]
					adists=[a for a in adists if type(a)==type('a')]

					for i,dist in enumerate(rdists):
						actual_vote=[row for row in actual2012 if states[state].lower()==row[0].lower() and int(row[1])==int(dist)][0]

						cid3=cid2.drop_duplicates(['state_x','County','Name'])
						rdist_per=cid3[cid3['real_district']==dist]['g2012_USH_rv'].astype(float).sum()/(cid3[cid3['real_district']==dist]['g2012_USH_rv'].astype(float).sum()+cid3[cid3['real_district']==dist]['g2012_USH_dv'].astype(float).sum())
						cid2['rvote']=cid2['g2012_USH_rv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						cid2['dvote']=cid2['g2012_USH_dv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						adist_per=cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()/(cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()+cid2[cid2['HouseDistrict']==adists[i]]['dvote'].astype(float).sum())						
						rrvotes=cid3[cid3['real_district']==dist]['g2012_USH_rv'].astype(float).sum()
						rdvotes=cid3[cid3['real_district']==dist]['g2012_USH_dv'].astype(float).sum()
						print state,year,dist,rdist_per,adist_per,rrvotes,rdvotes,actual_vote[2],actual_vote[3],int(actual_vote[2])/(int(actual_vote[2])+int(actual_vote[3]))

	# PA: match CountyFP to fips and then District to vtd (make sure you int em) just dv
			# if state=='PA':
			# 	if int(year)==2010:
			# 		tempcid=cid[cid['state_x']==state].copy()
			# 		tempcid['County']=tempcid['CountyFP'].astype(int)
			# 		field='vtd'
			# 		a=standard_statePA(field,temp,state,year,tempcid)
			# 		# return a

			# 		cid2=pd.merge(tempcid,a,on=['County','District'],how='outer')
			# 		temp2=temp[['vtd','g2012_USH_dv','g2012_USH_rv']]
			# 		cid2=pd.merge(cid2,temp2,on=['vtd'],how='outer')

			# 		cid2['g2012_USH_rv']=cid2['g2012_USH_rv'].replace('', np.nan)
			# 		cid2['g2012_USH_dv']=cid2['g2012_USH_dv'].replace('', np.nan)

			# 		# calculate % r for each real district and each algo district
			# 		rdists=list(set(cid2['real_district']))
			# 		adists=list(set(cid2['HouseDistrict']))
			# 		rdists=[r for r in rdists if type(r)==type('a')]
			# 		adists=[a for a in adists if type(a)==type('a')]

			# 		for i,dist in enumerate(rdists):
			# 			actual_vote=[row for row in actual2012 if states[state].lower()==row[0].lower() and int(row[1])==int(dist)][0]

			# 			cid3=cid2.drop_duplicates(['state_x','County','Name'])
			# 			rdist_per=cid3[cid3['real_district']==dist]['g2012_USH_rv'].astype(float).sum()/(cid3[cid3['real_district']==dist]['g2012_USH_rv'].astype(float).sum()+cid3[cid3['real_district']==dist]['g2012_USH_dv'].astype(float).sum())
			# 			cid2['rvote']=cid2['g2012_USH_rv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
			# 			cid2['dvote']=cid2['g2012_USH_dv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
			# 			adist_per=cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()/(cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()+cid2[cid2['HouseDistrict']==adists[i]]['dvote'].astype(float).sum())						
			# 			rrvotes=cid3[cid3['real_district']==dist]['g2012_USH_rv'].astype(float).sum()
			# 			rdvotes=cid3[cid3['real_district']==dist]['g2012_USH_dv'].astype(float).sum()
			# 			print state,year,dist,rdist_per,adist_per,rrvotes,rdvotes,actual_vote[2],actual_vote[3],int(actual_vote[2])/(int(actual_vote[2])+int(actual_vote[3]))


			if state=='CO' or state=='NC':
				if int(year)==2010:
					tempcid=cid[cid['state_x']==state]
					temp2=temp[['vtd','g2010_USH_dv','g2010_USH_rv']]
					temp2['District']=temp2['vtd']
					del temp2['vtd']
					cid2=pd.merge(tempcid,temp2,on=['District'],how='outer')

					cid2['g2010_USH_rv']=cid2['g2010_USH_rv'].replace('', np.nan)
					cid2['g2010_USH_dv']=cid2['g2010_USH_dv'].replace('', np.nan)

					# calculate % r for each real district and each algo district
					rdists=list(set(cid2['real_district']))
					adists=list(set(cid2['HouseDistrict']))
					rdists=[r for r in rdists if type(r)==type('a')]
					adists=[a for a in adists if type(a)==type('a')]

					for i,dist in enumerate(rdists):
						actual_vote=[row for row in actual2010 if states[state].lower()==row[0].lower() and int(row[1])==int(dist)][0]
						cid3=cid2.drop_duplicates(['state_x','County','Name'])
						rdist_per=cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()/(cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()+cid3[cid3['real_district']==dist]['g2010_USH_dv'].astype(float).sum())
						cid2['rvote']=cid2['g2010_USH_rv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						cid2['dvote']=cid2['g2010_USH_dv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						adist_per=cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()/(cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()+cid2[cid2['HouseDistrict']==adists[i]]['dvote'].astype(float).sum())						
						rrvotes=cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()
						rdvotes=cid3[cid3['real_district']==dist]['g2010_USH_dv'].astype(float).sum()
						print state,year,dist,rdist_per,adist_per,rrvotes,rdvotes,actual_vote[2],actual_vote[3],int(actual_vote[2])/(int(actual_vote[2])+int(actual_vote[3]))

			if state=='KS' or state=='TX':
				if int(year)==2010:
					tempcid=cid[cid['state_x']==state]
					field='vtd'
					a=standard_state(field,temp,state,year,tempcid,field2='District')

					cid2=pd.merge(tempcid,a,on=['District','County'],how='outer')
					temp2=temp[['vtd','g2010_USH_dv','g2010_USH_rv']]
					cid2=pd.merge(cid2,temp2,on=['vtd'],how='outer')

					cid2['g2010_USH_rv']=cid2['g2010_USH_rv'].replace('', np.nan)
					cid2['g2010_USH_dv']=cid2['g2010_USH_dv'].replace('', np.nan)

					# calculate % r for each real district and each algo district
					rdists=list(set(cid2['real_district']))
					adists=list(set(cid2['HouseDistrict']))
					rdists=[r for r in rdists if type(r)==type('a')]
					adists=[a for a in adists if type(a)==type('a')]

					# cid2.ix[cid2.real_district=='01','g2010_USH_rv']=cid2[cid2['real_district']=='01']['g2010_USH_dv']
					# cid2.ix[cid2.real_district=='01','g2010_USH_dv']=0

					for i,dist in enumerate(rdists):
						actual_vote=[row for row in actual2010 if states[state].lower()==row[0].lower() and int(row[1])==int(dist)][0]
						cid3=cid2.drop_duplicates(['state_x','County','District'])
						rdist_per=cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()/(cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()+cid3[cid3['real_district']==dist]['g2010_USH_dv'].astype(float).sum())
						cid2['rvote']=cid2['g2010_USH_rv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						cid2['dvote']=cid2['g2010_USH_dv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						adist_per=cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()/(cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()+cid2[cid2['HouseDistrict']==adists[i]]['dvote'].astype(float).sum())						
						rrvotes=cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()
						rdvotes=cid3[cid3['real_district']==dist]['g2010_USH_dv'].astype(float).sum()
						print state,year,dist,rdist_per,adist_per,rrvotes,rdvotes,actual_vote[2],actual_vote[3],int(actual_vote[2])/(int(actual_vote[2])+int(actual_vote[3]))

				elif int(year)==2012 and state!='TX':
					tempcid=cid[cid['state_x']==state]
					field='vtd'
					a=standard_state(field,temp,state,year,tempcid,field2='District')

					cid2=pd.merge(tempcid,a,on=['District','County'],how='outer')
					temp2=temp[['vtd','g2012_USH_dv','g2012_USH_rv']]
					cid2=pd.merge(cid2,temp2,on=['vtd'],how='outer')

					cid2['g2012_USH_rv']=cid2['g2012_USH_rv'].replace('', np.nan)
					cid2['g2012_USH_dv']=cid2['g2012_USH_dv'].replace('', np.nan)

					# calculate % r for each real district and each algo district
					rdists=list(set(cid2['real_district']))
					adists=list(set(cid2['HouseDistrict']))
					rdists=[r for r in rdists if type(r)==type('a')]
					adists=[a for a in adists if type(a)==type('a')]

					# cid2.ix[cid2.real_district=='01','g2012_USH_rv']=cid2[cid2['real_district']=='01']['g2012_USH_dv']
					# cid2.ix[cid2.real_district=='01','g2012_USH_dv']=0

					for i,dist in enumerate(rdists):
						actual_vote=[row for row in actual2012 if states[state].lower()==row[0].lower() and int(row[1])==int(dist)][0]

						cid3=cid2.drop_duplicates(['state_x','County','District'])
						rdist_per=cid3[cid3['real_district']==dist]['g2012_USH_rv'].astype(float).sum()/(cid3[cid3['real_district']==dist]['g2012_USH_rv'].astype(float).sum()+cid3[cid3['real_district']==dist]['g2012_USH_dv'].astype(float).sum())
						cid2['rvote']=cid2['g2012_USH_rv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						cid2['dvote']=cid2['g2012_USH_dv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						adist_per=cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()/(cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()+cid2[cid2['HouseDistrict']==adists[i]]['dvote'].astype(float).sum())						
						rrvotes=cid3[cid3['real_district']==dist]['g2012_USH_rv'].astype(float).sum()
						rdvotes=cid3[cid3['real_district']==dist]['g2012_USH_dv'].astype(float).sum()
						try:
							print state,year,dist,rdist_per,adist_per,rrvotes,rdvotes,actual_vote[2],actual_vote[3],int(actual_vote[2])/(int(actual_vote[2])+int(actual_vote[3]))
						except:
							print dist

			if state=='MN':
				if int(year)==2010:
					# MN: match county to int(County) and then Name to precinct. g2010_USH_dv	g2010_USH_rv
					tempcid=cid[cid['state_x']==state]
					temp['county'] = temp['county'].map('{0:0>3}'.format)
					field='precinct'
					a=standard_stateMN(field,temp,state,year,tempcid,field2='Name')

					cid2=pd.merge(tempcid,a,on=['Name','CountyFP'],how='outer')
					temp2=temp[['precinct','g2010_USH_dv','g2010_USH_rv']]
					cid2=pd.merge(cid2,temp2,on=['precinct'],how='outer')

					cid2['g2010_USH_rv']=cid2['g2010_USH_rv'].replace('', np.nan)
					cid2['g2010_USH_dv']=cid2['g2010_USH_dv'].replace('', np.nan)

					# calculate % r for each real district and each algo district
					rdists=list(set(cid2['real_district']))
					adists=list(set(cid2['HouseDistrict']))
					rdists=[r for r in rdists if type(r)==type('a')]
					adists=[a for a in adists if type(a)==type('a')]

					# cid2.ix[cid2.real_district=='01','g2010_USH_rv']=cid2[cid2['real_district']=='01']['g2010_USH_dv']
					# cid2.ix[cid2.real_district=='01','g2010_USH_dv']=0

					for i,dist in enumerate(rdists):
						actual_vote=[row for row in actual2010 if states[state].lower()==row[0].lower() and int(row[1])==int(dist)][0]

						cid3=cid2.drop_duplicates(['state_x','County','Name'])
						rdist_per=cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()/(cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()+cid3[cid3['real_district']==dist]['g2010_USH_dv'].astype(float).sum())
						cid2['rvote']=cid2['g2010_USH_rv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						cid2['dvote']=cid2['g2010_USH_dv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						adist_per=cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()/(cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()+cid2[cid2['HouseDistrict']==adists[i]]['dvote'].astype(float).sum())						
						rrvotes=cid3[cid3['real_district']==dist]['g2010_USH_rv'].astype(float).sum()
						rdvotes=cid3[cid3['real_district']==dist]['g2010_USH_dv'].astype(float).sum()
						print state,year,dist,rdist_per,adist_per,rrvotes,rdvotes,actual_vote[2],actual_vote[3],int(actual_vote[2])/(int(actual_vote[2])+int(actual_vote[3]))

				elif int(year)==2012:
					# MN: match county to int(County) and then Name to precinct. g2010_USH_dv	g2010_USH_rv
					tempcid=cid[cid['state_x']==state]
					temp['county'] = temp['county'].map('{0:0>3}'.format)
					field='precinct'
					a=standard_stateMN(field,temp,state,year,tempcid,field2='District')

					cid2=pd.merge(tempcid,a,on=['District','CountyFP'],how='outer')
					temp2=temp[['precinct','g2012_USH_dv','g2012_USH_rv']]
					cid2=pd.merge(cid2,temp2,on=['precinct'],how='outer')

					cid2['g2012_USH_rv']=cid2['g2012_USH_rv'].replace('', np.nan)
					cid2['g2012_USH_dv']=cid2['g2012_USH_dv'].replace('', np.nan)

					# calculate % r for each real district and each algo district
					rdists=list(set(cid2['real_district']))
					adists=list(set(cid2['HouseDistrict']))
					rdists=[r for r in rdists if type(r)==type('a')]
					adists=[a for a in adists if type(a)==type('a')]

					# cid2.ix[cid2.real_district=='01','g2012_USH_rv']=cid2[cid2['real_district']=='01']['g2012_USH_dv']
					# cid2.ix[cid2.real_district=='01','g2012_USH_dv']=0

					for i,dist in enumerate(rdists):
						actual_vote=[row for row in actual2010 if states[state].lower()==row[0].lower() and int(row[1])==int(dist)][0]

						cid3=cid2.drop_duplicates(['state_x','County','Name'])
						rdist_per=cid3[cid3['real_district']==dist]['g2012_USH_rv'].astype(float).sum()/(cid3[cid3['real_district']==dist]['g2012_USH_rv'].astype(float).sum()+cid3[cid3['real_district']==dist]['g2012_USH_dv'].astype(float).sum())
						cid2['rvote']=cid2['g2012_USH_rv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						cid2['dvote']=cid2['g2012_USH_dv'].astype(float)*cid2['percent_district_in_AlgoHouseDist']
						adist_per=cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()/(cid2[cid2['HouseDistrict']==adists[i]]['rvote'].astype(float).sum()+cid2[cid2['HouseDistrict']==adists[i]]['dvote'].astype(float).sum())						
						rrvotes=cid3[cid3['real_district']==dist]['g2012_USH_rv'].astype(float).sum()
						rdvotes=cid3[cid3['real_district']==dist]['g2012_USH_dv'].astype(float).sum()
						print state,year,dist,rdist_per,adist_per,rrvotes,rdvotes,actual_vote[2],actual_vote[3],int(actual_vote[2])/(int(actual_vote[2])+int(actual_vote[3]))



def standard_state(field1,temp,state,year,tempcid,field2='Name'):
	temp['county']=temp.apply(lambda x: x['county'].lower(),axis=1)
	temp['county']=temp.apply(lambda x: x['county'].replace('county','').strip(),axis=1)
	temp['county']=temp.apply(lambda x: x['county'].replace(' ',''),axis=1)
	temp['county']=temp.apply(lambda x: x['county'].replace('.',''),axis=1)
	temp[field1] = temp.apply(lambda x: x[field1].lower(),axis=1)

	counties=list(set(tempcid[tempcid['state_x']==state]['County']))
	counties=[county.replace('county','').strip().lower() for county in counties]
	master_list=[]
	for county in [county for county in counties if county!='ouachita' and county!='union' and county!='columbia']:
		# print state,county
		# print county
		# print temp.iloc[1450:1470]
		compare2=list(set(temp[temp['county']==county][field1]))
		compare1=list(set(tempcid[tempcid['County']==county][field2]))

		# print county
		# print 'C1',compare1
		# print 'C2',compare2
		matches=unique_matcher(compare1,compare2)
		for key in matches.keys():
			master_list.append([county,key,matches[key]])

	a=pd.DataFrame(master_list,columns=['County',field2,field1])
	return a

def standard_stateMN(field1,temp,state,year,tempcid,field2='Name'):
	temp['county']=temp.apply(lambda x: x['county'].lower(),axis=1)
	temp['county']=temp.apply(lambda x: x['county'].replace('county','').strip(),axis=1)
	temp['county']=temp.apply(lambda x: x['county'].replace(' ',''),axis=1)
	temp['county']=temp.apply(lambda x: x['county'].replace('.',''),axis=1)
	temp[field1]=temp.apply(lambda x: x[field1].lower(),axis=1)

	counties=list(set(tempcid[tempcid['state_x']==state]['CountyFP']))
	counties=[county.replace('county','').strip().lower() for county in counties]
	master_list=[]
	for county in [county for county in counties]:
		# print state,county
		# print county
		# print temp.iloc[1450:1470]
		compare2=list(set(temp[temp['county']==county][field1]))
		compare1=list(set(tempcid[tempcid['CountyFP']==county][field2]))

		# print 'C1',compare1
		# print 'C2',compare2
		if(len(compare2)>0):
			matches=unique_matcher(compare1,compare2)
			for key in matches.keys():
				master_list.append([county,key,matches[key]])

	a=pd.DataFrame(master_list,columns=['CountyFP',field2,field1])
	return a

def standard_statePA(field1,temp,state,year,tempcid,field2='District'):
	temp['county']=temp['county'].astype(int)
	print len(tempcid)
	counties=list(set(tempcid['CountyFP'].astype(int)))
	# counties=[county.replace('county','').strip().lower() for county in counties]
	master_list=[]
	for county in [county for county in counties]:
		print county
		print list(set(temp['county']))
		# print state,county
		# print county
		# print temp.iloc[1450:1470]
		compare2=list(set(temp[temp['county']==county][field1]))
		compare1=list(set(tempcid[tempcid['CountyFP']==county][field2]))

		# print 'C1',compare1
		# print 'C2',compare2
		if(len(compare2)>0):
			matches=unique_matcher(compare1,compare2)
			for key in matches.keys():
				master_list.append([county,key,matches[key]])

	a=pd.DataFrame(master_list,columns=['CountyFP',field2,field1])
	return a

def load_sample(state,year,field):
	dfile='/Users/austinc/Desktop/Current Work/Redistricting-Algorithms/Raw Data/precinct_votes/'+state+'_'+year+'.tab'

	with open(dfile,'rU') as csvfile:
		reader=csv.reader(csvfile,delimiter='\t')
		temp=[row for row in reader]

	temp=pd.DataFrame(temp[1:],columns=temp[0])
	return temp
	temp['county']=temp.apply(lambda x: x['county'].lower(),axis=1)
	temp['county']=temp.apply(lambda x: x['county'].replace('county','').strip(),axis=1)
	temp['county']=temp.apply(lambda x: x['county'].replace(' ',''),axis=1)
	temp['county']=temp.apply(lambda x: x['county'].replace('.',''),axis=1)
	temp[field]=temp.apply(lambda x: x[field].lower(),axis=1)
	return temp

def fuzzy_matcher(string,comparison):
	# function for fuzzy matching. Cutoff is an important variable - make it bigger and it will prevent false matches
	# but may miss true matches, and if there is a match for every string it won't matter so I err on the side of making
	# it smaller.
	try:
		res=difflib.get_close_matches(string.lower(),[c.lower() for c in comparison],cutoff=.4)[0]
	except:
		res=''
	return res

def unique_matcher(cidlist,harvardlist):
	# print cidlist,harvardlist
	# takes lists of precinct names and matches them uniquely ie one harvard name to one cidname
	dict={}
	harvardlist=[prec for prec in harvardlist if 'provisional' not in prec and 'absentee' not in prec]

	# the stupid way to do this is just to go through all possible matches, find the highest in that iteration, take that match,
	# then repeat with the two lists minus those matched items. So yeah. That's how I'm going to do it.

	# ooooook never mind that way is way too slow with certain counties. Instead:
	# 1) build a len(cidlist) x len(harvardlist) matrix where each entry is the difflib score
	# 2) find the max value in the matrix, and use that i,j to identify the harvard/cid match
	# 3) find the next highest value, check to make sure i,j haven't already been used...
	# 4) etc.
	# as it turns out, just zero out i and j

	nplist=[]
	for prec in cidlist:
		rowlist=[]
		for prec2 in harvardlist:
			score = difflib.SequenceMatcher(None,prec,prec2).ratio()
			rowlist.append(score)
		nplist.append(rowlist)

	score_array=np.array(nplist)

	for i in range(0,len(cidlist)):
		maxi=score_array.max()
		loc=np.argmax(score_array)
		row=int(math.floor(loc/len(harvardlist)))
		col=int(loc-(row*len(harvardlist)))

		if maxi>.2:
			# print row,col,cidlist[row],harvardlist[col],maxi
			dict[cidlist[row]]=harvardlist[col]
			score_array[row]=0
			score_array[:,col]=0

	return dict

def countymatch(county):
	harvardlist=list(set(temp[(temp['county']==county)]['precinct']))
	cidlist=list(set(cid[(cid['state_x']=='AL') & (cid['County']==county)]['Name']))
	pp.pprint(unique_matcher(cidlist,harvardlist))




























