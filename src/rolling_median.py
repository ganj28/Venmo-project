#############################################################################
#### Program name - rolling_median.py
#### Written by - Anusha Gopalakrishnan
#### Date - July 11, 2016x
#### This program calculates the median degree of a vertex in a
#### graph and updates  each time a new Venmo payment appears.
#### The median is calculated the med#### across a 60-second sliding window.
##############################################################################


import datetime
import numpy
from collections import Counter    
from datetime import datetime 
global mygraph
global maxtime


def RollingMedian():
	'''  This function calculates the median and outputs it to a file '''
	global mygraph
	final=[]
	for entry in mygraph:                                                                                            
        	final.append(entry['actor'])                          
        	final.append(entry['target'])
	inter=Counter(final)
        degrees = inter.values()
	med = numpy.median(degrees)
	output.write(str.format("{0:.2f}", med)+ '\n')


def PopulateGraph(mydata):
	''' This function constructs the graph from the given input '''
	global mygraph
	mygraph.append(mydata)
	

def RemoveEntry(indiceslist):
	''' This function removes the nodes from the graph that are older then 60 seconds from the current entry '''
	global mygraph
	indiceslist.sort(reverse=True)
	for index in indiceslist:
		mygraph.remove(mygraph[index])

def TimeDiff(time1, time2):
	''' This function calculates the time difference between 2 transactions' timestamps '''
	time1 = datetime.strptime(time1, '%Y-%m-%dT%H:%M:%SZ')
        time2 = datetime.strptime(time2, '%Y-%m-%dT%H:%M:%SZ')
	tdiff = time2-time1
	tdiff = tdiff.total_seconds()
	return tdiff


def CheckMultipleConnections(mydata):
	''' This function checks for multiple connections between same users within the 60 seconds time window'''
	global mygraph
	indlist=[]
	for eachline in mygraph:
        	if mydata['target']==eachline['target'] and mydata['actor']==eachline['actor'] and mydata['created_time']!=eachline['created_time']:
			delta=TimeDiff(mydata['created_time'],eachline['created_time'])
			if delta < 0:
				indlist.append(mygraph.index(eachline))
			else:
				indlist.append(mygraph.index(mydata))
			RemoveEntry(indlist)

def ProcessTime(mydata):
	''' This function calculates the position of the current entry in the 60 second window and returns entries to be removed if older than 60 sec ''' 
	global mygraph
	global maxtime
	del_indices_list = []
	tcurr = mydata['created_time']
	tdiff=TimeDiff(maxtime, tcurr)
	if tdiff < -60:
		time_flag=0
	elif tdiff <= 0 and tdiff >= -60:
		time_flag=1
	else:
		time_flag=2
		maxtime = mydata['created_time']
		for eachline in mygraph:
			tline = eachline['created_time']
			tline_diff=TimeDiff(maxtime, tline)
			if tline_diff < -60:
				del_indices_list.append(mygraph.index(eachline))
	return time_flag, del_indices_list
	

''' Main logic to Read Input file, manipulate graph and write to ouput '''

first_line = 1
output = open('./venmo_output/output.txt','w')
with open('./venmo_input/venmo-trans.txt','r') as f:
	 for myline in f:
		mydata=eval(myline)
         	if first_line:
			mygraph=[]
			PopulateGraph(mydata)
			maxtime = mydata['created_time']
			first_line=0
		else:
			[time_flag, del_indices_list]=ProcessTime(mydata)
			if time_flag==1:
				PopulateGraph(mydata)
			elif time_flag==2:
				RemoveEntry(del_indices_list)
				PopulateGraph(mydata)
			CheckMultipleConnections(mydata)
		RollingMedian()
output.close()
