###################################################################################
##      append data into merge csv  which do not have sub categories
###################################################################################


import os
import csv
import datetime
from datetime import datetime
import pytz
import pandas as pd
import shutil
import ConfigParser
import re
import glob
from dateutil.tz import *
# This contains the local timezone 
local = tzlocal()

import warnings
warnings.filterwarnings("ignore")

from subprocess import call
data_list=[]
data_list.append('date')


try:
    ROOT = os.path.dirname(os.path.abspath(__file__))
except NameError:  # We are the main py2exe script, not a module
    import sys
    ROOT = os.path.dirname(os.path.abspath(sys.argv[0]))
    
path = lambda x: os.path.join(
    os.path.dirname(os.path.realpath(__file__)), x)   

settings = ConfigParser.ConfigParser()
settings.read(path('insights.conf'))

filepath=settings.get('facebook', 'output_destination')

import datetime
diff = datetime.timedelta(days=1)
ext=datetime.datetime.strftime((datetime.datetime.today()-diff),'%m%d%Y')
#function to move files to hdfs from local
def movehdfs(filename):
   for i in filename:
	  
	  regex = re.compile('[^a-zA-Z_]')
	  ii=regex.sub('', i.split('.')[0])
  	  comm = 'hdfs dfs -moveFromLocal -f '+'/home/daasuser/facebook/'+i+ ' /user/daasuser/facebook/'+ ii+'/'
   	  call(comm,shell = True)

#=====================================================================================================#
#         make the time of page_fans_online according to nigeria timezone                             #
#=====================================================================================================#
dat=pd.read_csv(filepath+'insights'+'/page_fans_online.csv')
dat=dat.dropna()
for i in range(0,len(dat)) :           
            time=str(dat['date'][i])+' '+ str(dat['period'][i])
            
            
            timeDate=datetime.datetime.strptime(time, '%Y-%m-%d %H')
            timeDate = timeDate.replace(tzinfo = local)
            tz=pytz.timezone('Africa/Lagos')
            time_out = timeDate.astimezone(tz)
            
            #print(time_out)
            dat['period'][i]=time_out.strftime('%H')

dat.to_csv(filepath+'insights'+'/page_fans_online.csv',index=False)
###################################################################################
#to get headers for main csv file
for f in os.listdir(filepath+'insights/'):
    #print(f)
    dat=pd.read_csv(filepath+'insights/'+ f)
    if len(dat)<=3:
	if filepath+'insights/'+ f =='page_views_total.csv':
		data_list.append('page_views') 
        else:
		data_list.append(f.split(".")[0]) 
    else:
        dat.dropna()
	from_path=filepath+'insights/'+f.split(".")[0]+ext+'.csv'
	
	to_path=filepath+f.split(".")[0]+ext+'.csv'
        #dat.to_csv(filepath+'insights/'+ f,index=None)
	dat.to_csv(filepath+'insights/'+f.split(".")[0]+ext+'.csv',index=None)
        #shutil.move(filepath+'insights/'+f,filepath+f)
	shutil.move(from_path,to_path)	
csv_name='page_insights'+ext+'.csv'
main_csv=open(os.path.join(filepath,csv_name),'wb')
mai=csv.writer(main_csv)
mai.writerow(data_list)
main_csv.close()

###################################################################################

#append data into merge csv  which do not have sub categories

diff = datetime.timedelta(days=1)
date=datetime.datetime.strftime((datetime.datetime.today()-diff),'%m/%d/%Y')
#csv_name='page_insights'+date+'.csv'
#main_file=pd.read_csv('C:/Users/admin/Desktop/fbinsights/page_insights.csv')
main_file=pd.read_csv(os.path.join(filepath,csv_name))
ss=main_file[main_file['date'].str.contains(date)]
if len(ss)==0 :
    main_file.loc[0,'date'] =date

for i in range(1,len(data_list)):
    dat=pd.read_csv(filepath+'insights/'+data_list[i]+'.csv')
    print(data_list[i])
    main_file.loc[0,data_list[i]] =dat['metric_values'][0]              
main_file.to_csv(filepath+csv_name,index=False)


######################################################################################
#get all the csv's at the directory and move them to hdfs by calling movehdfs function
extension = 'csv'
os.chdir('/home/daasuser/facebook')
csv_files = [i for i in glob.glob('*.{}'.format(extension))]
#print(csv_files)
movehdfs(csv_files)
print("successfully loaded data into tables")

