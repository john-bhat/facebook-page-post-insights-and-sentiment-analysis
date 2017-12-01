# -*- coding: utf-8 -*-
#==============================================================================================================================#
#                   Script to clean the text in status message to get a well formatted CSV for hive                              #
# 			storage( removing commans and new line characters)                                                     #
#==============================================================================================================================#

import csv
import pandas as pd
import re
from subprocess import call



#function to copy files to hdfs from local
def movePostsHdfs():
  	  comm = 'hdfs dfs -copyFromLocal -f /home/daasuser/facebook/posts_comments/MTNLoaded_facebook_statuses.csv /user/daasuser/facebook/MTNLoaded_facebook_statuses/'
   	  call(comm,shell = True)

def moveCommentsHdfs():	 
	  comm = 'hdfs dfs -copyFromLocal -f /home/daasuser/facebook/posts_comments/MTNLoaded_facebook_comments.csv /user/daasuser/facebook/MTNLoaded_facebook_comments/'
   	  call(comm,shell = True)
status=pd.read_csv('/home/daasuser/facebook/posts_comments/MTNLoaded_facebook_statuses.csv')
regex=re.compile('[,\n]')
for i in range(0,len(status)):
               status.loc[i,'status_message']=regex.sub(' ',status.loc[i,'status_message'])
	       status.loc[i,'status_message']=re.sub('\n+',' ',status.loc[i,'status_message'])
              
status.to_csv('/home/daasuser/facebook/posts_comments/MTNLoaded_facebook_statuses.csv',index=None)
movePostsHdfs()
moveCommentsHdfs()

