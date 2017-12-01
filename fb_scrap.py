# -*- coding: utf-8 -*-
# Facebook Insights (OI:OPEN INSIGHTS)
# Data Science Team
# John Mohd Bhat



from datetime import datetime,timedelta
import os
import csv
import datetime
import shutil
import os.path
import itertools
import datetime
import ConfigParser
import csv, codecs
import re
import logging
import json
import urllib2
import urllib
import urlparse
import BaseHTTPServer
import webbrowser
import pandas as pd
from pandas.io.common import EmptyDataError
import sys
reload(sys)
sys.setdefaultencoding('utf8')
#from __future__ import unicode_literals
# commented metrics was not required in the project. Uncomment if you need any of below metric
insights_groups = {
    "page-impressions": [
                        'page_impressions',
                        'page_impressions_unique',
                        #'page_impressions_paid',
                        #'page_impressions_paid_unique',
                        #'page_impressions_organic',
                        #'page_impressions_organic_unique',
                        #'page_impressions_viral',
                        #'page_impressions_viral_unique',
                        #'page_impressions_by_story_type',
                        #'page_impressions_by_story_type_unique',
                        'page_impressions_by_city_unique',
                        #'page_impressions_by_country_unique',
                        'page_impressions_by_age_gender_unique',
                        #'page_impressions_frequency_distribution',
                        #'page_impressions_viral_frequency_distribution',
                        #'page_impressions_by_paid_non_paid',
                        #'page_impressions_by_paid_non_paid_unique',
    ],
    "page-engagement": [
                        'page_engaged_users',
                        'page_consumptions',
                        #'page_consumptions_unique',
                        #'page_consumptions_by_consumption_type',
                        #'page_consumptions_by_consumption_type_unique',
                        #'page_places_checkin_total',
                        #'page_places_checkin_total_unique',
                        #'page_places_checkin_mobile',
                        #'page_places_checkin_mobile_unique',
                        #'page_places_checkins_by_age_gender',
                        #'page_places_checkins_by_locale',
                        #'page_places_checkins_by_country',
                        'page_negative_feedback',
                        #'page_negative_feedback_unique',
                        'page_negative_feedback_by_type',
                       # 'page_negative_feedback_by_type_unique',
                        #'page_positive_feedback_by_type',
                        #'page_positive_feedback_by_type_unique',
                        'page_fans_online',
                        'page_fans_online_per_day',
    ],
    "page-user-demographics": [
                        'page_fans',
                        #'page_fans_locale',
                        'page_fans_city',
                        #'page_fans_country',
                        'page_fans_gender_age',
                        'page_fan_adds',
                       # 'page_fan_adds_unique',
                        #'page_fans_by_like_source',
                        #'page_fans_by_like_source_unique',
                        'page_fan_removes',
                        #'page_fan_removes_unique',
                        #'page_fans_by_unlike_source_unique',
    ],
    "page-views": [
                        'page_views_total',
                        #'page_views_unique',
                       # 'page_views_login',
                        #'page_views_login_unique',
                        #'page_views_logout',
                        'page_views_external_referrals',
    ],
    "page-posts": [
                        'page_posts_impressions',
                        'page_posts_impressions_unique',
                        #'page_posts_impressions_paid',
                        #'page_posts_impressions_paid_unique',
                        'page_posts_impressions_organic',
                        #'page_posts_impressions_organic_unique',
                        #'page_posts_impressions_viral',
                        #'page_posts_impressions_viral_unique',
                        #'page_posts_impressions_frequency_distribution',
                        #'page_posts_impressions_by_paid_non_paid',
                        #'page_posts_impressions_by_paid_non_paid_unique',
    ],
   

    "page-video-views": [
                        'page_video_views',
                        #Total number of times page's videos have been viewed for more than 3 seconds
                        'page_video_views_unique',
                        #Total number of times page's videos have been played for unique people for more than 3 seconds.
                        'page_video_complete_views_30s',
                        #Total number of times page's videos have been viewed for more than 30 seconds.
                       
    ],
}



try:
    ROOT = os.path.dirname(os.path.abspath(__file__))
except NameError:  # We are the main py2exe script, not a module
    import sys
    ROOT = os.path.dirname(os.path.abspath(sys.argv[0]))
    
path = lambda x: os.path.join(
    os.path.dirname(os.path.realpath(__file__)), x)   

settings = ConfigParser.ConfigParser()
settings.read(path('insights.conf'))

APP_ID = '1960277150920761'
APP_SECRET = '0331e3b6f735c453e57e61a9ad7e210b'
ENDPOINT = 'graph.facebook.com'
REDIRECT_URI = 'http://localhost:8080/'
LOCAL_FILE = '.fb_access_token'
class FacebookGraphAPI(object):

    def __init__(self):
        self.page = settings.get('facebook', 'app_or_page')
        self.token = settings.get('facebook', 'access_token')
        self.api_url = "https://graph.facebook.com"
        self.last_url = ''

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(name)
        else:
            def caller(**params):
                url = self.construct_api_url(name, params)
                return self.api_request(url)
            return caller

    def construct_api_url(self, method_name, params):
        """
        since parameter is taken as today because API gives data of today-1 day
        so we get the data of yesterday
        'since' should be a string in %Y-%m-%d format.
        """
        #since = params.get('since', None)
        today = datetime.date.today()
	#dif=timedelta(days=4)
        params['since'] = today
        api_method = "/".join(method_name.split('__'))
        params['access_token'] =self.token
        url = "%s/%s/%s?%s" % (self.api_url,
                               self.page,
                               api_method,
                               urllib.urlencode(params))
        print('Making request to "%s"' % (url))
        return url

    def api_request(self, url):
        handle = urllib2.urlopen(url)
        self.last_url = handle.geturl()
        return json.load(handle)

def get_url(path, args=None):
    args = args or {}
    if ACCESS_TOKEN:
        args['access_token'] = ACCESS_TOKEN
    if 'access_token' in args or 'client_secret' in args:
        endpoint = "https://"+ENDPOINT
    else:
        endpoint = "http://"+ENDPOINT
    return endpoint+path+'?'+urllib.urlencode(args)

def get(path, args=None):
    return urllib2.urlopen(get_url(path, args=args)).read()

try:
    INTRO_MESSAGE = '''\
     __   
    |  | | 
    |__| |
    
    '''
    print INTRO_MESSAGE

    try:
        api = FacebookGraphAPI()
        print('Connectd to facebook page or app.')
    except:
        api = None
        print('Cant connect to facebook page or app')

    print("ready")

    insight_path = 'insights'
    fullpath = os.path.join(settings.get('facebook', 'output_destination'), insight_path)
        #delete all files from directory
    try:
        map( os.unlink, (os.path.join( fullpath,f) for f in os.listdir(fullpath)) )
    except OSError :
        pass
    
    for name, value in settings.items('insights'):
        
    

        for k, v in insights_groups.iteritems():
            #print(k)
            if (k == name) :
                
                for metric in v:
                    filepath = os.path.join(fullpath, metric)
                    #create the paths
                    if not os.path.exists(fullpath):
                       print('Directory "%s" not exists. Creating it...' % (fullpath))
                       try:
                          os.makedirs(fullpath)
                       except OSError:
                            print('Error creating directory: "%s".' % (fullpath))

                    metric_name = 'insights__%s' % metric

                    #get insights data
                    insights = getattr(api, metric_name)()

                    #initialize the csv writer
                    csvname = '%s.csv' % (metric)
                    if not os.path.exists(os.path.join(fullpath,csvname)):
                        #print(csvname)

                        tsvh = csv.writer(open(os.path.join(fullpath, csvname), 'wb'))
                        header = ['date', 'period', 'metric_values']
                        tsvh.writerow(header)
                    #else:
                        #tsvh=open(os.path.join(filepath, csvname),'a')

                    for metri in insights['data']:
                 
			 
			    for row in metri['values']:
                                date = datetime.datetime.strptime(row['end_time'], '%Y-%m-%dT%H:%M:%S+0000').date()

				try:
                                        
                                        if type(row['value']) is dict and metri['period']=='day':
                                            values=list(row['value'].values())
                                                                                        #values.append(date.strftime("%Y-%m-%d"))
                                            headers=list(row['value'].keys())
                                            #remove bad data in headers
                                            for i in range (0,len(headers)-1):
                                                headers[i]=re.sub(', ','_',headers[i])
                                                headers[i]=re.sub(' ','',headers[i])
                                                headers[i]= headers[i].encode('ascii','ignore')
                                                headers[i]=re.sub('Ã','',headers[i])
                                                headers[i]=re.sub('´','',headers[i])
                                                headers[i]=re.sub("'",'',headers[i])
                                                headers[i]=re.sub("-",'_',headers[i])
                                            #print(headers)    
                                            Date=[date.strftime("%Y-%m-%d") for i in xrange(len(values)+1)]
                                            #columns=['date','name_type','value']
                                            with open(os.path.join(fullpath, csvname),'a') as f:
                                                    writer = csv.writer(f)
                                                    writer.writerow(header)
						   
                                                    p=zip(Date,headers,values)
                                                    writer.writerows(p)     
                                           
                                            f.close()  
                                            out = None
                                        ##########################################################################################
                                        # To get the metics with period lifetime                                                 #
                                        ##########################################################################################
                                        elif type(row['value']) is dict and metri['period']=='lifetime':
                                            values=list(row['value'].values())
                                            #print(values)
                                            headers=list(row['value'].keys())
                                            #headers.append('date')
                                            for i in range (0,len(headers)-1):
                                                headers[i]=re.sub(', ','_',headers[i])
                                                headers[i]=re.sub(' ','',headers[i])
                                                headers[i]= headers[i].encode('ascii','ignore')
                                                headers[i]=re.sub('Ã','',headers[i])
                                                headers[i]=re.sub('´','',headers[i])
                                                headers[i]=re.sub("'",'',headers[i])
                                                headers[i]=re.sub("-",'_',headers[i])
                                            Date=[date.strftime("%Y-%m-%d") for i in xrange(len(headers))]
                                            #columns=['date','name','value']
                                            with open(os.path.join(fullpath, csvname),'a') as f:
                                                    writer = csv.writer(f)
                                                    writer.writerow(header)
                                                    p=zip(Date,headers,values)
                                                    writer.writerows(p)                   
                                            
                                            f.close()                      
                                            out = None     
                                        else:
                                            if metri['period']=='day' or metric in ['page_fans','post_negative_feedback','page_views_total','page_fans_city','page_fans_gender_age','post_fan_reach','post_engaged_users','post_impressions','post_impressions_unique','post_consumptions_by_type','post_engaged_fan','page_impressions_by_city'] :
                                                 out = [date, metri['period'], row['value']]
                                            else :
                                                continue
                                except KeyError :
				    if 	metric=='page_views_total':	
                                    	out = [date, metri['period'], 0]
				   	tsvh=csv.writer(open(os.path.join(fullpath,csvname),'a'))
                                    	tsvh.writerow(out)
			            	out = None 
				    else:
					print(metric)
					out = [date, metri['period'], 0]
				   	tsvh=csv.writer(open(os.path.join(fullpath,csvname),'a'))
                                    	tsvh.writerow(out)
					tsvh.writerow(out)
					tsvh.writerow(out)
					tsvh.writerow(out)
			            	out = None  
                                #print (out)
                                if out!=None:

                                               tsvh=csv.writer(open(os.path.join(fullpath,csvname),'a'))
                                               tsvh.writerow(out)
					       out=None
                                
			
    print('Process Finished Correctly!')

except Exception, e:
            print e
            
