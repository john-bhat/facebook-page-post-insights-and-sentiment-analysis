# -*- coding: utf-8 -*-
# Facebook posts (OI:OPEN INSIGHTS)
# Data Science Team
# John Mohd Bhat


import urllib2
import json
import datetime
import csv
import time
import ConfigParser


settings = ConfigParser.ConfigParser()
settings.read(('insights.conf'))
#print(settings.get('facebook', 'access_token'))

fullpath='/home/daasuser/facebook/posts_comments/'
#app_secret  DO NOT SHARE WITH ANYONE!
page_id = ""
app_id=""
app_secret=""
#access_token=""
access_token = settings.get('facebook', 'access_token')
#until=raw_input("Please enter until date in YYYY-MM-DD format:"
#since=raw_input("Please enter latest date in YYYY-MM-DD format:")


def request_until_succeed(url):
    req = urllib2.Request(url)
    success = False
    while success is False:
        try: 
            response = urllib2.urlopen(req)
            if response.getcode() == 200:
                success = True
        except Exception, e:
            print e
            time.sleep(5)

            print "Error for URL %s: %s" % (url, datetime.datetime.now())
            print "Retrying."

    return response.read()

# unicode correctly to csv
def unicode_normalize(text):
    return text.translate({ 0x2018:0x27, 0x2019:0x27, 0x201C:0x22, 0x201D:0x22,
                            0xa0:0x20 }).encode('utf-8')

def getFacebookPageFeedData(page_id, access_token, num_statuses):
    #&until=2015-01-30&since=2015-01-01
    base = "https://graph.facebook.com/v2.6"
    node = "/%s/posts" % page_id 
    fields = "/?fields=message,link,permalink_url,created_time,type,name,id," + \
            "comments.limit(0).summary(true),shares,reactions" + \
            ".limit(0).summary(true)"
    parameters = "&limit=%s&access_token=%s" % (num_statuses, access_token)
    #date="&until=%s&since=%s" % (until,since)
    url = base + node + fields + parameters 
    # retrieve data
    data = json.loads(request_until_succeed(url))
    
    return data

def getReactionsForStatus(status_id, access_token):

    base = "https://graph.facebook.com/v2.6"
    node = "/%s" % status_id
    reactions = "/?fields=" \
            "reactions.type(LIKE).limit(0).summary(total_count).as(like)" \
            ",reactions.type(LOVE).limit(0).summary(total_count).as(love)" \
            ",reactions.type(WOW).limit(0).summary(total_count).as(wow)" \
            ",reactions.type(HAHA).limit(0).summary(total_count).as(haha)" \
            ",reactions.type(SAD).limit(0).summary(total_count).as(sad)" \
            ",reactions.type(ANGRY).limit(0).summary(total_count).as(angry)"
    parameters = "&access_token=%s" % access_token
    
    url = base + node + reactions + parameters 

    # retrieve data
    data = json.loads(request_until_succeed(url))
     
    return data


def processFacebookPageFeedStatus(status, access_token):

    # The status is now a Python dictionary, so for top-level items,
    # we can simply call the key.

    # Additionally, some items may not always exist,
    # so must check for existence first

    status_id = status['id']
    status_message = '' if 'message' not in status.keys() else \
            unicode_normalize(status['message'])
    link_name = '' if 'name' not in status.keys() else \
            unicode_normalize(status['name'])
    status_type = status['type']
    status_link = '' if 'link' not in status.keys() else \
            unicode_normalize(status['link'])
    status_permalink_url = '' if 'permalink_url' not in status.keys() else \
            unicode_normalize(status['permalink_url'])
    # Time needs special care since a) it's in UTC and
    # b) it's not easy to use in statistical programs.

    status_published = datetime.datetime.strptime(
            status['created_time'],'%Y-%m-%dT%H:%M:%S+0000')

    status_published = status_published + \
            datetime.timedelta(hours=-5) # EST
    status_published = status_published.strftime(
            '%Y-%m-%d %H:%M:%S') # best time format for spreadsheet programs
   

    # Nested items require chaining dictionary keys.

    num_reactions = 0 if 'reactions' not in status else \
            status['reactions']['summary']['total_count']
    num_comments = 0 if 'comments' not in status else \
            status['comments']['summary']['total_count']
    num_shares = 0 if 'shares' not in status else status['shares']['count']

    # Counts of each reaction separately; good for sentiment
    # Only check for reactions if past date of implementation:
    #reactions available from 24th feb 2016 on fb
    reactions = getReactionsForStatus(status_id, access_token) if \
            status_published > '2016-02-24 00:00:00' else {}

    num_likes = 0 if 'like' not in reactions else \
            reactions['like']['summary']['total_count']

    # Special case: Set number of Likes to Number of reactions for pre-reaction
    # statuses

    num_likes = num_reactions if status_published < '2016-02-24 00:00:00' \
            else num_likes

    def get_num_total_reactions(reaction_type, reactions):
        if reaction_type not in reactions:
            return 0
        else:
            return reactions[reaction_type]['summary']['total_count']

    num_loves = get_num_total_reactions('love', reactions)
    num_wows = get_num_total_reactions('wow', reactions)
    num_hahas = get_num_total_reactions('haha', reactions)
    num_sads = get_num_total_reactions('sad', reactions)
    num_angrys = get_num_total_reactions('angry', reactions)
    
    #Post Insights
    post_metrics={'post_video_views':0,'post_video_views_unique':0,'post_consumptions':0,'post_fan_reach':0,'post_engaged_users':0,'post_impressions':0,'post_impressions_unique':0,'post_impressions_fan':0}
    for i in post_metrics:
	fb_graph_url="https://graph.facebook.com/"+status_id +"/insights/"+i+"?access_token="+access_token
        #api_request = urllib2.Request(fb_graph_url)
        #api_response = urllib2.urlopen(api_request)
        post_data =json.loads(request_until_succeed(fb_graph_url))
	a=[]
	for j in post_data['data'] :
	    for x in j['values']:
		a.append(x['value'])

	post_metrics[i]=max(a)		       

    # Return a tuple of all processed data

    return (status_id, status_message, link_name, status_type, status_link, status_permalink_url,
            status_published, num_reactions, num_comments, num_shares,
            num_likes, num_loves, num_wows, num_hahas, num_sads, num_angrys,post_metrics['post_video_views'],post_metrics['post_video_views_unique'],
            post_metrics['post_consumptions'],post_metrics['post_fan_reach'],post_metrics['post_engaged_users'],post_metrics['post_impressions'],post_metrics['post_impressions_unique'],post_metrics['post_impressions_fan'])

def scrapeFacebookPageFeedStatus(page_id, access_token):
    with open(fullpath+'%s_facebook_statuses.csv' % page_id, 'wb') as file:
        w = csv.writer(file)
        w.writerow(["status_id", "status_message", "link_name", "status_type",
                    "status_link", "permalink_url", "status_published", "num_reactions",
                    "num_comments", "num_shares", "num_likes", "num_loves",
                    "num_wows", "num_hahas", "num_sads", "num_angrys","post_video_views","post_video_views_unique",
                    "post_consumptions","post_fan_reach","post_engaged_users","post_impressions","post_impressions_unique","post_impressions_fan"])

        has_next_page = True
        num_processed = 0   # keep a count on how many we've processed
        scrape_starttime = datetime.datetime.now()

        print "Scraping %s Facebook Page: %s\n" % (page_id, scrape_starttime)

        statuses = getFacebookPageFeedData(page_id, access_token, 100)

        while has_next_page:
            for status in statuses['data']:
                global co
                co=1
                # Ensure it is a status with the expected metadata
                if 'reactions' in status:
                    dat=processFacebookPageFeedStatus(status,
                        access_token)
                    today = time.strftime("%Y-%m-%d")
                    today=datetime.datetime.strptime(today,"%Y-%m-%d")
                    someday=datetime.datetime.strptime(dat[6],'%Y-%m-%d %H:%M:%S')
                    someday=someday.strftime("%Y-%m-%d")
                    someday = datetime.datetime.strptime(someday,"%Y-%m-%d")
                    limit =datetime.datetime.strptime("2017-08-28","%Y-%m-%d")               
                    if someday -limit ==datetime.timedelta(0):
                        
                         print("reached limit 2017-08-28")
                         has_next_page = False
                         co=0
                         exit()
                    else:
                        w.writerow(processFacebookPageFeedStatus(status,
                           access_token))
                # output progress occasionally to make sure code is not
                # stalling

                num_processed += 1
                if num_processed % 100 == 0:
                    print "%s Statuses Processed: %s" % \
                        (num_processed, datetime.datetime.now())
                 
                if co==0:
                    has_next_page = False
                    exit()   

            # if there is no next page, we're done.
            if 'paging' in statuses.keys():
                statuses = json.loads(request_until_succeed(
                                        statuses['paging']['next']))
            else:
                has_next_page = False


        print "\nDone!\n%s Statuses Processed in %s" % \
                (num_processed, datetime.datetime.now() - scrape_starttime)


if __name__ == '__main__':
    scrapeFacebookPageFeedStatus(page_id, access_token)


