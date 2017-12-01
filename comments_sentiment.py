#-*- coding: utf-8 -*-
"""
Created on Fri Jul 17 11:59:37 2017

@author: admin
"""


import os,codecs
import numpy as np
import pandas as pd
import re
from nltk.tokenize import word_tokenize,sent_tokenize as st
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from textblob import TextBlob
from unidecode import unidecode
import HTMLParser
html_parser = HTMLParser.HTMLParser()
path='/home/daasuser/facebook/posts_comments/'
data=pd.read_csv(path+'MTNLoaded_facebook_comments.csv',encoding='utf-8')
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import warnings
warnings.filterwarnings("ignore")

#filtering the comments


test=pd.DataFrame(index=data.index, columns=data.columns)

for i in range (0,len(data)):
    if 'brand' in data.loc[i,'comment_message'].lower() or len(data.loc[i,'comment_message'])<4 or 'sugar mummies' in data.loc[i,'comment_message'].lower() or 'sugar mummy' in data.loc[i,'comment_message'].lower()\
    or 'recruitment' in data.loc[i,'comment_message'].lower() or 'wanted!' in data.loc[i,'comment_message'].lower()\
    or 'betting' in data.loc[i,'comment_message'].lower() or 'vehicle' in data.loc[i,'comment_message'].lower()\
    or 'training' in data.loc[i,'comment_message'].lower() or 'dream'in data.loc[i,'comment_message'].lower()\
    or 'admission' in data.loc[i,'comment_message'].lower() or 'real estate' in data.loc[i,'comment_message'].lower() \
    or 'visa'in data.loc[i,'comment_message'].lower() or 'promote'in data.loc[i,'comment_message'].lower() or ('http' in data.loc[i,'comment_message'].lower()  and 'mtn' not in data.loc[i,'comment_message'].lower()):
        continue
    else:  
        test.loc[i]=data.loc[i,]
test=test.dropna(how='all')
test.to_csv(path+'MTNLoaded_facebook_comments.csv',encoding='utf-8',index=None) 
#len(test)

test=pd.read_csv(path+'MTNLoaded_facebook_comments.csv',encoding='utf-8')

#cleaning the data




def pre_process(temp):

            regex=re.compile("[,\:\\<\>\!\[\]\.'\r\n\|\*]")
	    a = html_parser.unescape(temp)
	    #print(a)    
	    a = a.decode("utf-8").encode('ascii','ignore')
            #a = re.sub('(\\n|\\r|,|\#|&amp;*|\\s{2,})',' ',temp)+r""
            a=regex.sub(' ',a)
            a=re.sub('\s+',' ',a)
	    a=a.replace('-','')
            a=a.lower()
	    a= ''.join(i for i in a if ord(i)<128)
	    #print(a)
            return(a)
def tagging(test):
    test["label"]=''
    for i in range(len(test)):
        if "data" in test.iloc[i,3]:
            test.iloc[i,8] = "data"
        elif "network" in test.iloc[i,3]:
            test.iloc[i,8] = "network"
        elif "service" in test.iloc[i,3]:
            test.iloc[i,8] = "service"
        elif "roaming" in test.iloc[i,3]:
            test.iloc[i,8] = "roaming"
        elif "gb " in test.iloc[i,3]:
            test.iloc[i,8] = "gb"
        elif "mb " in test.iloc[i,3]:
            test.iloc[i,8] = "mb"   
        elif "recharge" in test.iloc[i,3]:
            test.iloc[i,8] ="recharge"    
        elif "sim " in test.iloc[i,3]:
            test.iloc[i,8] ="sim"
    	else:
		test.iloc[i,8] ="others"
    #return test


#to get the root words by doing stemming and by removing stop words
def  stem_news(temp):
    stem_sent=[]
    for i in temp:
         words = word_tokenize(i)
         ps=PorterStemmer()
         stemmed=''
         #for w in words:
             #stemmmed.append=ps.stem(w)
         stemmed=[ps.stem(word) for word in words if not word in set(stopwords.words('english'))]
         sent=' '.join(stemmed)
         stem_sent.append(sent)
    #print("stem_news run successfully")     
    return stem_sent    

#splitting the sentence into tokens
def split(self, text):

        wordlist = self._word_pattern.findall(text)
        return wordlist

        
#calculate the pos/neg/neutral score of each sentence   
# since the script runs daily on server and process limited comments  so i have manually added some strong sentiment words which ultimately increased accuracy without taking much execution time
#this code will run for  all comments you pass through status ids.     
test['sentiment_score']=''
negative_words=['frustrating','shitty','not good','Fraudsters','fraud','please stop','pls stop','block','blocked','freaking','expensive','hate','worst','hell','bad', 'fuck','fucking','sucks']
positive_words=['best','awesome','good','favourite','nice','fan']
def  main(dt):
     for i in range(0,len(dt)):               #article  level
         cleaned_data=pre_process(test['comment_message'][i])
         #sent_tokenized=st(cleaned_data)        #get the list of sentences in an article
         #sent=stem_news(sent_tokenized)         # stemming the words to root word and removing the stop words
         #sent=sent_tokenized
         #te=TextBlob(cleaned_data)
         #score1=te.sentiment.polarity
         #print(score1)
         #data['sent_score_textBlob'][i]=score1
	 test['comment_message'][i]=cleaned_data
         score=0
         for j in negative_words:
             if j in cleaned_data:
                 score=-1
                 test['sentiment_score'][i]=score
                 pass
         if score !=-1:
		for s in positive_words:
			if s in  cleaned_data:
				score=1
                 		test['sentiment_score'][i]=score
                 		pass
		if score !=1:
           		 te=TextBlob(cleaned_data)
            		 score=te.sentiment.polarity
             		 test['sentiment_score'][i]=score


query1=['how','why','is','what','who','whom','where','whose','which']
query2=['how could','why you','what','is it','is this','pin','customer care','why mtn','help','how dare','what is', 'need help', 'pls', 'please','want','need']
query3='?'
test['reply']=''
cmnt_id=0
#to check the response rate of customer executive
def query_reply(test):
	for i in range(0,len(test)-1):
    	#sent=pre_process(test.comment_message[i])
    		sent=test.comment_message[i]
    		if len(sent)>=3:
        		sent_token=sent.split()
        		if ((sent_token[0].lower in query1 or query3 in test.comment_message[i].lower() or
             		   len([x for x in query2 if x in test.comment_message[i]] )!=0) and test.comment_author[i] !='MTN Nigeria'):
             			cmnt_id=test.comment_id[i]
           
           			for j in range(i+1,i+2):
                			 if cmnt_id  == test['parent_id'][j] and test.comment_author[j]=='MTN Nigeria':
                       				 #print('replied')
                      				  test.iloc[i,9]='replied'
                      				  cmnt_id=0
                        
                        
             			if  test.iloc[i,9] !='replied':
                        		test.iloc[i,9]='not replied'
                        		cmnt_id=0
                          
       
        		else:
            			test.iloc[i,9]='not a query'
          
    		else:
        		test.iloc[i,9]='not a query'

main(test)
tagging(test)
query_reply(test)
test.to_csv(path+'MTNLoaded_facebook_comments.csv',index=None,quotechar='"')


