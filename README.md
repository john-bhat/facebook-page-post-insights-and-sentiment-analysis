# Facebook-page and post insights

#Scripts are designed and deployed on client server and are working properly


#put the access token of your facebook page in conf files


#Fb_scrap is the file to get the page insights data . you will get seperate file for each Insight metric

#merge_script takes all the page insights metric files which have period "day" and put it in a sigle file

#post script get all the posts till 28th Aug(manually limited ). by removing it you can get all the posts till end

#It also gets the post metrics like post impression, post engagement etc

#passing the post ids into comments script will give you all the comments and replies.

#comments Sentiment script gives the sentiment score of each comment passed.

#move posts and comments script is designed to move the files to HDFS


#Facebook has done many changes in the graph API from time to time. this program is based on the data retrieved in 2017. You are request to modify your code according to new Metrices of the API
