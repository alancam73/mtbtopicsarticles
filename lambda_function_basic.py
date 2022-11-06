# Lambda function to send a random video URL to active MtbTopics Users every 24h
# based on their topicPreferences. 
# Leverages several Dynamo DB tables. Uses SES to send Email from our Verified domain

import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import os
import botocore
from botocore.exceptions import ClientError
import uuid
import time
from datetime import datetime, time
import string
import random
import argparse


ddbResource = boto3.resource('dynamodb')
logsClient = boto3.client('logs')


# put user & topic details in the DynamoDB table
def recordArticlesPushed(user, articleId, table, articlesPushed):
    
    # if the entry/key does not exist then initialize it to 1 else increment
    if 'articlesPushedCt' not in articlesPushed.keys():
        articlesPushed['articlesPushedCt'] = 1
    else: 
        articlesPushed['articlesPushedCt'] += 1
    
    if 'articlesPushedIds' not in articlesPushed.keys():
        articlesPushed['articlesPushedIds'] = set()
    articlesPushed['articlesPushedIds'].add(articleId)
    
    item = {
        'userId': user['userId'],
        'emailAddress': user['emailAddress'],
        'articlesPushedCt': articlesPushed['articlesPushedCt'],
        'articlesPushedIds': articlesPushed['articlesPushedIds']
    }
    
#    print('Item details: ', 'Item: ', item)
    
    data = table.put_item(Item=item)


# send email via SES w/ link to the new article
def sendSESemail(user, articleId, articleData, sesRegion, sesSender):
    
    SENDER = sesSender              # eg mail@mtbtopics.com
    RECIPIENT = user['emailAddress']
    SES_AWS_REGION = sesRegion      # eg us-west-2

    # make sure we have valid sender, recipient and region
    if (not SENDER) or (not RECIPIENT) or (not SES_AWS_REGION):
        return 0

    sesClient = boto3.client('ses', region_name=SES_AWS_REGION)
    
    # The subject line for the email.
    SUBJECT = "MTB Topic of the Day"
    
    articleURL = articleData['mtbURL']
    userId = user['userId']
    
    # The HTML body of the email.
    BODY_HTML = f"<html><head></head><body><h1>MTB Topic of the Day!</h1><p><b><i>"
    BODY_HTML += userId
    BODY_HTML += "</i></b> : Here is your daily MTB topic/video based on your preferences: -<br><br><a href='"
    BODY_HTML += articleURL
    BODY_HTML += "'>"
    BODY_HTML += articleURL
    BODY_HTML += "</a><br><br>"
    BODY_HTML += "<img src='https://mtbtopics-assets.s3.us-west-2.amazonaws.com/mtb-header-logo-300x50.jpg' alt='MTB Topic of the Day'>"
    BODY_HTML += "<br><br>Enjoy!</p>"
    BODY_HTML += "<p><small>You can unsubscribe at any time <a href='https://www.articles.mtbtopics.com'>here</a></small></p>"
    BODY_HTML += "</body></html>"
    
    CHARSET = "UTF-8"
    
    # Try to send the email.
    try:
        #Provide the contents of the email.
        response = sesClient.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        print(e.response['Error']['Message'])
        return 0
    else:
        print("Email sent! Message ID:", response['MessageId']),
        return 1
        
    


# calc the bitMask and fill in the string datetime
def calcArticlesBitMask(articlesTbl, articlesResponse):
    
    articleData = articlesResponse['Items']
    
    for item in articleData:
        # if the item exists and has string date already then ignore it
        if 'dateAddedStrCalc' in item.keys():
            if item['dateAddedStrCalc'] and item['dateAddedStrCalc'].strip():
                continue
                 
        item['dateAddedStrCalc'] = datetime.fromtimestamp(int(item['dateAddedEpoch']))

        # calc the bitMask so that its easier to do Math ops - order is important
        articlesBitMask = 0
        if 'topic1Jumping' in item.keys() and item['topic1Jumping']: 
            articlesBitMask ^= 1 << 0
        if 'topic2Downhill' in item.keys() and item['topic2Downhill']:
            articlesBitMask ^= 1 << 1
        if 'topic3Tech' in item.keys() and item['topic3Tech']:
            articlesBitMask ^= 1 << 2
        if 'topic4Maint' in item.keys() and item['topic4Maint']:
            articlesBitMask ^= 1 << 3
        if 'topic5Scenic' in item.keys() and item['topic5Scenic']:
            articlesBitMask ^= 1 << 4

        print("bitMask: ", articlesBitMask, " Date: ", item['dateAddedStrCalc'])
        item['topicsBitMaskCalc'] = articlesBitMask
        
        # update the Articles DynamoDB table 
        update = articlesTbl.update_item(
            Key={'articleId': item['articleId']},
            UpdateExpression="set dateAddedStrCalc = :d1, topicsBitMaskCalc=:t1",
            ExpressionAttributeValues={
                ':d1': str(item['dateAddedStrCalc']),
                ':t1': int(item['topicsBitMaskCalc'])
            },
            ReturnValues="UPDATED_NEW"
        )
        

# match >= 1 bit of bitMask and push to user
def pushArticle(user, articlesResponse, articlesPushedTbl, articlesPushedResponse, sesRegion, sesSender):
    
    # first get the topics bitMask of the user
    bitMask = int(user['topics'])
    
    articleData = articlesResponse['Items']
    
    # construct the list of articles that satisfy the user's AND bitMask 
    matchArticlesList = []
    for item in articleData:
        if bitMask & int(item['topicsBitMaskCalc']):
            matchArticlesList.append(item['articleId'])

    print("user", user['userId'], "matched articles are: ", matchArticlesList)
    
    articlesPushed = articlesPushedResponse['Items']
    articlesPushedIds = set()
    articlesPushedIdMatch = {}
    for item in articlesPushed:
        if user['userId'] == item['userId']:
            articlesPushedIds = item['articlesPushedIds']
            articlesPushedIdMatch = item
            break
    
    # now go thru articleId's and push if never previously pushed
    for el in matchArticlesList:
        wasPushed = el in articlesPushedIds
        if not wasPushed:
            print("user", user['userId'], "gets article: ", el)
            
            # get the corresponding dict entry of articleData for el (articleId)
            articleElDict = [d for d in articleData if d['articleId'] == el]
            
            # send a link to the article via SES to the user
            emailSuccess = sendSESemail(user, el, articleElDict[0], sesRegion, sesSender)
            
            # add it to the mtbTopics-Users-ArticlesPushed table so we dont push it again
            if emailSuccess: 
                recordArticlesPushed(user, el, articlesPushedTbl, articlesPushedIdMatch)
            
            break
        
        


# main lambda function handler
def lambda_handler(event, context):
  
    args = None
    sesRegion = None
    sesSender = None
    
    # allow local Python execution testing as well as Lambda env
    execEnv = str(os.getenv('AWS_EXECUTION_ENV'))
    if execEnv.startswith("AWS_Lambda"):
        logGroupParam = os.getenv('log_group_envvar')
        log_group = str(logGroupParam)
        sesRegion = str(os.getenv('sesRegion'))
        sesSender = str(os.getenv('sesSender'))
    else:
        log_group = '/aws/lambda/mtbtopicsarticles'
        parser = argparse.ArgumentParser()
 
        # Adding positional arguments
        parser.add_argument("sesRegion", help = "AWS Region for SES usage")
        parser.add_argument("sesSender", help = "Email address from our MTB Topics domain")
        args = parser.parse_args()
        sesRegion = args.sesRegion
        sesSender = args.sesSender
    print ("SESRegion: ", sesRegion, " SESSender: ", sesSender)
        

    # do pre-calcs as necessary ie fill in missing DynamoDB fields
    articlesTbl = ddbResource.Table('mtbTopics-Articles-Topics')
    articlesResponse = articlesTbl.scan()
    calcArticlesBitMask(articlesTbl, articlesResponse)

    userTable = ddbResource.Table('mtbTopics-Users-Topics')
    response = userTable.scan()
    userData = response['Items']

    articlesPushedTbl = ddbResource.Table('mtbTopics-Users-ArticlesPushed')
    articlesPushedResponse = articlesPushedTbl.scan()
#    print (userData)
    
    for user in userData:
        keys = user.keys()
        if user['accountActive'] and 'emailAddress' in keys and int(user['topics']) > 0:
            print (user)
            # now match valid users with >= 1 topic from their bitMask
            pushArticle(user, articlesResponse, articlesPushedTbl, articlesPushedResponse, sesRegion, sesSender)
            
            
    return None


# allow local Python execution testing
if __name__ == '__main__':
    lambda_handler(None,None)
    