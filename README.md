# mtbtopicsarticles

## Overview
This repository contains an AWS Lambda which reads DynamoDB table(s) and pushes new Mountain Biking articles to subscribed users every 24h
The articles are tailored/selected by the user's "topics" interests ie Downhill v Scenic v Jumping etc

## Dependent Repositories

To complete the picture you also need: -
* https://github.com/alancam73/mtbtopicsapp - AWS Amplify project built with Figma UI that accepts Topics events 
* https://github.com/alancam73/mtbtopicsload - (optional) - simple python code that loads new articles into the DynamoDB table from a JSON file
