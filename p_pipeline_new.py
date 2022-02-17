#######################################################################################
# Name           	: pinterest/pipeline.py
# Description    	: Python script to request Ad Campaign Metrics from Pinterest API
# Created Date   	: June 16, 2020
# Created By     	: Eric Wuerfel
#######################################################################################
import time

print("")
print("WEEKLY PINTEREST CAMPAIGN PERFORMANCE PIPELINE")
print("")
#########
# Imports
import requests
from datetime import datetime, timedelta
from time import sleep
import sys
import os

#from util import *
from p_util_new import *

# Sleep 5 hours
# sleep(60*60*5)

# # Historical Load
# def find_start_end_dates():
#     return [('2021-11-05', '2021-11-13')]


###############################################
# Authorization Header & Command-Line Variables
token = "xyz"
auth = "Bearer " + token
headers = {"Authorization": auth}

#landingDir = sys.argv[1]
landingDir="C:\\Users\\rg449k\\PycharmProjects\\pintrest\\output"
os.system("echo Retrieving Ad-Group Level Data")
# Create Report
ad_group_tokens = []
#date_ranges = find_start_end_dates()
date_ranges=[('2021-11-13', '2021-11-20')]
for date_range in date_ranges:
    token = post_async_report(headers, 'AD_GROUP', date_range, 5)
    print(token)
    ad_group_tokens.append(token)

# Clear Dir
#clear_dir = f"rm {landingDir}/*"
#os.system(clear_dir)
ad_groups = {}
count = 1
ad_group_tokens = list(set(ad_group_tokens))
print("hello")
print(ad_group_tokens)

for token in ad_group_tokens:
    print(token)
    #download_url = get_async_report(token, headers)
    path = f"{landingDir}/ad_groups_{count}.json"
    count += 1
    # Download Report
    #download(download_url, path)
    # Read Report
    data = read_json(path)
    #print(data)
    # Get Ad Groups
    transform_data(ad_groups, data)

##############
# VALIDATION #
##############
#hierarchy = {}
#campaigns = {}
#get_ad_group_ids(hierarchy, headers)
#get_ad_group_names(ad_groups, headers)
# # Get Daily Sums directly from JSON Reports
print(f"Ad Group Count: {len(ad_groups)}")

ad_group_sums = {}
count=0
#print(ad_groups)
#sleep(180)
#time.sleep(120)
for ad_group in ad_groups:
    for i in range(len(ad_groups[ad_group]['date_start'])):
        date = ad_groups[ad_group]['date_start'][i]
        if date not in ad_group_sums:
            ad_group_sums[date] = {}
            ad_group_sums[date]['spend'] = 0.0
            ad_group_sums[date]['clicks'] = 0
            ad_group_sums[date]['impressions'] = 0
        ad_group_sums[date]['spend'] += float(ad_groups[ad_group]['spend'][i])
        ad_group_sums[date]['clicks'] += int(ad_groups[ad_group]['clicks'][i])
        ad_group_sums[date]['impressions'] += int(ad_groups[ad_group]['impressions'][i])
        count=count+1
dates = []
print(f"Ad Group Count:" + str(count))
for date in ad_group_sums:
    if date not in dates:
        dates.append(date)
dates.sort()
max_chars = 20
for date in dates:
    spend = str(ad_group_sums[date]['spend'])
    click = str(ad_group_sums[date]['clicks'])
    impression = str(ad_group_sums[date]['impressions'])
    print(f"{date}: {spend}{' '*(max_chars-len(spend))}{click}{' '*(max_chars-len(click))}{impression}{' '*(max_chars-len(impression))}")
##################
# END VALIDATION #
##################
#sleep(300)
# Build Hierarchy
hierarchy = {}
campaigns = {}
os.system("echo Building Ad, Ad-Group, Campaign Relationships")

##########################
# Get Campaign IDs & Names
os.system(f"echo Finding Campaigns")
get_campaigns(campaigns, hierarchy, headers)
print("************************************************************************************************************")
#print(campaigns)
#print(hierarchy)
os.system(f"echo    - Found {len(hierarchy)} Campaigns!")
#time.sleep(120)
##################
# Get Ad Group IDs & Names
os.system(f"echo Finding Ad Groups")
get_ad_group_ids(hierarchy, headers)
print("*****************************************  second    *******************************************************************")
#print(hierarchy)
#get_ad_group_names(ad_groups, headers)

# Get Campaigns
#os.system(f"echo Adding Campaign info to Ad Groups Data")
#add_campaigns_to_ad_groups(hierarchy, campaigns, ad_groups)

# Send Ad Groups to DF
#os.system(f"echo Transforming Ad Group Data")
#ad_groups_df = ad_groups_to_df(ad_groups)
#print(ad_groups_df.shape)

##############
# VALIDATION #
##############
def get_daily_sums(df):
    dates = list(set(df['date_start'].tolist()))
    dates.sort()

    print(f"DATE:{' '*7}SPEND{' '*15}CLICKS{' '*14}IMPRESSIONS{' '*9}")
    print("")
    for date in dates:
        daily_df = df[df['date_start'] == date]
        spend_sum = str(sum(daily_df['spend'].tolist()))
        click_sum = str(sum(daily_df['clicks'].tolist()))
        impression_sum = str(sum(daily_df['impressions'].tolist()))

        max_chars = 20
        print(f"{date}: {spend_sum}{' '*(max_chars-len(spend_sum))}{click_sum}{' '*(max_chars-len(click_sum))}{impression_sum}{' '*(max_chars-len(impression_sum))}")
#get_daily_sums(ad_groups_df)
##################
# END VALIDATION #
##################

# Export Ad Groups as CSV
#os.system(clear_dir)
#ad_groups_path = f"{landingDir}/ad_groups.txt"
#ad_groups = ad_groups_df.drop_duplicates()
#ad_groups.to_csv(ad_groups_path, sep='\u0001', index=False)

#os.system("echo Retrieving Ad Level Data")

# Get Ad-Level Data now
#pin_tokens = []
#for date_range in date_ranges:
#    token = post_async_report(headers, 'PIN_PROMOTION', date_range, 5)
#    pin_tokens.append(token)

count = 1
pins = {}
#for token in pin_tokens:
#    download_url = get_async_report(token, headers)
#    ads_path = f"{landingDir}/ads_{count}.json"
#    count += 1
#    download(download_url, ads_path)
#    data = read_json(ads_path)

    # Add data to Pins dict
#    add_data_to_pins(data, pins)

#os.system("echo Getting Ad Relationships")
##############################
# Get Promoted Pin IDs & Names
# Authorization Header & Command-Line Variables
token = "xyz"
auth = "Bearer " + token
headers = {"Authorization": auth}

#get_promoted_pins(pins, hierarchy, headers)
#os.system("echo Linking Ads to Ad Groups, Campaigns")
# print(pins)
#link_pins_to_hierarchy(pins, ad_groups, campaigns, hierarchy)
#os.system("echo Transforming data to DF")

# Send Pins to DF
#pins_df = pins_to_df(pins)
#os.system("echo Exporting Pin Data")

# Send ad-level data to CSV
#pins_path = f"{landingDir}/ads.txt"
#pins_df.to_csv(pins_path, sep='\u0001', index=False)

# Remove JSONs
#os.system(f"rm {landingDir}/*.json")

print("")
print("END WEEKLY PINTEREST CAMPAIGN PERFORMANCE PIPELINE")
print("")
