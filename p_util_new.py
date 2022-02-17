import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import pandas as pd
from datetime import datetime, timedelta
import os,sys
from time import sleep
import json


#################
# Basic Functions
#################
# Basic Functions
def find_start_end_dates():
    today = datetime.now()
    yesterday = today - timedelta(1)
    one_month_1 = today - timedelta(30)
    one_month_0 = today - timedelta(31)
    two_month_1 = today - timedelta(60)
    two_month_0 = today - timedelta(61)
    three_month = today - timedelta(90)

    today_str = datetime.strftime(today, '%Y-%m-%d')
    yesterday_str = datetime.strftime(yesterday, '%Y-%m-%d')
    one_month_1str = datetime.strftime(one_month_1, '%Y-%m-%d')
    one_month_0str = datetime.strftime(one_month_0, '%Y-%m-%d')
    two_month_1str = datetime.strftime(two_month_1, '%Y-%m-%d')
    two_month_0str = datetime.strftime(two_month_0, '%Y-%m-%d')
    three_month_str = datetime.strftime(three_month, '%Y-%m-%d')

    return [(three_month_str, two_month_0str), (two_month_1str, one_month_0str), (one_month_1str, today_str)]


def requests_retry_session(
        retries=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
        session=None,):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


###########################
# Async Reporting Functions
def post_async_report(headers, level, date_range, retries):
    if retries <= 0:
        print("Retries exceeded")
        return None
    print(f"date range -> {date_range[0]} and  {date_range[1]} ")
    request_url = "https://api.pinterest.com/ads/v4/advertisers/549755856460/delivery_metrics/async/"
    #params = {'start_date': '2022-01-05', 'end_date': '2022-01-06','granularity': 'DAY', 'level': 'AD_GROUP', 'columns':['SPEND_IN_MICRO_DOLLAR']}
    params = {'start_date': date_range[0], 'end_date': date_range[1],'granularity': 'DAY', 'level': 'AD_GROUP', 'columns': ['OUTBOUND_CLICK_1','OUTBOUND_CLICK_2','CLICKTHROUGH_1','CLICKTHROUGH_2','IMPRESSION_1','IMPRESSION_2','SPEND_IN_MICRO_DOLLAR']}

    #headers={'Authorization': 'Bearer xyz'}

    request = requests_retry_session().post(request_url, json=params, headers=headers)
    response = request.json()

    if response is None:
        print("Retrying...")
        retries -= 1
        post_async_report(headers, level, date_range, retries)

    if 'data' in response:
        print(response)
        if 'token' in response['data']:
            return response['data']['token']
        else:
            print(response)
            print("Retrying...")
            retries -= 1
            post_async_report(headers, level, date_range, retries)
    else:
        print(response)
        print("Retrying...")
        retries -= 1
        post_async_report(headers, level, date_range, retries)

def get_async_report(token, headers):
    request_url = "https://api.pinterest.com/ads/v4/advertisers/549755856460/delivery_metrics/async/"
    params = {'token': token}

    request = requests_retry_session().get(request_url, params=params, headers=headers)
    response = request.json()
    print(response)
    if 'data' in response:
        while response['data']['report_status'] != "FINISHED":
            sleep(10)
            request = requests_retry_session().get(request_url, params=params, headers=headers)
            response = request.json()

        download_url = response['data']['url']
        return download_url

def download(download_url, path):
    request = requests_retry_session().get(download_url)

    with open(path, 'wb') as file:
        file.write(request.content)

def read_json(path):
    with open(path) as file:
        result = json.load(file)

    return result

####################
# Ad-Group Functions
def transform_data(ad_groups, data):
    for ad_group in data:
        # If ad_group hasn't been added yet, create a new dict
        if int(ad_group) not in ad_groups:
            ad_groups[int(ad_group)] = {}
            dates = []
            spends = []
            clicks = []
            impressions = []
        # If it has, grab the old lists
        else:
            dates = ad_groups[int(ad_group)]['date_start']
            spends = ad_groups[int(ad_group)]['spend']
            clicks = ad_groups[int(ad_group)]['clicks']
            impressions = ad_groups[int(ad_group)]['impressions']

        for entry in data[str(ad_group)]:
            # date
            if 'DATE' in entry:
                date = entry['DATE']
            else:
                continue

            # spend
            if 'SPEND_IN_MICRO_DOLLAR' in entry:
                micro_spend = entry['SPEND_IN_MICRO_DOLLAR']
                spend = float(int(micro_spend) / 1000000)
            else:
                spend = 0.0

            # clicks
            if date < '2021-01-01':
                if 'CLICKTHROUGH_1' in entry:
                    click1 = entry['CLICKTHROUGH_1']
                else:
                    click1 = 0
                if 'CLICKTHROUGH_2' in entry:
                    click2 = entry['CLICKTHROUGH_2']
                else:
                    click2 = 0
            else:
                if 'OUTBOUND_CLICK_1' in entry:
                    click1 = entry['OUTBOUND_CLICK_1']
                else:
                    click1 = 0
                if 'OUTBOUND_CLICK_2' in entry:
                    click2 = entry['OUTBOUND_CLICK_2']
                else:
                    click2 = 0

            click = int(click1) + int(click2)

            # impressions
            if 'IMPRESSION_1' in entry:
                impression1 = entry['IMPRESSION_1']
            else:
                impression1 = 0
            if 'IMPRESSION_2' in entry:
                impression2 = entry['IMPRESSION_2']
            else:
                impression2 = 0

            impression = int(impression1) + int(impression2)

            # add data to lists
            spends.append(spend)
            dates.append(date)
            clicks.append(click)
            impressions.append(impression)

        ad_groups[int(ad_group)]['date_start'] = dates
        ad_groups[int(ad_group)]['spend'] = spends
        ad_groups[int(ad_group)]['clicks'] = clicks
        ad_groups[int(ad_group)]['impressions'] = impressions

def get_campaigns(campaigns, hierarchy, headers):
    print("Getting Campaign IDs & Names...{hierarchy}")
    requests_remaining = 500
    params = {'order': 'DESCENDING'}

    # Request Campaign IDs
    try:
        get_campaigns_url = 'https://api.pinterest.com/ads/v4/advertisers/549755856460/campaigns/'
        get_campaigns_request = requests_retry_session().get(get_campaigns_url, params=params, headers=headers)
        get_campaigns_response = get_campaigns_request.json()

        #print(get_campaigns_response)

        # Check rate limit
        if 'x-userendpoint-ratelimit-remaining' in get_campaigns_request.headers:
            requests_remaining = int(get_campaigns_request.headers['x-userendpoint-ratelimit-remaining'])
        else:
            requests_remaining -= 1
        # Get Campaign IDs if Available
        if 'data' in get_campaigns_response:
            for campaign in get_campaigns_response['data']:
                # Get Campaign ID
                campaign_id = int(campaign['id'])
                # Create a dict for each Campaign ID to hold Ad Group IDs
                if campaign_id not in hierarchy:
                    hierarchy[campaign_id] = {}
                # Get Campaign Name
                if campaign_id not in campaigns:
                    campaign_name = campaign['name']
                    campaigns[campaign_id] = campaign_name
        else:
            print("CAMPAIGNS NOT FOUND")
            sys.exit(1)

    except Exception as x:
        print('Failed at bookmark if:(', x.__class__.__name__)

    # Loop through bookmarks to get the rest of the Campaign IDs


def check_rate_limit(request, requests_remaining):
    if 'x-userendpoint-ratelimit-remaining' in request.headers:
        requests_remaining = int(request.headers['x-userendpoint-ratelimit-remaining'])
    else:
        requests_remaining -= 1

    return requests_remaining

def parse_ad_group_id(hierarchy, campaign_id, get_ad_group_id_response):
    # Check if Data is there
    print(hierarchy)
    print(campaign_id)
    print(get_ad_group_id_response)
    if 'data' in get_ad_group_id_response:
        data = get_ad_group_id_response['data']
        # Add IDs to Hierarchy & ad_groups
        ad_group_id=data['id']
        print(f" my data -> {data['id']}")
        #for ad_group_id in data['id']:
            #print(ad_group_id)
        if ad_group_id not in hierarchy[campaign_id]:
                hierarchy[campaign_id][ad_group_id] = {}
        print(f" my hierarchy -> {hierarchy}")
def parse_ad_group_name(ad_groups, ad_group, get_ad_group_name_response):
    # Look for Data
    if 'data' in get_ad_group_name_response:
        data = get_ad_group_name_response['data']

        # Add name to ad_groups
        if data is not None:
            ad_group_name = data['name']
            ad_groups[ad_group]['name'] = ad_group_name
        else:
            print(f"ERROR: {get_ad_group_name_response}")

def get_ad_group_ids3(hierarchy, headers):
    print("Rakesh")
    # need new requests_remaining
    requests_remaining = 500

    # Loop Through Campaigns to get ad_group_ids
    for campaign_id in hierarchy:
        print(campaign_id)

def get_ad_group_ids(hierarchy, headers):
    print("Getting Ad Group IDs...")
    # need new requests_remaining
    requests_remaining = 500
    adgroups = {}
    # Loop Through Campaigns to get ad_group_ids
    for campaign_id in hierarchy:
        sleep(.125)
        #if requests_remaining > 200:
            # Request Campaign info - contains 'ad_group_ids'
        try:
                get_ad_group_id_url = f"https://api.pinterest.com/ads/v3/campaigns/{str(campaign_id)}/"
                #get_ad_group_id_url = f"https://api.pinterest.com/ads/v4/advertisers/549755856460/campaigns/{str(campaign_id)}/"
                #get_ad_group_id_url = f"https://api.pinterest.com/ads/v4/advertisers/549755856460/ad_groups/"
                get_ad_group_id_request = requests_retry_session().get(get_ad_group_id_url, headers=headers)
                get_ad_group_id_response = get_ad_group_id_request.json()
                print(campaign_id)
                print(f" my response --> {get_ad_group_id_response}")

                # v3
                if 'data' in get_ad_group_id_response:
                    data = get_ad_group_id_response['data']
                    for i in data['ad_group_ids']:
                        print(f"ad group -> {data['name']}")
                        print("hello")
                        if i not in adgroups:
                            adgroup_name = data['name']
                            adgroups[i] = adgroup_name
                print(f"my ad groyup id list --> {adgroups}")

                # v4
                if 'data' in get_ad_group_id_response:
                    for adgroup_data in get_ad_group_id_response['data']:
                        adgroup_id = int(adgroup_data['id'])
                        if adgroup_id not in hierarchy:
                            hierarchy[adgroup_id] = {}
                        if adgroup_id not in adgroups:
                            adgroup_name = adgroup_data['name']
                            adgroups[adgroup_id] = adgroup_name

                # Check Rate Limit
                #requests_remaining = check_rate_limit(get_ad_group_id_request, requests_remaining)

                # Check if Data is there
                #parse_ad_group_id(hierarchy, campaign_id, get_ad_group_id_response)

        except Exception as x:
                print('Failed at get_ad_group_ids first try: ', x.__class__.__name__)
                sleep(30)
                print(f"Retrying {campaign_id}")

def get_ad_group_ids2(hierarchy, headers):
    print("Getting Ad Group IDs...")
    # need new requests_remaining
    requests_remaining = 500

    # Loop Through Campaigns to get ad_group_ids
    for campaign_id in hierarchy:
        sleep(.125)
        #if requests_remaining > 200:
            # Request Campaign info - contains 'ad_group_ids'
        try:
                #get_ad_group_id_url = f"https://api.pinterest.com/ads/v3/campaigns/{str(campaign_id)}/"
                get_ad_group_id_url = f"https://api.pinterest.com/ads/v4/advertisers/549755856460/ad_groups/2680059853753"
                get_ad_group_id_request = requests_retry_session().get(get_ad_group_id_url, headers=headers)
                get_ad_group_id_response = get_ad_group_id_request.json()
                print(f" my response --> {get_ad_group_id_response}")
                # Check Rate Limit
                #requests_remaining = check_rate_limit(get_ad_group_id_request, requests_remaining)

                # Check if Data is there
                parse_ad_group_id(hierarchy, campaign_id, get_ad_group_id_response)

        except Exception as x:
                print('Failed at get_ad_group_ids first try: ', x.__class__.__name__)
                sleep(30)
                print(f"Retrying {campaign_id}")

                try:
                    #get_ad_group_id_url = f"https://api.pinterest.com/ads/v4/campaigns/{str(campaign_id)}/"
                    get_ad_group_id_url = f"https://api.pinterest.com/ads/v4/advertisers/549755856460/campaigns/{str(campaign_id)}/"
                    get_ad_group_id_request = requests_retry_session().get(get_ad_group_id_url, headers=headers)
                    get_ad_group_id_response = get_ad_group_id_request.json()

                    # Check Rate Limit
                    #requests_remaining = check_rate_limit(get_ad_group_id_request, requests_remaining)

                    # Check if Data is there
                    parse_ad_group_id(hierarchy, campaign_id, get_ad_group_id_response)

                except Exception as x:
                    print('Failed at get_ad_group_ids first try exception: ', x.__class__.__name__)

        #else:
            #print(f"{requests_remaining} Requests Remaining... Sleeping...")
            #sleep(120)
            #requests_remaining = 201

            # Try Again!
        try:
                #get_ad_group_id_url = f"https://api.pinterest.com/ads/v4/campaigns/{str(campaign_id)}/"
                get_ad_group_id_url = f"https://api.pinterest.com/ads/v4/advertisers/549755856460/campaigns/{str(campaign_id)}/"
                get_ad_group_id_request = requests_retry_session().get(get_ad_group_id_url, headers=headers)
                get_ad_group_id_response = get_ad_group_id_request.json()

                # Check Rate Limit
                #requests_remaining = check_rate_limit(get_ad_group_id_request, requests_remaining)

                # Check if Data is there
                parse_ad_group_id(hierarchy, campaign_id, get_ad_group_id_response)

        except Exception as x:
                print('Failed get_ad_group_ids second try : ', x.__class__.__name__)
                sleep(30)
                print(f"Retrying {campaign_id}")

                try:
                    #get_ad_group_id_url = f"https://api.pinterest.com/ads/v4/campaigns/{str(campaign_id)}/"
                    get_ad_group_id_url = f"https://api.pinterest.com/ads/v4/advertisers/549755856460/campaigns/{str(campaign_id)}/"
                    get_ad_group_id_request = requests_retry_session().get(get_ad_group_id_url, headers=headers)
                    get_ad_group_id_response = get_ad_group_id_request.json()

                    # Check Rate Limit
                    #requests_remaining = check_rate_limit(get_ad_group_id_request, requests_remaining)

                    # Check if Data is there
                    parse_ad_group_id(hierarchy, campaign_id, get_ad_group_id_response)

                except Exception as x:
                    print('Failed get_ad_group_ids second try exception: ', x.__class__.__name__)

def get_ad_group_names(ad_groups, headers):
    print("Getting Ad Group Names...")
    requests_remaining = 500

    # Loop ad groups -
    for ad_group in ad_groups:
        sleep(.125)
        #if requests_remaining > 200:
            # Request Data
        try:
                #2680070397836
                #get_ad_group_name_url = f"https://api.pinterest.com/ads/v3/ad_groups/{str(ad_group)}/"
                get_ad_group_name_url = f"https://api.pinterest.com/ads/v4/advertisers/549755856460/ad_groups/{str(ad_group)}/"
                get_ad_group_name_request = requests_retry_session().get(get_ad_group_name_url, headers=headers)
                get_ad_group_name_response = get_ad_group_name_request.json()

                # Check Rate Limit
                #requests_remaining = check_rate_limit(get_ad_group_name_request, requests_remaining)

                # Look for Data
                parse_ad_group_name(ad_groups, ad_group, get_ad_group_name_response)

        except Exception as x:
                print('Failed at get_ad_group_names ', x.__class__.__name__)
                sleep(30)
                print(f"Retrying {ad_group}")

                try:
                    #get_ad_group_name_url = f"https://api.pinterest.com/ads/v3/ad_groups/{str(ad_group)}/"
                    get_ad_group_name_url = f"https://api.pinterest.com/ads/v4/advertisers/549755856460/ad_groups/{str(ad_group)}/"
                    get_ad_group_name_request = requests_retry_session().get(get_ad_group_name_url, headers=headers)
                    get_ad_group_name_response = get_ad_group_name_request.json()

                    # Check Rate Limit
                    #requests_remaining = check_rate_limit(get_ad_group_name_request, requests_remaining)

                    # Look for Data
                    parse_ad_group_name(ad_groups, ad_group, get_ad_group_name_response)

                except Exception as x:
                    print('SKIPPED DUE TO ERROR: ', x.__class__.__name__)


        #else:
         #   print(f"{requests_remaining} Requests Remaining... Sleeping...")
         #   sleep(120)
          #  requests_remaining = 201

            # Try Again!
        try:
                #get_ad_group_name_url = f"https://api.pinterest.com/ads/v3/ad_groups/{str(ad_group)}/"
                get_ad_group_name_url = f"https://api.pinterest.com/ads/v4/advertisers/549755856460/ad_groups/{str(ad_group)}/"
                get_ad_group_name_request = requests_retry_session().get(get_ad_group_name_url, headers=headers)
                get_ad_group_name_response = get_ad_group_name_request.json()

                # Check Rate Limit
                #requests_remaining = check_rate_limit(get_ad_group_name_request, requests_remaining)

                # Look for Data
                parse_ad_group_name(ad_groups, ad_group, get_ad_group_name_response)

        except Exception as x:
                print('Failed get_ad_group_names 2: ', x.__class__.__name__)
                sleep(30)
                print(f"Retrying {ad_group}")

                try:
                    #get_ad_group_name_url = f"https://api.pinterest.com/ads/v3/ad_groups/{str(ad_group)}/"
                    get_ad_group_name_url = f"https://api.pinterest.com/ads/v4/advertisers/549755856460/ad_groups/{str(ad_group)}/"
                    get_ad_group_name_request = requests_retry_session().get(get_ad_group_name_url, headers=headers)
                    get_ad_group_name_response = get_ad_group_name_request.json()

                    # Check Rate Limit
                    #requests_remaining = check_rate_limit(get_ad_group_name_request, requests_remaining)

                    # Look for Data
                    parse_ad_group_name(ad_groups, ad_group, get_ad_group_name_response)

                except Exception as x:
                    print('SKIPPED DUE TO ERROR: ', x.__class__.__name__)

def add_campaigns_to_ad_groups(hierarchy, campaigns, ad_groups):
    for campaign in hierarchy:
        for ad_group in hierarchy[campaign]:
            if ad_group in ad_groups:
                ad_groups[ad_group]['campaign_id'] = campaign
                ad_groups[ad_group]['campaign_name'] = campaigns[campaign]


def ad_groups_to_df(ad_groups):
    ad_group_ids = []
    ad_group_names = []
    campaign_ids = []
    campaign_names = []
    dates = []
    spends = []
    clicks = []
    impressions = []
    publishers = []

    for ad_group in ad_groups:
        length = len(ad_groups[ad_group]['date_start'])

        for i in range(length):
            if 'campaign_id' in ad_groups[ad_group]:
                campaign_id = ad_groups[ad_group]['campaign_id']
            else:
                # print(f"AD GROUP {ad_group} IS MISSING campaign_id")
                continue

            if 'campaign_name' in ad_groups[ad_group]:
                campaign_name = ad_groups[ad_group]['campaign_name']
            else:
                # print(f"AD GROUP {ad_group} IS MISSING campaign_name")
                continue

            if 'name' in ad_groups[ad_group]:
                ad_group_name = ad_groups[ad_group]['name']
            else:
                # print(f"AD GROUP {ad_group} IS MISSING name")
                continue

            if 'date_start' in ad_groups[ad_group]:
                date = ad_groups[ad_group]['date_start'][i]
            else:
                # print(f"AD GROUP {ad_group} IS MISSING dates")
                continue

            if 'spend' in ad_groups[ad_group]:
                spend = ad_groups[ad_group]['spend'][i]
            else:
                # print(f"AD GROUP {ad_group} IS MISSING spend")
                continue

            if 'clicks' in ad_groups[ad_group]:
                click = ad_groups[ad_group]['clicks'][i]
            else:
                # print(f"AD GROUP {ad_group} IS MISSING clicks")
                continue

            if 'impressions' in ad_groups[ad_group]:
                impression = ad_groups[ad_group]['impressions'][i]
            else:
                # print(f"AD GROUP {ad_group} IS MISSING impressions")
                continue

            publisher = "Pinterest"

            campaign_ids.append(campaign_id)
            campaign_names.append(campaign_name)
            ad_group_ids.append(ad_group)
            ad_group_names.append(ad_group_name)
            dates.append(date)
            spends.append(spend)
            clicks.append(click)
            impressions.append(impression)
            publishers.append(publisher)

    return pd.DataFrame({'ad_group_id': ad_group_ids,
                         'ad_group_name': ad_group_names,
                         'campaign_id': campaign_ids,
                         'campaign_name': campaign_names,
                         'date_start': dates,
                         'spend': spends,
                         'clicks': clicks,
                         'impressions': impressions,
                         'publisher': publishers})


####################
# Ad (Pin) Functions
def add_data_to_pins(data, pins):
    # Loop through data
    for pin in data:
        # Create a dict for metrics if pin isn't already there
        if int(pin) not in pins:
            pins[int(pin)] = {}
            spends = []
            dates = []
            clicks = []
            impressions = []
        # If it is there, grab the existing metrics
        else:
            spends = pins[int(pin)]['spend']
            dates = pins[int(pin)]['dates']
            clicks = pins[int(pin)]['clicks']
            impressions = pins[int(pin)]['impressions']

        for entry in data[pin]:
            # date
            if 'DATE' in entry:
                date = entry['DATE']
            else:
                continue

            # spend
            if 'SPEND_IN_MICRO_DOLLAR' in entry:
                micro_spend = entry['SPEND_IN_MICRO_DOLLAR']
                spend = float(int(micro_spend) / 1000000)
            else:
                spend = 0.0

            if date < '2021-01-01':
                if 'CLICKTHROUGH_1' in entry:
                    click1 = entry['CLICKTHROUGH_1']
                else:
                    click1 = 0
                if 'CLICKTHROUGH_2' in entry:
                    click2 = entry['CLICKTHROUGH_2']
                else:
                    click2 = 0
            else:
                if 'OUTBOUND_CLICK_1' in entry:
                    click1 = entry['OUTBOUND_CLICK_1']
                else:
                    click1 = 0
                if 'OUTBOUND_CLICK_2' in entry:
                    click2 = entry['OUTBOUND_CLICK_2']
                else:
                    click2 = 0
            click = int(click1) + int(click2)

            # impressions
            if 'IMPRESSION_1' in entry:
                impression1 = entry['IMPRESSION_1']
            else:
                impression1 = 0
            if 'IMPRESSION_2' in entry:
                impression2 = entry['IMPRESSION_2']
            else:
                impression2 = 0
            impression = int(impression1) + int(impression2)

            # add data to lists
            spends.append(spend)
            dates.append(date)
            clicks.append(click)
            impressions.append(impression)

        pins[int(pin)]['spend'] = spends
        pins[int(pin)]['dates'] = dates
        pins[int(pin)]['clicks'] = clicks
        pins[int(pin)]['impressions'] = impressions


def get_promoted_pins(pins, hierarchy, headers):
    print("Getting Promoted Pin IDs & Names...")
    start = datetime.now()
    requests_remaining = 500
    count = 0

    # Loop through ad_group_ids
    for campaign_id in hierarchy:
        if hierarchy[campaign_id] != {}:
            for ad_group_id in hierarchy[campaign_id]:
                count += 1
                if requests_remaining > 200:
                    # Request Data
                    try:
                        #get_pin_ids_url = f"https://api.pinterest.com/ads/v3/ad_groups/{str(ad_group_id)}/pin_promotions/"
                        get_pin_ids_url = 'https://api.pinterest.com/ads/v4/advertisers/549755856460/ad_groups/'
                        get_pin_ids_request = requests_retry_session().get(get_pin_ids_url, headers=headers)
                        get_pin_ids_response = get_pin_ids_request.json()

                        # Check Rate Limit
                        if 'x-userendpoint-ratelimit-remaining' in get_pin_ids_request.headers:
                            requests_remaining = int(get_pin_ids_request.headers['x-userendpoint-ratelimit-remaining'])
                        else:
                            requests_remaining -= 1

                        # Check for data
                        if 'data' in get_pin_ids_response:
                            # Loop through pins
                            for pin in get_pin_ids_response['data']:
                                pin_id = int(pin['id'])
                                # Add to Hierarchy & Pins
                                if pin_id not in hierarchy[campaign_id][ad_group_id]:
                                    # Create a Dict to store metrics
                                    hierarchy[campaign_id][ad_group_id][pin_id] = {}
                                if pin_id in pins:
                                    pin_name = pin['name']
                                    pins[pin_id]['name'] = pin_name

                        if count%100==0:
                            print(f"{count}: {datetime.now()}")

                    except Exception as x:
                        print('Failed :(', x.__class__.__name__)
                        sleep(10)
                        print(f"Retrying {ad_group_id}")

                        #get_pin_ids_url = f"https://api.pinterest.com/ads/v3/ad_groups/{str(ad_group_id)}/pin_promotions/"
                        get_pin_ids_url = 'https://api.pinterest.com/ads/v4/advertisers/549755856460/ad_groups/'
                        get_pin_ids_request = requests_retry_session().get(get_pin_ids_url, headers=headers)
                        get_pin_ids_response = get_pin_ids_request.json()

                        # Check Rate Limit
                        if 'x-userendpoint-ratelimit-remaining' in get_pin_ids_request.headers:
                            requests_remaining = int(get_pin_ids_request.headers['x-userendpoint-ratelimit-remaining'])
                        else:
                            requests_remaining -= 1

                        # Check for data
                        if 'data' in get_pin_ids_response:
                            # Loop through pins
                            for pin in get_pin_ids_response['data']:
                                pin_id = int(pin['id'])
                                # Add to Hierarchy & Pins
                                if pin_id not in hierarchy[campaign_id][ad_group_id]:
                                    # Create a Dict to store metrics
                                    hierarchy[campaign_id][ad_group_id][pin_id] = {}
                                if pin_id in pins:
                                    pin_name = pin['name']
                                    pins[pin_id]['name'] = pin_name

                        if count%100==0:
                            print(f"{count}: {datetime.now()}")

                else:
                    print(f"Sleeping...{requests_remaining} Requests Remaining")
                    sleep(120)
                    requests_remaining = 201

                    # Request Data
                    try:
                        #get_pin_ids_url = f"https://api.pinterest.com/ads/v3/ad_groups/{str(ad_group_id)}/pin_promotions/"
                        get_pin_ids_url = 'https://api.pinterest.com/ads/v4/advertisers/549755856460/ad_groups/'
                        get_pin_ids_request = requests_retry_session().get(get_pin_ids_url, headers=headers)
                        get_pin_ids_response = get_pin_ids_request.json()

                        # Check Rate Limit
                        if 'x-userendpoint-ratelimit-remaining' in get_pin_ids_request.headers:
                            requests_remaining = int(get_pin_ids_request.headers['x-userendpoint-ratelimit-remaining'])
                        else:
                            requests_remaining -= 1

                        # Check for data
                        if 'data' in get_pin_ids_response:
                            # Loop through pins
                            for pin in get_pin_ids_response['data']:
                                pin_id = int(pin['id'])
                                # Add to Hierarchy & Pins
                                if pin_id not in hierarchy[campaign_id][ad_group_id]:
                                    # Create a Dict to store metrics
                                    hierarchy[campaign_id][ad_group_id][pin_id] = {}
                                if pin_id in pins:
                                    pin_name = pin['name']
                                    pins[pin_id]['name'] = pin_name

                        if count%100==0:
                            print(f"{count}: {datetime.now()}")

                    except Exception as x:
                        print('Failed: ', x.__class__.__name__)
                        sleep(10)
                        print(f"Retrying {ad_group_id}")

                        #get_pin_ids_url = f"https://api.pinterest.com/ads/v3/ad_groups/{str(ad_group_id)}/pin_promotions/"
                        get_pin_ids_url = 'https://api.pinterest.com/ads/v4/advertisers/549755856460/ad_groups/'
                        get_pin_ids_request = requests_retry_session().get(get_pin_ids_url, headers=headers)
                        get_pin_ids_response = get_pin_ids_request.json()

                        # Check Rate Limit
                        if 'x-userendpoint-ratelimit-remaining' in get_pin_ids_request.headers:
                            requests_remaining = int(get_pin_ids_request.headers['x-userendpoint-ratelimit-remaining'])
                        else:
                            requests_remaining -= 1

                        # Check for data
                        if 'data' in get_pin_ids_response:
                            # Loop through pins
                            for pin in get_pin_ids_response['data']:
                                pin_id = int(pin['id'])
                                # Add to Hierarchy & Pins
                                if pin_id not in hierarchy[campaign_id][ad_group_id]:
                                    # Create a Dict to store metrics
                                    hierarchy[campaign_id][ad_group_id][pin_id] = {}
                                if pin_id in pins:
                                    pin_name = pin['name']
                                    pins[pin_id]['name'] = pin_name

                        if count%100==0:
                            print(f"{count}: {datetime.now()}")


def link_pins_to_hierarchy(pins, ad_groups, campaigns, hierarchy):
    for campaign in hierarchy:
        for ad_group in hierarchy[campaign]:
            for pin in hierarchy[campaign][ad_group]:
                if pin in pins:
                    pins[pin]['ad_group_id'] = ad_group
                    pins[pin]['campaign_id'] = campaign

                    if ad_group in ad_groups:
                        if 'name' in ad_groups[ad_group]:
                            pins[pin]['ad_group_name'] = ad_groups[ad_group]['name']
                        else:
                            pins[pin]['ad_group_name'] = "NULL"
                    else:
                        pins[pin]['ad_group_name'] = "NULL"

                    if campaign in campaigns:
                        pins[pin]['campaign_name'] = campaigns[campaign]
                    else:
                        pins[pin]['campaign_name'] = "NULL"


def pins_to_df(pins):
    # Instantiate Lists
    ad_ids = []
    ad_names = []
    ad_group_ids = []
    ad_group_names = []
    campaign_ids = []
    campaign_names = []
    dates = []
    spends = []
    clicks = []
    impressions = []
    publishers = []

    # Loop pins
    for pin in pins:
        # Make sure metrics were collected properly
        assert len(pins[pin]['spend']) == len(pins[pin]['dates']) == len(pins[pin]['clicks']) == len(pins[pin]['impressions'])

        # Make sure everything else is there
        for i in range(len(pins[pin]['spend'])):
            if 'name' in pins[pin]:
                if 'ad_group_id' in pins[pin]:
                    if 'ad_group_name' in pins[pin]:
                        if 'campaign_id' in pins[pin]:
                            if 'campaign_name' in pins[pin]:

                                # Add to Lists
                                ad_ids.append(pin)
                                ad_names.append(pins[pin]['name'])
                                ad_group_ids.append(pins[pin]['ad_group_id'])
                                ad_group_names.append(pins[pin]['ad_group_name'])
                                campaign_ids.append(pins[pin]['campaign_id'])
                                campaign_names.append(pins[pin]['campaign_name'])
                                dates.append(pins[pin]['dates'][i])
                                spends.append(pins[pin]['spend'][i])
                                clicks.append(pins[pin]['clicks'][i])
                                impressions.append(pins[pin]['impressions'][i])
                                publishers.append("Pinterest")

                            else:
                                continue
                                # print(f"{'campaign_name'} NOT FOUND FOR PIN: {pin}")
                        else:
                            continue
                            # print(f"{'campaign_id'} NOT FOUND FOR PIN: {pin}")
                    else:
                        continue
                        # print(f"{'ad_group_name'} NOT FOUND FOR PIN: {pin}")
                else:
                    continue
                    # print(f"{'ad_group_id'} NOT FOUND FOR PIN: {pin}")
            else:
                continue
                # print(f"{'name'} NOT FOUND FOR PIN: {pin}")

    # Return DataFrame
    return pd.DataFrame({'ad_id': ad_ids,
                         'ad_name': ad_names,
                         'ad_group_id': ad_group_ids,
                         'ad_group_name': ad_group_names,
                         'campaign_id': campaign_ids,
                         'campaign_name': campaign_names,
                         'date_start': dates,
                         'spend': spends,
                         'clicks': clicks,
                         'impressions': impressions,
                         'publisher': publishers})
