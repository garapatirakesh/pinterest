def get_ad_group_names(ad_groups, headers):
    print(f"Getting Ad Group Names...{ad_groups}")
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
                print(get_ad_group_name_response)
                # Look for Data
                #parse_ad_group_name(ad_groups, ad_group, get_ad_group_name_response)

                if 'data' in get_ad_group_name_response:
                    data = get_ad_group_name_response['data']
                    # Add name to ad_groups
                    if data is not None:
                        ad_group_camp_id = data['campaign_id']
                        ad_groups[ad_group]['campaign_id'] = ad_group_camp_id
                        ad_group_name = data['name']
                        ad_groups[ad_group]['name'] = ad_group_name

                        params = {'order': 'DESCENDING'}

                        # Request Campaign IDs
                        try:
                            get_campaigns_url = f"https://api.pinterest.com/ads/v4/advertisers/549755856460/campaigns/{str(ad_group_camp_id)}/"
                            get_campaigns_request = requests_retry_session().get(get_campaigns_url, params=params,
                                                                                 headers=headers)
                            get_campaigns_response = get_campaigns_request.json()
                            #print(get_campaigns_response)
                            if 'data' in get_campaigns_response:
                                data = get_ad_group_name_response['data']
                                if data is not None:
                                    # Get Campaign ID
                                    #print("inside for")
                                    #print(data)
                                    campaign_id = int(data['id'])
                                    campaign_name=data['name']
                                    print(campaign_id, campaign_name)
                                    ad_groups[ad_group]['campaign_name'] = campaign_name
                                    ad_groups[ad_group]['ad_group_id'] = campaign_id
                                    ad_groups[ad_group]['campaign_id'] = ad_group_camp_id
                        except Exception as x:
                            print('Failed at campaign name:(', x.__class__.__name__)

        except Exception as x:
                print('Failed at get_ad_group_names ', x.__class__.__name__)
                sleep(30)
                print(f"Retrying {ad_group}")
                


                
                
                
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
        print(ad_group)
        print(length)
        for i in range(length):
            if 'campaign_id' in ad_groups[ad_group]:
                print("yes")
                campaign_id = ad_groups[ad_group]['campaign_id']
            else:
                print(f"AD GROUP {ad_group} IS MISSING campaign_id")
                continue

            if 'campaign_name' in ad_groups[ad_group]:
                campaign_name = ad_groups[ad_group]['campaign_name']
            else:
                print(f"AD GROUP {ad_group} IS MISSING campaign_name")
                continue

            if 'name' in ad_groups[ad_group]:
                ad_group_name = ad_groups[ad_group]['name']
            else:
                print(f"AD GROUP {ad_group} IS MISSING name")
                continue

            if 'date_start' in ad_groups[ad_group]:
                date = ad_groups[ad_group]['date_start'][i]
            else:
                print(f"AD GROUP {ad_group} IS MISSING dates")
                continue

            if 'spend' in ad_groups[ad_group]:
                spend = ad_groups[ad_group]['spend'][i]
            else:
                print(f"AD GROUP {ad_group} IS MISSING spend")
                continue

            if 'clicks' in ad_groups[ad_group]:
                click = ad_groups[ad_group]['clicks'][i]
            else:
                print(f"AD GROUP {ad_group} IS MISSING clicks")
                continue

            if 'impressions' in ad_groups[ad_group]:
                impression = ad_groups[ad_group]['impressions'][i]
            else:
                print(f"AD GROUP {ad_group} IS MISSING impressions")
                continue
            print(campaign_id,campaign_name,ad_group_name,date)
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
