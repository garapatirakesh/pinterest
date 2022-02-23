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
