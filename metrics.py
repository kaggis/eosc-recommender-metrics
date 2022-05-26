#!/usr/bin/env python3
import pandas as pd
import numpy as np


class Runtime:
    def __init__(self):
        self.users=None
        self.services=None
        self.user_actions=None
        self.recommendations=None

# decorator to add the text attribute to function
def doc(r):
    def wrapper(f):
        f.text = r
        return f
    return wrapper


# Metrics


@doc('The initial date where metrics are calculated on')
def start(object):
    """
    Calculate the start date where metrics are calculated on
    found in min value between Pandas DataFrame object user_action
    and recommendation
    """
    return str(min(min(object.user_actions['Timestamp']),min(object.recommendations['Timestamp'])))

@doc('The final date where metrics are calculated on')
def end(object):
    """
    Calculate the end date where metrics are calculated on
    found in max value between Pandas DataFrame object user_action
    and recommendation
    """
    return str(max(max(object.user_actions['Timestamp']),max(object.recommendations['Timestamp'])))

@doc('The total number of unique users found in users.csv (if provided), otherwise in user_actions.csv')
def users(object):
    """
    Calculate the total number of unique users 
    found in Pandas DataFrame object users (if provided)
    or user_actions otherwise
    """
    if isinstance(object.users, pd.DataFrame):
        return int(object.users['User'].nunique())
    else:
        return int(object.user_actions.nunique()['User'])


@doc('The total number of unique services found in services.csv (if provided), otherwise in user_actions.csv')
def services(object):
    """
    Calculate the total number of unique services
    found in Pandas DataFrame object services (if provided)
    or user_actions otherwise (from both Source and Target Service)
    """
    if isinstance(object.services, pd.DataFrame):
        return int(object.services.nunique()['Service'])
    else:
        return len(np.unique(np.concatenate([object.user_actions['Source_Service'].unique(),object.user_actions['Target_Service'].unique()])))


@doc('The total number of recommendations found in recommendations.csv')
def recommendations(object):
    """
    Calculate the total number of recommendations
    found in Pandas DataFrame object recommendations
    """
    return len(object.recommendations.index)

@doc('The total number of recommendations for registered users found in recommendations.csv')
def recommendations_registered(object):
    """
    Calculate the total number of recommendations for registered users
    found in Pandas DataFrame object recommendations
    """
    return len(object.recommendations[object.recommendations['User'] != -1].index)


@doc('The total number of recommendations for anonymous users found in recommendations.csv')
def recommendations_anonymous(object):
    """
    Calculate the total number of recommendations for anonymous users
    found in Pandas DataFrame object recommendations
    """
    return recommendations(object)-recommendations_registered(object)


@doc('The percentage (%) of recommendations for registered users to the total recommendations')
def recommendations_registered_perc(object):
    """
    Calculate the percentage (%) of recommendations occurred 
    by registered users to the total recommendations
    found in Pandas DataFrame object recommendations (in two decimals)
    """
    return round(recommendations_registered(object)*100.0/recommendations(object),2)


@doc('The percentage (%) of recommendations for anonymous users to the total recommendations')
def recommendations_anonymous_perc(object):
    """
    Calculate the percentage (%) of recommendations occurred 
    by anonymous users to the total recommendations
    found in Pandas DataFrame object recommendations (in two decimals)
    """
    return round(100.0-recommendations_registered_perc(object),2)


@doc('The total number of user actions found in user_actions.csv')
def user_actions(object):
    """
    Calculate the total number of user_actions
    found in Pandas DataFrame object user_actions
    """
    return len(object.user_actions.index)


@doc('The total number of user actions occurred by registered users found in user_actions.csv')
def user_actions_registered(object):
    """
    Calculate the total number of user_actions occurred by registered users
    found in Pandas DataFrame object user_actions
    """
    return len(object.user_actions[object.user_actions['User'] != -1].index)


@doc('The total number of user actions occurred by anonymous users found in user_actions.csv')
def user_actions_anonymous(object):
    """
    Calculate the total number of user_actions occurred by anonymous users
    found in Pandas DataFrame object user_actions
    """
    return user_actions(object)-user_actions_registered(object)


@doc('The percentage (%) of user actions occurred by registered users to the total user actions')
def user_actions_registered_perc(object):
    """
    Calculate the percentage (%) of user actions occurred 
    by registered users to the total user actions
    found in Pandas DataFrame object user_actions (in two decimals)
    """
    return round(user_actions_registered(object)*100.0/user_actions(object),2)


@doc('The percentage (%) of user actions occurred by anonymous users to the total user actions')
def user_actions_anonymous_perc(object):
    """
    Calculate the percentage (%) of user actions occurred 
    by anonymous users to the total user actions
    found in Pandas DataFrame object user_actions (in two decimals)
    """
    return round(100.0-user_actions_registered_perc(object),2)


@doc('The total number of user actions led to order found in user_actions.csv')
def user_actions_order(object):
    """
    Calculate the total number of user_actions led to order
    found in Pandas DataFrame object user_actions
    """
    return len(object.user_actions[object.user_actions['Reward'] == 1.0].index)


@doc('The total number of user actions led to order by registered users found in user_actions.csv')
def user_actions_order_registered(object):
    """
    Calculate the total number of user_actions led to order by registered users
    found in Pandas DataFrame object user_actions
    """
    return len(object.user_actions[(object.user_actions['Reward'] == 1.0) & (object.user_actions['User'] != -1)].index)


@doc('The total number of user actions led to order by anonymous users found in user_actions.csv')
def user_actions_order_anonymous(object):
    """
    Calculate the total number of user_actions led to order by anonymous users
    found in Pandas DataFrame object user_actions
    """
    return user_actions_order(object)-user_actions_order_registered(object)


@doc('The percentage (%) of user actions occurred by registered users and led to order to the total user actions that led to order')
def user_actions_order_registered_perc(object):
    """
    Calculate the percentage (%) of user actions occurred 
    by registered users and led to order to the total user actions that led to order
    found in Pandas DataFrame object user_actions (in two decimals)
    """
    return round(user_actions_order_registered(object)*100.0/user_actions_order(object),2)


@doc('The percentage (%) of user actions occurred by anonymous users and led to order to the total user actions that led to order')
def user_actions_order_anonymous_perc(object):
    """
    Calculate the percentage (%) of user actions occurred 
    by anonymous users and led to order to the total user actions that led to order
    found in Pandas DataFrame object user_actions (in two decimals)
    """
    return round(100.0-user_actions_order_registered_perc(object),2)


@doc('The total number of user actions assosicated with the recommendation panel found in user_actions.csv')
def user_actions_panel(object):
    """
    Calculate the total number of user_actions assosicated with the recommendation panel
    found in Pandas DataFrame object user_actions
    """
    return len(object.user_actions[object.user_actions['Action'] == 'recommendation_panel'].index)


@doc('The percentage (%) of user actions assosicated with the recommendation panel to the total user actions')
def user_actions_panel_perc(object):
    """
    Calculate the percentage (%) of user actions assosicated with 
    the recommendation panel to the total user actions
    found in Pandas DataFrame object user_actions (in two decimals)
    """
    return round(user_actions_panel(object)*100.0/user_actions(object),2)


@doc('The total number of unique services found in recommendations.csv')
def catalog_coverage(object):
    """
    Calculate the total number of unique services 
    found in recommendations.csv
    """
    return int(object.recommendations.nunique()['Service'])


@doc('The percentage (%) of unique services found in recommedations.csv to the total number of services (provided or found otherwise in user_actions.csv)')
def catalog_coverage_perc(object):
    """
    Calculate the percentage (%) of unique services 
    found in recommedations.csv to the total number 
    of services (provided or found otherwise in user_actions.csv)
    """
    return round(catalog_coverage(object)*100.0/services(object),2)


@doc('The total number of unique users found in recommendations.csv')
def user_coverage(object):
    """
    Calculate the total number of unique users 
    found in recommendations.csv
    """
    return int(object.recommendations.nunique()['User'])


@doc('The percentage (%) of unique users found in recommedations.csv to the total number of users (provided or found otherwise in user_actions.csv)')
def user_coverage_perc(object):
    """
    Calculate the percentage (%) of unique users 
    found in recommedations.csv to the total number 
    of users (provided or found otherwise in user_actions.csv)
    """
    return round(user_coverage(object)*100.0/users(object),2)

@doc('The ratio of user hits divided by the total number of users (user hit: a user that has accessed at least one service that is also a personal recommendation)')
def hit_rate(object):
    """
    For each user get the recommended services and the services the user accessed
    Check if the user has at least one accessed service in recommendations. If yes increase number of hits by one
    Divide by the total number of users
    """
    users = object.users.values.tolist()
    recs = object.recommendations.values.tolist()
    # Fill lookup dictionary with all services recommender per user id
    user_recs = dict()
    for item in recs:
        # skip anonymous users
        if item == -1:
            continue
        user_id = item[0]
        service_id = item[1]
        if user_id in user_recs.keys():
            user_recs[user_id].append(service_id)
        else:
            user_recs[user_id] = [service_id]
    
    hits = 0
    # For each user in users check if his accessed services are in his recommendations
    
    for user in users:
        user_id = user[0]
        # create a set of unique accessed services by user
        services = set(user[1])
        if user_id in user_recs.keys():
            # create a set of unique recommended services to the user
            recommendations = set(user_recs.get(user_id))
            # intersection should include services that have been both accessed by and recommended to the user 
            intersection = services.intersection(recommendations)
            # If the user has at least one service (both recommended and accessed), this user is considered a hit
            if len(intersection) > 0: 
                hits = hits + 1

    

    return round(hits/len(users),5)
