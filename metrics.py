#!/usr/bin/env python3
import pandas as pd
import numpy as np


class Runtime:
    def __init__(self):
        self.users = None
        self.services = None
        self.user_actions = None
        self.user_actions_all = None
        self.recommendations = None
        self.categories = None
        self.scientific_domains = None
        self.provider = None


# decorator to add the text attribute to function as major metric
def metric(txt):
    def wrapper(f):
        f.kind = "metric"
        f.doc = txt
        return f

    return wrapper


# decorator to add the text attribute to function
def statistic(txt):
    def wrapper(f):
        f.kind = "statistic"
        f.doc = txt
        return f

    return wrapper


# Metrics


@statistic("The initial date where metrics are calculated on")
def start(object):
    """
    Calculate the start date where metrics are calculated on
    found in min value between Pandas DataFrame object user_action
    and recommendation
    """
    return str(
        min(
            min(object.user_actions["timestamp"]),
            min(object.recommendations["timestamp"]),
        )
    )


@statistic("The final date where metrics are calculated on")
def end(object):
    """
    Calculate the end date where metrics are calculated on
    found in max value between Pandas DataFrame object user_action
    and recommendation
    """
    return str(
        max(
            max(object.user_actions["timestamp"]),
            max(object.recommendations["timestamp"]),
        )
    )


@statistic("The total number of unique registered users in the system")
def users(object):
    """
    Calculate the total number of unique users
    found in Pandas DataFrame object users (if provided)
    or user_actions otherwise
    """
    return int(object.users["id"].nunique())


@statistic("The total number of unique published services in the system")
def services(object):
    """
    Calculate the total number of unique services
    found in Pandas DataFrame object services (if provided)
    or user_actions otherwise (from both Source and Target Service)
    """
    return int(object.services["id"].nunique())


@statistic("The total number of recommended items")
def recommended_items(object):
    """
    Calculate the total number of recommended items
    found in Pandas DataFrame object recommendations
    """
    return len(object.recommendations.index)


@statistic("The total number of recommended items towards registered users")
def recommended_items_to_registered_users(object):
    """
    Calculate the total number of recommendations for registered users
    found in Pandas DataFrame object recommendations
    """
    return len(object.recommendations[object.recommendations["user_id"] != -1]
               .index)


@statistic("The total number of recommended items towards anonymous users")
def recommended_items_to_anonymous_users(object):
    """
    Calculate the total number of recommendations for anonymous users
    found in Pandas DataFrame object recommendations
    """
    return (recommended_items(object) -
            recommended_items_to_registered_users(object))


@statistic(
    "The percentage (%) of recommended items towards registered users to "
    "the total recommended items"
)
def recommended_items_to_registered_users_perc(object):
    """
    Calculate the percentage (%) of recommendations occurred
    by registered users to the total recommendations
    found in Pandas DataFrame object recommendations (in two decimals)
    """
    return round(
        recommended_items_to_registered_users(object)
        * 100.0
        / recommended_items(object),
        2,
    )


@statistic(
    "The percentage (%) of recommended items towards anonymous users to the "
    "total recommended items"
)
def recommended_items_to_anonymous_users_perc(object):
    """
    Calculate the percentage (%) of recommendations occurred
    by anonymous users to the total recommendations
    found in Pandas DataFrame object recommendations (in two decimals)
    """
    return round(100.0 - recommended_items_to_registered_users_perc(object), 2)


@statistic("The total number of user actions")
def user_actions(object):
    """
    Calculate the total number of user_actions
    found in Pandas DataFrame object user_actions
    """
    return len(object.user_actions.index)


@statistic("The total number of user actions occurred by registered users")
def user_actions_registered(object):
    """
    Calculate the total number of user_actions occurred by registered users
    found in Pandas DataFrame object user_actions
    """
    return len(object.user_actions[object.user_actions["user_id"] != -1].index)


@statistic("The total number of user actions occurred by anonymous users")
def user_actions_anonymous(object):
    """
    Calculate the total number of user_actions occurred by anonymous users
    found in Pandas DataFrame object user_actions
    """
    return user_actions(object) - user_actions_registered(object)


@statistic(
    "The percentage (%) of user actions occurred by registered users to the "
    "total user actions"
)
def user_actions_registered_perc(object):
    """
    Calculate the percentage (%) of user actions occurred
    by registered users to the total user actions
    found in Pandas DataFrame object user_actions (in two decimals)
    """
    return round((user_actions_registered(object) * 100.0
                  / user_actions(object)), 2)


@statistic(
    "The percentage (%) of user actions occurred by anonymous users to the "
    "total user actions"
)
def user_actions_anonymous_perc(object):
    """
    Calculate the percentage (%) of user actions occurred
    by anonymous users to the total user actions
    found in Pandas DataFrame object user_actions (in two decimals)
    """
    return round(100.0 - user_actions_registered_perc(object), 2)


@statistic("The total number of user actions led to order")
def user_actions_order(object):
    """
    Calculate the total number of user_actions led to order
    found in Pandas DataFrame object user_actions
    """
    return len(object.user_actions[object.user_actions["reward"] == 1.0].index)


@statistic("The total number of user actions led to order by registered users")
def user_actions_order_registered(object):
    """
    Calculate the total number of user_actions led to order by registered users
    found in Pandas DataFrame object user_actions
    """
    return len(
        object.user_actions[
            (object.user_actions["reward"] == 1.0) &
            (object.user_actions["user_id"] != -1)
        ].index
    )


@statistic("The total number of user actions led to order by anonymous users")
def user_actions_order_anonymous(object):
    """
    Calculate the total number of user_actions led to order by anonymous users
    found in Pandas DataFrame object user_actions
    """
    return user_actions_order(object) - user_actions_order_registered(object)


@statistic(
    "The percentage (%) of user actions occurred by registered users and led "
    "to order to the total user actions that led to order"
)
def user_actions_order_registered_perc(object):
    """
    Calculate the percentage (%) of user actions occurred
    by registered users and led to order to the total user actions that led to
    order found in Pandas DataFrame object user_actions (in two decimals)
    """
    try:
        return round((user_actions_order_registered(object) * 100.0 /
                      user_actions_order(object)), 2)
    except ZeroDivisionError:
        return 0


@statistic(
    "The percentage (%) of user actions occurred by anonymous users and led "
    "to order to the total user actions that led to order"
)
def user_actions_order_anonymous_perc(object):
    """
    Calculate the percentage (%) of user actions occurred
    by anonymous users and led to order to the total user actions that led
    to order found in Pandas DataFrame object user_actions (in two decimals)
    """
    return round(100.0 - user_actions_order_registered_perc(object), 2)


@statistic("The total number of user actions assosicated with the "
           "recommendation panel")
def user_actions_panel(object):
    """
    Calculate the total number of user_actions assosicated with
    the recommendation panel found in Pandas DataFrame object user_actions
    """
    return len(
        object.user_actions[
            object.user_actions["panel"] == "recommendation_panel"
        ].index
    )


@statistic(
    "The percentage (%) of user actions assosicated with the recommendation "
    "panel to the total user actions"
)
def user_actions_panel_perc(object):
    """
    Calculate the percentage (%) of user actions assosicated with
    the recommendation panel to the total user actions
    found in Pandas DataFrame object user_actions (in two decimals)
    """
    return round(user_actions_panel(object) * 100.0 / user_actions(object), 2)


@statistic("The total number of unique recommended items")
def total_unique_recommended_items(object):
    """
    Calculate the total number of unique items found in recommendations
    """
    return int(object.recommendations.nunique()["resource_id"])


@metric("The percentage (%) of unique services  to the total number "
        "of services")
def catalog_coverage(object):
    """
    Calculate the percentage (%) of unique services
    found to the total number of services
    """
    return round((total_unique_recommended_items(object) * 100.0 /
                  services(object)), 2)


@statistic("The total number of unique users found in recommendations")
def total_unique_users_recommended(object):
    """
    Calculate the total number of unique users found in recommendations
    """
    return int(object.recommendations.nunique()["user_id"])


@metric("The percentage (%) of unique users to the total number of users")
def user_coverage(object):
    """
    Calculate the percentage (%) of unique users  to the total number of users
    """
    return round((total_unique_users_recommended(object) * 100.0 /
                  users(object)), 2)


@metric(
    "The ratio of user hits divided by the total number of users "
    "(user hit: a user that has accessed at least one service "
    "that is also a personal recommendation)"
)
def hit_rate(object):
    """
    1) For each user get the recommended services and the services the user
    accessed
    2) Check if the user has at least one accessed service in recommendations
    3) If yes increase number of hits by one
    4) Divide by the total number of users
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
    # For each user in users check if his accessed services are in
    # his recommendations

    for user in users:
        user_id = user[0]
        # create a set of unique accessed services by user
        services = set(user[1])
        if user_id in user_recs.keys():
            # create a set of unique recommended services to the user
            recommendations = set(user_recs.get(user_id))
            # intersection should include services that have been both
            # accessed by and recommended to the user
            intersection = services.intersection(recommendations)
            # If the user has at least one service (both recommended and
            # accessed), this user is considered a hit
            if len(intersection) > 0:
                hits = hits + 1

    return round(hits / len(users), 5)


@metric(
    "The number of user clicks through recommendations panels divided by the "
    "total times recommendation panels were presented to users. "
    "Takes into account all historical data of user actions"
)
def click_through_rate(object):
    """
    Get only the user actions that present a recommendation panel to the user
    in the source page
    Those are actions with the following source paths:
     - /services
     - /services/
     - /services/c/{any category name}
    1) Count the items in above list as they represent the times
    recommendations panels were
    presented to the users of the portal
    2) Narrow the above list into a new subset by selecting only user actions
    that originate
    from a recommendation panel
    3) Those are actions that have the 'recommendation' string in the
    Action column
    4) Count the items in the subset as they represent the times users clicked
    through recommendations
    5) Divide the items of the subset with the items of the first list to get
    the click-through rate
    """

    # get user actions
    user_actions_recpanel_views = object.user_actions[
        object.user_actions['source_path'].isin(
            ['/services', '/services/']
        ) |
        object.user_actions['source_path'].str.startswith('/services/c/')
    ]

    user_actions_recpanel_clicks = user_actions_recpanel_views[
        user_actions_recpanel_views['panel'] == 'recommendation_panel'
    ]

    return round(
        len(user_actions_recpanel_clicks) / len(user_actions_recpanel_views), 2
    )


@metric(
    "The diversity of the recommendations according to Shannon Entropy. "
    "The entropy is 0 when a single item is always chosen or recommended, "
    "and log n when n items are chosen or recommended equally often."
)
def diversity(object, anonymous=False):
    """
    Calculate Shannon Entropy. The entropy is 0 when a single item is always
    chosen or recommended, and log n when n items are chosen or recommended
    equally often.
    """
    # keep recommendations with or without anonymous suggestions
    # based on anonymous flag (default=False, i.e. ignore anonymous)
    if anonymous:
        recs = object.recommendations
    else:
        recs = object.recommendations[
            (object.recommendations["user_id"] != -1)
        ]

    # this variable keeps the sum of user_norm (where user_norm is
    # the count of how many times a User has been suggested)
    # however since no cutoff at per user recommendations is applied and
    # also since each recommendation entry is one-to-one <user id> <service id>
    # then the total number of recommendations is equal to this sum

    # remember that recommendations have been previously filtered based
    # on the existance of users and services

    # item_count
    # group recommendations entries by service id and
    # then count how many times each service has been suggested
    gr_service = recs.groupby(["resource_id"]).count()

    # create a dictionary of item_count in order to
    # map the service id to the respective item_count
    # key=<service id> and value=<item_count>
    d_service = gr_service["user_id"].to_dict()

    # each element represent the service's recommendations occurance
    # e.g. [1,6,7]
    # a service was recommended 1 time, another 6 times and another 7 times
    services_recommendation_count = np.array(list(d_service.values()))

    # the total number of recommendations
    n_recommendations = services_recommendation_count.sum()

    # element-wise computations
    # (division for each service's recommendations occurance)
    recommended_probability = services_recommendation_count / n_recommendations

    # H=-Sum(p*logp) [element-wise]
    shannon_entropy = -np.sum(
        recommended_probability * np.log2(recommended_probability)
    )

    return round(shannon_entropy, 4)


@metric("The novelty expresses how often new and unseen items are"
        " recommended to users")
def novelty(object):
    """Calculate novelty of recommendations
    using the n=SUM(-log(p(i)))/|R| formula"""
    # published services
    services_pub = object.services["id"]
    # recommended services to authenticated users
    services_rec = (object
                    .recommendations[object.recommendations["user_id"] != -1]
                    ["resource_id"])

    # services that are published and recommended
    services_recpub = services_rec[services_rec
                                   .isin(services_pub)].drop_duplicates()

    # user actions
    ua = object.user_actions
    # user actions filtered if src and target the same. Also filter out
    # if target equals -1 and filter out anonymous users
    ua_serv_view = ua[
        (ua["source_resource_id"] != ua["target_resource_id"])
        & (ua["target_resource_id"] != -1)
        & (ua["user_id"] != -1)
    ]

    # count service views by service id (sorted by service id)
    services_viewed = (ua_serv_view["target_resource_id"]
                       .value_counts().sort_index())

    # create a table for each recommended service with columns
    # for number of views, p(i) and -log(pi)
    r_services = pd.DataFrame(index=services_recpub).sort_index()
    # add views column to assign views to each recommended service
    r_services["views"] = services_viewed

    # count the total service views in order to compute the portions p(i)
    total_views = r_services["views"].sum()

    # count the total recommended services |R|
    total_services = len(r_services)
    # compute the p(i) of each recommeneded service
    r_services["pi"] = r_services["views"] / total_views
    # calculate the negative log of the p(i).
    r_services["-logpi"] = -1 * np.log2(r_services["pi"])

    # calculate novelty based on formula n=SUM(-log(p(i)))/|R|
    novelty = r_services["-logpi"].sum() / total_services

    return round(novelty, 4)


@metric(
    "The diversity of the recommendations according to GiniIndex. "
    "The index is 0 when all items are "
    "chosen equally often, and 1 when a single item is always chosen."
)
def diversity_gini(object, anonymous=False):
    """
    Calculate GiniIndex based on
    https://elliot.readthedocs.io/en/latest/_modules/elliot/evaluation
    /metrics/diversity/gini_index/gini_index.html#GiniIndex
    (see book https://link.springer.com/10.1007/978-1-4939-7131-2_110158)
    """
    # keep recommendations with or without anonymous suggestions
    # based on anonymous flag (default=False, i.e. ignore anonymous)
    if anonymous:
        recs = object.recommendations
    else:
        recs = object.recommendations[
            (object.recommendations["user_id"] != -1)
        ]

    # this variable keeps the sum of user_norm (where user_norm is
    # the count of how many times a User has been suggested)
    # however since no cutoff at per user recommendations is applied and
    # also since each recommendation entry is one-to-one <user id> <service id>
    # then the total number of recommendations is equal to this sum
    free_norm = len(recs.index)

    # item_count
    # group recommendations entries by service id and
    # then count how many times each service has been suggested
    gr_service = recs.groupby(["resource_id"]).count()

    # create a dictionary of item_count in order to
    # map the service id to the respective item_count
    # key=<service id> and value=<item_count>
    d_service = gr_service["user_id"].to_dict()

    # total number of recommended services
    n_recommended_items = len(d_service)

    # total number of services
    num_items = services(object)

    # create a zero list
    # to calculate gini index including elements with 0 occurance
    zeros = [0] * (num_items - n_recommended_items)

    gini = sum(
        [
            (2 * (j + 1) - num_items - 1) * (cs / free_norm)
            for j, cs in enumerate(zeros + sorted(d_service.values()))
        ]
    )

    gini /= num_items - 1

    return round(gini, 4)


@metric("The Top 5 recommended services according to recommendations entries")
def top5_services_recommended(
    object, k=5, base="https://marketplace.eosc-portal.eu", anonymous=False
):
    """
    Calculate the Top 5 recommended service according to
    the recommendations entries.
    Return a list of list with the elements:
        #   (i) service id
        #  (ii) service name
        # (iii) service page appended with base (to create the URL)
        #  (iv) total number of recommendations of the service
        #   (v) percentage of the (iv) to the total number of recommendations
        #       expressed in %, with or without anonymous,
        #       based on the function's flag
    Service's info is being retrieved from the external source
    (i.e. each line forms: service_id, service_name, page_id)
    """
    # keep recommendations with or without anonymous suggestions
    # based on anonymous flag (default=False, i.e. ignore anonymous)
    if anonymous:
        recs = object.recommendations
    else:
        recs = object.recommendations[
            (object.recommendations["user_id"] != -1)
        ]

    # item_count
    # group recommendations entries by service id and
    # then count how many times each service has been suggested
    gr_service = recs.groupby(["resource_id"]).count()

    # create a dictionary of item_count in order to
    # map the service id to the respective item_count
    # key=<service id> and value=<item_count>
    d_service = gr_service["user_id"].to_dict()

    # convert dictionary to double list (list of lists)
    # where the sublist is <service_id> <item_count>
    # and sort them from max to min <item_count>
    l_service = list(map(lambda x: [x, d_service[x]], d_service))
    l_service.sort(key=lambda x: x[1], reverse=True)

    # get only the first k elements
    l_service = l_service[:k]

    topk_services = []

    for service in l_service:
        # get service's info from dataframe
        _df_service = (object
                       .services[(object
                                  .services["id"]
                                  .isin([service[0]]))])

        # append a list with the elements:
        #   (i) service id
        #  (ii) service name
        # (iii) service page appended with base (to create the URL)
        #  (iv) total number of recommendations of the service
        #   (v) percentage of the (iv) to the total number of recommendations
        #       expressed in %, with or without anonymous,
        #       based on the function's flag
        topk_services.append(
            {
                "service_id": service[0],
                "service_name": str(_df_service["name"].item()),
                "service_url": base + str(_df_service["path"].item()),
                "recommendations": {
                    "value": service[1],
                    "percentage": round(100 * service[1] / len(recs.index), 2),
                    "of_total": len(recs.index),
                },
            }
        )

    return topk_services


@metric("The Top 5 ordered services according to user actions entries")
def top5_services_ordered(
    object, k=5, base="https://marketplace.eosc-portal.eu", anonymous=False
):
    """
    Calculate the Top 5 ordered services according to user actions entries.
    User actions with Target Pages that lead to unknown services (=-1)
    are being ignored.
    Return a list of list with the elements:
        #   (i) service id
        #  (ii) service name
        # (iii) service page appended with base (to create the URL)
        #  (iv) total number of orders of the service
        #   (v) percentage of the (iv) to the total number of orders
        #       expressed in %, with or without anonymous,
        #       based on the function's flag
    """
    # keep user actions with or without anonymous suggestions
    # based on anonymous flag (default=False, i.e. ignore anonymous)
    # user_actions with Target Pages that lead to unknown services (=-1)
    # are being ignored
    if anonymous:
        uas = object.user_actions[
            (object.user_actions["reward"] == 1.0)
            & (object.user_actions["target_resource_id"] != -1)
            & (object.user_actions["user_id"] != -1)
        ]
    else:
        uas = object.user_actions[
            (object.user_actions["reward"] == 1.0)
            & (object.user_actions["target_resource_id"] != -1)
        ]

    # item_count
    # group user_actions entries by service id and
    # then count how many times each service has been suggested
    gr_service = uas.groupby(["target_resource_id"]).count()

    # create a dictionary of item_count in order to
    # map the service id to the respective item_count
    # key=<service id> and value=<item_count>
    d_service = gr_service["user_id"].to_dict()

    # convert dictionary to double list (list of lists)
    # where the sublist is <service_id> <item_count>
    # and sort them from max to min <item_count>
    l_service = list(map(lambda x: [x, d_service[x]], d_service))
    l_service.sort(key=lambda x: x[1], reverse=True)

    # get only the first k elements
    l_service = l_service[:k]

    topk_services = []

    for service in l_service:
        # get service's info from dataframe
        _df_service = object.services[object.services["id"]
                                      .isin([service[0]])]
        # append a list with the elements:
        #   (i) service id
        #  (ii) service name
        # (iii) service page appended with base (to create the URL)
        #  (iv) total number of orders of the service
        #   (v) percentage of the (iv) to the total number of orders
        #       expressed in %, with or without anonymous,
        #       based on the function's flag
        topk_services.append(
            {
                "service_id": service[0],
                "service_name": str(_df_service["name"].item()),
                "service_url": base + str(_df_service["path"].item()),
                "orders": {
                    "value": service[1],
                    "percentage": round(100 * service[1] / len(uas.index), 2),
                    "of_total": len(uas.index),
                },
            }
        )

    return topk_services


@metric("The Top 5 recommended categories according to recommendations entries"
        )
def top5_categories_recommended(object, k=5, anonymous=False):
    """
    Calculate the Top 5 recommended categories according to
    the recommendations entries.
    Return a list of list with the elements:
        #  (i) category id
        #  (ii) category name (according to category collection)
        #  (iii) total number of recommendations of the category
        #  (iv) percentage of the (iii) to the total number of recommendations
        #       expressed in %, with or without anonymous,
        #       based on the function's flag
    Category's info is being retrieved from the Cyfronet MongoDB source
    """
    # keep recommendations with or without anonymous suggestions
    # based on anonymous flag (default=False, i.e. ignore anonymous)
    if anonymous:
        recs = object.recommendations
    else:
        recs = object.recommendations[
            (object.recommendations["user_id"] != -1)
        ]

    # rename the column at a copy (not in place) for more readable processing
    _services = object.services.rename(columns={'id': 'resource_id'})

    # create an inner join between the recommendations collection
    # and the services collection
    # since a category is a list of category ids
    # the rows are further expanded (exploded) into rows per category id
    # inner join means that categories must exist in both services collection
    # and recommendations collection
    merged = recs.merge(_services, on='resource_id')
    exp = merged.explode('category')

    # count the total orders where the associated services have categories
    total = len(merged.dropna(subset=['category']))

    # count categories' ids and covert pandas series to dataframe
    # user_id holds the same values with all other columns
    # it is just the first column
    cat = exp.groupby(["category"])['user_id'].count().to_frame()

    # reset indexes and rename columns accordingly
    cat = cat.reset_index().rename(columns={'category': 'id',
                                            'user_id': 'count'})

    # create a second inner join with the categories
    # in order to retrieve the name of the categories
    # inner join means that category must exist in both categories collection
    # and cat collection
    full_cat = cat.merge(object.categories, on='id')

    # sort based on count
    # get top k
    # and convert it to a list of dictionaries
    # where each dictionary equals to the df's record,
    # and each column of the df is the key of the dictionary
    topk = full_cat.sort_values('count', ascending=False).head(k).to_dict(
                                orient='records')

    # create similar format to other kpis
    topk_categories = []

    for category in topk:

        # append a list with the elements:
        #  (i) category id
        #  (ii) category name (retrieved from category collection)
        #  (iii) total number of recommendations of the service
        #  (iv) percentage of the (iii) to the total number of recommendations
        #       expressed in %, with or without anonymous,
        #       based on the function's flag
        topk_categories.append(
            {
                "category_id": category['id'],
                "category_name": category['name'],
                "recommendations": {
                    "value": category['count'],
                    "percentage": round(100 * category['count'] / total, 2),
                    "of_total": total,
                },
            }
        )

    return topk_categories


@metric("The Top 5 recommended scientific domains according to recommendations\
entries")
def top5_scientific_domains_recommended(object, k=5, anonymous=False):
    """
    Calculate the Top 5 recommended scientific domains according to
    the recommendations entries.
    Return a list of list with the elements:
    #  (i) scientific domain id
    #  (ii) scientific domain name (according to scientific_domain collection)
    #  (iii) total number of recommendations of the scientific domain
    #  (iv) percentage of the (iii) to the total number of recommendations
    #       expressed in %, with or without anonymous,
    #       based on the function's flag
    Scientif. domain's info is being retrieved from the Cyfronet MongoDB source
    """
    # keep recommendations with or without anonymous suggestions
    # based on anonymous flag (default=False, i.e. ignore anonymous)
    if anonymous:
        recs = object.recommendations
    else:
        recs = object.recommendations[
            (object.recommendations["user_id"] != -1)
        ]

    # rename the column at a copy (not in place) for more readable processing
    _services = object.services.rename(columns={'id': 'resource_id'})

    # create an inner join between the recommendations collection
    # and the services collection
    # since a scientific domain is a list of scientific domain ids
    # the rows are further expanded (exploded) into rows per scient. domain id
    # inner join means that scient. domains must exist in both services
    # and recommendations collection
    merged = recs.merge(_services, on='resource_id')
    exp = merged.explode('scientific_domain')

    # count the total orders where the associated services have scient. domain
    total = len(merged.dropna(subset=['scientific_domain']))

    # count scientific domains' ids and covert pandas series to dataframe
    # user_id holds the same values with all other columns
    # it is just the first column
    cat = exp.groupby(["scientific_domain"])['user_id'].count().to_frame()

    # reset indexes and rename columns accordingly
    cat = cat.reset_index().rename(columns={'scientific_domain': 'id',
                                            'user_id': 'count'})

    # create a second inner join with the scientific domains
    # in order to retrieve the name of the scientific domains
    # inner join means that scientific domain must exist in both
    # scientific domains collection and cat collection
    full_cat = cat.merge(object.scientific_domains, on='id')

    # sort based on count
    # get top k
    # and convert it to a list of dictionaries
    # where each dictionary equals to the df's record,
    # and each column of the df is the key of the dictionary
    topk = full_cat.sort_values('count', ascending=False).head(k).to_dict(
                                orient='records')

    # create similar format to other kpis
    topk_scientific_domains = []

    for scientific_domain in topk:

        # append a list with the elements:
        #  (i) scientific domains id
        #  (ii) scientific domains name (retrieved from
        #                                scientific_domain collection)
        #  (iii) total number of recommendations of the service
        #  (iv) percentage of the (iii) to the total number of recommendations
        #       expressed in %, with or without anonymous,
        #       based on the function's flag
        topk_scientific_domains.append(
            {
                "scientific_domain_id": scientific_domain['id'],
                "scientific_domain_name": scientific_domain['name'],
                "recommendations": {
                    "value": scientific_domain['count'],
                    "percentage": round(100 * scientific_domain['count']
                                        / total, 2),
                    "of_total": total,
                },
            }
        )

    return topk_scientific_domains


@statistic("A dictionary of the number of recommended items per day")
def recommended_items_per_day(object):
    """
    It returns a a timeseries of recommended item counts per day.
    Each timeseries item has two fields: date and value
    """
    # count recommendations for each day found in entries
    res = (
        object.recommendations.groupby(by=object
                                       .recommendations["timestamp"].dt.date)
        .count()
        .iloc[:, 0]
    )

    # create a Series with period's start and end times and value of 0
    init = pd.Series(
        [0, 0],
        index=[
            pd.to_datetime(start(object)).date(),
            pd.to_datetime(end(object)).date(),
        ],
    )

    # remove duplicate entries for corner cases where start and end time match
    init.drop_duplicates(keep="first", inplace=True)

    # append above two indexes and values (i.e. 0) to the Series
    # with axis=1, same indexes are being merged
    # since dataframe is created, get the first column
    res = pd.concat([res, init], ignore_index=False, axis=1).iloc[:, 0]

    # convert Nan values created by the concatenation to 0
    # and change data type back to int
    res = res.fillna(0).astype(int)

    # fill the in between days with zero user_actions
    res = res.asfreq("D", fill_value=0)

    # convert datetimeindex to string
    res.index = res.index.format()

    # convert series to dataframe with extra column having the dates
    res = res.to_frame().reset_index()

    # rename columns to date, value
    res.rename(columns={res.columns[0]: "date", res.columns[1]: "value"},
               inplace=True)

    # return a list of objects with date and value fields
    return res.to_dict(orient="records")


@statistic("A dictionary of the number of recommended items per month")
def recommended_items_per_month(object):
    """
    It returns a a timeseries of recommended item counts per month.
    Each timeseries item has two fields: date and value
    """
    # count recommendations for each day found in entries
    res = (
        object.recommendations.groupby(by=object
                                       .recommendations["timestamp"].dt.date)
        .count()
        .iloc[:, 0]
    )

    # create a Series with period's start and end times and value of 0
    init = pd.Series(
        [0, 0],
        index=[
            pd.to_datetime(start(object)).date(),
            pd.to_datetime(end(object)).date(),
        ],
    )

    # remove duplicate entries for corner cases where start and end time match
    init.drop_duplicates(keep="first", inplace=True)

    # append above two indexes and values (i.e. 0) to the Series
    # with axis=1, same indexes are being merged
    # since dataframe is created, get the first column
    res = pd.concat([res, init], ignore_index=False, axis=1).iloc[:, 0]

    # convert Nan values created by the concatenation to 0
    # and change data type back to int
    res = res.fillna(0).astype(int)

    # fill the in between days with zero user_actions
    res = res.asfreq("D", fill_value=0)

    # resample results in Monthly granularity
    res = res.resample('M').sum()

    # convert datetimeindex to string
    res.index = res.index.format()

    # convert series to dataframe with extra column having the dates
    res = res.to_frame().reset_index()

    # rename columns to date, value
    res.rename(columns={res.columns[0]: "date", res.columns[1]: "value"},
               inplace=True)

    # keep YYYY-MM format in date field
    res['date'] = res['date'].str[:-3]

    # return a list of objects with date and value fields
    return res.to_dict(orient="records")


@statistic("A dictionary of the number of user actions per day")
def user_actions_per_day(object):
    """
    It returns a statistical report in dictionary format. Specifically, the key
    is set for each particular day found and its value contains the respective
    number of user_actions committed. The dictionary includes all in-between
    days (obviously, with the count set to zero). User_actions are already
    filtered by those where the user or service does not exist in users'
    or services' catalogs.
    """
    # Since user_actions is in use, user actions when user
    # or service does not exist in users' or services'
    # catalogs have been removed

    # count user_actions for each day found in entries
    res = (
        object.user_actions.groupby(by=object.user_actions["timestamp"]
                                    .dt.date)
        .count()
        .iloc[:, 0]
    )

    # create a Series with period's start and end times and value of 0
    init = pd.Series(
        [0, 0],
        index=[
            pd.to_datetime(start(object)).date(),
            pd.to_datetime(end(object)).date(),
        ],
    )

    # remove duplicate entries for corner cases where start and end time match
    init.drop_duplicates(keep="first", inplace=True)

    # append above two indexes and values (i.e. 0) to the Series
    # with axis=1, same indexes are being merged
    # since dataframe is created, get the first column
    res = pd.concat([res, init], ignore_index=False, axis=1).iloc[:, 0]

    # convert Nan values created by the concatenation to 0
    # and change data type back to int
    res = res.fillna(0).astype(int)

    # fill the in between days with zero user_actions
    res = res.asfreq("D", fill_value=0)

    # convert datetimeindex to string
    res.index = res.index.format()

    # convert series to dataframe with extra column having the dates
    res = res.to_frame().reset_index()

    # rename columns to date, value
    res.rename(columns={res.columns[0]: "date", res.columns[1]: "value"},
               inplace=True)

    # return a list of objects with date and value fields
    return res.to_dict(orient="records")


@statistic("A dictionary of the number of user actions per month")
def user_actions_per_month(object):
    """
    It returns a statistical report in dictionary format. Specifically, the key
    is set for each specific month found and its value contains the respective
    number of user_actions committed. The dictionary includes all in-between
    months (obviously, with the count set to zero). User_actions are already
    filtered by those where the user or service does not exist in users'
    or services' catalogs.
    """
    # Since user_actions is in use, user actions when user
    # or service does not exist in users' or services'
    # catalogs have been removed

    # count user_actions for each day found in entries
    res = (
        object.user_actions.groupby(by=object.user_actions["timestamp"]
                                    .dt.date)
        .count()
        .iloc[:, 0]
    )

    # create a Series with period's start and end times and value of 0
    init = pd.Series(
        [0, 0],
        index=[
            pd.to_datetime(start(object)).date(),
            pd.to_datetime(end(object)).date(),
        ],
    )

    # remove duplicate entries for corner cases where start and end time match
    init.drop_duplicates(keep="first", inplace=True)

    # append above two indexes and values (i.e. 0) to the Series
    # with axis=1, same indexes are being merged
    # since dataframe is created, get the first column
    res = pd.concat([res, init], ignore_index=False, axis=1).iloc[:, 0]

    # convert Nan values created by the concatenation to 0
    # and change data type back to int
    res = res.fillna(0).astype(int)

    # fill the in between days with zero user_actions
    res = res.asfreq("D", fill_value=0)

    # resample results in Monthly granularity
    res = res.resample('M').sum()

    # convert datetimeindex to string
    res.index = res.index.format()

    # convert series to dataframe with extra column having the dates
    res = res.to_frame().reset_index()

    # rename columns to date, value
    res.rename(columns={res.columns[0]: "date", res.columns[1]: "value"},
               inplace=True)

    # keep YYYY-MM format in date field
    res['date'] = res['date'].str[:-3]

    # return a list of objects with date and value fields
    return res.to_dict(orient="records")


@metric(
    "The mean value of the accuracy score found for each user defined by the "
    "fraction of the number of the correct predictions by the total number "
    "of predictions"
)
def accuracy(object):
    """
    Calculate the accuracy score found for each and retrieve the mean value.
    The score is calculated by dividing the number of the correct predictions
    by the total number of predictions.
    """
    # a list of unique services' ids found in Datastore
    services_list = object.services["id"].unique().tolist()
    # the length of the above value
    len_services = services(object)

    def score(x):
        """
        Inner function called at each row of the final dataframe
        in order to calculate the accuracy score for each row (=user)
        """
        # 'Services' header indicates the accessed services' list,
        # while the 'Service' header indicates the recommended services' list
        # if accessed or recommended services' list is empty
        # it does not calculate any further computations
        # else for each service found in services_list,
        # put 1 or 0 if it is also found in the accessed or
        # recommended services respectively
        if not x["accessed_resources"]:
            true_values = np.array([0] * len_services)
        else:
            true_values = np.array(
                list(map(lambda s: 1 if s in x["accessed_resources"] else 0,
                     services_list))
            )
        if not x["resource_id"]:
            pred_values = np.array([0] * len_services)
        else:
            pred_values = np.array(
                list(map(lambda s: 1 if s in x["resource_id"] else 0,
                     services_list))
            )

        # Calculate the accuracy score by computing the average of the
        # returned array.
        # The returned array is a True/False array when the respective
        # element of true_values is equal or not to the respective
        # element of pred_values
        x["resource_id"] = np.average(true_values == pred_values)
        # return the row, where the 'resources' column has the accuracy
        # score now
        return x

    # a matrix of User ids and the respective accessed services' ids
    access_df = object.users[["id", "accessed_resources"]]

    # a matrix of User ids and the respective recommended services' ids
    rec_df = (
        object.recommendations[["user_id", "resource_id"]]
        .groupby(["user_id"])
        .agg({"resource_id": lambda x: x.unique().tolist()})
        .reset_index()
    )

    # performs a left join on User id, which means that nan values
    # are set for cases where no recommendations were made
    data = pd.merge(access_df, rec_df, left_on="id", right_on="user_id",
                    how="left")
    # convert nan values to zeros, in order to be handled easily
    # by the inner function
    data.fillna(0, inplace=True)
    # apply the score function row-wise
    data = data.apply(score, axis=1)

    # return the mean value of all users' accuracy score
    # up to 4 digits precision
    return round(data["resource_id"].mean(), 4)
