#!/usr/bin/env python3
import pandas as pd
import numpy as np


class Runtime:
    def __init__(self, legacy=False):
        self.schema = 'legacy' if legacy else 'current'
        self.id_field = 'user_id' if legacy else 'aai_uid'
        self.users = None
        self.items = None
        self.user_actions = None
        self.user_actions_all = None
        self.recommendations = None
        self.categories = None
        self.scientific_domains = None
        self.provider = None
        self.errors = []


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


# decorator to continue the procedure
# after fatal error in statistic/metric calculation
def pass_on_error(func):
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            print('Error occurred in: {}. "{}"'.format(func.__name__, str(e)))

            # find the object which contains the errors variable
            # append it with function names for those that exceptions occurred
            _args = list(filter(lambda x: isinstance(x, Runtime), args))
            if _args:
                _args[0].errors.append(func.__name__)

            return None

        return result

    return wrapper

# Metrics


@statistic("The initial date where metrics are calculated on")
@pass_on_error
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
@pass_on_error
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
@pass_on_error
def users(object):
    """
    Calculate the total number of unique users
    found in Pandas DataFrame object users (if provided)
    or user_actions otherwise
    """
    return int(object.users["id"].nunique())


@statistic("The total number of unique registered users in the system")
@pass_on_error
def registered_users(object):
    """
    Calculate the total number of unique users
    found in Pandas DataFrame object users (if provided)
    or user_actions otherwise
    """
    return object.user_actions[object.user_actions["registered"]][
                                                   object.id_field].nunique()


@statistic("The total number of unique anonymous users in the system")
@pass_on_error
def anonymous_users(object):
    """
    Calculate the total number of unique users
    found in Pandas DataFrame object users (if provided)
    or user_actions otherwise
    """
    return users(object)-registered_users(object)


@statistic("The number of unique published items in the evaluated RS")
@pass_on_error
def items(object):
    """
    Calculate the number of unique items
    found in Pandas DataFrame object items (if provided)
    or user_actions otherwise (from both Source and Target item)
    """
    return int(object.items["id"].nunique())


@statistic("The number of recommended items in the evaluated RS")
@pass_on_error
def recommended_items(object):
    """
    Calculate the number of recommended items
    found in Pandas DataFrame object recommendations
    """
    return len(object.recommendations.index)


@statistic("The total number of user actions")
@pass_on_error
def user_actions_all(object):
    """
    Calculate the total number of user_actions
    found in Pandas DataFrame object user_actions
    """
    return len(object.user_actions_all.index)


@statistic("The number of filtered user actions")
@pass_on_error
def user_actions(object):
    """
    Calculate the number of filtered user_actions
    found in Pandas DataFrame object user_actions
    """
    return len(object.user_actions.index)


@statistic("The number of filtered user actions occurred by registered users")
@pass_on_error
def user_actions_registered(object):
    """
    Calculate the number of filtered user_actions occurred by registered users
    found in Pandas DataFrame object user_actions
    """
    return len(object.user_actions[object.user_actions["registered"]].index)


@statistic("The number of filtered user actions occurred by anonymous users")
@pass_on_error
def user_actions_anonymous(object):
    """
    Calculate the number of filtered user_actions occurred by anonymous users
    found in Pandas DataFrame object user_actions
    """
    return user_actions(object) - user_actions_registered(object)


@statistic(
    "The percentage (%) of filtered user actions occurred by registered users "
    "to the total user actions"
)
@pass_on_error
def user_actions_registered_perc(object):
    """
    Calculate the percentage (%) of filtered user actions occurred
    by registered users to the total user actions
    found in Pandas DataFrame object user_actions (in two decimals)
    """
    return round((user_actions_registered(object) * 100.0
                  / user_actions(object)), 2)


@statistic(
    "The percentage (%) of filtered user actions occurred by anonymous users "
    "to the total user actions"
)
@pass_on_error
def user_actions_anonymous_perc(object):
    """
    Calculate the percentage (%) of filtered user actions occurred
    by anonymous users to the total user actions
    found in Pandas DataFrame object user_actions (in two decimals)
    """
    return round(100.0 - user_actions_registered_perc(object), 2)


@statistic("The total number of item views by the users")
@pass_on_error
def item_views_all(object):
    """
    Calculate the total number of user_actions led to item views
    found in Pandas DataFrame object user_actions
    """
    # if target path is the search page remove it
    # if the main page of the source and target paths are the same
    # then remove it because it's a walk around the service
    # items apart from services have not walk around implementations

    _df = object.user_actions_all[
            (object.user_actions_all["target_resource_id"] != -1)
            & (object.user_actions_all["target_resource_id"] != '-1')
            & (object.user_actions_all["target_resource_id"] is not None)
        ].copy()

    if object.schema == 'legacy':
        pattern = r"/services/([^/]+)/"
        _df = _df[_df["target_path"].str.match(pattern) &
                  ~_df["target_path"].str.startswith("/services/c/")]

    else:
        pattern = r"search%2F(?:all|dataset|software|service" + \
                  r"|data-source|training|guideline|other)"
        _df = _df[~_df["target_path"].str.match(pattern)]

    _df['source'] = _df['source_path'].str.extract(r"/services/(.*?)/")
    _df['target'] = _df['target_path'].str.extract(r"/services/(.*?)/")

    _df = _df[_df['source'] != _df['target']]

    return len(_df.index)


@statistic("The number of filtered item views by the users")
@pass_on_error
def item_views(object):
    """
    Calculate the number of filtered user_actions led to item views
    found in Pandas DataFrame object user_actions
    """
    # if target path is the search page remove it
    # if the main page of the source and target paths are the same
    # then remove it because it's a walk around the service
    # items apart from services have not walk around implementations

    _df = object.user_actions[
            (object.user_actions["target_resource_id"] != -1)
            & (object.user_actions["target_resource_id"] != '-1')
            & (object.user_actions["target_resource_id"] is not None)
        ].copy()

    if object.schema == 'legacy':
        pattern = r"/services/([^/]+)/"
        _df = _df[_df["target_path"].str.match(pattern) &
                  ~_df["target_path"].str.startswith("/services/c/")]

    else:
        pattern = r"search%2F(?:all|dataset|software|service" + \
                  r"|data-source|training|guideline|other)"
        _df = _df[~_df["target_path"].str.match(pattern)]

    _df['source'] = _df['source_path'].str.extract(r"/services/(.*?)/")
    _df['target'] = _df['target_path'].str.extract(r"/services/(.*?)/")

    _df = _df[_df['source'] != _df['target']]

    return len(_df.index)


@statistic("The number of item views by the registered users")
@pass_on_error
def item_views_registered(object):
    """
    Calculate the number of user_actions led by registered users
    led to item views found in Pandas DataFrame object user_actions
    """
    # if target path is the search page remove it
    # if the main page of the source and target paths are the same
    # then remove it because it's a walk around the service
    # items apart from services have not walk around implementations
    _df = object.user_actions[
            (object.user_actions["target_resource_id"] != -1)
            & (object.user_actions["target_resource_id"] != '-1')
            & (object.user_actions["target_resource_id"] is not None)
        ].copy()

    if object.schema == 'legacy':
        pattern = r"/services/([^/]+)/"
        _df = _df[_df["target_path"].str.match(pattern) &
                  ~_df["target_path"].str.startswith("/services/c/")]

    else:
        pattern = r"search%2F(?:all|dataset|software|service" + \
                  r"|data-source|training|guideline|other)"
        _df = _df[~_df["target_path"].str.match(pattern)]

    _df['source'] = _df['source_path'].str.extract(r"/services/(.*?)/")
    _df['target'] = _df['target_path'].str.extract(r"/services/(.*?)/")

    _df = _df[_df['source'] != _df['target']]

    _df = _df[_df['registered']]

    return len(_df.index)


@statistic("The number of item views by the anonymous users")
@pass_on_error
def item_views_anonymous(object):
    """
    Calculate the number of user_actions led by anonymous users
    led to item views found in Pandas DataFrame object user_actions
    """
    return item_views(object) - item_views_registered(object)


@statistic(
    "The percentage (%) of user_actions led by registered users to item views"
)
@pass_on_error
def item_views_registered_perc(object):
    """
    Calculate the percentage (%) of user_actions led by registered users to
    item views found in Pandas DataFrame object user_actions (in two decimals)
    """
    try:
        return round((item_views_registered(object) * 100.0 /
                      item_views(object)), 2)
    except ZeroDivisionError:
        return 0


@statistic(
    "The percentage (%) of user_actions led by anonymous users to item views"
)
@pass_on_error
def item_views_anonymous_perc(object):
    """
    Calculate the percentage (%) of user_actions led by anonymous users to
    item views found in Pandas DataFrame object user_actions (in two decimals)
    """
    return round(100.0 - item_views_registered_perc(object), 2)


@statistic("The total number of unique recommended items")
@pass_on_error
def total_unique_recommended_items(object):
    """
    Calculate the total number of unique items found in recommendations
    """
    return int(object.recommendations.nunique()["resource_id"])


@statistic("The total number of unique users found in recommendations")
@pass_on_error
def total_unique_users_recommended(object):
    """
    Calculate the total number of unique users found in recommendations
    """
    return int(object.recommendations.nunique()[object.id_field])


@statistic("A dictionary of the number of user actions per day")
@pass_on_error
def user_actions_per_day(object):
    """
    It returns a statistical report in dictionary format. Specifically, the key
    is set for each particular day found and its value contains the respective
    number of user_actions committed. The dictionary includes all in-between
    days (obviously, with the count set to zero). User_actions are already
    filtered by those where the user or item does not exist in users'
    or items' catalogs.
    """
    # Since user_actions is in use, user actions when user
    # or item does not exist in users' or items'
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
@pass_on_error
def user_actions_per_month(object):
    """
    It returns a statistical report in dictionary format. Specifically, the key
    is set for each specific month found and its value contains the respective
    number of user_actions committed. The dictionary includes all in-between
    months (obviously, with the count set to zero). User_actions are already
    filtered by those where the user or item does not exist in users'
    or items' catalogs.
    """
    # Since user_actions is in use, user actions when user
    # or item does not exist in users' or items'
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


@statistic("A dictionary of the number of recommended items per day")
@pass_on_error
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
@pass_on_error
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


@metric("The percentage (%) of unique items  to the total number "
        "of items")
@pass_on_error
def catalog_coverage(object):
    """
    Calculate the percentage (%) of unique items
    found to the total number of items
    """
    return round((total_unique_recommended_items(object) * 100.0 /
                  items(object)), 2)


@metric("The percentage (%) of unique users to the total number of users")
@pass_on_error
def user_coverage(object):
    """
    Calculate the percentage (%) of unique users  to the total number of users
    """
    return round((total_unique_users_recommended(object) * 100.0 /
                  users(object)), 2)


@metric(
    "The ratio of user hits divided by the total number of users "
    "(user hit: a user that has accessed at least one item "
    "that is also a personal recommendation)"
)
@pass_on_error
def hit_rate(object):
    """
    1) For each user get the recommended items and the items the user
    accessed
    2) Check if the user has at least one accessed item in recommendations
    3) If yes increase number of hits by one
    4) Divide by the total number of users
    """
    # object.users contains already only the registered ones
    # a matrix of User ids and the respective accessed items' ids
    access_df = object.users[["id", "accessed_resources"]]

    # a matrix of User ids and the respective recommended items' ids
    rec_df = (
        object.recommendations[[object.id_field, "resource_id"]]
        .groupby([object.id_field])
        .agg({"resource_id": lambda x: x.unique().tolist()})
        .reset_index()
    )

    # performs a left join on User id, which means that nan values
    # are set for cases where no recommendations were made
    data = pd.merge(access_df, rec_df, left_on="id", right_on=object.id_field,
                    how="inner")

    # calculate hits per user
    # performs an interection of access and recommended items per user (row)
    data['intersect'] = data.apply(lambda row: list(set(
        row['accessed_resources']).intersection(row['resource_id'])), axis=1)
    # hits = the length of the intersection
    data['intersect_len'] = data['intersect'].apply(len)

    # calculate the average value
    total_hits = data['intersect_len'].sum()

    return round(total_hits/len(object.users), 5)


@metric(
    "The number of user clicks through recommendations panels divided by the "
    "total times recommendation panels were presented to users. "
    "Takes into account all historical data of user actions"
)
@pass_on_error
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
    if object.schema == 'legacy':
        user_actions_recpanel_views = object.user_actions[
            object.user_actions['source_path'].isin(
                ['/services', '/services/']
            ) |
            object.user_actions['source_path'].str.startswith('/services/c/')
        ]
    else:
        user_actions_recpanel_views = object.user_actions[
            object.user_actions['source_path'].str.startswith('search%2F')
        ]

    user_actions_recpanel_clicks = user_actions_recpanel_views[
        user_actions_recpanel_views['panel'] == 'recommendation_panel'
    ]
    try:
        return round(
            len(user_actions_recpanel_clicks)
            / len(user_actions_recpanel_views), 2
        )
    except ZeroDivisionError:
        return 0.00


@metric(
    "The diversity of the recommendations according to Shannon Entropy. "
    "The entropy is 0 when a single item is always chosen or recommended, "
    "and log n when n items are chosen or recommended equally often."
)
@pass_on_error
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
            (object.recommendations[object.id_field]
                .find_registered(object.schema))
        ]

    # this variable keeps the sum of user_norm (where user_norm is
    # the count of how many times a User has been suggested)
    # however since no cutoff at per user recommendations is applied and
    # also since each recommendation entry is one-to-one <user id> <item id>
    # then the total number of recommendations is equal to this sum

    # remember that recommendations have been previously filtered based
    # on the existance of users and items

    # item_count
    # group recommendations entries by item id and
    # then count how many times each item has been suggested
    gr_item = recs.groupby(["resource_id"]).count()

    # create a dictionary of item_count in order to
    # map the item id to the respective item_count
    # key=<item id> and value=<item_count>
    d_item = gr_item[object.id_field].to_dict()

    # each element represent the item's recommendations occurance
    # e.g. [1,6,7]
    # a item was recommended 1 time, another 6 times and another 7 times
    items = np.array(list(d_item.values()))

    # the total number of recommendations
    n_recommendations = items.sum()

    # element-wise computations
    # (division for each item's recommendations occurance)
    recommended_probability = items / n_recommendations

    # H=-Sum(p*logp) [element-wise]
    shannon_entropy = -np.sum(
        recommended_probability * np.log2(recommended_probability)
    )

    return round(shannon_entropy, 4)


@metric(
    "The diversity of the recommendations according to GiniIndex. "
    "The index is 0 when all items are "
    "chosen equally often, and 1 when a single item is always chosen."
)
@pass_on_error
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
            (object.recommendations[object.id_field]
                .find_registered(object.schema))
        ]
    # this variable keeps the sum of user_norm (where user_norm is
    # the count of how many times a User has been suggested)
    # however since no cutoff at per user recommendations is applied and
    # also since each recommendation entry is one-to-one <user id> <item id>
    # then the total number of recommendations is equal to this sum
    free_norm = len(recs.index)

    # item_count
    # group recommendations entries by item id and
    # then count how many times each item has been suggested
    gr_item = recs.groupby(["resource_id"]).count()

    # create a dictionary of item_count in order to
    # map the item id to the respective item_count
    # key=<item id> and value=<item_count>
    d_item = gr_item[object.id_field].to_dict()

    # total number of recommended itemss
    n_recommended_items = len(d_item)

    # total number of items
    num_items = items(object)

    # create a zero list
    # to calculate gini index including elements with 0 occurance
    zeros = [0] * (num_items - n_recommended_items)

    gini = sum(
        [
            (2 * (j + 1) - num_items - 1) * (cs / free_norm)
            for j, cs in enumerate(zeros + sorted(d_item.values()))
        ]
    )

    gini /= num_items - 1

    return round(gini, 4)


@metric("The novelty expresses how often new and unseen items are"
        " recommended to users")
@pass_on_error
def novelty(object):
    """Calculate novelty of recommendations
    using the n=SUM(-log(p(i)))/|R| formula"""
    # published items
    items_pub = object.items["id"]
    # recommended items to authenticated users
    items_rec = (object
                 .recommendations[object.recommendations[object.id_field]
                                  .find_registered(
                                     object.schema)]["resource_id"])

    # items that are published and recommended
    items_recpub = items_rec[items_rec
                             .isin(items_pub)].drop_duplicates()

    # user actions
    ua = object.user_actions
    # user actions filtered if src and target the same. Also filter out
    # if target equals -1 and filter out anonymous users
    ua_serv_view = ua[
        (ua["source_resource_id"] != ua["target_resource_id"])
        & (ua["target_resource_id"] != -1)
        & (ua["target_resource_id"] != '-1')
        & (ua["target_resource_id"] is not None)
        & (ua[object.id_field].find_registered(object.schema))
    ]

    # count item views by item id (sorted by item id)
    items_viewed = (ua_serv_view["target_resource_id"]
                    .value_counts().sort_index())

    # create a table for each recommended item with columns
    # for number of views, p(i) and -log(pi)
    r_items = pd.DataFrame(index=items_recpub).sort_index()
    # add views column to assign views to each recommended item
    r_items["views"] = items_viewed

    # count the total item views in order to compute the portions p(i)
    total_views = r_items["views"].sum()

    # count the total recommended items |R|
    total_items = len(r_items)
    # compute the p(i) of each recommeneded item
    r_items["pi"] = r_items["views"] / total_views
    # calculate the negative log of the p(i).
    r_items["-logpi"] = -np.log2(r_items["pi"])

    # calculate novelty based on formula n=SUM(-log(p(i)))/|R|
    novelty = r_items["-logpi"].sum() / total_items

    return round(novelty, 4)


@metric(
    "The mean value of the accuracy score found for each user defined by the "
    "fraction of the number of the correct predictions by the total number "
    "of predictions"
)
@pass_on_error
def accuracy(object):
    """
    Calculate the accuracy score found for each and retrieve the mean value.
    The score is calculated by dividing the number of the correct predictions
    by the total number of predictions.
    """
    # a list of unique items' ids found in Datastore
    items_list = object.items["id"].unique().tolist()
    # the length of the above value
    len_items = items(object)

    def score(x):
        """
        Inner function called at each row of the final dataframe
        in order to calculate the accuracy score for each row (=user)
        """
        # 'Items' header indicates the accessed items' list,
        # while the 'Items' header indicates the recommended items' list
        # if accessed or recommended items' list is empty
        # it does not calculate any further computations
        # else for each item found in items_list,
        # put 1 or 0 if it is also found in the accessed or
        # recommended items respectively
        if not x["accessed_resources"]:
            true_values = np.array([0] * len_items)
        else:
            true_values = np.array(
                list(map(lambda s: 1 if s in x["accessed_resources"] else 0,
                     items_list))
            )
        if not x["resource_id"]:
            pred_values = np.array([0] * len_items)
        else:
            pred_values = np.array(
                list(map(lambda s: 1 if s in x["resource_id"] else 0,
                     items_list))
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

    # a matrix of User ids and the respective accessed items' ids
    access_df = object.users[["id", "accessed_resources"]]

    # a matrix of User ids and the respective recommended items' ids
    rec_df = (
        object.recommendations[[object.id_field, "resource_id"]]
        .groupby([object.id_field])
        .agg({"resource_id": lambda x: x.unique().tolist()})
        .reset_index()
    )

    # performs a left join on User id, which means that nan values
    # are set for cases where no recommendations were made
    data = pd.merge(access_df, rec_df, left_on="id", right_on=object.id_field,
                    how="left")
    # convert nan values to zeros, in order to be handled easily
    # by the inner function
    data.fillna(0, inplace=True)
    # apply the score function row-wise
    data = data.apply(score, axis=1)

    # return the mean value of all users' accuracy score
    # up to 4 digits precision
    return round(data["resource_id"].mean(), 4)


@metric("The Top 5 recommended items according to recommendations entries")
@pass_on_error
def top5_items_recommended(object, k=5):
    """
    Calculate the Top 5 recommended items according to
    the recommendations entries.
    Return a list of list with the elements:
        #   (i) item id
        #  (ii) item name
        # (iii) item page appended with base (to create the URL)
        #  (iv) total number of recommendations of the item
        #   (v) percentage of the (iv) to the total number of recommendations
        #       expressed in %, with or without anonymous,
        #       based on the function's flag
    Item's info is being retrieved from the external source
    (i.e. each line forms: item_id, item_name, page_id)
    """
    recs = object.recommendations[
            (object.recommendations[object.id_field]
                .find_registered(object.schema))
        ]

    # item_count
    # group recommendations entries by item id and
    # then count how many times each item has been suggested
    gr_item = recs.groupby(["resource_id"]).count()

    # create a dictionary of item_count in order to
    # map the item id to the respective item_count
    # key=<item id> and value=<item_count>
    d_item = gr_item[object.id_field].to_dict()

    # convert dictionary to double list (list of lists)
    # where the sublist is <item_id> <item_count>
    # and sort them from max to min <item_count>
    l_item = list(map(lambda x: [x, d_item[x]], d_item))
    l_item.sort(key=lambda x: x[1], reverse=True)

    # get only the first k elements
    l_item = l_item[:k]

    topk_items = []

    for item in l_item:
        # get item's info from dataframe
        _df_item = (object
                    .items[(object
                            .items["id"]
                            .isin([item[0]]))])

        if _df_item["type"].item() in ["service", "data_source"]:
            url = "https://marketplace.eosc-portal.eu/{}".format(
                  str(_df_item["path"].item()))
        elif _df_item["type"].item() in ["training"]:
            url = "https://search.marketplace.eosc-portal.eu/{}".format(
                  str(_df_item["path"].item()))
        else:
            url = "https://explore.eosc-portal.eu/search/{}?softwareId={}"\
                  .format(
                      _df_item["type"].item(), str(_df_item["id"].item())[
                          len("50|"):].lstrip() if
                      str(_df_item["id"].item()).startswith("50|") else
                      str(_df_item["id"].item()))

        # append a list with the elements:
        #   (i) item id
        #  (ii) item name
        # (iii) item page appended with base (to create the URL)
        #  (iv) total number of recommendations of the item
        #   (v) percentage of the (iv) to the total number of recommendations
        #       expressed in %, with or without anonymous,
        #       based on the function's flag
        topk_items.append(
            {
                "item_id": item[0],
                "item_name": str(_df_item["name"].item()),
                "item_url": url,
                "recommendations": {
                    "value": item[1],
                    "percentage": round(100 * item[1] / len(recs.index), 2),
                    "of_total": len(recs.index),
                },
            }
        )

    return topk_items


@metric("The Top 5 viewed items according to user actions entries")
@pass_on_error
def top5_items_viewed(object, k=5):
    """
    Calculate the Top 5 viewed items according to user actions entries.
    User actions with Target Pages that lead to unknown items (=-1)
    are being ignored.
    Return a list of list with the elements:
        #   (i) item id
        #  (ii) item name
        # (iii) item page appended with base (to create the URL)
        #  (iv) total number of orders of the item
        #   (v) percentage of the (iv) to the total number of orders
        #       expressed in %, with or without anonymous,
        #       based on the function's flag
    """
    uas = object.user_actions[
            (object.user_actions["target_resource_id"] != -1)
            & (object.user_actions["target_resource_id"] != '-1')
            & (object.user_actions["target_resource_id"] is not None)
        ]

    _df = uas.copy()

    if object.schema == 'legacy':
        pattern = r"/services/([^/]+)/"
        _df = _df[_df["target_path"].str.match(pattern) &
                  ~_df["target_path"].str.startswith("/services/c/")]

    else:
        pattern = r"search%2F(?:all|dataset|software|service" + \
                  r"|data-source|training|guideline|other)"
        _df = _df[~_df["target_path"].str.match(pattern)]

    _df['source'] = _df['source_path'].str.extract(r"/services/(.*?)/")
    _df['target'] = _df['target_path'].str.extract(r"/services/(.*?)/")

    uas = _df[_df['source'] != _df['target']]

    # item_count
    # group user_actions entries by item id and
    # then count how many times each item has been viewed
    gr_item = uas.groupby(["target_resource_id"]).count()

    # create a dictionary of item_count in order to
    # map the item id to the respective item_count
    # key=<item id> and value=<item_count>
    d_item = gr_item[object.id_field].to_dict()

    # convert dictionary to double list (list of lists)
    # where the sublist is <item_id> <item_count>
    # and sort them from max to min <item_count>
    l_item = list(map(lambda x: [x, d_item[x]], d_item))
    l_item.sort(key=lambda x: x[1], reverse=True)

    # get only the first k elements
    l_item = l_item[:k]

    topk_items = []

    for item in l_item:
        # get items's info from dataframe
        _df_item = object.items[object.items["id"].isin([item[0]])]

        if _df_item["type"].item() in ["service", "data_source"]:
            url = "https://marketplace.eosc-portal.eu/{}".format(
                  str(_df_item["path"].item()))
        elif _df_item["type"].item() in ["training"]:
            url = "https://search.marketplace.eosc-portal.eu/{}".format(
                  str(_df_item["path"].item()))
        else:
            url = "https://explore.eosc-portal.eu/search/{}?softwareId={}"\
                  .format(
                      _df_item["type"].item(), str(_df_item["id"].item())[
                          len("50|"):].lstrip() if
                      str(_df_item["id"].item()).startswith("50|") else
                      str(_df_item["id"].item()))

        # append a list with the elements:
        #   (i) item id
        #  (ii) item name
        # (iii) item page appended with base (to create the URL)
        #  (iv) total number of orders of the item
        #   (v) percentage of the (iv) to the total number of orders
        #       expressed in %, with or without anonymous,
        #       based on the function's flag
        topk_items.append(
            {
                "item_id": item[0],
                "item_name": str(_df_item["name"].item()),
                "item_url": url,
                "orders": {
                    "value": item[1],
                    "percentage": round(100 * item[1] / len(uas.index), 2),
                    "of_total": len(uas.index),
                },
            }
        )

    return topk_items


# internal function
def __top5_recommended(object, k=5, element='category'):
    """
    Calculate the Top 5 recommended elements according to
    the recommendations entries.
    Return a list of list with the elements:
        #  (i) element id
        #  (ii) element name (according to element collection)
        #  (iii) total number of recommendations of the element
        #  (iv) percentage of the (iii) to the total number of recommendations
        #       expressed in %, with or without anonymous,
        #       based on the function's flag
    Element's info is being retrieved from the marketplace_rs MongoDB source
    """
    recs = object.recommendations[
            (object.recommendations[object.id_field]
                .find_registered(object.schema))
        ]

    # rename the column at a copy (not in place) for more readable processing
    _items = object.items.rename(columns={'id': 'resource_id'})

    # create an inner join between the recommendations collection
    # and the items collection
    # since a element is a list of element ids
    # the rows are further expanded (exploded) into rows per element id
    # inner join means that elements must exist in both items collection
    # and recommendations collection
    merged = recs.merge(_items, on='resource_id')
    exp = merged.explode(element)

    # count the total orders where the associated items have elements
    total = len(merged.dropna(subset=[element]))

    # count elements' ids and covert pandas series to dataframe
    # user_id holds the same values with all other columns
    # it is just the first column
    cat = exp.groupby([element])[object.id_field].count().to_frame()

    # reset indexes and rename columns accordingly
    cat = cat.reset_index().rename(columns={element: 'id',
                                            object.id_field: 'count'})

    if object.schema == 'legacy':
        # create a second inner join with the elements
        # in order to retrieve the name of the elements
        # inner join means that element must exist in both elements collection
        # and cat collection
        if element == 'category':
            _input = object.categories
        else:
            _input = object.scientific_domains

        full_cat = cat.merge(_input, on='id')

    else:
        full_cat = cat.rename(columns={'id': 'name'})
        # work only on the upper class
        full_cat = full_cat[~full_cat['name'].str.contains(">")]

    # sort based on count
    # get top k
    # and convert it to a list of dictionaries
    # where each dictionary equals to the df's record,
    # and each column of the df is the key of the dictionary
    topk = full_cat.sort_values('count', ascending=False).head(k).to_dict(
                                orient='records')

    # create similar format to other kpis
    topk_elements = []

    for entry in topk:
        # append a list with the elements:
        #  (i) element id
        #  (ii) element name (retrieved from element collection)
        #  (iii) total number of recommendations of the item
        #  (iv) percentage of the (iii) to the total number of recommendations
        #       expressed in %, with or without anonymous,
        #       based on the function's flag
        topk_elements.append(
            {
                element+"_name": entry['name'],
                "recommendations": {
                    "value": entry['count'],
                    "percentage": round(100 * entry['count'] / total, 2),
                    "of_total": total,
                },
            }
        )

    return topk_elements


# internal function
def __top5_viewed(object, element='category', k=5):
    """
    Calculate the Top 5 viewed elements according to user actions entries.
    Return a list of list with the elements:
        #  (i) element id
        #  (ii) element name (according to element collection)
        #  (iii) total number of orders of the element
        #  (iv) percentage of the (iii) to the total number of orders
        #       expressed in %, with or without anonymous,
        #       based on the function's flag
    Element's info is being retrieved from the marketplace_rs MongoDB source
    """
    uas = object.user_actions[
            (object.user_actions["target_resource_id"] != -1)
            & (object.user_actions["target_resource_id"] != '-1')
            & (object.user_actions["target_resource_id"] is not None)
        ]

    _df = uas.copy()

    if object.schema == 'legacy':
        pattern = r"/services/([^/]+)/"
        _df = _df[_df["target_path"].str.match(pattern) &
                  ~_df["target_path"].str.startswith("/services/c/")]

    else:
        pattern = r"search%2F(?:all|dataset|software|service" + \
                  r"|data-source|training|guideline|other)"
        _df = _df[~_df["target_path"].str.match(pattern)]

    _df['source'] = _df['source_path'].str.extract(r"/services/(.*?)/")
    _df['target'] = _df['target_path'].str.extract(r"/services/(.*?)/")

    uas = _df[_df['source'] != _df['target']]

    # rename the column at a copy (not in place) for more readable processing
    _items = object.items.rename(columns={'id': 'target_resource_id'})

    # create an inner join between the recommendations collection
    # and the items collection
    # since a element is a list of element ids
    # the rows are further expanded (exploded) into rows per element id
    # inner join means that elements must exist in both items collection
    # and recommendations collection
    merged = uas.merge(_items, on='target_resource_id')
    exp = merged.explode(element)

    # count the total orders where the associated items have elements
    total = len(merged.dropna(subset=[element]))

    # count elements' ids and covert pandas series to dataframe
    # user_id holds the same values with all other columns
    # it is just the first column
    cat = exp.groupby([element])[object.id_field].count().to_frame()

    # reset indexes and rename columns accordingly
    cat = cat.reset_index().rename(columns={element: 'id',
                                            object.id_field: 'count'})

    if object.schema == 'legacy':
        # create a second inner join with the elements
        # in order to retrieve the name of the elements
        # inner join means that element must exist in both elements collection
        # and cat collection
        if element == 'category':
            _input = object.categories
        else:
            _input = object.scientific_domains

        full_cat = cat.merge(_input, on='id')

    else:
        full_cat = cat.rename(columns={'id': 'name'})
        # work only on the upper class
        full_cat = full_cat[~full_cat['name'].str.contains(">")]

    # sort based on count
    # get top k
    # and convert it to a list of dictionaries
    # where each dictionary equals to the df's record,
    # and each column of the df is the key of the dictionary
    topk = full_cat.sort_values('count', ascending=False).head(k).to_dict(
                                orient='records')

    # create similar format to other kpis
    topk_elements = []

    for entry in topk:

        # append a list with the elements:
        #  (i) element id
        #  (ii) element name (retrieved from element collection)
        #  (iii) total number of recommendations of the item
        #  (iv) percentage of the (iii) to the total number of recommendations
        #       expressed in %, with or without anonymous,
        #       based on the function's flag
        topk_elements.append(
            {
                element+"_name": entry['name'],
                "orders": {
                    "value": entry['count'],
                    "percentage": round(100 * entry['count'] / total, 2),
                    "of_total": total,
                },
            }
        )

    return topk_elements


@metric("The Top 5 recommended categories according to recommendations entries"
        )
@pass_on_error
def top5_categories_recommended(object, k=5):
    return __top5_recommended(object, k=5, element='category')


@metric("The Top 5 recommended scientific domains according to recommendations\
entries")
@pass_on_error
def top5_scientific_domains_recommended(object, k=5):
    return __top5_recommended(object, k=5, element='scientific_domain')


@metric("The Top 5 viewed categories according to recommendations entries"
        )
@pass_on_error
def top5_categories_viewed(object, k=5):
    return __top5_viewed(object, k=5, element='category')


@metric("The Top 5 viewed scientific domains according to recommendations\
entries")
@pass_on_error
def top5_scientific_domains_viewed(object, k=5):
    return __top5_viewed(object, k=5, element='scientific_domain')
