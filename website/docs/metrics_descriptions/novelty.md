---
id: novelty
title: Novelty
sidebar_position: 7
---

# Novelty

## Summary
The novelty metric  expresses the ability of the system to recommend items not generally seen before by the population of users.

## Description
Calculating novelty of the recommender system based on the user actions using the following formula: 
$$
Novelty=\frac{\sum\nolimits_{i \in R}-log(p(i))}{|R|}
$$ 
For each service item $i$ belonging to the set of recommended services $R$ calculate the portion $p(i)$ of the times the service has been viewed to the total views of the services produced by the user actions data.

## Output

| Type | Float |
| --- | ----------- |
| Min | 0 |
| Max | +$$\infty$$ |

:::info
Novelty expresses the ability of the system to recommend items that are novel (not seen before) by the population of users. A smaller number expresses that more services are being recommended that the users have not seen before.
:::

## Prerequisites:
* all available recommendations associated with registered users
* a subset of the available user actions associated with registered users that expresses transitions to service pages

## Process Flow:
* ### Clean up
Recommendations and user actions clean up; entries removal where users or services are not found in "users" or "services" files accordingly
* ### User actions that target services
Identify and keep user actions that express transition to target pages that are views of services. Additionally, user actions where the source and the target page belong to the same service's space are removed from the process.
* ### Calculate views for each service
Group and count user actions that express views for each recommended service id
* ### Calculate view propability p(i) of each service
Calculation of the view propability of each service which is the fraction of the service's views to the total service views
* ### Overall Novelty computation
Computation of the overall value by summing the negative log of all recommended service views from previous step and dividing them by the total number of recommended services


