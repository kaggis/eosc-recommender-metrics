---
id: click_through_rate
title: Click-Through Rate
sidebar_position: 3
---

# Click-Through Rate

## Summary
The number of user clicks through recommendations panels divided by the total times recommendation panels were presented to users.

## Description
The number of user clicks through recommendations panels divided by the total times recommendation panels were presented to users. Takes into account all historical data of user actions.
<p>
The metric is expressed by the formula: $$Click-Through Rate=\frac{clicks}{views}$$
</p>

## Output

| Type | Float |
| --- | ----------- |
| Min | 0 |
| Max |  +$$\infty$$ |

:::info
A value of 0 indicates that no clicks through recommendations panels occurred.
:::

## Prerequisites:
* all available user actions

## Process Flow:
* ### Retrieve user actions with recommendation panel
Get only the user actions that present a recommendation panel to the user in the source page. Those are actions with the following source paths:
  * `/services`
  * `/services`
  * `/services/c/{any category name}`
* ### Count user actions with recommendation panel
Count the items in the above list as they represent the times recommendations panels were presented to the users of the portal
* ### Filter list
Narrow the above list into a new subset by selecting only user actions that originate from a recommendation panel. Those are actions that have the 'recommendation' string in the Action column
* ### Count user actions with clicks through recommendation panel
Count the items in the subset as they represent the times users clicked through recommendations
* ### Calculate ratio
Divide the items of the subset with the items of the first list to get the click-through rate

