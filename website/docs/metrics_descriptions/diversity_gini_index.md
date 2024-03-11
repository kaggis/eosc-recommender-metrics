---
id: diversity_gini_index
title: Diversity Gini Index
sidebar_position: 4
---

# Diversity Gini Index

## Summary
Measures Recommendations' diversity. The index is 0 when all items are chosen equally often, and 1 when a single item is always chosen.

## Description
The diversity ($$G$$) of the recommendations according to Gini Index.  The index is 0 when all items are chosen equally often,  and 1 when a single item is always chosen (see [book](https://link.springer.com/10.1007/978-1-4939-7131-2_110158)). Generally, the Gini Index mathematical expression is defined as:
<p> 
    $$G=\frac{1}{n-1}\sum_{j=1}^{n}(2j-n-1)p(i_j)$$, where $$i_1,\ldots,i_n$$ is the list of items ordered according to increasing $$p(i)$$ and each item $$i$$ accounts for a proportion $$p(i)$$ of user recommendations.
In RS Metrics the computation is determined by the following formula:
    $$Diversity=\frac{1}{n-1}\sum_{j=1}^{n}(2j-n-1)\left(\frac{count(j)}{recommendations}\right)$$
</p>

## Output

| Type | Float |
| --- | ----------- |
| Min | 0 |
| Max | 1 |

:::info
The index is 0 when all items are chosen equally often, and 1 when a single item is always chosen.
:::

## Prerequisites:
* recommendations without anonymous users
* all available services

## Process Flow:
* ### Clean up
Recommendations clean up; entries removal where users or services are not found in "users" or "services" files accordingly
* ### Services Impact
Calculation of the impact of the services, by counting how many times each service i was suggested to all possible users: count(j)
* ### Sort Services Impact from low to high
Sort the number of how many times each service (i.e. i) was suggested from the lower to the higher value, in order to apply the respective weight (j). The computation includes services with 0 recommendation occurrence
* ### Recommended Probability of the Services
For each service calculate its recommended probability by dividing the number of service's occurrence found in the recommendations to the total number of recommendations
* ### Service-based product computation
Calculation of the product of the recommended probability from previous step and services' respective index j, for each service individually
* ### Gini Index computation
Computation of the overall value by summing all values from previous step



