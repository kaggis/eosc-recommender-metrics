---
id: diversity_shannon_entropy
title: Diversity Shannon Entropy
sidebar_position: 5
---

# Diversity Shannon Entropy

## Summary
Measures Recommendations' diversity. The entropy is 0 when a single item is always chosen or recommended, and log n when n items are chosen or recommended equally often.

## Description
The diversity ($$H$$) of the recommendations according to Shannon Entropy. The entropy is 0 when a single item is always chosen or recommended, and log(n) when n items are chosen or recommended equally often (see [book](https://link.springer.com/10.1007/978-1-4939-7131-2_110158)). Generally, the Shannon Entropy mathematical expression is defined as: $$H=-\sum_{i=1}^{n}p(i)\log_2 p(i)$$
<p>
In RS Metrics the formula is determined as: $$Diversity=-\sum_{i=1}^{services}\left(\frac{count(i)}{recommendations}\right)\log_2 \left(\frac{count(i)}{recommendations}\right)$$
</p>

## Output

| Type | Float |
| --- | ----------- |
| Min | 0 |
| Max | +$$\infty$$ |

:::info
The entropy is 0 when a single item is always chosen or recommended, and log n when n items are chosen or recommended equally often.
:::

## Prerequisites:
* recommendations without anonymous users
* all available services

## Process Flow:
* ### Clean up
Recommendations clean up; entries removal where users or services are not found in "users" or "services" files accordingly
* ### Services Impact
Calculation of the impact of the services, by counting how many times each service i was suggested to all possible users: count(i)
* ### Recommended Probability of the Services
For each service calculate its recommended probability by dividing the number of service's occurrences found in the recommendations to the total number of recommendations
* ### Service-based product computation
Calculation of the product of the recommended probability from previous step and the logarithmic value of it, for each service individually
* ### Shannon Entropy computation
Computation of the overall value by summing all values from previous step

