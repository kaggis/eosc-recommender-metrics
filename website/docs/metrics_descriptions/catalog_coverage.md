---
id: catalog_coverage
title: Catalog Coverage
sidebar_position: 2
---

# Catalog Coverage

## Summary
The percentage (%) of the division of the unique services found in recommendations to the total number of published services.

## Description
The Catalog Coverage is described by the formula: $\frac{unique\_rec\_services}{services}$

## Output

| Type | Float |
| --- | ----------- |
| Min | 0 |
| Max | 100 |

:::info
Catalog Coverage is 0 when none of the services is being recommended, and 100 when all of them are being recommended.
:::

## Prerequisites:
* all available recommendations
* all available services

## Process Flow:
* ### Retrieve recommendations
Retrieve all available recommendations found in source
* ### Gather all unique services
Gather all unique services found in all available recommendations
* ### Retrieve services
Retrieve all available published services found in source
* ### Calculate ratio
Calculate the percentage (%) of the division of the unique services found in recommendations to the total number of published services

