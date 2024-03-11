---
id: accuracy
title: Accuracy
sidebar_position: 1
---

# Accuracy

## Summary
Measures Recommendations' accuracy based on users' access to the services. A value of 1, indicates that the RS model got all the predictions right, and a value of 0 indicates that the RS model did not make a single correct prediction

## Description
The accuracy $$(A$$ of the recommendations is based on users' access to the services. A value of 1, indicates that the RS model got all the predictions right, and a value of 0 indicates that the RS model did not make a single correct prediction. Generally, the Accuracy mathematical expression is defined as: 
$$A=\frac{Number\;of\;correct\;predictions}{Total\;number\;of\;predictions}$$
<p>
In RS Metrics the computation is determined by the following formula: 
    $$Accuracy=\frac{Number\;of\;correctly\;recommended\;services}{Total\;number\;of\;services}$$where correctness is defined as if the service is both accessed by the user and also it is recommended by the RS
</p>

## Output

| Type | Float |
| --- | ----------- |
| Min | 0 |
| Max | 1 |

:::info
A value of 1, indicates that the RS model got all the predictions right, and a value of 0 indicates that the RS model did not make a single correct prediction.
:::

## Prerequisites:
* recommendations without anonymous users
* all available users (with their accessed services)
* all available services

## Process Flow:
* ### Clean up
Recommendations clean up; entries removal where users or services are not found in "users" or "services" accordingly
* ### Vector creation of the Accessed Services
For each user create a vector at the size of the number of the services, and assign a binary value for each service with a value of 1 if it is found in the user's accessed services, or 0 if it is not
* ### Vector creation of the Recommended Services
For each user create a vector at the size of the number of the services, and assign a binary value for each service with a value of 1 if it is recommended to the user, or 0 if it is not
* ### Accuracy score computation
For each user compute the average value of the difference vector; a vector which states True if service is found in both accessed and recommended vectors or False if it is not
* ### Mean value of Accuracy score
Computation of the overall value by calculating the mean value of each user's accuracy score 
