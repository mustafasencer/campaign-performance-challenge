# Campaign Performance Analysis

We would like to know if users find the reward attractive enough to recommend this product on our [recommendation page](referral.png). Each customer has a custom recommendation landing page. We have multiple customers and each has a different customer_id, the example in the page is IONOS. Customer is a company that offers the product and needs referral marketing, user is a person who wants to potentially buy the product and interacts with the page. 

We collect data everytime a user visits one of the recommendation pages (`ReferralPageLoad` event) and clicks on the "Recommend IONOS" button (`ReferralRecommendClick` event).

Your task is to build a data pipeline that reads the events and answers these questions:

- `page_loads`: How many `ReferralPageLoad` events happened for each customer for each hour?
- `clicks`: How many `ReferralRecommendClick` events happened for each customer for each hour?
- `unique_user_clicks`: How many unique users did `ReferralRecommendClick` for each customer for each hour?
- `click_through_rate`: How many `ReferralRecommendClick` events directly related to `ReferralPageLoad` happened for each customer for each hour?
Feel free to make an assumption on how to relate the events to each other, it can be subsequent based on the time window
or based on a combination of fields in the event. Please explain your assumption in your submission.  

For the sake of this task, assume that the events were collected into log files and ordered by timestamp. 
You can find a sample in `aklamio_challenge.json` Every line is a valid json https://jsonlines.org/ 

Data looks like this:
`event_id | customer_id | user_id | fired_at | event_type | ip | email`

## Scenario 1

You can read all the data into memory. Assume that all data is available at the processing time. All you need to do is read all the data,
clean it up and calculate the aggregations asked above. 
Scheduling is out of scope.

## Scenario 2 (optional)

Assume there is too much data and we cannot read it all into the memory. You need to read the data line by line to memory and
update the aggregations upon reading each line. For this case, you can omit deduplication of events.

## Write to postgres/ model the report lines

Decide on how you would want to model the reports and create queries to create the tables and update them to a postgres instance. Feel free to use
the dockerized postgres instance for convenience. We would like to see how you would create the tables and how you would write to the database. So that we
can actually see the output you generated, you can provide some csv or json files. 

# General Requirements

- You have used a minimum amount of libraries doing some data processing written in Python (pandas is fine but no fully fledged framework like flask please)
- In a state that you consider production ready (clean code, tests, good design where I/O and business logic is separate)
- No sensitive data included
- Please submit the git repository as .zip together with the commit history
- Please do not publish our coding challenge on a public platform
