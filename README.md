Movie Ratings Analysis with Spark Structured API
This project analyzes movie ratings and user engagement data using Apache Spark Structured APIs. The goal is to extract insights related to binge-watching patterns, churn risk users, and movie-watching trends.

Objective:
Analyze the dataset to extract insights on:
Binge-Watching Patterns: Identify the percentage of users in each age group who binge-watch movies.
Churn Risk Users: Identify users who are at risk of churn based on canceled subscriptions and low engagement (watch time).
Movie-Watching Trends: Analyze trends in movie-watching behavior over the years and identify peak years for movie consumption.

Task 1: Binge-Watching Patterns
Objective:
Identify which age groups binge-watch movies the most by analyzing users who watched 3 or more movies in a single day.

Approach:
Filter Users:

Select users who have IsBingeWatched = True.
Group by Age Group:

Count the number of binge-watchers in each age category.
Calculate Proportions:

Determine the percentage of binge-watchers within each age group.
Output:

A summary of binge-watching trends by age group, including the total number and percentage of binge-watchers.
Example Output:
Age Group	Binge Watchers	Percentage
Teen	1200	45%
Adult	900	38%
Senior	300	22%



Task 2: Churn Risk Users
Objective:
Identify users who are at risk of churn by identifying those with canceled subscriptions and low watch time (<100 minutes).

Approach:
Filter Users:
Select users who have SubscriptionStatus = 'Canceled' and WatchTime < 100.
Count Churn Risk Users:
Calculate the total number of such users.
Output:
A CSV file with:
Churn Risk Users: The count of churn risk users.
Total Users: The total number of churn risk users (same value in this case).
Example Output:
Churn Risk Users	Total Users
350	350


Task 3: Movie-Watching Trends
Objective:
Analyze how movie-watching trends have changed over the years and find peak years for movie consumption.

Approach:
Group by Watched Year:

Count the number of movies watched each year.
Analyze Trends:

Compare the year-over-year growth in movie consumption.
Find Peak Years:

Highlight the years with the highest number of movies watched.
Output:

A CSV file summarizing movie-watching trends over the years.
Example Output:
Watched Year	Movies Watched
2020	1200
2021	1500
2022	2100
2023	2800


Steps to Run the Project:
```bash
python generate_dataset.py
```

Run Task 1 - Binge-Watching Patterns: To execute Task 1 (Binge-Watching Patterns), run the following Spark submit command:
```bash
spark-submit src/task1_binge_watching_patterns.py
```

Run Task 2 - Churn Risk Users: To execute Task 2 (Churn Risk Users), run the following Spark submit command:

```bash 
spark-submit src/task2_churn_risk_users.py
```

Run Task 3 - Movie-Watching Trends: To execute Task 3 (Movie-Watching Trends), run the following Spark submit command:
```bash
spark-submit src/task3_movie_watching_trends.py
```

Check the Output: After running the tasks, the results will be saved in the Outputs/ folder:

binge_watching_patterns.csv: Contains the binge-watching trends by age group.
churn_risk_users.csv: Contains the count of churn risk users.
movie_watching_trends.csv: Contains the movie-watching trends over the years.


Findings:
Task 1: The dataset generated contains user information related to binge-watching behavior. The analysis shows the percentage of users in each age group who binge-watch movies.

Task 2: We identified users who are at risk of churn based on their canceled subscriptions and low engagement (watch time). The number of churn risk users is calculated and stored in a CSV file.

Task 3: We analyzed movie-watching trends over the years and identified the years with the most significant growth in movie consumption.

Conclusion:
This project demonstrates how to use Spark Structured Streaming APIs to perform large-scale analysis on movie ratings data. The analysis provides actionable insights on binge-watching behavior, churn risk, and movie-watching trends.