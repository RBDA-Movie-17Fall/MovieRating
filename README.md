# RBDA-17fall-Movie
Predictive analysis for movie ratings.

![MovieRating](https://nycdatascience.com/blog/wp-content/uploads/2016/08/Screen-Shot-2016-08-21-at-11.54.05-PM-1200x480.png)

## Data source
- [Kaggle TMDB 5000](https://www.kaggle.com/tmdb/tmdb-movie-metadata/data)
- [Douban Movie Short Comments](https://www.kaggle.com/utmhikari/doubanmovieshortcomments)
- [Data World IMDB 5000](https://data.world/popculture/imdb-5000-movie-dataset)

## Dumbo HPC Environment:
**Java** 1.7.0_79

**Apache Spark MLlib** 1.6.0

**Apache Maven** 3.2.1

## Clean and format data

Run the MetadataETL MapReduce code on input data from [Data World IMDB 5000](https://data.world/popculture/imdb-5000-movie-dataset), the output is in [LIBSVM](https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/) format. Rename the output file as "tmdb_5000movies.txt".

The features' IDs and their names in LIBSVM output are listed below:

| Feature_ID | Feature_name                 |
| ---------- | ---------------------------- |
| 1          | number of critic for reviews |
| 2          | duration                     |
| 3          | director Facebook likes      |
| 4          | actor 3 Facebook likes       |
| 5          | actor 1 Facebook likes       |
| 6          | revenue                      |
| 7          | number of voted users        |
| 8          | cast total Facebook likes    |
| 9          | face number in posters       |
| 10         | number of users for reviews  |
| 11         | budget                       |
| 12         | year                         |
| 13         | actor 2 Facebook likes       |
| 14         | aspect ratio                 |
| 15         | movie Facebook likes         |

## Load data to HDFS

Go to folder ./RandomForest/input:

```language=bash
hdfs dfs -mkdir input
hdfs dfs -put tmdb_5000_movies.txt input
```

## Run random forest regression

To create the Maven project package (the project already exists, no need to create new one): 
> /opt/maven/bin/mvn archetype:generate 

To build the Maven project, go to folder ./RandomForest: 

> /opt/maven/bin/mvn package 

To run the task on Spark, go to folder ./RandomForest/target: 

> spark-submit --class "RandomForestRegression" rbda-movie-1.0-SNAPSHOT.jar

Remember to put the input TXT file in HDFS under 'input' folder
