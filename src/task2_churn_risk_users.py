from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def initialize_spark(app_name="Task2_Churn_Risk_Users"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the movie ratings data from a CSV file into a Spark DataFrame.
    """
    schema = """
        UserID INT, MovieID INT, MovieTitle STRING, Genre STRING, Rating FLOAT, ReviewCount INT, 
        WatchedYear INT, UserLocation STRING, AgeGroup STRING, StreamingPlatform STRING, 
        WatchTime INT, IsBingeWatched BOOLEAN, SubscriptionStatus STRING
    """
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def identify_churn_risk_users(spark, df):
    """
    Identify users with canceled subscriptions and low watch time (<100 minutes).
    """
    
    churn_risk_users = df.filter((df["SubscriptionStatus"] == "Canceled") & (df["WatchTime"] < 100))
    
    #
    churn_risk_count = churn_risk_users.count()

    
    total_users_count = df.filter((df["SubscriptionStatus"] == "Canceled") & (df["WatchTime"] < 100)).count()

    
    result_df = spark.createDataFrame([(churn_risk_count, total_users_count)], ["Churn Risk Users", "Total Users"])
    
    return result_df


def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.
    """
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    """
    Main function to execute Task 2.
    """
    spark = initialize_spark()

    input_file = "input/movie_ratings_data.csv"
    output_file = "Outputs/churn_risk_users.csv"

    df = load_data(spark, input_file)
    result_df = identify_churn_risk_users(spark,df)  # Call function here
    write_output(result_df, output_file)

    spark.stop()

if __name__ == "__main__":
    main()
