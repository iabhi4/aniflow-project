package com.project;

import com.project.dao.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static org.apache.spark.sql.functions.*;

public class AniflowApplication {

    //Constants for keyspace and tables
    private static final String ANIME_KEYSPACE = "anime";

    private static final String GENDER_ANALYSIS_TABLE = "gender_analysis";

    private static final String AGE_ANALYSIS_TABLE = "age_analysis";

    private static final String AGE_GENDER_ANALYSIS_TABLE = "age_gender_analysis";

    private static final String ANIME_SCORE_ANALYSIS_TABLE = "anime_score_analysis";

    private static final String ANIME_GENRE_TABLE = "anime_genre_analysis";

    private static final String GENRE_ANALYSIS_TABLE = "genre_analysis";

    public static void main(String[] args) throws SQLException {

        //Configuring Cassandra and creating output table
        String cassandraIp = args[0];
        int cassandraPort = Integer.parseInt(args[1]);
        String operation = args[2].toLowerCase();
        CassandraUtil cassandraUtil = new CassandraUtil(cassandraIp, cassandraPort);
        cassandraUtil.createTables(operation);

        SparkConf conf = setupSparkConf(args);
        JavaSparkContext sc = new JavaSparkContext(conf);
        jobToRun(args, cassandraUtil); // a method which will analyze which spark job to run
        sc.stop();
    }

    private static SparkConf setupSparkConf(String[] args) {  //Setting up Spark Application
        SparkConf conf = new SparkConf();
        conf.setAppName("Aniflow");
        conf.set("spark.cassandra.connection.host", args[0]);
        conf.set("spark.cassandra.connection.port", args[1]);
        return conf;
    }

    private static Dataset<Row> fetchAnimeListsData() {  //Fetching data from db table - animedata
        // Creating spark session which will be the entry point for reading data,
        // executing queries, and performing various Spark operations.
        SparkSession spark = SparkSession.builder().appName("Aniflow").getOrCreate();
        Map<String, String> cassandraOptions = new HashMap<>();
        cassandraOptions.put("keyspace", ANIME_KEYSPACE);
        cassandraOptions.put("table", "animedata");

        Dataset<Row> animeData = spark.read()   //Distributed collection of data organized into named columns
                .format("org.apache.spark.sql.cassandra")
                .options(cassandraOptions)
                .load();

        return animeData;
    }

    private static Dataset<Row> fetchAnimeInfoData() {  //Fetching animeinfo data
        SparkSession spark = SparkSession.builder().appName("Aniflow").getOrCreate();
        Map<String, String> cassandraOptions = new HashMap<>();
        cassandraOptions.put("keyspace", ANIME_KEYSPACE);
        cassandraOptions.put("table", "animeinfo");

        Dataset<Row> animeData = spark.read()
                .format("org.apache.spark.sql.cassandra")
                .options(cassandraOptions)
                .load();

        return animeData;
    }

    private static void jobToRun(String[] args, CassandraUtil cassandraUtil) throws SQLException {
        String operation = args[2].toLowerCase();    // Decides which spark job to run based on the parameter we pass
        if(operation.equals(GENRE_ANALYSIS_TABLE)) {
            Dataset<Row> animeInfo = fetchAnimeInfoData();
            if(args[3] != null && !args[3].isEmpty())
                analyzeStudioPortfolio(animeInfo, args[3]);
            else
                analyzeAllGenres(animeInfo);
        } else {
            Dataset<Row> data = fetchAnimeListsData();
            switch (operation) {
                case ANIME_GENRE_TABLE:
                    analyzeBestAnimeGenre(data, cassandraUtil);
                    break;
                case ANIME_SCORE_ANALYSIS_TABLE:
                    analyzeAverageAnimeScore(data);
                    break;
                case GENDER_ANALYSIS_TABLE:
                    analyzeGenderDistribution(data);
                    break;
                case AGE_ANALYSIS_TABLE:
                    analyzeAgeDistribution(data);
                    break;
                case AGE_GENDER_ANALYSIS_TABLE:
                    analyzeAgeGenderDistribution(data);
                    break;
            }
        }
    }

    public static void analyzeGenderDistribution(Dataset<Row> animeData) {
        // analyzes the gender distribution of the people who watch anime

        // removes duplicate rows from the dataset based on the 'user_id' column
        Dataset<Row> uniqueUsers = animeData.dropDuplicates("user_id");

        // groups by gender and calculate count for each one
        Dataset<Row> genderDistribution = uniqueUsers.groupBy("gender")
                .count();
        long totalUsers = uniqueUsers.count();

        // Calculate the percentage of users for each gender because we already have totalUsers
        Dataset<Row> genderPercentage = genderDistribution
                .withColumn("percentage", col("count").divide(totalUsers).multiply(100));

        // It maps each row to a GenderAnalysisResult object with 'gender' and 'percentage' fields.
        JavaRDD<GenderAnalysisResult> resultRDD = genderPercentage
                .toJavaRDD()
                .map(row -> {
                    GenderAnalysisResult result = new GenderAnalysisResult();
                    result.setGender(row.getString(0));
                    result.setPercentage(row.getDouble(2));
                    return result;
                });
        javaFunctions(resultRDD)
                .writerBuilder(ANIME_KEYSPACE, GENDER_ANALYSIS_TABLE, mapToRow(GenderAnalysisResult.class))
                .saveToCassandra();
    }

    public static void analyzeAgeDistribution(Dataset<Row> animeData) {
        //This method analyzes the diversity of age group watching anime
        Dataset<Row> uniqueUsers = animeData.dropDuplicates("user_id");

        Dataset<Row> ageDistribution = uniqueUsers.groupBy("age")
                .count();

        JavaRDD<AgeAnalysisResult> resultRDD = ageDistribution
                .toJavaRDD()
                .map(row -> {
                    AgeAnalysisResult result = new AgeAnalysisResult();
                    result.setAge(row.getLong(0)); // Convert Long to String
                    result.setCount(row.getLong(1));
                    return result;
                });

        javaFunctions(resultRDD)
                .writerBuilder(ANIME_KEYSPACE, AGE_ANALYSIS_TABLE, mapToRow(AgeAnalysisResult.class))
                .saveToCassandra();
    }

    public static void analyzeAgeGenderDistribution(Dataset<Row> animeData) {
        // this method analyzes the gender diversity of each age group
        Dataset<Row> uniqueUsers = animeData.dropDuplicates("user_id");

        Dataset<Row> ageGenderDistribution = uniqueUsers.groupBy("age", "gender")
                .agg(count("*").as("count"));

        // Pivot the result to have separate columns for male and female counts
        Dataset<Row> pivotedResult = ageGenderDistribution.groupBy("age")
                .pivot("gender")
                .agg(first("count").as("count"))
                .na().fill(0);

        // Rename columns for more clarity
        Dataset<Row> finalResult = pivotedResult
                .withColumnRenamed("Male", "male_count")
                .withColumnRenamed("Female", "female_count");

        JavaRDD<AgeGenderAnalysisResult> resultRDD = finalResult
                .toJavaRDD()
                .map(row -> {
                    AgeGenderAnalysisResult result = new AgeGenderAnalysisResult();
                    result.setAge(row.getLong(0)); // Convert Long to String
                    result.setMale(row.getLong(2));
                    result.setFemale(row.getLong(1));
                    return result;
                });
        javaFunctions(resultRDD)
                .writerBuilder(ANIME_KEYSPACE, AGE_GENDER_ANALYSIS_TABLE, mapToRow(AgeGenderAnalysisResult.class))
                .saveToCassandra();
    }

    public static void analyzeAverageAnimeScore(Dataset<Row> animeData) {
        //this method analyzes average score for different animes from dataset
        // Group by anime_id and calculate average score
        Dataset<Row> averageScores = animeData.groupBy("anime_id")
                .agg(avg("my_score").alias("average_score"));

        JavaRDD<AnimeScoreAnalysisResult> resultRDD = averageScores
                .toJavaRDD()
                .map(row -> {
                    AnimeScoreAnalysisResult result = new AnimeScoreAnalysisResult();
                    result.setAnimeId(row.getLong(0));
                    result.setAverageScore(row.getDouble(1));
                    return result;
                });
        javaFunctions(resultRDD)
                .writerBuilder(ANIME_KEYSPACE, ANIME_SCORE_ANALYSIS_TABLE, mapToRow(AnimeScoreAnalysisResult.class))
                .saveToCassandra();
    }

    public static void analyzeBestAnimeGenre(Dataset<Row> animeData, CassandraUtil cassandraUtil) throws SQLException {
        // Analyzes the genre groups which is maximum times in the dataset so that
        // businesses can use this insight and try to invest in these specific genres.
        Dataset<Row> animeIdNameDistribution = animeData.groupBy("anime_id")
                .count();

        // Found the row with the maximum count (most frequent anime_id)
        Row maxCountRow = animeIdNameDistribution
                .orderBy(org.apache.spark.sql.functions.desc("count"))
                .first();
        long animeId = maxCountRow.getLong(0);
        long maxCount = maxCountRow.getLong(maxCountRow.fieldIndex("count"));

        System.out.println("Anime_id with the maximum occurrences: " + animeId);
        System.out.println("Count: " + maxCount);

        //String genreName = cassandraUtil.fetchGenreNameByAnimeId(animeId);
        Dataset<Row> animeinfo = fetchAnimeInfoData();
        String genreName = findGenreNameByAnimeId(animeId, animeinfo);

        AnimeGenreAnalysisResult animeGenreAnalysisResult = new AnimeGenreAnalysisResult();
        animeGenreAnalysisResult.setAnimeId(animeId);
        animeGenreAnalysisResult.setAnimeGenre(genreName);
        cassandraUtil.saveAnimeGenreData(animeGenreAnalysisResult); //saving data to output table
    }

    public static String findGenreNameByAnimeId(long animeId, Dataset<Row> animeInfoDataset) {
        // Helper method to find Genre Name by anime id from animeinfo table
        Dataset<Row> filteredData = animeInfoDataset.filter(col("anime_id").equalTo(animeId));
        if (filteredData.count() > 0) {
            String genreName = filteredData.select("genre").first().getString(0);
            return genreName;
        } else {
            return "Genre not found for the specified animeId";
        }
    }

    public static void analyzeStudioPortfolio(Dataset<Row> animeData, String targetStudio) {
        // This method analyzes the portfolio of a studio and the diversity of genre it is invested in
        System.out.println("StudioName: " + targetStudio);
        Dataset<Row> studioData = animeData.filter(col("studio").equalTo(targetStudio));

        // Split genres into separate rows using explode because the format is 'Action,Adventure'
        Dataset<Row> genresData = studioData
                .select("anime_id", "genre", "studio")
                .withColumn("genre", explode(split(col("genre"), ",\\s*")));

        Dataset<Row> genreCount = genresData.groupBy("genre")
                .agg(count("*").as("count"));

        JavaRDD<GenreAnalysisResult> resultRDD = genreCount
                .toJavaRDD()
                .map( row -> {
                    GenreAnalysisResult result = new GenreAnalysisResult();
                    result.setGenreName(row.getString(0));
                    result.setCount(row.getLong(1));
                    return result;
                });
        System.out.println("Saving data in " + GENRE_ANALYSIS_TABLE);
        javaFunctions(resultRDD)
                .writerBuilder(ANIME_KEYSPACE, "genre_analysis", mapToRow(GenreAnalysisResult.class))
                .saveToCassandra();
    }

    public static void analyzeAllGenres(Dataset<Row> animeData) {
        // This method analyzes the genre distribution across various anime in the dataset
        Dataset<Row> genresData = animeData
                .select("anime_id", "genre", "studio")
                .withColumn("genre", explode(split(col("genre"), ",\\s*")));

        Dataset<Row> genreCount = genresData.groupBy("genre")
                .agg(count("*").as("count"));

        JavaRDD<GenreAnalysisResult> resultRDD = genreCount
                .toJavaRDD()
                .map(row -> {
                    GenreAnalysisResult result = new GenreAnalysisResult();
                    result.setGenreName(row.getString(0));
                    result.setCount(row.getLong(1));
                    return result;
                });
            javaFunctions(resultRDD)
                    .writerBuilder(ANIME_KEYSPACE, GENRE_ANALYSIS_TABLE, mapToRow(GenreAnalysisResult.class))
                    .saveToCassandra();
    }
}