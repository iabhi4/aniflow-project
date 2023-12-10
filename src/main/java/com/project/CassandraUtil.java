package com.project;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import com.project.dao.AnimeGenreAnalysisResult;

import java.net.InetSocketAddress;
import java.time.Duration;

public class CassandraUtil {
//Creating and managing the results table in cassandra

    private static final String ANIME_KEYSPACE = "anime";
    private static final String GENDER_ANALYSIS_TABLE = "gender_analysis";
    private static final String AGE_ANALYSIS_TABLE = "age_analysis";

    private static final String AGE_GENDER_ANALYSIS_TABLE = "age_gender_analysis";

    private static final String ANIME_SCORE_ANALYSIS_TABLE = "anime_score_analysis";

    private static final String ANIME_GENRE_TABLE = "anime_genre_analysis";

    private static final String GENRE_ANALYSIS_TABLE = "genre_analysis";

    private String ipAddress;

    private int port;

    CassandraUtil(String ipAddress, int port) {
        this.ipAddress = ipAddress;
        this.port = port;
    }

    public void createTables(String operation) {
        DriverConfigLoader loader = createDriverConfigLoader();

        try (CqlSession session = createCqlSession(loader)) {
            dropTablesIfExists(session, operation);
            createTables(session, operation);
        }
    }

    private DriverConfigLoader createDriverConfigLoader() {
        return DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10))
                .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(10))
                .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(10))
                .build();
    }

    private CqlSession createCqlSession(DriverConfigLoader loader) {
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(ipAddress, port))
                .withLocalDatacenter("datacenter1")
                .withConfigLoader(loader)
                .build();
    }

    private void dropTablesIfExists(CqlSession session, String operation) {
        if(operation.startsWith(GENRE_ANALYSIS_TABLE)) operation = GENRE_ANALYSIS_TABLE;
        switch (operation) {
            case GENRE_ANALYSIS_TABLE:
                dropTable(session, ANIME_KEYSPACE, GENRE_ANALYSIS_TABLE);
                break;
            case ANIME_GENRE_TABLE:
                dropTable(session, ANIME_KEYSPACE, ANIME_GENRE_TABLE);
                break;
            case ANIME_SCORE_ANALYSIS_TABLE:
                dropTable(session, ANIME_KEYSPACE, ANIME_SCORE_ANALYSIS_TABLE);
                break;
            case GENDER_ANALYSIS_TABLE:
                dropTable(session, ANIME_KEYSPACE, GENDER_ANALYSIS_TABLE);
                break;
            case AGE_ANALYSIS_TABLE:
                dropTable(session, ANIME_KEYSPACE, AGE_ANALYSIS_TABLE);
                break;
            case AGE_GENDER_ANALYSIS_TABLE:
                dropTable(session, ANIME_KEYSPACE, AGE_GENDER_ANALYSIS_TABLE);
                break;
        }
    }

    private void dropTable(CqlSession session, String keyspace, String tableName) {
        Drop dropTable = SchemaBuilder.dropTable(keyspace, tableName).ifExists();
        session.execute(dropTable.build());
    }

    private void createTables(CqlSession session, String operation) {
        if(operation.startsWith(GENRE_ANALYSIS_TABLE)) operation = GENRE_ANALYSIS_TABLE;
        switch (operation) {
            case GENRE_ANALYSIS_TABLE:
                createProductionPortfolioTable(session);
                break;
            case ANIME_GENRE_TABLE:
                createAnimeGenreResultTable(session);
                break;
            case ANIME_SCORE_ANALYSIS_TABLE:
                createAnimeScoreAnalysisResultTable(session);
                break;
            case GENDER_ANALYSIS_TABLE:
                createGenderAnalysisResultTable(session);
                break;
            case AGE_ANALYSIS_TABLE:
                createAgeAnalysisResultTable(session);
                break;
            case AGE_GENDER_ANALYSIS_TABLE:
                createAgeGenderAnalysisResultTable(session);
                break;
        }
    }

    private void createGenderAnalysisResultTable(CqlSession session) {
        CreateTable createTable = SchemaBuilder.createTable(ANIME_KEYSPACE, GENDER_ANALYSIS_TABLE)
                .withPartitionKey("gender", DataTypes.TEXT)
                .withColumn("percentage", DataTypes.FLOAT);

        session.execute(createTable.build());
        System.out.println("Gender Analysis Table created");
    }

    private void createAgeAnalysisResultTable(CqlSession session) {
        CreateTable createTable = SchemaBuilder.createTable(ANIME_KEYSPACE, AGE_ANALYSIS_TABLE)
                .withPartitionKey("age", DataTypes.BIGINT)
                .withColumn("count", DataTypes.BIGINT);

        session.execute(createTable.build());
        System.out.println("Age Analysis Table created");
    }

    private void createAgeGenderAnalysisResultTable(CqlSession session) {
        CreateTable createTable = SchemaBuilder.createTable(ANIME_KEYSPACE, AGE_GENDER_ANALYSIS_TABLE)
                .withPartitionKey("age", DataTypes.BIGINT)
                .withColumn("male", DataTypes.BIGINT)
                .withColumn("female", DataTypes.BIGINT);

        session.execute(createTable.build());
        System.out.println("Age-Gender Analysis Table created");
    }

    private void createAnimeScoreAnalysisResultTable(CqlSession session) {
        CreateTable createTable = SchemaBuilder.createTable(ANIME_KEYSPACE, ANIME_SCORE_ANALYSIS_TABLE)
                .withPartitionKey("anime_id", DataTypes.BIGINT)
                .withColumn("average_score", DataTypes.FLOAT);

        session.execute(createTable.build());
        System.out.println("Anime-Score Analysis Table created");
    }

    private void createAnimeGenreResultTable(CqlSession session) {
        CreateTable createTable = SchemaBuilder.createTable(ANIME_KEYSPACE, ANIME_GENRE_TABLE)
                .withPartitionKey("anime_id", DataTypes.BIGINT)
                .withColumn("anime_genre", DataTypes.TEXT);

        session.execute(createTable.build());
        System.out.println("Anime-Genre Analysis Table created");
    }

    private void createProductionPortfolioTable(CqlSession session) {
        CreateTable createTable = SchemaBuilder.createTable(ANIME_KEYSPACE, GENRE_ANALYSIS_TABLE)
                .withPartitionKey("genre_name", DataTypes.TEXT)
                .withColumn("count", DataTypes.BIGINT);

        session.execute(createTable.build());
        System.out.println("Studio Analysis Table created");
    }

    /*public String fetchGenreNameByAnimeId(long animeId) throws SQLException {
        System.out.println("Fetching Genre Name");
        DriverConfigLoader loader = createDriverConfigLoader();
        CqlSession cqlSession = createCqlSession(loader);
        String query = "SELECT genre FROM anime.animeinfo WHERE anime_id = ?";
        PreparedStatement preparedStatement = cqlSession.prepare(query);
        BoundStatement boundStatement = preparedStatement.bind(animeId);
        SinglePageResultSet resultSet = (SinglePageResultSet) cqlSession.execute(boundStatement);

        if (resultSet.one() != null) {
            Row row = resultSet.one();
            if (row != null && row.getString("genre") != null) {
                return row.getString("genre");
            }
        }

        System.out.println("Genre not found");
        return "";
    }*/

    public void saveAnimeGenreData(AnimeGenreAnalysisResult animeGenreAnalysisResult) {
        System.out.println("Saving Genre Name");
        DriverConfigLoader loader = createDriverConfigLoader();
        CqlSession cqlSession = createCqlSession(loader);
        String insertQuery = "INSERT INTO anime.anime_genre_analysis (anime_id, anime_genre) VALUES (?, ?)";
        SimpleStatement statement = SimpleStatement.builder(insertQuery)
                .addPositionalValues(animeGenreAnalysisResult.getAnimeId(), animeGenreAnalysisResult.getAnimeGenre())
                .build();
        cqlSession.execute(statement);
        System.out.println("Saved Genre Name");
    }
}