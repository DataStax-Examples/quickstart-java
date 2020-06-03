package com.datastax.quickstart.netflix;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.LocalDate;
import java.util.List;

/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specifi c language governing permissions and
 * limitations under the License.
 *
 * <p>
 * DataStax Getting Started Java with Netflix
 * <p>
 * Utilizes the Netflix dataset containing data from all Netflix
 * titles and movies as of 2019.
 * <p>
 * Source:https://www.kaggle.com/shivamb/netflix-shows
 * License:Creative Commons:Public Domain
 */

public class CassandraWithNetflix
{

    private static final String TABLE_NETFLIX_MASTER = "netflix_master";
    private static final String TABLE_NETFLIX_TITLES_BY_DATE = "netflix_titles_by_date";
    private static final String TABLE_NETFLIX_TITLES_BY_RATING = "netflix_titles_by_rating";

    private static final String TITLE_PULP_FICTION = "Pulp Fiction";
    private static final String TITLE_LIFE_OF_JIMMY = "Life of Jimmy";
    private static final int SHOW_ID_LIFE_OF_JIMMY = 100000000;
    private static final int SHOW_ID_PULP_FICTION = 100000001;

    private static final String KEYSPACE_NAME = "demo";
    private static final int KEYSPACE_REPLICATION_FACTOR = 1;
    private static final String LOCAL_DC = "datastax-desktop";
    private static final String CONTACT_POINT_ADDRESS = "127.0.0.1";
    private static final int CONTACT_POINT_PORT = 9042;

    private static final Logger log = LoggerFactory.getLogger(CassandraWithNetflix.class);


    /**
     * Execute the main() method of this class
     * to create keyspace, tables, and insert
     * Netflix information into your running
     * Cassandra database.  Enjoy!
     */
    public static void main(String[] args)
    {

        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(CONTACT_POINT_ADDRESS, CONTACT_POINT_PORT))
                .withLocalDatacenter(LOCAL_DC)
                .build())
        {

            // 1. Create the keyspace using our newly constructed CQL session.
            createKeyspace(KEYSPACE_NAME, KEYSPACE_REPLICATION_FACTOR, session);

            // 2. Create the foundation Netflix tables.
            createMasterTable(session);
            createTitlesByDateTable(session);
            createTitlesByRatingTable(session);

            // 3. Write Netflix data into the three newly created tables.
            // Inserting two records into each.
            insertMaster(session);
            insertTitlesByDate(session);
            insertTitlesByRating(session);


            // 4. Read from the new Netflix tables.
            print(readAll(session, TABLE_NETFLIX_MASTER));
            print(readAll(session, TABLE_NETFLIX_TITLES_BY_DATE));
            print(readAll(session, TABLE_NETFLIX_TITLES_BY_RATING));

            // 5. Read all from master table and read using Pulp Fiction title
            print(readAllInMasterByTitle(session, TITLE_PULP_FICTION));
            print(readDirectorInMasterByTitle(session, TITLE_PULP_FICTION));

            // 6. Update the Pulp Fiction movie to have the director's full name.
            // Then, show the updated record.
            updateDirectorInMaster(session,
                    SHOW_ID_PULP_FICTION, TITLE_PULP_FICTION, List.of("Quentin Jerome Tarantino"));

            print(readDirectorInMasterByTitle(session, TITLE_PULP_FICTION));

        }
        catch (Exception ex)
        {
            log.error("An error occurred while running the Netflix Java Example.  Error: {} ", ex.getMessage());
        }
    }

    private static void createKeyspace(String keyspaceName, int replicationFactor, CqlSession session)
    {
        log.info("Creating Keyspace: {}", keyspaceName);
        session.execute(SchemaBuilder.createKeyspace(keyspaceName)
                .ifNotExists()
                .withSimpleStrategy(replicationFactor)
                .build());
    }

    private static void createMasterTable(CqlSession session)
    {
        log.info("Creating Master Table");
        session.execute(SchemaBuilder.createTable(KEYSPACE_NAME, TABLE_NETFLIX_MASTER)
                .ifNotExists()
                .withPartitionKey("title", DataTypes.TEXT)
                .withClusteringColumn("show_id", DataTypes.INT)
                .withColumn("cast", DataTypes.listOf(DataTypes.TEXT))
                .withColumn("country", DataTypes.listOf(DataTypes.TEXT))
                .withColumn("date_added", DataTypes.DATE)
                .withColumn("description", DataTypes.TEXT)
                .withColumn("director", DataTypes.listOf(DataTypes.TEXT))
                .withColumn("duration", DataTypes.TEXT)
                .withColumn("listed_in", DataTypes.listOf(DataTypes.TEXT))
                .withColumn("rating", DataTypes.TEXT)
                .withColumn("release_year", DataTypes.INT)
                .withColumn("title", DataTypes.TEXT)
                .withColumn("type", DataTypes.TEXT)
                .build());
    }

    private static void createTitlesByDateTable(CqlSession session)
    {
        log.info("Creating Titles By Date Table");
        session.execute(SchemaBuilder.createTable(KEYSPACE_NAME, TABLE_NETFLIX_TITLES_BY_DATE)
                .ifNotExists()
                .withPartitionKey("release_year", DataTypes.INT)
                .withClusteringColumn("date_added", DataTypes.DATE)
                .withClusteringColumn("show_id", DataTypes.INT)
                .withColumn("title", DataTypes.TEXT)
                .withClusteringOrder("date_added", ClusteringOrder.DESC)
                .build());
    }

    private static void createTitlesByRatingTable(CqlSession session)
    {
        log.info("Creating Titles By Rating Table");
        session.execute(SchemaBuilder.createTable(KEYSPACE_NAME, TABLE_NETFLIX_TITLES_BY_RATING)
                .ifNotExists()
                .withPartitionKey("rating", DataTypes.TEXT)
                .withClusteringColumn("show_id", DataTypes.INT)
                .withColumn("title", DataTypes.TEXT)
                .build());
    }

    private static void insertMaster(CqlSession session)
    {

        final String query = String.format("INSERT INTO %s.%s", KEYSPACE_NAME, TABLE_NETFLIX_MASTER) +
                " (title, show_id, cast, country, date_added, " +
                "description, director, duration, listed_in, rating, release_year, type) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
        log.debug("Master insert query: {}", query);

        log.info("Inserting into Master Table for '{}' ", TITLE_LIFE_OF_JIMMY);
        session.execute(SimpleStatement.builder(query)
                .addPositionalValues(TITLE_LIFE_OF_JIMMY, SHOW_ID_LIFE_OF_JIMMY, List.of("Jimmy"), List.of("United States"),
                        LocalDate.of(2020, 6, 1), "Experiences of a guitar playing DataStax developer", List.of("Franky J"),
                        "42 min", List.of("Action"), "TV-18", 2020, "Movie")
                .build());

        log.info("Inserting into Master Table for '{}' ", TITLE_PULP_FICTION);
        session.execute(SimpleStatement.builder(query)
                .addPositionalValues(TITLE_PULP_FICTION, SHOW_ID_PULP_FICTION, List.of("John Travolta", "Samuel L. Jackson", "Uma Thurman", "Harvey Keitel", "Tim Roth",
                        "Amanda Plummer", "Maria de Medeiros", "Ving Rhames", "Eric Stoltz", "Rosanna Arquette", "Christopher Walken",
                        "Bruce Willis"), List.of("United States"), LocalDate.of(2019, 1, 19),
                        "This stylized crime caper weaves together stories ...", List.of("Quentin Tarantino"),
                        "154 min", List.of("Classic Movies", "Cult Movies", "Dramas"), "R", 1994, "Movie")
                .build());
    }

    private static void insertTitlesByDate(CqlSession session)
    {

        final String query = String.format("INSERT INTO %s.%s", KEYSPACE_NAME, TABLE_NETFLIX_TITLES_BY_DATE) +
                " (title, show_id, release_year, date_added) VALUES (?,?,?,?)";
        log.debug("TitlesByDate insert query: {}", query);

        log.info("Inserting into TitlesByDate Table for '{}' ", TITLE_LIFE_OF_JIMMY);
        session.execute(SimpleStatement.builder(query)
                .addPositionalValues(TITLE_LIFE_OF_JIMMY, SHOW_ID_LIFE_OF_JIMMY, 2020, LocalDate.of(2020, 6, 1))
                .build());

        log.info("Inserting into TitlesByDate Table for '{}' ", TITLE_PULP_FICTION);
        session.execute(SimpleStatement.builder(query)
                .addPositionalValues(TITLE_PULP_FICTION, SHOW_ID_PULP_FICTION, 2020, LocalDate.of(2020, 6, 1))
                .build());
    }

    private static void insertTitlesByRating(CqlSession session)
    {

        final String query =
                String.format("INSERT INTO %s.%s", KEYSPACE_NAME, TABLE_NETFLIX_TITLES_BY_RATING) +
                        " (title, show_id, rating) VALUES (?,?,?)";
        log.debug("TitlesByRating insert query: {}", query);

        log.info("Inserting into TitlesByRating Table for '{}' ", TITLE_LIFE_OF_JIMMY);
        session.execute(SimpleStatement.builder(query)
                .addPositionalValues(TITLE_LIFE_OF_JIMMY, SHOW_ID_LIFE_OF_JIMMY, "TV-18")
                .build());

        log.info("Inserting into TitlesByRating Table for '{}' ", TITLE_PULP_FICTION);
        session.execute(SimpleStatement.builder(query)
                .addPositionalValues(TITLE_PULP_FICTION, SHOW_ID_PULP_FICTION, "R")
                .build());
    }

    private static ResultSet readAll(CqlSession session, String tableName)
    {
        log.info("ReadAll From: {}", tableName);
        return session.execute(SimpleStatement.builder(
                String.format("SELECT * FROM %s.%s", KEYSPACE_NAME, tableName))
                .build()
        );
    }

    private static ResultSet readAllInMasterByTitle(CqlSession session, String title)
    {
        log.info("ReadAll from Master, Filtering by Title: '{}'", title);
        return session.execute(SimpleStatement.builder(
                String.format("SELECT * FROM %s.%s WHERE title = ?",
                        KEYSPACE_NAME, TABLE_NETFLIX_MASTER))
                .addPositionalValue(title)
                .build()
        );
    }

    private static ResultSet readDirectorInMasterByTitle(CqlSession session, String title)
    {
        log.info("Read of Director from Master, Filter by Title: '{}'", title);
        return session.execute(SimpleStatement.builder(
                String.format("SELECT director FROM %s.%s WHERE title = ?",
                        KEYSPACE_NAME, TABLE_NETFLIX_MASTER))
                .addPositionalValue(title)
                .build()
        );
    }

    private static void updateDirectorInMaster(CqlSession session, Integer showId,
                                               String title,
                                               List<String> directors)
    {
        log.info("Update of Director in Master by Show Id: {} and Title: '{}'", showId, title);
        session.execute(
                SimpleStatement.builder(
                        String.format("UPDATE %s.%s SET director = ? WHERE show_id = ? and title = ? ",
                                KEYSPACE_NAME, TABLE_NETFLIX_MASTER))
                        .addPositionalValues(directors, showId, title)
                        .build());
    }

    private static void print(ResultSet resultSet)
    {
        resultSet.all().forEach(r -> log.info(r.getFormattedContents()));
    }
}
