/**
 * Created by Igor on 3/28/2017.
 */

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;


public class Main {

    public static void main(String[] arg){

        // Connect to cassandra
        String serverIP = "127.0.0.1";
        String keyspace = "system";

        QueryOptions options= new QueryOptions();
        options.setConsistencyLevel(ConsistencyLevel.ALL);

        Cluster cluster = Cluster.builder()
                .withQueryOptions(options )
                .addContactPoints(serverIP)
                .build();

        Session session = cluster.connect(keyspace);

        // Create new keyspace
        String cqlStatement = "CREATE KEYSPACE IF NOT EXISTS hellofromjava WITH " +
                "replication = {'class':'SimpleStrategy','replication_factor':1}";

        session.execute(cqlStatement);
        session.close();

        System.out.println("Connected successfully");

        //----------------------------------------------------------

        //Creating a ColumnFamily
        // based on the above keyspace, we would change the cluster and session as follows:
        session = cluster.connect("hellofromjava");

        cqlStatement = "CREATE TABLE IF NOT EXISTS users (" +
                " user_name varchar PRIMARY KEY," +
                " password varchar " +
                ");";

        session.execute(cqlStatement);

        System.out.println("New table created");
        //----------------------------------------------------------

        System.out.println("Inserting.............");

        // for all three it works the same way (as a note the 'system' keyspace cant
        // be modified by users so below im using a keyspace name 'exampkeyspace' and
        // a table (or columnfamily) called users
        session = cluster.connect("hellofromjava");

        cqlStatement = "INSERT INTO users (user_name, password) " +
                "VALUES ('user1', 'pass1')";

        session.execute(cqlStatement); // interchangeable, put any of the statements u wish.


        cqlStatement = "INSERT INTO users (user_name, password) " +
                "VALUES ('user2', 'pass2')";

        session.execute(cqlStatement); // interchangeable, put any of the statements u wish.


        // by query builder
        Statement builderStatement = QueryBuilder.insertInto("hellofromjava","users").value("user_name","user3").value("password","pass3");
        builderStatement.setConsistencyLevel(ConsistencyLevel.ALL);

        System.out.println(builderStatement.toString());
        ResultSet result =  session.execute(builderStatement);
        System.out.println(result.toString());


        //----------------------------------------------------------
        System.out.println("Selecting.............");


        // Read

        cqlStatement = "SELECT * FROM users";
        for (Row row : session.execute(cqlStatement)) {

            System.out.println(row.toString());
        }

        System.out.println("Selecting by query builder.............");

        //by query builder
        builderStatement =QueryBuilder.select("password").from("hellofromjava","users")
                .where(QueryBuilder.eq("user_name","user1"));

        result = session.execute(builderStatement);
        for (Row row : result) {
            System.out.format("%s \n", row.getString("password"));
        };

        //----------------------------------------------------------
        //Update

        System.out.println("Updating.............");

        cqlStatement = "UPDATE users " +
                "SET password = 'zzaEcvAf32hla' " +
                "WHERE user_name = 'user1';";


        session.execute(cqlStatement); // interchangeable, put any of the statements u wish.


        System.out.println("Updating by query builder.............");

        //by query builder
        builderStatement =QueryBuilder.update("hellofromjava","users")
                .with(QueryBuilder.set("password","new"))
                .where(QueryBuilder.eq("user_name","user3"));

        session.execute(builderStatement);

        cqlStatement = "SELECT * FROM users";
        for (Row row : session.execute(cqlStatement)) {

            System.out.println(row.toString());
        }

        //----------------------------------------------------------

        //Delete
        System.out.println("Deleting.............");

        cqlStatement = "DELETE FROM users " +
                "WHERE user_name = 'user2';";
        session.execute(cqlStatement); // interchangeable, put any of the statements u wish.


        //by query builder

        System.out.println("Deleting by query builder.............");

        //by query builder
        builderStatement =QueryBuilder.delete().from("hellofromjava","users")
                .where(QueryBuilder.eq("user_name","user3"));
        session.execute(builderStatement);

        cqlStatement = "SELECT * FROM users";
        for (Row row : session.execute(cqlStatement)) {

            System.out.println(row.toString());
        }

        session.close();
    }
}
