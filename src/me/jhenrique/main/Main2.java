package me.jhenrique.main;

import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.Driver;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;


/**
 * Created by Utente on 23/06/2016.
 * Main 2 java elaborating
 */
public class Main2 {

    public static void main(String[] args) {
        ArrayList<String> tweets = readTweets();
        for(String tweet : tweets){
            System.out.println(""+sentiment(tweet));
        }
    }

    private static ArrayList<String> readTweets(){
        ArrayList<String> tweets = new ArrayList<>();

        Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "guido" ) );
        Session session = driver.session();
        StatementResult result = session.run( "MATCH (n:Tweet) RETURN n.Text AS text LIMIT 10" );
            while ( result.hasNext() ){
                Record record = result.next();
                tweets.add(record.get( "text" ).asString());
            }

        session.close();
        driver.close();
        return tweets;
    }

    private static int sentiment(String t){
        int sent=0;
        try {
            Class.forName("org.sqlite.JDBC");
            Connection conn = DriverManager.getConnection("jdbc:sqlite:C:\\Users\\Utente\\Desktop\\dizionario.db");
            java.sql.PreparedStatement stat = conn.prepareStatement("select value from dizionario where word LIKE ? ;");

            String[] result =t.split(" ");

            for(String p : result){
                stat.setString(1, p.toLowerCase());
                ResultSet rs = stat.executeQuery();
                if(rs.next()){
                   String value = rs.getString("value");
                  if(value.equals("P")){
                        sent++;
                      System.out.println("P ->"+p);
                    }else if(value.equals("N")){
                        sent--;
                      System.out.println("N ->"+p);
                    }
                }
                rs.close();
            }
            conn.close();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return sent;
    }

}
