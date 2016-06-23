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
        ArrayList<myTweet> tweets = readTweets();
        for(myTweet tweet : tweets){
            System.out.println(""+sentiment(tweet));
        }
    }

    private static ArrayList<myTweet> readTweets(){
        ArrayList<myTweet> tweets = new ArrayList<>();

        Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "guido" ) );
        for(int cand=1; cand<5;cand++) {
            Session session = driver.session();
            StatementResult result = session.run("MATCH (n:Tweet) WHERE n.candidato = {cand} RETURN n.Text AS text, n.Fav as fav, n.RT as rt",
                    Values.parameters("cand", cand));
            while (result.hasNext()) {
                Record record = result.next();
                myTweet t = new myTweet();
                t.text = record.get("text").asString();
                t.fav = record.get("fav").asInt();
                t.rt = record.get("rt").asInt();
                tweets.add(t);
            }
            session.close();
        }
        driver.close();
        return tweets;
    }

    private static int sentiment(myTweet t){
        String text = t.text;
        int sent=0;
        try {
            Class.forName("org.sqlite.JDBC");
            Connection conn = DriverManager.getConnection("jdbc:sqlite:C:\\Users\\Utente\\Desktop\\dizionario.db");
            java.sql.PreparedStatement stat = conn.prepareStatement("select value from dizionario where word LIKE ? ;");

            String[] result =text.split(" ");
            text.replace(".", "").replace(",", "").replace(";", "").replace(":", "").replace(")", "").replace("(", "").replace("-", "");
            for(String p : result){
                stat.setString(1, p.toLowerCase());
                ResultSet rs = stat.executeQuery();
                if(rs.next()){
                   String value = rs.getString("value");
                  if(value.equals("P")){
                        sent++;
                      //System.out.println("P ->"+p);
                    }else if(value.equals("N")){
                        sent--;
                      //System.out.println("N ->"+p);
                    }
                }
                rs.close();
            }
            conn.close();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return (sent * (t.rt+1))+ (Integer.signum(sent)*t.fav);
    }

}
