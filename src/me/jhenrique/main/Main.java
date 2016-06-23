package me.jhenrique.main;

import com.opencsv.CSVReader;
import me.jhenrique.manager.TweetManager;
import me.jhenrique.manager.TwitterCriteria;
import me.jhenrique.model.Tweet;
import org.neo4j.driver.v1.*;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class Main {
    private static final String [] QUERY = {"giannilettieri", "mbrambilla69", "ValeriaValente_", "elezioninapoli", "elezioninapoli2016",
            "elezioniamministrative2016", "Valentesindaco", "stavotaLettieri", "lettierisindaco",
            "demagistrissindaco", "stalotalettieri", "BrambillaSindaco", "elezionicomunali",
            "Napoliè5stelle", "comunali2016", "comunaliNapoli", "liberiamoNapoli", "napoliVale", "demagistris"};

    public static void main(String[] args) {
        loadDictionary();
        //loadTweets();

    }
    private static void loadDictionary(){
        CSVReader reader;
        Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "guido" ) );

        Session session1 = driver.session();
        session1.run( "MERGE (p:Sentiment {name:'Positive'}) MERGE (n:Sentiment {name:'Negative'});");
        session1.close();
        try {

            reader = new CSVReader(new FileReader("C:\\Users\\Utente\\Desktop\\utfPulito.csv"),';');
            String [] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                // nextLine[] is an array of values from the line
                System.out.println(nextLine[0]+" "+nextLine[1]);
                Session session = driver.session();
                session.run( "MERGE ( w:Word { word:{word} }) WITH w " +
                                "MATCH (p:Sentiment {name:'Positive'}) MERGE (w)-[:Sentiment  {value: [{pos}] }]->(p) WITH w "+
                                "MATCH (n:Sentiment {name:'Negative'}) MERGE (w)-[:Sentiment  {value: [{neg}] }]->(n);"
                        , Values.parameters("word",nextLine[0],"pos",nextLine[1], "neg",nextLine[2]));
                session.close();

            }
            driver.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void loadTweets(){
        TwitterCriteria criteria;
        List<Tweet> tweets;
        Tweet t;

        /**
         * Twitter Query
         */
        for (String q : QUERY) {
            criteria = TwitterCriteria.create()
                    .setQuerySearch(q)
                    .setSince("2016-05-5")
                    .setUntil("2016-06-8");
            tweets = TweetManager.getTweets(criteria);
            System.out.println("N° of tweets for " + q + " = "+ tweets.size());


            //Neo4j

            Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "guido" ) );
            Session session = driver.session();



            for (Tweet tweet : tweets) {
                t = tweet;

                session.run( "MERGE ( t"+t.getId()+":Tweet { name:{username}, RT:{rt}, Fav:{fav}, Text:{text}, mentions:{mentions}," +
                                " hashtag:{hashtag}, date:{date}, Geo:{geo}, permalink:{permalink}}) " +
                                "MERGE ( u"+t.getUsername()+":User {name:{username}}) " +
                                "MERGE (u"+t.getUsername()+")-[:Tweeted]->(t"+t.getId()+");"
                        , Values.parameters("tId","t"+t.getId(),"username","u"+t.getUsername(),
                                "rt", t.getRetweets(),"fav",t.getFavorites(),"text",t.getText(),"mentions", t.getMentions(),"hashtag",t.getHashtags(),
                                "date",t.getDate().toString(),"geo",t.getGeo(),"permalink",t.getPermalink()));

            }

             /*StatementResult result = session.run( "MATCH (a:Person) WHERE a.name = 'Arthur' RETURN a.name AS name, a.title AS title" );
            while ( result.hasNext() )
            {
                Record record = result.next();
                System.out.println( record.get( "title" ).asString() + " " + record.get("name").asString() );
            }*/

            session.close();
            driver.close();

        }
    }

}