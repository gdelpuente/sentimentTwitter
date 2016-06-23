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
    private static final String [] QUERY2 = {"giannilettieri", "mbrambilla69", "ValeriaValente_", "elezioninapoli", "elezioninapoli2016",
            "elezioniamministrative2016", "Valentesindaco", "stavotaLettieri", "lettierisindaco",
            "demagistrissindaco", "stalotalettieri", "BrambillaSindaco", "elezionicomunali",
            "Napoliè5stelle", "comunali2016", "comunaliNapoli", "liberiamoNapoli", "napoliVale", "demagistris"};
    private static final String [] QUERY = {"giannilettieri"};

    public static void main(String[] args) {
        /*Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "guido" ) );
        Session session = driver.session();
        session.close();
        driver.close();
        loadDictionary();*/
        loadTweets();
        System.out.println("Loaded Tweets");
        digest();
        System.out.println("digest terminated");
        createTemp();
        System.out.println("Temp link created");
        sentiment();
        System.out.println("The end");

    }

    private static void digest(){
        Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "guido" ) );
        Session session = driver.session();
        session.run( "MATCH (n:Tweet) " +
                "WHERE n.analyzed = FALSE " +
                "WITH n, replace(replace(tolower(n.Text),'.',''),',','') as normalized "+
                "WITH n, [w in split(normalized,' ') | trim(w)] as words "+
                "UNWIND words as word " +
                "CREATE (tw:TweetWords {word:word}) " +
                "WITH n, tw " +
                "CREATE (tw)-[r:IN_TWEET]->(n) " +
                "WITH count(r) as wordCount, n " +
                "SET n.wordCount = wordCount;");
        session.close();
        driver.close();
    }

    private static void createTemp(){
        Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "guido" ) );
        Session session = driver.session();
        session.run( "MATCH (n:Tweet)-[:IN_TWEET]-(TweetWord) " +
                "WITH distinct TweetWord " +
                "MATCH  (wordSentiment:Word) " +
                "WHERE TweetWord.word = wordSentiment.word AND (wordSentiment)-[:Sentiment]->(:Sentiment) " +
                "MERGE (TweetWord)-[:TEMP]->(wordSentiment);");
        session.close();
        driver.close();
    }

    private static void sentiment(){
        Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "guido" ) );
        Session session = driver.session();
        session.run( "MATCH (n:Tweet)-[rr:IN_TWEET]-(w)-[r:TEMP]-(word)-[:Sentiment]-(:Sentiment) " +
                "OPTIONAL MATCH pos = (n:Tweet)-[:IN_TWEET]-(TweetWord)-[:TEMP]-(word)-[:Sentiment]-(:Sentiment {name:'Positive'}) " +
                "WITH n, toFloat(count(pos)) as plus " +
                "OPTIONAL MATCH neg = (n:Tweet)-[:IN_TWEET]-(TweetWord)-[:TEMP]-(word)-[:Sentiment]-(:Sentiment {name:'Negative'}) " +
                "WITH ((plus - COUNT(neg))/n.wordCount) as score, n " +
                "SET n.sentimentScore = score;");
        session.close();

        Session session1 = driver.session();
        session1.run( "MATCH (n:Tweet)-[rr:IN_TWEET]-(w)-[r:TEMP]-(word)-[:Sentiment]-(:Sentiment) " +
                "WHERE n.sentimentScore >= (.01) " +
                "SET n.sentiment = 'positive', n.analyzed = TRUE "+
                "DELETE w, r, rr; " );
        session1.close();

        Session session2 = driver.session();
        session2.run("MATCH (n:Tweet)-[rr:IN_TWEET]-(w)-[r:TEMP]-(word)-[:Sentiment]-(:Sentiment) " +
                "WHERE n.sentimentScore <= (-.001) " +
                "SET n.sentiment = 'negative', n.analyzed = TRUE " +
                "DELETE w, r, rr; ");
        session2.close();

        Session session3 = driver.session();
        session3.run("MATCH (n:Tweet)-[rr:IN_TWEET]-(w)-[r:TEMP]-(word)-[:Sentiment]-(:Sentiment) " +
                "WHERE (.01) > n.sentimentScore > (-.01) " +
                "SET n.sentiment = 'neutral', n.analyzed =TRUE " +
                "DELETE w, r, rr;");
        session3.close();

        driver.close();
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

                session.run( "MERGE ( t:Tweet { id:{tid}, name:{username}, RT:{rt}, Fav:{fav}, Text:{text}, mentions:{mentions}," +
                                " hashtag:{hashtag}, date:{date}, Geo:{geo}, permalink:{permalink}, analyzed:FALSE}) " +
                                "MERGE ( u:User {name:{username}}) " +
                                "MERGE (u)-[:Tweeted]->(t);"
                        , Values.parameters("tid",t.getId(),"username","u"+t.getUsername(),
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