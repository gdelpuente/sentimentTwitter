package me.jhenrique.main;

import com.opencsv.CSVReader;
import me.jhenrique.manager.TweetManager;
import me.jhenrique.manager.TwitterCriteria;
import me.jhenrique.model.Tweet;
import org.neo4j.driver.v1.*;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

public class Main {
    private static final String [] QUERY = {"giannilettieri", "mbrambilla69", "ValeriaValente_", "elezioninapoli", "elezioninapoli2016",
            "elezioniamministrative2016", "Valentesindaco", "stavotaLettieri", "lettierisindaco",
            "demagistrissindaco", "stalotalettieri", "BrambillaSindaco", "elezionicomunali",
            "Napoliè5stelle", "comunali2016", "comunaliNapoli", "liberiamoNapoli", "napoliVale", "demagistris"};
    private static final String [] DEMAGISTRIS = {"#demagistris", "@demagistris", "demagistrissindaco", "#dema"};
    private static final String [] LETTIERI = {"#lettieri", "giannilettieri", "stavotaLettieri", "lettierisindaco","stalotalettieri","liberiamoNapoli"};
    private static final String [] VALENTE = {"ValeriaValente_", "#valente", "Valentesindaco", "napoliVale"};
    private static final String [] BRAMBILLA = {"#brambilla", "#BrambillaSindaco", "mbrambilla69", "MatteoBrambillaSindaco","Napoliè5stelle"};
    private static final String [] ELEZIONI = {"elezioninapoli", "elezioninapoli2016", "elezioniamministrative2016", "comunali2016","comunaliNapoli"};

    public static void main(String[] args) {

        /*Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "guido" ) );
        Session session = driver.session();
        session.close();
        driver.close();
        loadDictionary();
        System.out.println(new Timestamp(new java.util.Date().getTime()));
        loadTweets(QUERY);
        loadTweets(DEMAGISTRIS,1);
        loadTweets(LETTIERI,2);
        loadTweets(VALENTE,3);
        loadTweets(BRAMBILLA,4);
        loadTweets(ELEZIONI,5);
        System.out.println("Loaded Tweets");
        loadDictionary();*/
        /*System.out.println(new Timestamp(new java.util.Date().getTime()));
        digest();
        System.out.println(new Timestamp(new java.util.Date().getTime()));
        System.out.println("digest terminated");
        sentiment();
        System.out.println(new Timestamp(new java.util.Date().getTime()));
        System.out.println("The end");*/
        stampMesure(1);
    }

    private static void stampMesure(int cand){
        Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "guido" ) );
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        DateTimeFormatter format2 = DateTimeFormatter.ofPattern("yyyyMMdd");;

        try {
            Date startDate = format.parse("20160505");
            Date endDate = format.parse("20160608");
            LocalDate start = startDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            LocalDate end = endDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

            for (LocalDate date = start; !date.isAfter(end); date = date.plusDays(1)) {
                Session session = driver.session();
                StatementResult result = session.run("MATCH (n:Tweet {candidato:{cand}, date:{date}}) RETURN sum(n.sentimentScore) as score;",
                        Values.parameters("cand", cand, "date", date.format(format2)));
                if(result.hasNext()){
                    Record record = result.next();
                    double value = record.get("score").asDouble();
                    System.out.println(cand + ";" + date + ";" +  value);
                }
                session.close();
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        driver.close();
    }

    private static void digest(){
        Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "guido" ) );
        Session session = driver.session();
        session.run( "MATCH (n:Tweet) " +
                "WHERE n.analyzed = FALSE " +
                "WITH n, replace(replace(tolower(n.Text),'.',''),',','') as normalized "+
                "WITH n, [w in split(normalized,' ') | trim(w)] as words "+
                "UNWIND words as word " +
                "MERGE (w:Word {word:word}) " +
                "WITH n, w " +
                "CREATE (w)-[r:IN_TWEET]->(n) " +
                "WITH count(r) as wordCount, n " +
                "SET n.wordCount = wordCount;");
        session.close();
        driver.close();
    }

    private static void sentiment(){
        Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "guido" ) );
        Session session = driver.session();
        session.run( "MATCH (n:Tweet)<-[rr:IN_TWEET]-(w)-[:Sentiment]->(:Sentiment) " +
                "OPTIONAL MATCH pos = (n:Tweet)<-[:IN_TWEET]-(w)-[:Sentiment]->(:Sentiment {name:'Positive'}) " +
                "WITH n, toFloat(count(pos)) as plus " +
                "OPTIONAL MATCH neg = (n:Tweet)<-[:IN_TWEET]-(w)-[:Sentiment]->(:Sentiment {name:'Negative'}) " +
                "WITH ((plus - toFloat(count(neg)))/n.wordCount) as score, n " +
                "SET n.sentimentScore = score;");
        session.close();

        Session session1 = driver.session();
        session1.run( "MATCH (n:Tweet) " +//<-[rr:IN_TWEET]-(w)-[:Sentiment]->(:Sentiment) " +
                "WHERE n.sentimentScore >= (.01) " +
                "SET n.sentiment = 'positive', n.analyzed = TRUE ");
        session1.close();

        Session session2 = driver.session();
        session2.run("MATCH (n:Tweet) " +//-[rr:IN_TWEET]-(w)-[r:TEMP]-(word)-[:Sentiment]-(:Sentiment) " +
                "WHERE n.sentimentScore <= (-.001) " +
                "SET n.sentiment = 'negative', n.analyzed = TRUE ");
        session2.close();

        Session session3 = driver.session();
        session3.run("MATCH (n:Tweet) " +//-[rr:IN_TWEET]-(w)-[r:TEMP]-(word)-[:Sentiment]-(:Sentiment) " +
                "WHERE (.01) > n.sentimentScore > (-.01) " +
                "SET n.sentiment = 'neutral', n.analyzed =TRUE ");
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

            reader = new CSVReader(new FileReader("C:\\Users\\Utente\\Desktop\\dizionPULIT.csv"),';');
            String [] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                // nextLine[] is an array of values from the line
                System.out.println(nextLine[0]);
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

    private static void loadTweets(String[] THEQUERY,int cand){
        TwitterCriteria criteria;
        List<Tweet> tweets;
        Tweet t;
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        /**
         * Twitter Query
         */
        for (String q : THEQUERY) {
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
                                " hashtag:{hashtag}, date:{date}, Geo:{geo}, permalink:{permalink}, analyzed:FALSE, candidato:{cand}}) " +
                                "MERGE ( u:User {name:{username}}) " +
                                "MERGE (u)-[:Tweeted]->(t);"
                        , Values.parameters("tid",t.getId(),"username","u"+t.getUsername(),
                                "rt", t.getRetweets(),"fav",t.getFavorites(),"text",t.getText(),"mentions", t.getMentions(),"hashtag",t.getHashtags(),
                                "date",format.format(t.getDate()),"geo",t.getGeo(),"permalink",t.getPermalink(),"cand",cand));

            }

            session.close();
            driver.close();

        }
    }

}