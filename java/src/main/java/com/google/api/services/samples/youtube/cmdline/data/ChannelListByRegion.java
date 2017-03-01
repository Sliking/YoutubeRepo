package com.google.api.services.samples.youtube.cmdline.data;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.samples.youtube.cmdline.Auth;
import com.google.api.services.youtube.YouTube;
import com.google.api.services.youtube.model.Channel;
import com.google.api.services.youtube.model.ChannelListResponse;
import com.google.api.services.youtube.model.SearchListResponse;
import com.google.api.services.youtube.model.SearchResult;
import com.google.api.services.youtube.model.Video;
import com.google.api.services.youtube.model.VideoListResponse;
import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;

public class ChannelListByRegion {

    private static final String PROPERTIES_FILENAME = "youtube.properties";
    static String apiKey = null;
    
    //Youtube
    private static final long NUMBER_OF_VIDEOS_RETURNED = 50;
    private static YouTube youtube;
    
    //Database
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
    static final String DB_URL = "jdbc:mysql://localhost/YoutubeChannels";
    static final String USER = "miguel";
    static final String PASS = "pass123";
    
	static PreparedStatement stmt = null;
	static PreparedStatement stmtverify = null;
	static Connection conn = null;


    public static void main(String[] args) {
    	
    	connectToDB();
    	
    	loadKey();
    	
    	locationCircles();
    	
    	//lisbonArea();
    	
    	//perCountryAndDescription();
    }
    
    public static void loadKey(){
    	Properties properties = new Properties();
        try {
            InputStream in = ChannelListByRegion.class.getResourceAsStream("/" + PROPERTIES_FILENAME);
            properties.load(in);

        } catch (IOException e) {
            System.err.println("There was an error reading " + PROPERTIES_FILENAME + ": " + e.getCause()
                    + " : " + e.getMessage());
            System.exit(1);
        }
        
        apiKey = properties.getProperty("youtube.apikey");
    }
    
    public static void testInSmallGroup(){
    	
    }
    
    public static void perCountryAndDescription(){

        try {
            youtube = new YouTube.Builder(Auth.HTTP_TRANSPORT, Auth.JSON_FACTORY, new HttpRequestInitializer() {
                @Override
                public void initialize(HttpRequest request) throws IOException {
                }
            }).setApplicationName("youtube-cmdline-channellistbyregion").build();
            
            YouTube.Search.List search = youtube.search().list("snippet");

            String nextToken = "";
            List<SearchResult> searchResults = new ArrayList<SearchResult>();
            
            //double increment = 0.4499640028797696; -- 50km
            double increment = 0.0899928005759539; //10km
            
            double limitW = -9.5020795;
            double limitE = -6.1928558;
            double limitN = 42.1438053;
            double limitS = 36.9655293;
            
            double latitude = limitN;
            double longitude = limitW;
            
            while(latitude >= limitS){
            	while(longitude <= limitE){
            		//Parameters    
                    search.setKey(apiKey);
                    search.setLocation("" + latitude +"," + longitude);
                    search.setLocationRadius("10km");
                    search.setType("video");
                    search.setMaxResults(NUMBER_OF_VIDEOS_RETURNED);
                    search.setFields("items(snippet/channelId)");
                    
                    do{
                    
        	            search.setPageToken(nextToken);
        	            SearchListResponse searchResponse = search.execute();
        	            searchResults.removeAll(searchResponse.getItems());
        	            searchResults.addAll(searchResponse.getItems());

        	            nextToken = searchResponse.getNextPageToken();
        	            
                    } while(nextToken != null);
                    
                    longitude += increment;
                    
                    System.out.println("Horizontal move - coordinates (" + latitude + "," + longitude + ")");
            	}
            	
            	longitude = limitW;
            	latitude -= increment;
            	System.out.println("================================================");
            	System.out.println("Vertical move - coordinates (" + latitude + "," + longitude + ")");
            	System.out.println("================================================");
            }	

            if (searchResults != null) {
            	
            	System.out.println("================================================");
            	System.out.println("Inserting in local database");
            	System.out.println("================================================");
            	
            	HashSet<String> hs = new HashSet<String>();
            	
            	for (SearchResult searchResult : searchResults) {
                	String channelID = searchResult.getSnippet().getChannelId();
            		if(getChannelCountry(channelID) || getChannelDescriptionLanguage(channelID)){
            			hs.add(channelID);
            		}
            	}
            	
            	int counter = 0;
            	for(String channelid : hs){
            		insertIntoDB(channelid);
            		counter++;
            		System.out.println("Added! Total channelid: " + counter);
            	}
            }
        }
            		      
		catch (GoogleJsonResponseException e) {
            System.err.println("There was a service error: " + e.getDetails().getCode() + " : "
                    + e.getDetails().getMessage());
        } catch (IOException e) {
            System.err.println("There was an IO error: " + e.getCause() + " : " + e.getMessage());
        } catch (Throwable t) {
            t.printStackTrace();
        } 
    }
    
    public static void lisbonArea(){
        try {
            youtube = new YouTube.Builder(Auth.HTTP_TRANSPORT, Auth.JSON_FACTORY, new HttpRequestInitializer() {
                @Override
                public void initialize(HttpRequest request) throws IOException {
                }
            }).setApplicationName("youtube-cmdline-channellistbyregion").build();
            
            YouTube.Search.List search = youtube.search().list("snippet");
                        
            String nextToken = "";
            List<SearchResult> searchResults = new ArrayList<SearchResult>();
            
            double radius = 0.1;
            double increment = 0.1;
            
            while(radius < 100){
        		//Parameters    
                search.setKey(apiKey);
                search.setLocation("38.722505,-9.139378");
                search.setLocationRadius(radius + "km");
                search.setType("video");
                search.setMaxResults(NUMBER_OF_VIDEOS_RETURNED);
                search.setFields("items(snippet/channelId)");
                
                do{
                
    	            search.setPageToken(nextToken);
    	            SearchListResponse searchResponse = search.execute();
    	            searchResults.removeAll(searchResponse.getItems());
    	            searchResults.addAll(searchResponse.getItems());

    	            nextToken = searchResponse.getNextPageToken();
    	            
                } while(nextToken != null);
                
                radius += increment;
                
                System.out.println("================================================");
            	System.out.println("Radius: " + radius);
            	System.out.println("================================================");
        	}

            if (searchResults != null) {
            	
            	System.out.println("================================================");
            	System.out.println("Inserting in local database");
            	System.out.println("================================================");
            	
            	HashSet<String> hs = new HashSet<String>();
            	
            	for (SearchResult searchResult : searchResults) { 
                	String channelID = searchResult.getSnippet().getChannelId();
            		if(getChannelCountry(channelID) && verifyIfExists(channelID)){
            			hs.add(channelID);
            		}
            	}
            	
            	int counter = 0;
            	for(String channelid : hs){
            		insertIntoDB(channelid);
            		counter++;
            		System.out.println("Added! Total channelid: " + counter);
            	}
            }
        }
            		      
		catch (GoogleJsonResponseException e) {
            System.err.println("There was a service error: " + e.getDetails().getCode() + " : "
                    + e.getDetails().getMessage());
        } catch (IOException e) {
            System.err.println("There was an IO error: " + e.getCause() + " : " + e.getMessage());
        } catch (Throwable t) {
            t.printStackTrace();
        } 
    }
    
    public static void locationCircles(){
        try {
            youtube = new YouTube.Builder(Auth.HTTP_TRANSPORT, Auth.JSON_FACTORY, new HttpRequestInitializer() {
                @Override
                public void initialize(HttpRequest request) throws IOException {
                }
            }).setApplicationName("youtube-cmdline-channellistbyregion").build();
            
            YouTube.Search.List search = youtube.search().list("snippet");
                        
            String nextToken = "";
            List<SearchResult> searchResults = new ArrayList<SearchResult>();
            
            //double increment = 0.4499640028797696; //-- 50km
            double increment = 0.0899928005759539; //10km
            
            double limitW = -9.5020795;
            double limitE = -6.1928558;
            double limitN = 42.1438053;
            double limitS = 36.9655293;
            
            double latitude = limitN;
            double longitude = limitW;
            
            while(latitude >= limitS){
            	while(longitude <= limitE){
            		//Parameters    
                    search.setKey(apiKey);
                    search.setLocation("" + latitude +"," + longitude);
                    search.setLocationRadius("10km");
                    search.setType("video");
                    search.setMaxResults(NUMBER_OF_VIDEOS_RETURNED);
                    search.setFields("items(snippet/channelId)");
                    
                    do{
                    
        	            search.setPageToken(nextToken);
        	            SearchListResponse searchResponse = search.execute();
        	            searchResults.removeAll(searchResponse.getItems());
        	            searchResults.addAll(searchResponse.getItems());

        	            nextToken = searchResponse.getNextPageToken();
        	            
                    } while(nextToken != null);
                    
                    longitude += increment;
                    
                    System.out.println("Horizontal move - coordinates (" + latitude + "," + longitude + ")");
            	}
            	
            	longitude = limitW;
            	latitude -= increment;
            	System.out.println("================================================");
            	System.out.println("Vertical move - coordinates (" + latitude + "," + longitude + ")");
            	System.out.println("================================================");
            }	

            if (searchResults != null) {
            	
            	System.out.println("================================================");
            	System.out.println("Inserting in local database");
            	System.out.println("================================================");
            	
            	HashSet<String> hs = new HashSet<String>();
            	
            	for (SearchResult searchResult : searchResults) {
            		String channelID = searchResult.getSnippet().getChannelId();
            		if(getChannelCountry(channelID) && verifyIfExists(channelID)){
            			hs.add(channelID);
            		}
            	}
            	
            	int counter = 0;
            	for(String channelid : hs){
            		insertIntoDB(channelid);
            		counter++;
            		System.out.println("Added! Total channelid: " + counter);
            	}
            }
        }
            		      
		catch (GoogleJsonResponseException e) {
            System.err.println("There was a service error: " + e.getDetails().getCode() + " : "
                    + e.getDetails().getMessage());
        } catch (IOException e) {
            System.err.println("There was an IO error: " + e.getCause() + " : " + e.getMessage());
        } catch (Throwable t) {
            t.printStackTrace();
        } 
    }
    
    public static void connectToDB(){    	
    	try{
    		Class.forName("com.mysql.cj.jdbc.Driver");
    	
    	    conn = DriverManager.getConnection(DB_URL,USER,PASS);

	    }catch(SQLException se){
		    se.printStackTrace();
		}catch(Exception e){
		    e.printStackTrace();
		}
    }
    
    public static void insertIntoDB(String id){
	    try {
	    	stmt = conn.prepareStatement("INSERT INTO channelid(channelid) VALUES (?)");
			stmt.setString(1, id);
			stmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}    
    }
    
    public static ResultSet getAllFromDB(){
    	try{
	    	PreparedStatement ps = conn.prepareStatement("SELECT * FROM channelid");
	    	ResultSet rs = ps.executeQuery();
	    	return rs;
    	} catch(SQLException e){
    		e.printStackTrace();
    		return null;
    	}
    }
    
    public static boolean verifyIfExists(String channelID){
    	try{
    		stmtverify = conn.prepareStatement("SELECT count(*) FROM channelid WHERE channelid = ?");
    		stmtverify.setString(1, channelID);
    		ResultSet resultSet = stmtverify.executeQuery();
    		if(resultSet.next()) {
    		    if(resultSet.getInt(1) > 0){
    		    	return false; //Existe
    		    }
    		    else{
    		    	return true; //Nao existe
    		    }
    		}
    	} catch (SQLException e) {
			e.printStackTrace();
		}  
    	
    	return false;
    }
    
    public static boolean getChannelCountry(String channelID){
        try {
        	
        	ChannelListResponse channelListResponse = youtube.channels().
                    list("snippet").setKey(apiKey).setId(channelID).execute();
        	
        	List<Channel> channelList = channelListResponse.getItems();
        	        	
        	if (channelList.isEmpty()) {
                System.out.println("Can't find a channel with ID: " + channelID);
                return false;
            }        	
    		if(channelList.get(0).getSnippet().getCountry() == null){
    			if(getChannelDescriptionLanguage(channelList.get(0).getSnippet().getDescription())){
    				return true;
    			}
    			else{
    				return false;
    			}
    		}
    		if(channelList.get(0).getSnippet().getCountry().equals("PT")){
    			return true;
    		}
        	
            
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        return false;
    }
    
    public static boolean getChannelDescriptionLanguage(String description){
    	try {       	      	
        	//load all languages:
    		List<LanguageProfile> languageProfiles;
			languageProfiles = new LanguageProfileReader().readAllBuiltIn();
			//build language detector:
    		LanguageDetector languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
    		        .withProfiles(languageProfiles)
    		        .build();

    		//create a text object factory
    		TextObjectFactory textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();

    		//query:
    		TextObject textObject = textObjectFactory.forText(description);
    		Optional<LdLocale> lang = languageDetector.detect(textObject);
    		
    		if(lang.isPresent()){
    			if(lang.get().getLanguage().equals("pt")){
        			return true;
        		}
    		}
    		       	
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return false;
    }
    
    public static boolean getVideoCountry(String videoID){
    	
    	YouTube.Videos.List listVideosRequest;
		try {
			listVideosRequest = youtube.videos().list("snippet,status").setId(videoID);
			listVideosRequest.setKey(apiKey);
	        VideoListResponse listResponse = listVideosRequest.execute();
	        List<Video> videoList = listResponse.getItems();
	        
	        if (videoList.isEmpty()) {
                System.out.println("Can't find a channel with ID: " + videoID);
                return false;
            }        	
	        	        
    		if(videoList.get(0).getSnippet().getDefaultLanguage() == null){
    			return false;
    		}
    		if(videoList.get(0).getSnippet().getDefaultLanguage().equals("PT")){
    			return true;
    		}
	        
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return false;
        
    }
}
