
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.ObjectInputStream.GetField;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.Date;

import org.json.JSONException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;


// "http://torahsummary.com/other/app/databases.csv"


public class SQLite {

	private static final int DB_VERION_NUM = 124;
	public static final String DB_NAME_PART = "test" + DB_VERION_NUM;
	public static final String DB_NAME_FULL = "testDBs/" + DB_NAME_PART + ".db";
	
	private static final boolean USE_TEST_FILES = true;
	private static final boolean API_ONLY = false;
	private static final boolean ONLY_COPY_DB = true;

	final static boolean ignoreSchemaError = false;

	final static String exportPath = "../Sefaria-Export/";

	protected static Map<String,Integer> booksInDB = new HashMap<String, Integer>(); 
	protected static Map<String,Integer> booksInDBbid = new HashMap<String, Integer>();
	protected static Map<String,Integer> booksInDBtextDepth = new HashMap<String, Integer>();
	protected static Map<Integer,String> booksInDBbid2Title = new HashMap<Integer,String>();




	protected static final int MAX_LEVELS = 6;
	protected static int textsFailedToUpload = 0;
	protected static int textsUploaded = 0;


	public static void println(String message){
		System.out.println(message);
	}

	public static void main( String args[] )
	{
		System.out.println("I'm starting this up...");

		try {
			Class.forName("org.sqlite.JDBC");
			if(ONLY_COPY_DB){
				//Huffman.test();
				String newDB = "testDBs/copy_129.db"; //
				String oldDB = "testDBs/117/UpdateForSefariaMobileDatabase.db";//"testDBs/test124.db";//
				copyNewDB(oldDB,newDB);
				return;
			}else{
				createTables();
				insertStuff();
			}
			System.out.println("Good stuff");
		}catch(Exception e){
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private final static String CREATE_TABLE_SETTINGS = "CREATE TABLE Settings (_id TEXT PRIMARY KEY, value INTEGER);";
	private final static String CREATE_TABLE_METADATA = " CREATE TABLE \"android_metadata\" (\"locale\" TEXT DEFAULT 'en_US')";

	public static void createTables(){

		Connection c = null;
		try{
			(new File(DB_NAME_FULL)).delete(); //delete old DB is it exists
			
			c = getDBConnection(DB_NAME_FULL);
			System.out.println("Opened database successfully");
			Statement stmt = c.createStatement();

			if(!API_ONLY){
				stmt.executeUpdate(Text.CREATE_TEXTS_TABLE);
				stmt.executeUpdate(Link.CREATE_TABLE_LINKS);
				stmt.executeUpdate(Link.CREATE_LINKS_SMALL);
				stmt.executeUpdate(Node.CREATE_NODE_TABLE);
				stmt.executeUpdate(Searching.CREATE_SEARCH);
			}
			stmt.executeUpdate(Book.CREATE_BOOKS_TABLE);
			stmt.executeUpdate(Header.CREATE_HEADES_TABLE);


			stmt.executeUpdate(CREATE_TABLE_SETTINGS);
			stmt.executeUpdate(" INSERT INTO Settings (_id, value) VALUES ('version'," + DB_VERION_NUM + ")");
			if(API_ONLY)
				stmt.executeUpdate(" INSERT INTO Settings (_id, value) VALUES ('api',1)");
			else
				stmt.executeUpdate(" INSERT INTO Settings (_id, value) VALUES ('api',0)");
			//not needed with new Links_small table
			//stmt.execute("Create index LinksIndex on Links (bida, level1a, level2a)");

			stmt.executeUpdate(CREATE_TABLE_METADATA);
			stmt.executeUpdate(" INSERT INTO \"android_metadata\" VALUES ('en_US')");

			stmt.close();
			System.out.println("Created tables");
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			try {
				c.close();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				System.err.println( e1.getClass().getName() + ": " + e1.getMessage() );
			}
		}
	}

	public static List<String> getFileLines() throws IOException{
		final String fileListPath = "scripts/fileList/";
		String fileList;
		if(USE_TEST_FILES)
			fileList = fileListPath + "fileList_test.txt";
		else
			fileList = fileListPath + "fileList.txt";
		return Files.readAllLines(Paths.get(fileList), Charset.forName("UTF-8"));
	}

	private static Connection getDBConnection(String dbName) throws SQLException{
		return DriverManager.getConnection("jdbc:sqlite:" + dbName);
	}

	private static void copyTable(Connection c, String tableName, String create, String newDB) throws SQLException{
		Statement stmt = c.createStatement();
		stmt.executeUpdate("DROP TABLE IF EXISTS \"" + newDB + "." + tableName + "\";");
		stmt.executeUpdate(create);
		stmt.close();
		c.prepareStatement("INSERT INTO " + tableName + " SELECT * FROM oldDB." + tableName).execute();
	}

	private static void copyTextTable(Connection newDBConnection, String oldDB) throws SQLException{
		Statement stmt = newDBConnection.createStatement();
		stmt.executeUpdate(Text.CREATE_COMPRESS_TEXTS_TABLE);
		stmt.close();

		String columns = "_id,bid,level1,level2,level3,level4," +
				"level5,level6," +
				"hasLink,parentNode";
		newDBConnection.prepareStatement("INSERT INTO Texts (" + columns + ") SELECT " + columns + " FROM oldDB.Texts").execute();
		
		Connection oldDBConnection = getDBConnection(oldDB);
		Huffman.compressAndMoveAllTexts(oldDBConnection, newDBConnection);
	}

	private static void copyNewDB(String oldDB, String newDB){
		System.out.println("Copying DB");
		try {
			File file = new File(newDB);
			file.delete();
			Connection c = getDBConnection(newDB);
			c.prepareStatement("ATTACH DATABASE \"" + oldDB + "\" AS oldDB").execute();
			copyTable(c, "Books", Book.CREATE_BOOKS_TABLE, newDB);
			copyTable(c, "Links_small", Link.CREATE_LINKS_SMALL, newDB);
			copyTable(c, "Nodes", Node.CREATE_NODE_TABLE, newDB);
			//copyTable(c, "Texts", Text.CREATE_TEXTS_TABLE, newDB);
			//copyTable(c, "Headers", Header.CREATE_HEADES_TABLE, newDB);
			//copyTable(c, "Links", Link.CREATE_TABLE_LINKS, newDB);
			copyTable(c, "android_metadata", CREATE_TABLE_METADATA, newDB);
			copyTable(c, "Settings", CREATE_TABLE_SETTINGS, newDB);
			copyTable(c, "Searching", Searching.CREATE_SEARCH, newDB);
			copyTextTable(c, oldDB);
			c.close();


		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("Finished Copying DB");
	}

	public static void insertStuff(){

		Connection c = null;
		try{
			c = getDBConnection(DB_NAME_FULL);
			c.setAutoCommit(false);
			
			int count = 0;
			int failedBooksCount = 0;
			List<String> lines= getFileLines();
			for(int i =0;i<lines.size();i++){
				String line = lines.get(i);
				System.out.println(String.valueOf(++count) + ". " + line);


				boolean doComplex = true;
				try{
					String tempLine = "";
					if(i+1<lines.size()){
						tempLine = lines.get(i+1);
					}
					JSONObject [] EnHeJSONs = getEnHeJSONs(line,tempLine,exportPath);
					JSONObject enJSON,heJSON;
					String title = "";
					enJSON = EnHeJSONs[0];
					heJSON = EnHeJSONs[1];
					if(enJSON != null && heJSON != null){
						i++;//then both lines were used and we should skip the used line.
						title = heJSON.getString("title");
						println("he && en");
					}else if(enJSON == null && heJSON != null){
						title = heJSON.getString("title");
						println("he");
					}else if(enJSON != null && heJSON == null){
						title = enJSON.getString("title");
						println("en");
					}else{
						System.err.print("BOTH JSONs are null! :(");
						continue;
					}

					try{
						//non complex texts
						Book.addBook(c,enJSON,heJSON,false);
						if(!API_ONLY){
							if(heJSON != null)
								Text.addText(c,heJSON);
							if(enJSON != null)
								Text.addText(c,enJSON);
						}
					}catch(JSONException e){ //IT's a COMPLEX TEXT
						if(e.toString().equals("org.json.JSONException: JSONObject[\"sectionNames\"] not found.")
								&& doComplex){
							System.out.println("Complex Text");
							Book.addBook(c, enJSON, heJSON,true);							
							if(!API_ONLY)
								Node.addText(c,enJSON,heJSON);//TODO add enJSON
						}else{
							System.err.println("Error2: " + e);
							failedBooksCount++;
						}
					}
					try{
						if(!API_ONLY){
							String schemaPath = exportPath + "schemas/" + title.replaceAll(" ", "_") + ".json";
							JSONObject schemas = openJSON(schemaPath);
							Node.addSchemas(c, schemas);
						}
					}catch(Exception e){
						if(!ignoreSchemaError || !(e.toString().contains("java.nio.file.NoSuchFileException: ") && e.toString().contains("schema")))
							System.err.println("Error adding Schema: " + e);
					}
					c.commit();

				}catch(Exception e){
					System.err.println("Error123: " + e);
					failedBooksCount++;
				}
			}
			System.out.println("Good Books: " + String.valueOf(count - failedBooksCount) + "\nFailed Books: " + failedBooksCount);
			//System.out.println("TEXTS: en: " + Text.en + " he: " + Text.he + " u2: " + Text.u2 + " u3: " + Text.u3 + " u4: " + Text.u4);


			if(!API_ONLY){
				Searching.putInCountWords(c);
				c.commit();
				System.out.println("ADDING LINKS...");
				CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream("scripts/links/links0.csv")));
				Link.addLinkFile(c, reader);
				c.commit();

				System.out.println("CHANGING displayNum (on Texts)...");
				Text.displayNum(c);
				System.out.println("CHANGING hasLink (on Texts)...");
				Text.setHasLink(c);
			}

			System.out.println("CHANGING book commentary wherePage to 3...");
			Book.convertCommWherePage(c);
			System.out.println("setTidMinMax...");
			Book.setTidMinMax(c);
			c.commit();
			System.out.println("ADDING HEADERS:");
			String folderName = "scripts/headers/headers/";
			Header.addAllHeaders(c, folderName);


			c.commit();
			c.close();
			System.out.println("Records created successfully\nTextUploaded: " + textsUploaded + "\nTextFailed: " + textsFailedToUpload);

		}catch(Exception e){
			try {
				c.close();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
		}

	}

	private static JSONObject openJSON(String line, String pathToJSONs) throws IOException{
		String jsonPath = pathToJSONs + "json/" +line;
		return openJSON(jsonPath);	
	}
	private static JSONObject [] getEnHeJSONs(String line, String tempLine, String path) throws IOException{
		if(!
				tempLine.replace("English/merged.json", "").replace("Hebrew/merged.json", "")
				.equals(
						line.replace("English/merged.json", "").replace("Hebrew/merged.json", ""))
				){
			tempLine = "";//They are not equal so don't use tempLine
		}

		String heLine = "";
		String enLine = "";
		if(line.contains("Hebrew/merged.json")){
			heLine = line;
		}else if(line.contains("English/merged.json")){
			enLine = line;
		}
		else{
			System.err.println("I don't know what the line type is");
		}
		if(tempLine.contains("Hebrew/merged.json")){
			heLine = tempLine;
		}else if(tempLine.contains("English/merged.json")){
			enLine = tempLine;
		}

		JSONObject enJSON = null ,heJSON = null;
		if(!enLine.equals("")){
			enJSON = openJSON(enLine,path);
		}
		if(!heLine.equals("")){
			heJSON = openJSON(heLine,path);
		}
		return new JSONObject [] {enJSON,heJSON};
	}


	static String readFile(String path) throws IOException {

		return readFile(path, StandardCharsets.UTF_8);

	}
	static String readFile(String path, Charset encoding) throws IOException 
	{
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
	}

	static JSONObject openJSON(String filename) throws IOException{

		String jsonText;
		JSONObject object = new JSONObject();
		jsonText = readFile(filename);
		object = (JSONObject) new JSONTokener(jsonText).nextValue();
		return object;
	}

	protected final static int LANG_HE = 2;
	protected final static int LANG_EN = 1;

	static int returnLangNums(String langString){
		if(langString.equals("en"))
			return LANG_EN;
		else if(langString.equals("he"))
			return LANG_HE;
		System.err.println("unrecignized lang:" + langString);
		return 0;

	}


	public static final String TABLE_BOOKS = "Books";
	public static final String KcommentsOn = "commentsOn";
	public static final String KsectionNames = "sectionNames";
	public static final String Kcategories = "categories";
	public static final String KtextDepth = "textDepth";
	public static final String KwherePage = "wherePage";
	public static final String Klengths = "lengths";
	public static final String Ktitle = "title";
	public static final String KheTitle = "heTitle";
	public static final String KdataVersion = "dataVersion";
	public static final String KversionTitle = "versionTitle";
	public static final String Kversions = "versions";
	public static final String Klanguages = "languages"; // en is 1, he is 2, both is 3.
	protected static final int DEFAULT_WHERE_PAGE = 2;

	public static final String Kbid = "bid";
	public static final String KenText = "enText";
	public static final String KheText = "heText";
	public static final String Klevel1 = "level1";
	public static final String Klevel2 = "level2";
	public static final String Klevel3 = "level3";
	public static final String Klevel4 = "level4";
	public static final String Klevel5 = "level5";
	public static final String Klevel6 = "level6";



	public static final String KconnType = "connType";
	public static final String Kbida = "bid" + "a";
	public static final String Klevel1a = "level1a";
	public static final String Klevel2a = "level2a";
	public static final String Klevel3a = "level3a";
	public static final String Klevel4a = "level4a";
	public static final String Klevel5a = "level5a";
	public static final String Klevel6a = "level6a";
	public static final String Kbidb = "bid" + "b";
	public static final String Klevel1b = "level1b";
	public static final String Klevel2b = "level2b";
	public static final String Klevel3b = "level3b";
	public static final String Klevel4b = "level4b";
	public static final String Klevel5b = "level5b";
	public static final String Klevel6b = "level6b";

	public static final String TABLE_LINKS = "Links" ;

	public static final String Kheader = "header";
	public static final String KdisplayNum = "displayNum";
	public static final String KdisplayLevelType = "displayLevelType";


	protected static final String TABLE_TEXTS = "Texts";
	protected static final String LINKS_SMALL = "Links_small";

	protected static final String HAS_TEXT_SQL_ERROR = "[SQLITE_CONSTRAINT]  Abort due to constraint violation (UNIQUE constraint failed: Texts.bid, Texts.level1, Texts.level2, Texts.level3, Texts.level4, Texts.level5, Texts.level6, Texts.parentNode)";
	public static final String TABLE_HEADERS = "Headers";




}
