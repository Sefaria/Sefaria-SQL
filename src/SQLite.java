
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.json.JSONException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;


// "http://torahsummary.com/other/app/databases.csv"


public class SQLite {

	public static final String DB_NAME = "test_101.db";
	private static final boolean USE_TEST_FILES = false;
	private static final int DB_VERION_NUM = 108;

	protected static Map<String,Integer> booksInDB= new HashMap<String, Integer>(); 
	protected static Map<String,Integer> booksInDBbid = new HashMap<String, Integer>();
	protected static Map<String,Integer> booksInDBtextDepth = new HashMap<String, Integer>();

	protected static int idCount = 0;


	protected static final String TABLE_TEXTS = "Texts";
	protected static final String LINKS_SMALL = "Links_small";



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

	protected static final int MAX_LEVELS = 6;
	protected static int textsFailedToUpload = 0;
	protected static int textsUploaded = 0;

	protected static final String HAS_TEXT_SQL_ERROR = "[SQLITE_CONSTRAINT]  Abort due to constraint violation (UNIQUE constraint failed: Texts.bid, Texts.level1, Texts.level2, Texts.level3, Texts.level4, Texts.level5, Texts.level6)";


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
    

	
	
    public static final String TABLE_HEADERS = "Headers";

	public static void main( String args[] )
	{
		System.out.println("I'm starting this up...");


		try {
			Class.forName("org.sqlite.JDBC");
			createTables();
			insertStuff();
			
			System.out.println("Good stuff");
		}catch(Exception e){
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
	}

	public static void createTables(){

		Connection c = null;
		try{

			c = DriverManager.getConnection("jdbc:sqlite:" + DB_NAME);
			System.out.println("Opened database successfully");
			Statement stmt = c.createStatement();
			stmt.executeUpdate("DROP TABLE IF EXISTS " + "\"android_metadata\"");
			stmt.executeUpdate("DROP TABLE IF EXISTS " + TABLE_HEADERS);			
			stmt.executeUpdate("DROP TABLE IF EXISTS " + TABLE_TEXTS);
			stmt.executeUpdate("DROP TABLE IF EXISTS " + TABLE_BOOKS);
			stmt.executeUpdate("DROP TABLE IF EXISTS " + TABLE_LINKS);
			stmt.executeUpdate("DROP TABLE IF EXISTS " + LINKS_SMALL);
			stmt.executeUpdate("DROP TABLE IF EXISTS " + "Nodes");
			stmt.executeUpdate("DROP TABLE IF EXISTS " + "Searching");
			stmt.executeUpdate("DROP TABLE IF EXISTS " + "Settings");
			
			
			stmt.executeUpdate(Text.CREATE_TEXTS_TABLE);
			stmt.executeUpdate(Book.CREATE_BOOKS_TABLE);
			stmt.executeUpdate(Link.CREATE_TABLE_LINKS);
			stmt.executeUpdate(Header.CREATE_HEADES_TABLE);
			stmt.executeUpdate(Link.CREATE_LINKS_SMALL);
			stmt.executeUpdate(Node.CREATE_NODE_TABLE);
			stmt.executeUpdate("CREATE TABLE Settings (_id TEXT PRIMARY KEY, value INTEGER);");
			stmt.executeUpdate(" INSERT INTO Settings (_id, value) VALUES ('version'," + DB_VERION_NUM + ")");
			//not needed with new Links_small table
			//stmt.execute("Create index LinksIndex on Links (bida, level1a, level2a)");

			stmt.executeUpdate(" CREATE TABLE \"android_metadata\" (\"locale\" TEXT DEFAULT 'en_US')");
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

	public static void insertStuff(){

		Connection c = null;
		try{



			c = DriverManager.getConnection("jdbc:sqlite:" + DB_NAME);
			c.setAutoCommit(false);

			String fileList;
			if(USE_TEST_FILES)
				fileList = "fileList_test.txt";
			else
				fileList = "fileList1.txt";
			
			List<String> lines=Files.readAllLines(Paths.get(fileList), Charset.forName("UTF-8"));
			int count = 0;
			int failedBooksCount = 0;
			for(String line:lines){
				System.out.println(String.valueOf(++count) + ". " + line);
				//if(count == 21)
					////break;
				try{
					JSONObject json = openJSON("F:/Google Drive/Programs/sefaria/Sefaria-Data/export/json/" + line);
					Book.addBook(c, json);
					Text.addText(c,json);
					c.commit();
				}catch(Exception e){
					System.err.println("Error: " + e);
					failedBooksCount++;
				}
			}
			System.out.println("Good Books: " + String.valueOf(count - failedBooksCount) + "\nFailed Books: " + failedBooksCount);
			//System.out.println("TEXTS: en: " + Text.en + " he: " + Text.he + " u2: " + Text.u2 + " u3: " + Text.u3 + " u4: " + Text.u4);
			
			
			Searching.putInCountWords(c);
			
			System.out.println("ADDING LINKS...");
			CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream("5link0.csv")));
			Link.addLinkFile(c, reader);
			c.commit();
			
			System.out.println("CHANGING displayNum (on Texts)...");
			Text.displayNum(c);
			System.out.println("CHANGING hasLink (on Texts)...");
			Text.setHasLink(c);
			System.out.println("CHANGING book commentary wherePage to 3...");
			Book.convertCommWherePage(c);
			System.out.println("setTidMinMax...");
			Book.setTidMinMax(c);
			c.commit();
			System.out.println("ADDING HEADERS:");
			String folderName = "headers/";
			Header.addAllHeaders(c, folderName);
			
			
			
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

	static int returnLangNums(String langString){
		int langs = 0;
		if(langString.equals("en"))
			langs = 1;
		else if(langString.equals("he"))
			langs = 2;
		return langs;

	}

}
