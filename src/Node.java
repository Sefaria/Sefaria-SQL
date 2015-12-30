import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Set;
import java.util.Stack;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class Node extends SQLite{

	private static final int NODE_BRANCH = 1;
	private static final int NODE_TEXTS = 2;
	private static final int NODE_REFS = 3;

	private static int nodeCount = 0;
	
	static String CREATE_NODE_TABLE = 
			"CREATE TABLE " +  "Nodes " + "(\r\n" + 
			"	_id INTEGER PRIMARY KEY,\r\n" + 
			"	bid INTEGER NOT NULL,\r\n" + 
			"	parentNode INTEGER,\r\n" + 
			"	nodeType INTEGER not null,\r\n" + 
			"	siblingNum INTEGER not null,\r\n" + //0 means the first sibling 
			"	enTitle TEXT,\r\n" + 
			"	heTitle TEXT,\r\n" + 
			
			"	sectionNames TEXT,\r\n" + 
			"	heSectionNames TEXT,\r\n" + 

			
			//for support of altStructs
			"	structNum INTEGER NOT NULL default 1,\r\n" + 
			"	textDepth INTEGER,\r\n" + 

			"	startTid INTEGER,\r\n" +  //maybe only used with refferences on alt structure
			"	endTid INTEGER,\r\n" +  //maybe only used with refferences on alt structure
			"	extraTids TEXT,\r\n" +  //maybe only used with refferences on alt structure ex. "[34-70,98-200]"
//maybe some stuff like to display chap name and or number (ei. maybe add some displaying info)
			
	//		"	FOREIGN KEY (bid) \r\n" + 
	//		"		REFERENCES Books (_id)\r\n" + 
	//		"		ON DELETE CASCADE,\r\n" + 
			"	FOREIGN KEY (parentNode) \r\n" + 
			"		REFERENCES Nodes (_id)\r\n" + 
			"		ON DELETE CASCADE,\r\n" + 
			"	CONSTRAINT uniqSiblingNum UNIQUE (bid,parentNode,siblingNum,structNum)\r\n" + //needs bid b/c otherwise parent is 0 for each root 

				")";
	



	protected static int addText(Connection c, JSONObject json) throws JSONException{
		int lang = returnLangNums(json.getString("language"));
		String title = json.getString("title");
		//printer(title);
		if(!booksInDB.containsKey(title)){
			System.err.println("Don't have book in DB and trying to add text");
			return -1;
		}
		int bid = booksInDBbid.get(title);
		JSONObject node = (JSONObject) json.get("schema");
		JSONObject text = (JSONObject) json.get("text");

		insertNode(c, node,text, 0,0,bid,0,lang);
		return 1; //it worked
	}

	
	
	private final static String INSERT_NODE = "INSERT INTO Nodes (" +
			"_id,bid,parentNode,nodeType,siblingNum,enTitle,heTitle,structNum,textDepth,startTid,endTid,extraTids)"
			+ "VALUES (?,?, ?, ?, ?, ?, ?, ?,?, ?, ?,?);";
	
	private static int insertNode(Connection c, JSONObject node,JSONObject text,int depth, int siblingNum,int bid,int parentNode,int lang){
		String heTitle = node.getString("heTitle");
		String enTitle = node.getString("enTitle");
		int nodeID = ++nodeCount;
		int nodeType;
		JSONArray nodes = null;
		try{
			nodes  =  node.getJSONArray("nodes");
			if(depth >0)
				text = text.getJSONObject(enTitle); //this is the test to determine the Node type
			nodeType = NODE_BRANCH;
		}catch(Exception e){
			nodeType =NODE_TEXTS;//leaf
		}
		
		PreparedStatement stmt = null;
		try{
			stmt = c.prepareStatement(INSERT_NODE);
			stmt.setInt(1, nodeID);
			stmt.setInt(2,bid); // Kbid
			stmt.setInt(3,parentNode);
			stmt.setInt(4,nodeType);
			stmt.setInt(5,siblingNum);
			stmt.setString(6,enTitle);
			stmt.setString(7,heTitle);
			stmt.setInt(8,1); //TODO will need changing //1=> default structure
			//stmt.setInt(6,);
			//stmt.setInt(6,);
			stmt.executeUpdate();
			stmt.close();
		}catch(Exception e){
			System.err.println("Error3: " + e + "--" + nodeID);
		}
		if(nodeType == NODE_BRANCH){
			for(int i =0;i<nodes.length();i++){
				insertNode(c, (JSONObject) nodes.get(i),text,depth+1,i,bid,nodeID,lang);
			}
		} else if(nodeType == NODE_TEXTS){
			JSONArray textArray = (JSONArray) text.get(enTitle);
			ArrayList<Integer> levels = new ArrayList<Integer>();
			int textDepth = insertTextArray(c, textArray, levels,bid,lang,nodeID);
			String updateStatement = "UPDATE Nodes set textDepth = ? WHERE _id = ?";
			try {
				stmt = c.prepareStatement(updateStatement);
				stmt.setInt(1, textDepth);
				stmt.setInt(2, nodeID);
				stmt.executeUpdate();
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
		
		return 0;
	}

	
	private static int insertTextArray(Connection c, JSONArray textArray,ArrayList<Integer> levels,int bid,int lang,int parentNodeID){
		try{
			JSONArray textArray2 = textArray.getJSONArray(0);
			//if this worked that means thats there's more levels
			int returnDepth = 0;
			for(int i=0;i<textArray.length();i++){
				ArrayList<Integer> levels2 = (ArrayList<Integer>) levels.clone();
				levels2.add(i);
				returnDepth = insertTextArray(c, textArray.getJSONArray(i),levels2,bid,lang,parentNodeID);
			}
			return returnDepth;
		}catch(Exception JSONException){
			levels.add(0);
			String title = "complex text:" + bid;
			int [] it = new int[MAX_LEVELS + 1];
			for(int i=0;i<textArray.length();i++){
				levels.set(levels.size()-1, i);
				for(int j=0;j<levels.size();j++)
					it[j+1] = levels.get(levels.size()-j-1);
				//String text = textArray.getString(i);
				//System.out.println(" " + levels + "---" + it);
				Text.insertValues(c, title, levels.size(), bid, textArray, lang, it,parentNodeID);

			}
			return levels.size();
		}
		
	}

	private static int addNodes(Connection c, JSONArray nodes,int bid,int parentNode, int structNum) throws JSONException{
		for(int j =0;j<nodes.length();j++){
			JSONObject node = nodes.getJSONObject(j);
			String enTitle = node.getString("title");
			String heTitle = node.getString("heTitle");
			String nodeTypeString =  node.getString("nodeType");
			int nodeType = -1;
			if(nodeTypeString.equals("ArrayMapNode")){
				nodeType = NODE_REFS;
			}
			else{
				System.err.println("Error not a ref");
				return -1;
			}
			JSONArray sectionNames = node.getJSONArray("sectionNames");
			JSONArray refs = node.getJSONArray("refs");
			System.out.println(enTitle + " " +  heTitle + " " + nodeType + " " + sectionNames + refs);
			int nodeID = ++nodeCount;
			PreparedStatement stmt = null;
			try{
				stmt = c.prepareStatement(INSERT_NODE);
				stmt.setInt(1, nodeID);
				stmt.setInt(2,bid); // Kbid
				stmt.setInt(3,parentNode);
				stmt.setInt(4,nodeType);
				stmt.setInt(5,j);
				stmt.setString(6,enTitle);
				stmt.setString(7,heTitle);
				stmt.setInt(8,structNum); //TODO will need changing //1=> default structure
				//stmt.setInt(6,);
				//stmt.setInt(6,);
				stmt.executeUpdate();
				stmt.close();
			}catch(Exception e){
				System.err.println("Error3: " + e + "--" + nodeID);
			}
			
		}
		return 1;//worked
	}
	
	protected static int addSchemas(Connection c, JSONObject schemas) throws JSONException{
		try{
			
			JSONObject alts = schemas.getJSONObject("alts");
			String bookTitle = schemas.getString("title");
			int bid = booksInDBbid.get(bookTitle);
			Object object;
			String [] altNames = alts.getNames(alts);
			for(int i=0;i<altNames.length;i++){
				//System.out.println(altNames[i]);
				JSONObject alt = alts.getJSONObject(altNames[i]);
				JSONArray nodes = alt.getJSONArray("nodes");
				int structNum = i+2;//1 is the default, so add 2 to the alt structs.
				addNodes(c, nodes, bid,0,structNum);
			}
		}catch(JSONException e1){
			if(!e1.toString().equals("org.json.JSONException: JSONObject[\"alts\"] not found.")){
				throw e1;//I don't know what it is so throw it back out there
			}
			//else it only has a single structure 
		}catch(Exception e){
			System.err.println("Error (addSchemas): " + e);
		}
		
		return 1; //it worked
	}

}	

