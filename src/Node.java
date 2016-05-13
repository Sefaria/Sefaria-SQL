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
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import javax.swing.text.html.parser.Entity;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class Node extends SQLite{
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
			"	startLevels TEXT,\r\n" +  //
			//maybe some stuff like to display chap name and or number (ei. maybe add some displaying info)

	//		"	FOREIGN KEY (bid) \r\n" + 
	//		"		REFERENCES Books (_id)\r\n" + 
	//		"		ON DELETE CASCADE,\r\n" + 
	"	FOREIGN KEY (parentNode) \r\n" + 
	"		REFERENCES Nodes (_id)\r\n" + 
	"		ON DELETE CASCADE,\r\n" + 
	"	CONSTRAINT uniqSiblingNum UNIQUE (bid,parentNode,siblingNum,structNum)\r\n" + //needs bid b/c otherwise parent is 0 for each root 

				")";





	protected static int addText(Connection c, JSONObject enJSON, JSONObject heJSON) throws JSONException{
		if(enJSON == null && heJSON == null){
			System.err.print("Both JSONs are null in Node.addText()");
			return -1;
		}
		int enLang =0,heLang=0;
		String title ="";
		JSONObject node = null;
		if(enJSON != null){
			enLang = returnLangNums(enJSON.getString("language"));
			title = enJSON.getString("title");
			node = (JSONObject) enJSON.get("schema");
		}		
		if(heJSON != null){
			heLang = returnLangNums(heJSON.getString("language"));
			title = heJSON.getString("title");
			node = (JSONObject) heJSON.get("schema");
		}
		int lang = enLang + heLang;
		/**
		 * check for errors
		 */
		if((enLang != SQLite.LANG_EN  && enLang != 0)|| (heLang != SQLite.LANG_HE  && heLang != 0)){
			System.err.println("Error in Node.addText(): not right lang numbers. enLang:" + enLang  + " heLang:" + heLang);
			return -1;
		}
		if(title.equals("")){
			System.err.println("no Title");
			return -1;
		}
		if(enJSON != null && heJSON != null){
			if(!heJSON.get("schema").toString().equals(enJSON.get("schema").toString())){
				System.err.println("en and he JSONs schemas don't match\n");	
				System.out.println(heJSON.get("schema"));
				System.out.println(enJSON.get("schema"));
				return -1;
			}
			if(!enJSON.getString("title").equals(heJSON.getString("title"))){
				System.err.println("en and he JSONs title don't match" + enJSON.getString("title") + " - " + heJSON.getString("title"));
				return -1;
			}

		}
		if(!booksInDB.containsKey(title)){
			System.err.println("Don't have book in DB and trying to add text");
			return -1;
		}
		int bid = booksInDBbid.get(title); 
		JSONObject enText = null, heText = null;
		if(enJSON != null){
			enText = (JSONObject) enJSON.get("text");
		}
		if(heJSON != null){
			heText = (JSONObject) heJSON.get("text");
		}
		insertNode(c, node,enText,heText, 0,0,bid,0,lang);
		return 1; //it worked
	}

	private static final int IS_COMPLEX = 2;
	private static final int IS_TEXT_SECTION = 4;
	private static final int IS_GRID_ITEM = 8;
	private static final int IS_REF = 16;
	/***
	 * returns an int which represents the nodeType using boolean flags
	 * @param isComplex
	 * @param isTextSection
	 * @param isGridItem
	 * @param isRef
	 * @return nodeType
	 */
	private static int getNodeType(boolean isComplex,boolean isTextSection,boolean isGridItem, boolean isRef ){
		int nodeType = 1; //start off with something that isn't 0, so that 0 is reserved for when it's got no info
		if(isComplex)
			nodeType += IS_COMPLEX;
		if(isTextSection)
			nodeType += IS_TEXT_SECTION;
		if(isGridItem)
			nodeType += IS_GRID_ITEM;
		if(isRef)
			nodeType += IS_REF;

		return nodeType;
	}

	private static void insertSingleNodeToDB(Connection c, Integer nid, Integer bid, Integer parentNode,Integer nodeType, Integer siblingNum,
			String enTitle, String heTitle, Integer structNum, Integer textDepth, Integer startTid, Integer endTid,
			String extraTids, String sectionNames, String heSectionNames, String startLevels
			){
		final String INSERT_NODE = "INSERT INTO Nodes (" +
				"_id,bid,parentNode,nodeType,siblingNum,enTitle,heTitle,structNum,textDepth,startTid,endTid,extraTids,sectionNames,heSectionNames,startLevels)"
				+ "VALUES (?,?, ?, ?, ?, ?, ?, ?,?, ?, ?,?,?,?,?);";

		PreparedStatement stmt;
		try {
			stmt = c.prepareStatement(INSERT_NODE);
			if(nid != null) stmt.setInt(1, nid);
			if(bid != null) stmt.setInt(2,bid);
			if(parentNode != null) stmt.setInt(3,parentNode);
			if(nodeType != null) stmt.setInt(4,nodeType);
			if(siblingNum != null) stmt.setInt(5,siblingNum);
			if(enTitle != null) stmt.setString(6,enTitle);
			if(heTitle != null) stmt.setString(7,heTitle);
			if(structNum != null) stmt.setInt(8,structNum); 
			if(startTid != null) stmt.setInt(10,startTid);
			if(endTid != null) stmt.setInt(11,endTid);
			if(extraTids != null) stmt.setString(12,extraTids);
			if(sectionNames != null) stmt.setString(13,sectionNames);
			if(heSectionNames != null) stmt.setString(14,heSectionNames);
			if(startLevels != null) stmt.setString(15,startLevels);
			stmt.executeUpdate();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	private static int insertNode(Connection c, JSONObject node,JSONObject enText,JSONObject heText,int depth, int siblingNum,int bid,int parentNode,int lang){
		if(enText == null && heText == null){
			System.err.println("Both JSONs are null in insertNode");
			return -1;
		}
		String heTitle = node.getString("heTitle");
		String enTitle = node.getString("enTitle");
		int nodeID = ++nodeCount;
		int nodeType;
		JSONArray nodes = null;
		try{
			nodes  =  node.getJSONArray("nodes");
			if(depth >0){
				if(enText != null)
					enText = enText.getJSONObject(enTitle); //this is the test to determine the Node type
				if(heText != null)
					heText = heText.getJSONObject(enTitle); //this is the test to determine the Node type
			}
			nodeType = getNodeType(true, false, false, false);
		}catch(Exception e){
			//System.out.println("Error in Nodes.insertNode(): " + e.getMessage() + "..." + enTitle + "-" + heTitle);
			//This whole block is to test if it's a IS_TEXT_SECTION

			boolean hasEnglish = false, hasHebrew = false;
			if(enText != null){
				JSONArray textArray = enText.getJSONArray(enTitle);
				try{
					textArray.getString(0);
					hasEnglish = true;
				}catch(Exception e1){}
			}
			
			if(heText != null){
				JSONArray textArray = (JSONArray) heText.getJSONArray(enTitle);
				try{
					textArray.getString(0);
					hasHebrew = true;
				}catch(Exception e1){}
			}
			nodeType = getNodeType(true, (hasEnglish || hasHebrew), false, false);
		}
		insertSingleNodeToDB(c, nodeID, bid, parentNode, nodeType, siblingNum, enTitle, heTitle, 1,
				null, null, null, null, null, null,null);
		if((nodeType & IS_TEXT_SECTION) == 0 && nodes != null){ //it's a branch and not a TEXT_SECTION
			for(int i =0;i<nodes.length();i++){
				insertNode(c, (JSONObject) nodes.get(i),enText,heText,depth+1,i,bid,nodeID,lang);
			}
		}else{
			
			int enTextDepth = 0, heTextDepth = 0;
			if(enText != null){
				JSONArray textArray = (JSONArray) enText.get(enTitle);
				ArrayList<Integer> levels = new ArrayList<Integer>();
				enTextDepth = insertTextArray(c, textArray, levels,bid,LANG_EN,nodeID);
			}
			if(heText != null){
				JSONArray textArray = (JSONArray) heText.get(enTitle);
				ArrayList<Integer> levels = new ArrayList<Integer>();
				heTextDepth = insertTextArray(c, textArray, levels,bid,LANG_HE,nodeID);
			}
			int textDepth = Math.max(enTextDepth, heTextDepth);
			String updateStatement = "UPDATE Nodes set textDepth = ? WHERE _id = ?";
			try {
				PreparedStatement stmt = c.prepareStatement(updateStatement);
				stmt.setInt(1, textDepth);
				stmt.setInt(2, nodeID);
				stmt.executeUpdate();
				stmt.close();
			} catch (SQLException e) {
				System.err.println("Error 1 in InsertNode: " + e);

			}

		}

		return 0;
	}


	/**
	 * //TODO have to deal with only hebrew getting into db.  
	 * @param connection
	 * @param textArray
	 * @param levels
	 * @param bid
	 * @param lang
	 * @param parentNodeID
	 * @return textDepth
	 */
	private static int insertTextArray(Connection c, JSONArray textArray,ArrayList<Integer> levels,int bid,int lang,int parentNodeID){
		try{
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

	/**
	 * add Nodes for alt structures.
	 * @param c
	 * @param nodes
	 * @param bid
	 * @param parentNode
	 * @param structNum
	 * @return
	 * @throws JSONException
	 */
	private static int addSchemaNodes(Connection c, JSONArray nodes,int bid,int parentNode, int structNum) throws JSONException{
		for(int j =0;j<nodes.length();j++){
			JSONObject node = nodes.getJSONObject(j);
			String enTitle = node.getString("title");
			String heTitle = node.getString("heTitle");
			String nodeTypeString =  node.getString("nodeType");
			int nodeType;
			if(!nodeTypeString.equals("ArrayMapNode")){
				System.err.println("Error not a ref");
				return -1;	
			}
			
			
			String sectionNames = node.getJSONArray("sectionNames").toString().replace("\"\"", "\"Section\"");
			String heSectionNames = sectionNames;//node.getJSONArray("heSectionNames").toString().replace("\"\"", "\"Section\"");
			int depth = node.getInt("depth");
			JSONArray refs;
			if (depth == 1){
				int nodeID = ++nodeCount;
				nodeType = getNodeType(true, false, false, false);//NODE_REFS;
				refs = node.getJSONArray("refs");
				insertSingleNodeToDB(c, nodeID, bid, parentNode, nodeType, j, enTitle, heTitle, structNum,
						null, null, null, null, sectionNames, heSectionNames,null);

				//System.out.println(enTitle + " " +  heTitle + " " + nodeType + " " + sectionNames + refs);
				int subParentNode = nodeID;
				for(int i=0;i<refs.length();i++){
					String ref = refs.getString(i);
					if(ref.equals("")) continue;
					nodeID = ++nodeCount;
					nodeType = getNodeType(true, true, true, true);
					RefValues refValues = ref2Tids(c,ref, bid);
					int startTID = refValues.startTid;
					int endTID = refValues.endTid;
					insertSingleNodeToDB(c, nodeID, bid, subParentNode, nodeType,
							i, "", "", structNum, null, startTID, endTID, ref,
							sectionNames, heSectionNames,refValues.startLevels);
				}
			}else if(depth == 0){ //so the refs aren't in a grid
				int nodeID = ++nodeCount;
				nodeType = getNodeType(true, true, false, true);
				//TODO get real heSection names
				String ref = node.getString("wholeRef");
				if(ref.equals("")) continue;
				RefValues refValues = ref2Tids(c,ref, bid);
				int startTID = refValues.startTid;
				int endTID = refValues.endTid;
				insertSingleNodeToDB(c, nodeID, bid, parentNode, nodeType,
						j, enTitle, heTitle, structNum, null, startTID, endTID, ref,
						sectionNames, heSectionNames,refValues.startLevels);
			}else{
				System.err.println("Node.addSchemaNodes(): I don't know how to deal with this depth:" + depth);
			}

		}
		return 1;//worked
	}

	private static int [] halfRef2Levels(String ref){
		String [] levelsStr  = ref.replace(" ", "").split(":");
		//println("levelsStr:" +levelsStr[0]+","+levelsStr[1] + "... " + ref);
		int [] levels = new int [levelsStr.length];
		for(int i=0;i<levelsStr.length;i++){
			levels[i] = Link.catchDafs(levelsStr[levelsStr.length-i-1]);
		}

		return levels;
	}

	private static class RefValues{
		
		public RefValues(int startTid,int endTid,int [] start){
			this.startTid = startTid;
			this.endTid = endTid;
			this.startLevels = Arrays.toString(start);
		}
		public int startTid;
		public int endTid;
		public String startLevels;
	}
	
	private static RefValues ref2Tids(Connection c, String ref, int bid){
		String title = booksInDBbid2Title.get(bid);
		int textDepth = booksInDBtextDepth.get(title);
		//println("ref: " + ref + " title: " + title);
		int startTid = 0,endTid =0;
		int [] start = null;
		try {
			String [] startStop = ref.replace(title, "").split("-");
			start = halfRef2Levels(startStop[0]);
			int [] stop = start;
			if(startStop.length == 2)
				stop = halfRef2Levels(startStop[1]);
			else if(startStop.length == 1)
				stop = start;
			else{
				System.err.println("Error in Node.Schema.ref2Tids: wrong number of startStop. ref:" + ref);
			}
			if(textDepth != start.length || textDepth != stop.length){
				if(stop.length < start.length){
					int [] tempStop = new int [start.length];
					for(int i=0;i<stop.length;i++){
						tempStop[i] = stop[i];
					}
					for(int i=stop.length;i<start.length;i++){
						tempStop[i] = start[i];
					}
					stop = tempStop;
					//println("ref:" + ref + "__" + start[0] + "," + start[1]+ "___"+ stop[0] + "," + stop[1]);
				}			
				start = add1sForMissingLevels(start, textDepth);
				stop = add1sForMissingLevels(stop, textDepth);
			}
			startTid = Text.getTid(c, bid, start, 0, textDepth,true,null);
			endTid = Text.getTid(c, bid, stop, 0, textDepth,true,null);
		} catch (Exception e) {
			System.err.println("Error in Node.Schema.ref2Tids: problem getting Tids. ref:" + ref);
		}


		return new RefValues(startTid, endTid, start);
	}
		
	static private int [] add1sForMissingLevels(int [] levels,int textDepth){
		if(levels.length < textDepth){
			int [] tempLevels = new int [textDepth];
			int diff = textDepth - levels.length;
			for(int i=0;i<diff;i++){
				tempLevels[i] = 1; //I just have to assume that it's referring to the 1st object
			}
			for(int i= diff;i<tempLevels.length;i++){
				tempLevels[i] = levels[i-diff];
			}
			return tempLevels;
		}else{
			return levels;
		}
		
	}

	protected static int addSchemas(Connection c, JSONObject schemas) throws JSONException{
		try{
			JSONObject alts = schemas.getJSONObject("alts");
			String bookTitle = schemas.getString("title");
			String default_struct = "__UNUSED__"; //didn't leave it blank in case 2 sturcts both have no name and I push them together.
			try{
				default_struct = schemas.getString("default_struct");
			}catch(Exception e){
				;
			}
			int bid = booksInDBbid.get(bookTitle);
			String [] altNames = JSONObject.getNames(alts);
			for(int i=0;i<altNames.length;i++){
				//System.out.println(altNames[i]);
				String altName = altNames[i];
				JSONObject alt = alts.getJSONObject(altName);
				Integer nodeID = ++nodeCount;
				int structNum;
				if(altName.equals(default_struct))
					structNum = 0; //this is even b/f the rigid one which is at number 1
				else
					structNum = i+2;//1 is the rigid one, so add 2 to the alt structs.
				String enTitle =  altName;
				String heTitle = enTitle; //TODO make real heTitle
				int nodeType = getNodeType(true, false, false, false);
				String sectionNames = "[\"" + altName + "\"]";
				String heSectionNames = sectionNames;
				insertSingleNodeToDB(c, nodeID, bid, 0, nodeType, 0,enTitle , heTitle, structNum,
						null, null, null, null, sectionNames, heSectionNames,null);
				JSONArray nodes;
				try{
					nodes = alt.getJSONArray("nodes");
				}catch(JSONException e){
					JSONObject [] altArray = new JSONObject [] {alt};
					nodes = new JSONArray(altArray);
				}
				
				addSchemaNodes(c, nodes, bid,nodeID,structNum);
			}
		}catch(JSONException e1){
			if(!e1.toString().equals("org.json.JSONException: JSONObject[\"alts\"] not found.")){
				throw e1;//I don't know what it is so throw it back out there
			}
			//else it only has a single structure 
		}catch(Exception e){
			System.err.println("Error (addSchemas): " + e.toString());
			e.printStackTrace();
		}

		return 1; //it worked
	}

}	

