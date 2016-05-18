import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class Link extends SQLite{

	public static final String CREATE_TABLE_LINKS = "CREATE TABLE " + TABLE_LINKS + "(\r\n" + 
			"	_id INTEGER PRIMARY KEY,\r\n" + 
			KconnType + " CHAR(3),\r\n" + 
			Kbida + "  INTEGER,\r\n" +  
			"	level1a INTEGER,\r\n" + 
			"	level2a INTEGER,\r\n" + 
			"	level3a INTEGER,\r\n" + 
			"	level4a INTEGER,\r\n" + 
			"	level5a INTEGER,\r\n" + 
			"	level6a INTEGER,\r\n" + 
			//"	FOREIGN KEY(" + Kbida + ") \r\n" + 
			//"		REFERENCES Books(bid)\r\n" + 
			//"		ON DELETE CASCADE,\r\n" + 
			Kbidb + "  INTEGER, " +  
			"	level1b INTEGER, " + 
			"	level2b INTEGER, " + 
			"	level3b INTEGER, " + 
			"	level4b INTEGER, " + 
			"	level5b INTEGER, " +  
			"	level6b INTEGER" + //"	level6b INTEGER,\r\n" + 
			//"	FOREIGN KEY(" + Kbidb + ") \r\n" + 
			//"		REFERENCES Books(bid)\r\n" + 
			//"		ON DELETE CASCADE\r\n" + 
			")";
	
	public static final String CREATE_LINKS_SMALL = "CREATE TABLE " + LINKS_SMALL +  " (" +
			"_id INTEGER PRIMARY KEY, " +
			"tid1 INTEGER NOT NULL," +
			"tid2 INTEGER NOT NULL, " +
			//KconnType + " CHAR(3), " +
			"CONSTRAINT LinkSmallUni UNIQUE(tid1, tid2)," +
			
			"	FOREIGN KEY (tid1) \r\n" + 
			"		REFERENCES Text (_id)\r\n" + 
			"		ON DELETE CASCADE,\r\n" + 
			"	FOREIGN KEY (tid2) \r\n" + 
			"		REFERENCES Text (_id)\r\n" + 
			"		ON DELETE CASCADE\r\n" + 
			
			");"
	
			+"CREATE INDEX tid2 ON " + LINKS_SMALL + " (tid2)"
			;

	static String getTitleFromComplex(String fullPath){
		
		int i = 0;
		while(!booksInDB.containsKey(fullPath) && i<10){
			int index = fullPath.lastIndexOf(",");
			if(index == -1)
				return null;
			fullPath = fullPath.substring(0,index);
			i++;
		}
		if(i>9)
			System.err.println("i went too HIGH");
		return fullPath;
	}
	
	static int [] getParentID(String title, String fullPath, int bid){
		if(title.equals(fullPath))
			return new int [] {0,booksInDBtextDepth.get(title)};
		String [] nodes = fullPath.split(", ");
		int parentNode = 0;
		int textDepth = 0;
		Node.NodePair nodePair = null;
		for(int i=0;i<nodes.length;i++){
			Node.NodeInfo nodeInfo = new Node.NodeInfo(bid, parentNode, nodes[i]);
			nodePair = allNodesInDB.get(nodeInfo);
			parentNode = nodePair.nid;
		}
		if(nodePair != null)
			textDepth = nodePair.textDepth;
		
		return new int [] {parentNode,textDepth};
	}
	
	static void addLinkFile(Connection c, CSVReader reader){
		
		String next[] = {};
		int linkCount = 0;
		int linkfailed = 0;

		try{
			PrintWriter writer = new PrintWriter("logs/linkErrors_" + SQLite.DB_NAME_PART + ".txt", "UTF-8");
			int groupCount = 0;
			while(true) {
				if((groupCount % 10000) == 0)
					System.out.print("\nLink count:" + groupCount);
				if((groupCount++ % 1000) == 0)
					System.out.print(".");
				next = reader.readNext();
				if(next != null) {
					linkCount++;
				
					try{
						String titleA = getTitleFromComplex(next[0]);
						String titleB = getTitleFromComplex(next[7]);
						if(titleA == null || titleB == null){ //we don't have the book (so don't add the link).
							linkfailed++;
							continue;
						}
						int bida =  booksInDBbid.get(titleA);
						int bidb = booksInDBbid.get(titleB);
						
						int textDeptha, textDepthb,parentIDa,parentIDb;
						int [] nodeValues = getParentID(titleA, next[0],bida);
						parentIDa = nodeValues[0];
						textDeptha = nodeValues[1];

						nodeValues = getParentID(titleB, next[7],bidb);
						parentIDb = nodeValues[0];
						textDepthb = nodeValues[1]; 
						
						next = repositionRow(next, bida, textDeptha, bidb, textDepthb);

						if(catchDafs(next[6]) == 0)
							next[6] = "1";
						if(catchDafs(next[13]) == 0)
							next[13] = "1";
						if(true){//catchDafs(next[6]) != 0 && catchDafs(next[13]) != 0){	//making it include even when ==0 (and convert to == 1)...
							String select1 = "SELECT T1._id FROM Texts T1 WHERE T1.bid = ? AND T1.level1 = ? AND T1.level2 = ? AND T1.level3 = ? AND T1.level4 = ? AND T1.level5 = ? AND T1.level6 = ? AND T1.parentNode = " + parentIDa;
							String select2 = "SELECT T2._id FROM Texts T2 WHERE T2.bid = ? AND T2.level1 = ? AND T2.level2 = ? AND T2.level3 = ? AND T2.level4 = ? AND T2.level5 = ? AND T2.level6 = ? AND T2.parentNode = " + parentIDb;
							String sql = "INSERT INTO Links_small (" +
									" tid1, tid2 " +
									//", connType" +
									" ) VALUES (" +
									"(" + select1 + ")," + 
									"(" + select2 + ")" +
									//",?" + 
									");";
							PreparedStatement stmt = c.prepareStatement(sql);
							stmt = putValues(stmt, next,bida, bidb, false);
							stmt.executeUpdate();
							stmt.close();
							
							/*
							//add Text.hasLink (boolean)
							sql = "UPDATE Texts SET hasLink = 1 WHERE _id in ("
							+ select1 + ") OR  + _id in ( " +  select2 + ");";
							stmt = c.prepareStatement(sql);
							stmt = putValues(stmt, next,bida, bidb, false);//there's no connType that I want to add, so false
							stmt.executeUpdate();
							stmt.close();
							*/
							
						}else{
						PreparedStatement stmtLink = c.prepareStatement("INSERT INTO Links (" +
									Kbida + ", " + 
									Klevel1a + ", " + 
									Klevel2a + ", " + 
									Klevel3a + ", " + 
									Klevel4a + ", " + 
									Klevel5a + ", " + 
									Klevel6a + ", " + 
									Kbidb + ", " + 
									Klevel1b + ", " + 
									Klevel2b + ", " + 
									Klevel3b + ", " + 
									Klevel4b + ", " + 
									Klevel5b + ", " + 
									Klevel6b + ", " +
									KconnType +
									") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");
						stmtLink = putValues(stmtLink, next,bida, bidb, true);
						stmtLink.executeUpdate();
						stmtLink.close();
						}

					} catch (Exception e){
						//e.printStackTrace();
						//System.err.println(e.getMessage());
						writer.println( linkCount + "\t" + e);
						//System.err.println("Error (link-" + linkCount + "): " + e);
						linkfailed++;
					}
				} else {
					break;
				}
				
			}
			writer.close();
			c.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Good Link: " + String.valueOf(linkCount - linkfailed) + "\nFailed Links: " + linkfailed);
		return;
	}
	
	/**
	 *  repositions the row so that it will be consistant (ignoring textdepth) when trying to get the values at each level.
	 * @param row
	 * @param bida
	 * @param textDeptha
	 * @param bidb
	 * @param textDepthb
	 * @return
	 */
	private static String [] repositionRow(String [] row, int bida, int textDeptha, int bidb, int textDepthb){
		int startingNum = 6;
		int endingNum = 1;
		while(row[startingNum - textDeptha + 1].equals("0")){
			for(int i = endingNum + 1; i<=startingNum  ; i++ ){
				row[i - 1] = row[i];
			}
			row[startingNum] = "0";
			//Log.d("sql_link_values", "preforming fix row[x] A-" + booka.title + " " + whileLoopC++);
		}

		startingNum = 13;
		endingNum = 8;
		while(row[startingNum - textDepthb + 1].equals("0")){
			for(int i = endingNum + 1; i<=startingNum  ; i++ ){
				row[i - 1] = row[i];
			}
			row[startingNum] = "0";
			//Log.d("sql_link_values", "preforming fix row[x] B-" + bookb.title+ " " + whileLoopC++ );
		}
		return row;
	}

	private static PreparedStatement putValues(PreparedStatement stmt, String [] row, int bida, int bidb, boolean addConnType) throws NumberFormatException, SQLException{
		//row shuold already be repositioned
			
			stmt.setInt(1, bida);
			stmt.setInt(2, catchDafs(row[6]));
			stmt.setInt(3, catchDafs(row[5]));
			stmt.setInt(4, catchDafs(row[4]));
			stmt.setInt(5, catchDafs(row[3]));
			stmt.setInt(6, catchDafs(row[2]));
			stmt.setInt(7, catchDafs(row[1]));

			stmt.setInt(8, bidb);
			stmt.setInt(9, catchDafs(row[13]));
			stmt.setInt(10, catchDafs(row[12]));
			stmt.setInt(11, catchDafs(row[11]));
			stmt.setInt(12, catchDafs(row[10]));
			stmt.setInt(13, catchDafs(row[9]));
			stmt.setInt(14, catchDafs(row[8]));

			try {
				if(addConnType)
					stmt.setString(15, row[14]);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		return stmt;
	}
	
	
	public static int catchDafs(String input){
		
		String test = new String ();
		test = input.replaceAll("-.+", ""); //replace 56-87 (it will only link to the first thing (for now). //TODO
		//if(!test.equals(input))	System.out.println(input + " only linking to " + test);
		input = test;
		
		if(input.matches("(([0-9]{1,3})(a|b))")){ //it's like a daf
			//System.out.println(input);
			input = input.replace("b", ".5");
			input = input.replace("a", "");
			input = String.valueOf(Math.round(Float.valueOf(input)*2) - 1);
			//System.out.println("CONVERTED TO: " + input);
		}
		return Integer.valueOf(input);	
		
	}

	
}
