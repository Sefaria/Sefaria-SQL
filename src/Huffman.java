import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.ObjectInputStream.GetField;
import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Delayed;

import org.sqlite.SQLiteConfig.Pragma;


public class Huffman extends SQLite{

	private static final Character ZERO = '\u0000';
	private static final Character ONE = '\u0001';
	
	private String plainText;
	private int count;
	private Huffman leftChild;
	private Huffman rightChild;
	private Huffman parent;
	private boolean isRight;
	
	
	private static int treeSize = 0;
	private static Map<String,Huffman> totalCounts = new HashMap<String, Huffman>();
	private static Huffman huffmanRoot = null;
	private static PriorityQueue<Huffman> heap;
	
	

	public Huffman(String plainText, int count){
		this.plainText = plainText;
		this.count = count;
	}

	public Huffman(){}

	public Huffman(Huffman h1, Huffman h2){
		h1.parent = this;
		h2.parent = this;
		h1.isRight = false;
		h2.isRight = true;
		leftChild = h1;
		rightChild = h2;
		count = h1.count + h2.count;
	}

	public static void addTextCount(String text){
		if(text == null) return;
		for(int i=0;i<text.length();i++){
			String plainText = getPlainText(text, i);
			i += plainText.length()-1;

			Huffman huffman = totalCounts.get(plainText);
			if(huffman == null){
				huffman = new Huffman(plainText, 1);
				totalCounts.put(plainText, huffman);
			}else{
				huffman.count++;
			}
		}
	}

	public static void addAllTexts(Connection c){
		System.out.println((new Date()).getTime() + " adding All texts...");
		totalCounts = new HashMap<String, Huffman>();
		String sql = "Select _id, enText,heText from Texts";
		String testStr = "";
		Statement stmt = null;
		try {
			stmt = c.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				int tid = rs.getInt("_id");
				if(tid % 200000 == 0)
					System.out.println("tid:" + tid);
				String enText = rs.getString("enText");
				String heText = rs.getString("heText");
				if(enText != null)
					testStr = enText;
				addTextCount(enText);
				addTextCount(heText);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		makeTree();
		System.out.println((new Date()).getTime() + "finished adding");
		List<Boolean> compressedTest = encode(testStr); 
		String deflated = Huffman.getDeflatedTree();
		System.out.println((new Date()).getTime() + "finished deflating");
		System.out.println("deflated size:"+ Huffman.utf8Length(deflated));
		huffmanRoot = Huffman.inflateTree(deflated);
		System.out.println((new Date()).getTime() + "finished inflating");
		if(testStr.equals(decode(compressedTest))){
			System.out.println("Good: decoding");
		}else
			System.err.println("Problem decoding:\n" + testStr  + "\n" + testStr);
		return;
	}

	private static byte [] bools2Bytes(List<Boolean> bools){
		byte [] bytes = new byte [(int) Math.ceil(bools.size()/8.0)];
		int byteNum = -1;
		for(int i=0;i<bools.size();i++){
			int mod = i % 8;
			if(mod == 0){
				byteNum++;
			}
			boolean bit = bools.get(i);
			if(bit)
				bytes[byteNum] += 1<<(7-mod); 
		}
		return bytes;
	}

	
	public static void compressAndMoveAllTexts(Connection oldDBConnection, Connection newDBConnection){

		try {
			Huffman.addAllTexts(oldDBConnection);
			String path = "testDBs/SefariaHuffmanDeflated.txt";
			String deflated  = Huffman.getDeflatedTree();
			Writer out = new BufferedWriter(new OutputStreamWriter(
				    new FileOutputStream(path), "UTF-8"));
			try {
			    out.write(deflated);
			} finally {
			    out.close();
			}
			treeSize = 0;
			getTreeSize(huffmanRoot);
			SQLite.setSettings("huffmanSize", ""+treeSize, newDBConnection);
			
		
			//adding texts
			newDBConnection.setAutoCommit(false);
			Statement stmt = null;
			PreparedStatement statement2 = null;
			String sql = "Select _id,enText,heText,displayNumber from Texts";

			stmt = oldDBConnection.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			
			
			while (rs.next()) {
				statement2 = newDBConnection.prepareStatement("UPDATE Texts SET enTextCompress = ?, heTextCompress= ?,flags=? where _id = ?");
				int tid = rs.getInt("_id");
				if(tid % 200000 == 0){
					System.out.println("tid:" + tid);
					newDBConnection.commit();
				}
				String enText = rs.getString("enText");
				String heText = rs.getString("heText");
				int displayNumber = rs.getInt("displayNumber");
				int enBitLength = 0;
				int heBitLength = 0;
				if(enText != null){
					List<Boolean> compressed = encode(enText);
					byte [] bytes = bools2Bytes(compressed);
					statement2.setBinaryStream(1,new ByteArrayInputStream(bytes),bytes.length);
					enBitLength = compressed.size() % 8;
				}
				if(heText != null){
					List<Boolean> compressed = encode(heText);
					byte [] bytes = bools2Bytes(compressed);
					statement2.setBinaryStream(2,new ByteArrayInputStream(bytes),bytes.length);
					heBitLength = compressed.size() % 8;
				}
				int flags = displayNumber + (enBitLength*2) + (heBitLength*16);
				statement2.setInt(3,flags);
				statement2.setInt(4,tid);

				statement2.executeUpdate();
			}
			stmt.close();
			statement2.close();
			newDBConnection.commit();
		}catch(Exception e){
			e.printStackTrace();
		}

	}

	private static void makeTree(){
		Collection<Huffman> allPlains = totalCounts.values();
		heap = new PriorityQueue<Huffman>(allPlains.size(), new Comparator<Huffman>(){
			public int compare(Huffman x, Huffman y)
			{
				if(x == null || y == null)
					return 0;
				return x.count - y.count;
			}
		});
		heap.addAll(allPlains);
		while(true) {
			Huffman huffman1 = heap.poll();
			Huffman huffman2 = heap.poll();
			if(huffman2 == null) {
				huffmanRoot = huffman1;
				break;
			}else if(huffman1 == null){
				System.err.println("huffman1 is null!!");
				break;
			}
			Huffman newNode = new Huffman(huffman1,huffman2);
			heap.add(newNode);
		}
		
		printTree(huffmanRoot, "");
		writer.flush();
		writer.close();
		System.out.println("Tree Printed.");
	}


	private static void getTreeSize(Huffman node){
		if(node == null)
			return;
		treeSize++;
		getTreeSize(node.leftChild);
		getTreeSize(node.rightChild);
	}
	
	
	private static PrintWriter writer;
	private static void printTree(Huffman node, String tabs){
		if(writer == null){
			 try {
				writer = new PrintWriter("logs/huffmanTree_" + SQLite.DB_NAME_PART + ".txt", "UTF-8");
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(node == null){
			return;
		}
		if(node.plainText != null)
			writer.println(tabs + ":_" + node.plainText + "_");
		printTree(node.leftChild, tabs + "0");
		printTree(node.rightChild, tabs + "1");
	}

	private static int plainTextCount = 0;
	private static String getPlainText(String text,int i){
		char let = text.charAt(i);
		boolean useSpaces = let > 'A' && let < 'z';
		useSpaces = false;
		if(useSpaces){
			//compress by words... this is smaller, but it actually takes more time to create tree in app
			int index = text.indexOf(" ",i);
			String str = "";
			if(index <0)
				str = text.substring(i);
			else 
				str = text.substring(i, index+1);
			//if(plainTextCount++ < 40) System.out.println("_" + str +"_");
			
			return str;
		}else{
			final int nGram = 2; 
			if(i<text.length()-nGram){
				String str = "";
				for(int j=0;j<nGram;j++){
					str += text.charAt(i+j);
				}
				return str;
			}
			else
				return text.charAt(i) + "";
		}
	}

	static void copyTable(Connection c, String tableName, String create, String newDB) throws SQLException{
		copyTable(c, tableName, create, newDB, null);
	}
	
	private static void copyTable(Connection c, String tableName, String create, String newDB, String columns) throws SQLException{
		Statement stmt = c.createStatement();
		stmt.executeUpdate("DROP TABLE IF EXISTS \"" + newDB + "." + tableName + "\";");
		stmt.executeUpdate(create);
		stmt.close();
		if(columns == null){
			c.prepareStatement("INSERT INTO " + tableName + " SELECT * FROM oldDB." + tableName).execute();
		}else{
			c.prepareStatement("INSERT INTO " + tableName + " (" + columns + ") SELECT " + columns + " FROM oldDB." + tableName).execute();
		}
	}

	private static void copyTextTable(Connection newDBConnection, String oldDB) throws SQLException{
		Statement stmt = newDBConnection.createStatement();
		stmt.executeUpdate(Text.CREATE_COMPRESS_TEXTS_TABLE);
		stmt.close();

		String columns = "_id,bid,level1,level2,level3,level4," +
				//"level5,level6," +
				"hasLink,parentNode";
		newDBConnection.prepareStatement("INSERT INTO Texts (" + columns + ") SELECT " + columns + " FROM oldDB.Texts").execute();
		//if(true) return;//do this if you don't want to copy the texts themselves 
		Connection oldDBConnection = SQLite.getDBConnection(oldDB);
		Huffman.compressAndMoveAllTexts(oldDBConnection, newDBConnection);
	}

	public static void copyNewDB(String oldDB, String newDB, Searching.SEARCH_METHOD searchMethod){
		System.out.println("Copying DB");
		try {
			File file = new File(newDB);
			file.delete();
			Connection c = getDBConnection(newDB);
			c.prepareStatement("ATTACH DATABASE \"" + oldDB + "\" AS oldDB").execute();
			copyTable(c, "Books", Book.CREATE_BOOKS_TABLE, newDB);
			copyTable(c, "Links_small", Link.CREATE_LINKS_SMALL, newDB, "tid1,tid2,connType");
			copyTable(c, "Nodes", Node.CREATE_NODE_TABLE, newDB);
			
			/*
			 * copyTable(c, "Texts", Text.CREATE_TEXTS_TABLE, newDB);
			 * copyTable(c, "Headers", Header.CREATE_HEADERS_TABLE, newDB);
			 * copyTable(c, "Links", Link.CREATE_TABLE_LINKS, newDB);
			 */
			copyTable(c, "android_metadata", CREATE_TABLE_METADATA, newDB);
			copyTable(c, "Settings", CREATE_TABLE_SETTINGS, newDB);
			//copyTable(c, "Searching", Searching.CREATE_SEARCH, newDB);
			Searching.makeSearching(searchMethod, c, oldDB, newDB);
			
			setSettings("version", DB_VERION_NUM +"", c);
			
			copyTextTable(c, oldDB);
			c.close();


		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("Finished Copying DB");
	}
	
	
	
	public static void copyNewHeTextOnlyDB(String oldDB, String newDB, Searching.SEARCH_METHOD searchMethod){
		System.out.println("Copying heText Only DB");
		try {
			File file = new File(newDB);
			
			file.delete();
			Connection c = getDBConnection(newDB);
			c.prepareStatement("ATTACH DATABASE \"" + oldDB + "\" AS oldDB").execute();
			
			
			
			Statement stmt = c.createStatement();
			stmt.executeUpdate("CREATE TABLE heTexts( heText TEXT\r)");
			stmt.close();
		
			String columns = "heText";
			c.prepareStatement("INSERT INTO heTexts (" + columns + ") SELECT " + "heTextCompress" + " FROM oldDB.Texts").execute();
						
			Searching.makeSearching(searchMethod, c, oldDB, newDB);
			setSettings("version", DB_VERION_NUM +"", c);
			
			c.close();


		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("Finished Copying DB");
	}
	
	public static void copyNewAPIDB(String oldDB, String newDB){
		System.out.println("Making API DB");
		try {
			File file = new File(newDB);
			file.delete();
			Connection c = getDBConnection(newDB);
			c.prepareStatement("ATTACH DATABASE \"" + oldDB + "\" AS oldDB").execute();
			copyTable(c, "Settings", CREATE_TABLE_SETTINGS, newDB);
			copyTable(c, "Books", Book.CREATE_BOOKS_TABLE, newDB);
			copyTable(c, "Nodes", Node.CREATE_NODE_TABLE, newDB);
			copyTable(c, "android_metadata", CREATE_TABLE_METADATA, newDB);
			
			setSettings("api", ""+1, c);
			setSettings("version", DB_VERION_NUM +"", c);
			c.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("Finished making API DB");
	}

	
	@Override
	public String toString() {
		if(plainText == null)
			return "" + count;
		return plainText + ": " + count;
	}

	private static int totalTextLength = 0;
	private static int totalCompressLength = 0;
	public static List<Boolean> encode(String text){
		List<Boolean> bits = new ArrayList<Boolean>();
		if(text == null) return bits;
		for(int i=0;i<text.length();i++){
			String plainText = getPlainText(text,i);
			//System.out.println("plaintext:" + plainText);
			i += plainText.length() -1;

			Huffman node = totalCounts.get(plainText);
			if(node == null){
				System.err.println("couldn't find huffman!");
				continue;
			}
			List<Boolean> tempBits = new ArrayList<Boolean>();
			while(true){
				boolean bit = node.isRight;
				node = node.parent;
				if(node == null)
					break;
				tempBits.add(0,bit);
			}
			for(Boolean bit:tempBits){
				//System.out.print((bit ? "1" : "0"));
				bits.add(bit);
			}
			//System.out.println();
		}
		totalTextLength += utf8Length(text);
		totalCompressLength += Math.ceil(bits.size()/8.0);
		return bits;
	}

	public static void compressEverything(Connection c){
		System.out.println("start Compress:");
		Huffman.makeTree();
		String sql = "Select _id, enText,heText from Texts";
		Statement stmt = null;
		try {
			stmt = c.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			final boolean checkDecode = false;
			while (rs.next()) {
				int tid = rs.getInt("_id");
				String enText = rs.getString("enText");
				String heText = rs.getString("heText");	

				if(enText != null){
					List<Boolean> encodedEnText = encode(enText);
					if(checkDecode){
						String decodedEntext = decode(encodedEnText); 
						if(!enText.equals(decodedEntext))
							System.err.println("couldn't decode:\n" + decodedEntext + "\n" + enText );
					}
				}
				if(heText != null){
					List<Boolean> encodedText = encode(heText);
					if(checkDecode){
						String decodedtext = decode(encodedText); 
						if(!heText.equals(decodedtext))
							System.err.println("couldn't decode:\n" + decodedtext + "\n" + heText );
					}
				}


			}
			stmt.close();

			System.out.println("lengths: " + totalTextLength + "->" + totalCompressLength);
			System.out.println("compression Ratio: " + (totalCompressLength)/(totalTextLength * 1.0) );
			Collection<Huffman> allPlains = totalCounts.values();
			System.out.println("Number of Keys:" + totalCounts.size());
			int sizeOfKeyStrings = 0;
			for(Huffman huffman: allPlains){
				sizeOfKeyStrings += utf8Length(huffman.plainText);
			}
			int treeSpace = (sizeOfKeyStrings + totalCounts.size());
			System.out.println("Tree space:" + treeSpace);
			int totalSize = totalCompressLength + treeSpace;
			System.out.println("Total Size: " + totalSize + " | " + + (totalSize)/(totalTextLength * 1.0)); 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void test(){
		totalCounts = new HashMap<String, Huffman>();
		String text ="the";//"the one thisa \the one thisa adsufh alksdjfh\na akdsfjhak dflakdfh aiudslfhaddfa pse87riau gsf87adsfj alhdsf kjb akuldshfhaldskjf gg adshfkjadsfh kaldsf adshfkajesdfh aksdjfadskfh akjjdsfh akdsjfhliuefjakldsfm adsfkla dsfjk";//"this is the place that you do things"
		addTextCount(text);
		makeTree();
		List<Boolean> encodedText = encode(text);
		System.out.println(decode(encodedText));
		String deflated = getDeflatedTree();
		System.out.println(deflated);
		huffmanRoot = inflateTree(deflated);
		
		//printTree(huffmanRoot, "");
		
		if(!text.equals(decode(encodedText)))
			System.err.println("problem with deflated thing");
		else{
			System.out.println("\nGood Work!!\n" + decode(encodedText));
		}

		System.exit(1);
	}
	private static String decode(List<Boolean> encoded){
		String decode = "";
		Huffman node = huffmanRoot;
		for(Boolean bit: encoded){
			if(bit){
				node = node.rightChild;
			}else{
				node = node.leftChild;
			}
			if(node.plainText != null){
				decode += node.plainText;
				node = huffmanRoot;
			}
		}

		return decode;
	}



	private static Huffman getPlacementNode(Huffman node, Huffman cameFrom){
		if(node.leftChild == null || node.rightChild == null)
			return node;
		else if(node.leftChild.plainText == null && cameFrom != node.leftChild){				
			return getPlacementNode(node.leftChild, null);
		}else{
			return getPlacementNode(node.parent, node);
		}
	}

	public static Huffman inflateTree(String deflated){
		Date date = new Date();
		long startTime = date.getTime();
		Huffman root = new Huffman();
		Huffman node = root;
		for(int i=1;i<deflated.length();i++){
			Character character = deflated.charAt(i);
			if(character ==  ONE){
				String tempText = "";
				while(i<deflated.length()-1){
					character = deflated.charAt(++i);
					if(character != ONE && character != ZERO)
						tempText += character;
					else{
						i--;
						break;
					}
				}
				Huffman tempNode = new Huffman(tempText,0);
				node = getPlacementNode(node, null);
				if(node.leftChild == null){
					node.leftChild = tempNode;
				}else{
					node.rightChild = tempNode;
				}
				tempNode.parent = node;
			}else{// if(character == ZERO){
				Huffman tempNode = new Huffman();
				node = getPlacementNode(node,null);
				if(node.leftChild == null){
					node.leftChild = tempNode;
				}else{
					node.rightChild = tempNode;
				}
				tempNode.parent = node;
				node = tempNode;				
			}
		}
		System.out.println("inflation took:" + ((new Date()).getTime() - startTime)/1000.0);
		return root;
	}

	public static String getDeflatedTree(){
		long startTime = (new Date()).getTime();
		StringBuilder stringBuilder = new StringBuilder();
		String deflated =  deflateTree(huffmanRoot, stringBuilder).toString();
		System.out.println("deflation took:" + ((new Date()).getTime() - startTime)/1000.0);
		return deflated;
	}

	private static StringBuilder deflateTree(Huffman node, StringBuilder previousString){
		if(node.plainText != null){
			previousString = previousString.append(ONE + node.plainText); //ONE + node.plainText;//
		}else
			previousString = previousString.append(ZERO); //ZERO;//

		if(node.leftChild != null)
			previousString = deflateTree(node.leftChild, previousString);
		if(node.rightChild != null)
			previousString = deflateTree(node.rightChild, previousString);

		return previousString;
	}

	public static int utf8Length(String sequence) {
		int count = 0;
		for (int i = 0, len = sequence.length(); i < len; i++) {
			char ch = sequence.charAt(i);
			if (ch <= 0x7F) {
				count++;
			} else if (ch <= 0x7FF) {
				count += 2;
			} else if (Character.isHighSurrogate(ch)) {
				count += 4;
				++i;
			} else {
				count += 3;
			}
		}
		return count;
	}


}
