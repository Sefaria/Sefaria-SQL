import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream.GetField;
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


public class Huffman {

	private String plainText;
	private int count;
	private Huffman leftChild;
	private Huffman rightChild;
	private Huffman parent;
	private boolean isRight;

	private static Map<String,Huffman> totalCounts = new HashMap<String, Huffman>();
	private static Huffman huffmanRoot = null;
	private static PriorityQueue<Huffman> heap;

	public Huffman(String plainText, int count){
		this.plainText = plainText;
		this.count = count;
	}

	public Huffman(){

	}

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

	public static void addAllTexts(Connection c, boolean freshStart){
		System.out.println((new Date()).getTime() + " adding All texts...");
		if(freshStart)
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
		if(freshStart){
			makeTree();
			System.out.println((new Date()).getTime() + "finished adding");
			List<Boolean> compressedTest = encode(testStr); 
			String deflated = Huffman.getDeflatedTree();
			System.out.println((new Date()).getTime() + "finished deflating");
			System.out.println("deflated size:"+ Huffman.utf8Length(deflated));
			huffmanRoot = Huffman.enflateTree(deflated);
			System.out.println((new Date()).getTime() + "finished enflating");
			if(testStr.equals(decode(compressedTest))){
				System.out.println("Good: " + testStr + "\n" + testStr);
			}else
				System.err.println("Problem decoding:\n" + testStr  + "\n" + testStr);
		}
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
			Huffman.addAllTexts(oldDBConnection, true);
			PreparedStatement preparedStatement = newDBConnection.prepareStatement("INSERT INTO Settings (_id,value) VALUES (\"huffman\",?)");
			String deflated  = Huffman.getDeflatedTree();
			preparedStatement.setString(1,deflated);
			preparedStatement.execute();
			//Testing getting deflated:
			Statement getDeflated = newDBConnection.createStatement();
			ResultSet resultSet = getDeflated.executeQuery("SELECT value FROM Settings WHERE _id= 'huffman'");
			if(resultSet.next()){
				String deflatedFromDB = resultSet.getString("value");
				
				if(!deflated.equals(deflatedFromDB)){
					System.err.println("not match:\n" );
				}else{
					System.out.println("match!!");
				}
			}
			
			

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
				if(tid % 10000 == 0){
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
		
	}

	private static void printTree(Huffman node, String tabs){
		if(node == null)
			return;
		System.out.println(tabs + node);
		printTree(node.leftChild, tabs + "\t");
		printTree(node.rightChild, tabs + "\t");
	}

	private static String getPlainText(String text,int i){
		final int nGram = 4;
		/*
		int index = text.indexOf(" ",i);
		String [] words = text.split(" ");
		String str = "";
		if(index <0)z
			str =  text.substring(i);
		else 
			str = text.substring(i, index);
		System.out.println(str);
		if(true)
			return str;
		 */
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
		String defalted = getDeflatedTree();
		System.out.println(defalted);
		huffmanRoot = enflateTree(defalted);
		//printTree(huffmanRoot, "");
		if(!text.equals(decode(encodedText)))
			System.err.println("problem with defalted thing");
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
				//System.out.print("1");
				node = node.rightChild;
			}else{
				//System.out.print("0");
				node = node.leftChild;
			}
			if(node.plainText != null){
				decode += node.plainText;
				node = huffmanRoot;
			}
		}

		return decode;
	}
	private static final Character ZERO = '\u0000';
	private static final Character ONE = '\u0001';


	private static Huffman getPlacementNode(Huffman node, Huffman cameFrom){
		if(node.leftChild == null || node.rightChild == null)
			return node;
		else if(node.leftChild.plainText == null && cameFrom != node.leftChild){				
			return getPlacementNode(node.leftChild, null);
		}else{
			return getPlacementNode(node.parent, node);
		}
	}

	public static Huffman enflateTree(String deflated){
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
		System.out.println("enflation took:" + ((new Date()).getTime() - startTime)/1000.0);
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
