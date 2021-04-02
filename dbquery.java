import java.io.*;
import java.nio.ByteBuffer;


public class dbquery {

	public static void main(String[] args) throws NumberFormatException, FileNotFoundException {
		// TODO Auto-generated method stub
		dbquery Dbquery = new dbquery();
		float start = System.currentTimeMillis();
		Dbquery.readCommand(args);
		float end = System.currentTimeMillis();
		System.out.println("Query time is" + (end - start) + "ms.");
	}

	private void readCommand(String[] args) throws NumberFormatException, FileNotFoundException {
		// TODO Auto-generated method stub
		readfile(args[0],Integer.valueOf(args[1]));
	}

	private void readfile(String string, int pagesize) throws FileNotFoundException {
		// TODO Auto-generated method stub
		boolean isPage = true;
		File heapFile = new File("heap." + pagesize);
		FileInputStream inputStream = new FileInputStream(heapFile);
		while(isPage) {
			
		}
	}

}
