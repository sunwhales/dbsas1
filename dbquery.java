import java.io.*;
import java.nio.ByteBuffer;



public class dbquery {

	public static void main(String[] args) throws NumberFormatException, IOException {
		// TODO Auto-generated method stub
		dbquery Dbquery = new dbquery();
		
		Dbquery.readCommand(args);
		
		
	}
//read command line in AWS linux instance
	private void readCommand(String[] args) throws NumberFormatException, IOException {
		// TODO Auto-generated method stub
		readheap(args[0],Integer.valueOf(args[1]));
	}
//read record in heap file
	private void readheap(String string, int pagesize) throws IOException {
		// TODO Auto-generated method stub
		boolean isPage = true;
		boolean isRecord = true;
		int rlength = 0;
		int rid2 = 0;
		int pcount = 0;
		int rcount = 0;
		File heapFile = new File("heap." + pagesize);
		FileInputStream inputStream = new FileInputStream(heapFile);
		String rec = null;
		String SDT_NAME = null;
		BufferedWriter Buffer = null;
		Buffer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("stdout",true),"utf-8"));
		float start = System.currentTimeMillis();
		while(isPage) {
			byte[] Pagesize = new byte[pagesize];
			byte[] Pagenum = new byte[4];
			inputStream.read(Pagesize,0,pagesize);
			System.arraycopy(Pagesize, Pagesize.length - 4, Pagenum, 0, 4);
			while(isRecord) {
				byte[] Record = new byte[348];
				byte[] rid = new byte[4];
				try {
					System.arraycopy(Pagesize, rlength, Record, 0, 348);
	                System.arraycopy(Record, 0, rid, 0, 4);
	                rid2 = ByteBuffer.wrap(rid).getInt();
	                if(rid2 != rcount) {
	                	isRecord = false;
	                }
	                else {
	                	rec = new String(Record);
	                	SDT_NAME = rec.substring(244, 348);
	                	if(SDT_NAME.toLowerCase().contains(string.toLowerCase())) {
	                		Buffer.write("=========");
	                		Buffer.write(new String(Record));
	                	}
	                	rlength += 348; 
	                }
	                rcount++;
				} catch (ArrayIndexOutOfBoundsException e) {
					// TODO: handle exception
					isPage = false;
					rlength = 0;
					rcount = 0;
					rid2 = 0;
				}
			}
			if(ByteBuffer.wrap(Pagenum).getInt() != pcount) {
				isPage = false;
			}
			pcount++;
		}
		float end = System.currentTimeMillis();
		Buffer.write("Query time is" + (end - start) + "ms.");
		inputStream.close();
		Buffer.close();
	}


}
