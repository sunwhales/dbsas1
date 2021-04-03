import java.io.*;
import java.nio.ByteBuffer;
import java.util.Base64;



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
		int rlength = 0;
		int endline = 0;
		int pcount = 0;
		int rcount = 0;
		int rid2 = 0;
		int rCount = 0;
		//read binary file from heap file
		File heapFile = new File("heap." + pagesize);
		FileInputStream inputStream = new FileInputStream(heapFile);
		String rec = "";
		String SDT_NAME = null;
		BufferedWriter Buffer = null;
		boolean isRecord = true;
		//output the result of querying and the query time into stdout
		Buffer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("stdout",true),"utf-8"));
		long start = System.currentTimeMillis();
		//read the heap file page by page
		while((endline = inputStream.read()) != -1) {
			
			byte[] Pagesize = new byte[pagesize];
			byte[] Pagenum = new byte[4];
			//read each page and store into Pagenum
			inputStream.read(Pagesize,0,pagesize);
			System.arraycopy(Pagesize, Pagesize.length - 4, Pagenum, 0, 4);
			//read record in each page
			while(isRecord) {
				byte[] Record = new byte[354];
				byte[] rid = new byte[4];
				//read each record and copy into Record
					System.arraycopy(Pagesize, rlength, Record, 0, 354);
					//record the id of this record
					System.arraycopy(Record, 0, rid, 0, 4);
	               //check id is the end of page or not 
					rid2 = ByteBuffer.wrap(rid).getInt();
	                rec = new String(Record);
	                
	            
	                if (rid2 != rCount)
	                  {
	                     isRecord = false;
	                  }
	                else {
	                	//compare text and the SDT_Name,if they matches,output this record into stdout
	                SDT_NAME = rec.substring(244,354);
	                if(SDT_NAME.toLowerCase().contains(string.toLowerCase())) {
	                	Buffer.write("=========");
	                	Buffer.write(rec);
	                }
	                }
	                rcount++;
	                rlength += 354; 
	                //check it is needed to change to new page or not
	                if(rlength > pagesize) {
	                	isRecord = false;
	                	rlength = 0;
	                    rcount = 0;
	                    rid2 = 0;
	                }
				
					
			}
			//check it is the end of the last page or not
			if(ByteBuffer.wrap(Pagenum).getInt() != pcount) {
				isPage = false;
			}
			pcount++;
			
			
		}
		long end = System.currentTimeMillis();
		//put query time into stdio file
		Buffer.write("Query time is " + (end - start) + " ms.\n");
		inputStream.close();
		Buffer.close();

	}
	


}
