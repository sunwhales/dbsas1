import java.io.*;
import java.util.*;
import java.nio.ByteBuffer;


public class dbload {
//main class to start program and calculate load time
	public static void main(String[] args) throws NumberFormatException, IOException {
		// TODO Auto-generated method stub
		dbload loadcsv = new dbload();		
		loadcsv.readcommand(args);
	}
//read command to launch program
	public void readcommand(String[] args) throws NumberFormatException, IOException {
		// TODO Auto-generated method stub
		if (args[0].equals("-p"))
        {
           readCsvFile(args[2], Integer.parseInt(args[1]));
        }
	}
//read csv file and load into heap file, count1 is used to check if it is needed to create new page, count2 is used to record the page number,
//count3 is used to record the record number
	private void readCsvFile(String string, int pagesize) throws IOException {
		// TODO Auto-generated method stub
		File file = new File("heap." + pagesize);
		File stdout = new File("stdout");
		BufferedReader Buffer = null;
		BufferedWriter Buffer2 = null;
		FileOutputStream outStream = null;
		byte[] record = new byte[348];
		byte[] record2 = new byte[348];
		int count1 = 0,count2 = 0,count3 = 0;
		// put stream into page
		outStream = new FileOutputStream(file);
		Buffer = new BufferedReader(new FileReader(string));
		Buffer2 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(stdout,false),"utf-8"));
		float start = System.currentTimeMillis();
		//read page line
		while(Buffer.readLine() != null) {
			String[] entry = Buffer.readLine().split("\t", -1);
			record2 = createrecord(record,entry,count1);
			count1++;
			outStream.write(record2);
			if((count1 + 1) * 348 > pagesize) {
				newpage(outStream,pagesize,count1,count2);
				count1 = 0;
				count2++;
			}
			count3++;
		}
		newpage(outStream, pagesize, count1, count2);
		count2++;
		
		float end = System.currentTimeMillis();
		Buffer2.write("The number of records loaded:" + count3 + "\n");
		Buffer2.write("The number of pages loaded:" + count2 + "\n");
		Buffer2.write("Load time is " + (end - start) + "ms.\n");
		outStream.close();
		Buffer.close();
		Buffer2.close();
	}
	//copy data to record
	public void insertRecord(String data,int a,int b,byte[] c) throws UnsupportedEncodingException {
		byte[] byte1 = new byte[a];
		byte[] byte2 = data.trim().getBytes("utf-8");
		if(data != "") {
			System.arraycopy(byte2, 0, byte1, 0, byte2.length);
		}
		System.arraycopy(byte1,0,c,b,byte1.length);
	}
	//create 4 byte as the gap at the end of each page
	private void newpage(FileOutputStream outStream, int pagesize, int count1, int count2) throws IOException {
	// TODO Auto-generated method stub
		byte[] page = new byte[pagesize - (244 * count1) - 4];
		byte[] page2 = transformInt(count2);
		outStream.write(page);
		outStream.write(page2);
	}
	//create record and allocate offset space
	private byte[] createrecord(byte[] byteString,String[] string,int count) throws UnsupportedEncodingException {
	// TODO Auto-generated method stub
		byte[] convertByte = transformInt(count);
		System.arraycopy(convertByte, 0, byteString, 0, convertByte.length);
		insertRecord(string[0], 100, 4, byteString);
		insertRecord(string[1], 4, 104, byteString);
		insertRecord(string[2], 10, 108, byteString);
		insertRecord(string[3], 4, 118, byteString);
		insertRecord(string[4], 10, 122, byteString);
		insertRecord(string[5], 4, 132, byteString);
		insertRecord(string[6], 4, 136, byteString);
		insertRecord(string[7], 100, 140, byteString);
		insertRecord(string[8], 4, 240, byteString);
		insertRecord(string[9] + string[1], 104, 244, byteString);
		return byteString;
	}
	//transform int attribute into byte to build binary file
	private byte[] transformInt(int count) {
		// TODO Auto-generated method stub
		ByteBuffer buffer = ByteBuffer.allocate(4);
		buffer.putInt(count);
		return buffer.array();
	}


}
