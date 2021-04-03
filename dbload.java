import java.io.*;
import java.util.*;

import javafx.scene.shape.Line;

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
//read csv file and load into heap file, count1 is used to record the record number in one page, count2 is used to record the page number,
//count3 is used to record the record number in all
	private void readCsvFile(String string, int pagesize) throws IOException {
		// TODO Auto-generated method stub
		File file = new File("heap." + pagesize);
		File stdout = new File("stdout");
		BufferedReader Buffer = null;
		BufferedWriter Buffer2 = null;
		FileOutputStream outStream = null;
		byte[] record = new byte[354];
		byte[] record2 = new byte[354];
		String[] entry = new String[500];
		String lineString = "";
		boolean firstline = true;
		int count1 = 0,count2 = 0,count3 = 0;
		// put stream into page
		outStream = new FileOutputStream(file);
		Buffer = new BufferedReader(new FileReader(string));
		Buffer2 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(stdout,false),"utf-8"));
		long start = System.currentTimeMillis();
		
		//read page line
		while((lineString = Buffer.readLine()) != null) {
			if(firstline) {
				firstline = false;
				continue;
			}
			//split each attribute in one row by ","
			entry = lineString.split(",");
			record2 = createrecord(record,entry);
			count1++;
			outStream.write(record2);
			//check to change to new page or not
			if((count1 + 1) * 354 > pagesize) {
				newpage(outStream,pagesize,count1,count2);
				count1 = 0;
				count2++;
			}
			count3++;
		}
		newpage(outStream, pagesize, count1, count2);
		count2++;
		
		long end = System.currentTimeMillis();
		//write time,record and pages into stdout
		Buffer2.write("The number of records loaded:" + count3 + "\n");
		Buffer2.write("The number of pages loaded:" + count2 + "\n");
		Buffer2.write("Load time is " + (end - start) + " ms.\n");
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
		byte[] page = new byte[pagesize - (354 * count1) - 4];
		byte[] page2 = transformInt(count2);
		outStream.write(page);
		outStream.write(page2);
	}
	//create record and allocate offset space,there are 11 attributes.
	private byte[] createrecord(byte[] byteString,String[] string) throws UnsupportedEncodingException {
	// TODO Auto-generated method stub
		byte[] convertId = transformInt(Integer.parseInt(string[0]));
		byte[] convertYear = transformInt(Integer.parseInt(string[2]));
		byte[] convertMdate = transformInt(Integer.parseInt(string[4]));
		byte[] convertTime = transformInt(Integer.parseInt(string[6]));
		byte[] convertSensorId = transformInt(Integer.parseInt(string[7]));
		byte[] convertHourly = transformInt(Integer.parseInt(string[9]));
		System.arraycopy(convertId, 0, byteString, 0, 4);
		insertRecord(string[1], 100, 4, byteString);
		System.arraycopy(convertYear, 0, byteString, 104, 4);
		insertRecord(string[3], 10, 108, byteString);
		System.arraycopy(convertMdate, 0, byteString, 118, 4);
		insertRecord(string[5], 10, 122, byteString);
		System.arraycopy(convertTime, 0, byteString, 132, 4);
		System.arraycopy(convertSensorId, 0, byteString, 136, 4);
		insertRecord(string[8], 100, 140, byteString);
		System.arraycopy(convertHourly, 0, byteString, 240, 4);
		insertRecord((string[0] + string[1]), 110, 244, byteString);
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
