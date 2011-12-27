/*
 * Copyright (©) 2011 JackBe Corporation
 * All Rights Reserved.
 *
 * This software is the confidential and proprietary information of JackBe
 * Corporation ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with JackBe.
 *
 * JACKBE MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY OF THE
 * SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 * PURPOSE, OR NON-INFRINGEMENT. FORTMOON SHALL NOT BE LIABLE FOR ANY DAMAGES
 * SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING OR DISTRIBUTING
 * THIS SOFTWARE OR ITS DERIVATIVES.
 */

package com.jackbe.mapreduce;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * @author Christopher Steel - JackBe Coproration
 *
 * @since Oct 26, 2011 11:01:33 AM
 * @version 1.0
 */
public class HAMPParser {
	private String countCol;
	private String sumCol;
	private String filename;
	private HashMap<Long, Long> map = new HashMap<Long, Long>();
	private String[] headers;
	private File xmlFile;
	
	public HAMPParser(String fileName) {
		this.filename = fileName;
		String xmlFilename = fileName.replaceFirst(".csv", ".xml");
		xmlFilename = "/Users/csteel/Dropbox/Jackbe/" + xmlFilename;
		xmlFile = new File(xmlFilename);
	}
	
	public void parse() {
		File file = null;
		LineNumberReader lineReader = null;
		int countColIndex = -1;
		long count = 0;
		long sum = 0;
		long numRecords = 1; //count header to sync with 'wc -l'
		String countStr = null;
		FileWriter writer = null;
		
		try {
			writer = new FileWriter(xmlFile);
			writer.write("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n");
			writer.write("<Entries>\n");
			file = new File(this.filename);
			FileReader reader = new FileReader(file);
			lineReader = new LineNumberReader(reader);
			String line = lineReader.readLine();
			System.out.println("Line = " + line);
			// Skip the first row, it is column headers
			StringTokenizer token = new StringTokenizer(line, ",");
			System.out.println("countCol = " + countCol);
			int tokenCount = token.countTokens();
			System.out.println("Token count = " + tokenCount);
			headers = new String[tokenCount];
			for(int i = 0; i < tokenCount; i++) {
				String header = token.nextToken();
				System.out.println("header = " + header);
				this.headers[i] = header;
				if(header.equalsIgnoreCase(countCol)) {
					countColIndex = i + 1;
					System.out.println("Count col index = " + countColIndex);
				}
			}
			
			String msa = null;
			line = lineReader.readLine();
			while(line != null) {
				numRecords++;
				token = new StringTokenizer(line, ",");
				writer.write("\t<Entry>\n");
				for(int j = 0; j < tokenCount; j++) {
					countStr = token.nextToken();
					countStr = countStr.replace('"', ' ');
					countStr = countStr.trim();
					if(j == countColIndex) {
						msa = countStr;
					}
					writer.write("\t\t<" + headers[j] + ">");
					writer.write(countStr);
					writer.write("</" + headers[j] + ">\n");
				}
				writer.write("\t</Entry>\n");
				
				//System.out.println("countStr = " + countStr);
				if (msa != null && msa.length() > 0) {
					Long code = Long.parseLong(msa);
					sum += code;
					if(map.get(code) != null) {
						map.put(code, map.get(code) + 1);
					}
					else {
						map.put(code, 1L);
					}
					count++;
				}
				line = lineReader.readLine();
			}
			writer.write("</Entries>\n");
			System.out.println("Count = " + count + " sum = " + sum + " last countStr = " + countStr + " num records (plus header) = " + numRecords);
		}
		catch(Exception e) {
			System.out.println("Exception: " + e);
			e.printStackTrace();
		}
		finally {
			if (lineReader != null) {
				try {
					lineReader.close();
					writer.flush();
					writer.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		System.out.println("Map = " + map.toString());
		Iterator i = map.keySet().iterator();
		/*
		while(i.hasNext()) {
			Long key = (Long) i.next();
			Long value = map.get(key);
			System.out.println("<Entry>");
			System.out.println("\t<CSA_Code>" + key + "</CSA_Code>");
			System.out.println("\t<Num_Mods>" + value + "</Num-Mods>");
			System.out.println("</Entry>");
		}
		*/
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		HAMPParser parser = new HAMPParser("HMP_Public_User_Mod_Data_1_20110916.csv");
		parser.setCountCol("prop_geoc_cnsus_msa_cd");
		parser.parse();
	}

	/**
	 * @return the countCol
	 */
	public String getCountCol() {
		return countCol;
	}

	/**
	 * @param countCol the countCol to set
	 */
	public void setCountCol(String countCol) {
		this.countCol = countCol;
	}

	/**
	 * @return the sumCol
	 */
	public String getSumCol() {
		return sumCol;
	}

	/**
	 * @param sumCol the sumCol to set
	 */
	public void setSumCol(String sumCol) {
		this.sumCol = sumCol;
	}

	/**
	 * @return the filename
	 */
	public String getFilename() {
		return filename;
	}

	/**
	 * @param filename the filename to set
	 */
	public void setFilename(String filename) {
		this.filename = filename;
	}

}
