package com.jackbe.mapreduce.examples.stock;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;
import java.util.Random;

/**
 * @author Christopher Steel - JackBe Corporation
 *
 * @since Jul 8, 2011 1:37:41 AM
 * @version 1.0
 */
public class StockGenerator {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String[] stocks = new String[] { "AAPL", "IBM", "INTC", "ORCL", "GOOG", "GLD", "SLV", "JKBE", "XOM", "TSLA" };
		float[] basePrice = new float[] { 325.0f, 95.0f, 22.50f, 24.00f, 425.0f, 152.0f, 35.0f, 60.0f, 85.0f, 28.0f };
		Random rand = new Random(System.currentTimeMillis());

		String filename = "stocks.csv";
		if (args != null && args.length > 0)
			filename = args[0];

		FileWriter fw = null;
		try {
			fw = new FileWriter(new File(filename));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//long quoteTime = System.currentTimeMillis() - 60600000;
		long quoteTime = 1310045400000l;
		System.out.println("Quote start time: " + quoteTime + " " + new Date(quoteTime));
		DateFormat df = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM);
		
		for (int i = 0; i < 25000; i++) {
			int index = rand.nextInt(stocks.length);
			float price = basePrice[index]
					+ rand.nextInt((int) (basePrice[index] * 0.25f)) + (0.01f * rand.nextInt(100)) ;
			quoteTime += 30000 + rand.nextInt(10); // Add 1 to 4 tenths seconds between quotes
			Date date = new Date(quoteTime);
			if(date.getHours() < 9) {
				date.setHours(9);
				date.setMinutes(30);
			}
			if(date.getHours() > 16) {
				date.setDate(date.getDate() + 1);
				date.setHours(9);
				date.setMinutes(30);
			}
			if(date.getDay() > 5) {
				System.out.println("Date was = " + date.toLocaleString());
				date.setDate(date.getDate() + 2);
				System.out.println("Date now = " + date.toLocaleString());
			}
			quoteTime = date.getTime();
				
			
			try {
				fw.write(df.format(date) + "," + stocks[index] + "," + price + '\n');
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			if(fw != null) {
				fw.flush();
				fw.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Stock quotes generated to file: " + filename);
	}

}
