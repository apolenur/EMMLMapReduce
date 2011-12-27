package com.jackbe.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;


/**
 * The reduce method in the Reduce class is called for each set of keys from the 
 * Combiner. In this case it is called for every symboldate pair. The reduce 
 * method will then calculate the Daily moving average for each symbol.
 *   
 * @author Christopher Steel - JackBe
 *
 * @since Jul 13, 2011 10:24:21 AM
 * @version 1.0
 */
@SuppressWarnings("deprecation")
public class EMMLReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	private static Logger log = Logger.getLogger(EMMLReducer.class);
	private EMMLMapReduce mapReduce;
	private EMMLRunner emmlRunner;
	private static Iterator<Text> reducerIterator;
	
	public static Iterator<Text> getIterator() {
		return reducerIterator;
	}
	
	public static void setIterator(Iterator<Text> iterator) {
		reducerIterator = iterator;
	}
	
	public void reduce(Text key, Iterator<Text> iterator,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		EMMLReducer.setIterator(iterator);
		
		if(log.isDebugEnabled())
			log.debug("called.");
		
		try {
			mapReduce = EMMLMapReduce.getInstance();
			emmlRunner = mapReduce.getEmmlRunner();
		} catch (Exception e) {
			log.error(e, e);
			throw new IOException(e);
		}
		// Set the Iterator so that the EMML can retrieve it.
		emmlRunner.executeReducerScript(mapReduce.getReducerScript(), key, iterator, output);

	} //reduce

} // classe Reduce
