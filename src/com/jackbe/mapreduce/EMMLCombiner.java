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
 * The Combine class acts as an intermediary reducer. It will receive all the 
 * mapper key/values for a given key and can further combine (reduce the number)
 * before sending off to the reducer so that the reducer is overwhelmed.
 * 
 * @author Christopher Steel - JackBe
 *
 * @since Jul 13, 2011 10:07:24 AM
 * @version 1.0
 */
@SuppressWarnings("deprecation")
public class EMMLCombiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	static Logger log = Logger.getLogger(EMMLCombiner.class);
	private static Iterator<Text> combinerIterator;
	private EMMLMapReduce mapReduce;
	private EMMLRunner emmlRunner;

	public void reduce(Text key, Iterator<Text> iterator,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		if(log.isInfoEnabled())
			log.info("called for key:" + key);
		
		try {
			mapReduce = EMMLMapReduce.getInstance();
			emmlRunner = mapReduce.getEmmlRunner();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
		emmlRunner.executeReducerScript(mapReduce.getCombinerScript(), key, iterator, output);

	}  //reduce
	
} //class Combine
