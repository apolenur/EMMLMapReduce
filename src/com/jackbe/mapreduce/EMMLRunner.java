package com.jackbe.mapreduce;

import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

public interface EMMLRunner {
	public void executeMapperScript(  String scriptName, Text key, String value,            OutputCollector<Text, Text> output);
	public void executeReducerScript( String scriptName, Text key, Iterator<Text> iterator, OutputCollector<Text, Text> output);
}