package com.jackbe.mapreduce.stats;

import org.apache.commons.math.random.RandomData;
import org.apache.commons.math.random.RandomDataImpl;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

public class AggregateStatistics {
    SummaryStatistics sstats = new SummaryStatistics();

    public AggregateStatistics() {

    }
    
    public void generateRandomData( double num ) {
        RandomData randomData = new RandomDataImpl();
        for (long i = 0; i < num; i++) {
            sstats.addValue( randomData.nextLong(1, 10) );
        }
    }
    
    public void clear() {
        sstats.clear();
    }
    
    public void addValue( double d ) {
        sstats.addValue( d );
    }
    
    public void addValues( double[] d ) {
          for( double a : d ) {
              sstats.addValue( a );
          }
    }
    
    public String getSummary() {
        return  sstats.getSummary().toString();
    }
    
    public double getMax() {
        return sstats.getMax();
    }
    
    public double getMean() {
        return sstats.getMean();
    }
    
    public double getMin() {
        return sstats.getMin();
    }
    
    public double getStandardDeviation() {
        return sstats.getStandardDeviation();
    }
    
    public double getGeometricMean() {
        return sstats.getStandardDeviation();
    }
    
    public double getSecondMoment() {
        return sstats.getSecondMoment();
    }
    
    public double getNumberOfValues() {
        return sstats.getN();
    }
    
    public double getSumOfLogs() {
        return sstats.getSumOfLogs();
    }
    
    public double getVariance() {
        return sstats.getVariance();
    }
    
    public static void main(String[] args) {
    	System.out.println("Calculating for: " + Float.MAX_VALUE);
    	AggregateStatistics stats = new AggregateStatistics();
    	int percent = 0;
    	double check = Float.MAX_VALUE/100000f; 
    	for(double i = 1; i < Float.MAX_VALUE; i++) {
    		stats.addValue(i);
    		if(i % (check) == 0) {
    			System.out.println("Percent complete: " + percent++);
    		}
    	}
    	System.out.println("Mean = " + stats.getMean());
    	System.out.println("Variabnce = " + stats.getVariance());
    }
}
