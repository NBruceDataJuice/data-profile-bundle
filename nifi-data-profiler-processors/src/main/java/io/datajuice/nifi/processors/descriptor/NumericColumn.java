package io.datajuice.nifi.processors.descriptor;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.moment.SecondMoment;

public class NumericColumn {

    int numRows =  0;
    long numValues;
    double max;
    double min;
    double sum;
    double mean;
    double standardDeviation;
    double variance;
    double geometricMean;
    double sumOfSquares;
    double secondMoment;
    double kurtosis;
    double skewness;
    double median;
    double percentile25;
    double percentile75;

    public NumericColumn(String column, DataFileStream<GenericData.Record> stream){
        describeNumeric(column, stream);
    }

    private void describeNumeric(String column, DataFileStream<GenericData.Record> stream) {
        DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
        for (GenericRecord record: stream){

            // TODO added to complete tests. 100% sure this needs to be cleaned up
            if ( record.getSchema().getField(column).schema().getType().toString().toLowerCase().equals("long") ){
                descriptiveStatistics.addValue(((Long) record.get(column)).doubleValue());
            } else if ( record.getSchema().getField(column).schema().getType().toString().toLowerCase().equals("int") ){
                descriptiveStatistics.addValue((Integer) record.get(column));
            }
            else {
                descriptiveStatistics.addValue((Double) record.get(column));
            }
            numRows++;
        }

        numValues = descriptiveStatistics.getN();
        max = descriptiveStatistics.getMax();
        min = descriptiveStatistics.getMin();
        sum = descriptiveStatistics.getSum();
        mean = descriptiveStatistics.getMean();
        standardDeviation = descriptiveStatistics.getStandardDeviation();
        variance = descriptiveStatistics.getVariance();
        geometricMean = descriptiveStatistics.getGeometricMean();
        sumOfSquares = descriptiveStatistics.getSumsq();
        secondMoment = new SecondMoment().evaluate(descriptiveStatistics.getValues());
        kurtosis = descriptiveStatistics.getKurtosis();
        skewness = descriptiveStatistics.getSkewness();
        median = descriptiveStatistics.getPercentile(50.0);
        percentile25 = descriptiveStatistics.getPercentile(25.0);
        percentile75 = descriptiveStatistics.getPercentile(75.0);
    }

    public int getNumRows() {
        return numRows;
    }

    public long getNumValues() {
        return numValues;
    }

    public double getMax() {
        return max;
    }

    public double getMin() {
        return min;
    }

    public double getSum() {
        return sum;
    }

    public double getMean() {
        return mean;
    }

    public double getStandardDeviation() {
        return standardDeviation;
    }

    public double getVariance() {
        return variance;
    }

    public double getGeometricMean() {
        return geometricMean;
    }

    public double getSumOfSquares() {
        return sumOfSquares;
    }

    public double getSecondMoment() {
        return secondMoment;
    }

    public double getKurtosis() {
        return kurtosis;
    }

    public double getSkewness() {
        return skewness;
    }

    public double getMedian() {
        return median;
    }

    public double getPercentile25() {
        return percentile25;
    }

    public double getPercentile75() {
        return percentile75;
    }

}
