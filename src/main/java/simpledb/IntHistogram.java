package simpledb;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private List<AtomicInteger> histList = new ArrayList<>();

    private double minF;
    private double maxF;
    private int numB;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        int range = max - min + 1;
        int bucketsNumber = range < buckets ? range:buckets;
        for (int i = 0; i < bucketsNumber; i++) {
            histList.add(new AtomicInteger(0));
        }
        minF = (double)min;
        maxF = (double)max;
        numB = bucketsNumber;
    }

    /**|
     * get index of the histogram
     * @param value to decide index
     * @return value < minF:-1
     *          value > maxF: numB
     */
    private int getIndex(int value) {
        double val = (double)value;
        double blockSize = getW();
        if (val < minF)
            return -1;
        else if (val > maxF)
            return numB;
        int index = (int)((val-minF) / blockSize);
        return index;
    }

    /**
     * get the width of the bucket
     * @return
     */
    private double getW() {
        return (maxF - minF + 1.0D) / ((double) numB);
    }

    /**
     * get the height of histogram in specific index
     * @param index
     * @return
     */
    private int getHeight(int index) {
        return index < 0 ? 0 : index >= numB ? 0 : histList.get(index).get();
    }

    private int getNumberOfTuples() {
        int numberOfTuples = 0;
        for (int i = 0; i < numB; i++) {
            numberOfTuples += histList.get(i).get();
        }
        return numberOfTuples;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = getIndex(v);
        index = index < 0 ? 0 : index >= numB ? numB - 1 : index;
        histList.get(index).incrementAndGet();
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        double selectivity = -1.0;
        int ntups = getNumberOfTuples();
        double w = getW();
        switch (op) {
            case EQUALS:
                selectivity = 0D;
                int index = getIndex(v);
                int h = getHeight(index);
                selectivity = ((double)h / w) / (double) ntups;
                break;
            case GREATER_THAN_OR_EQ:
                selectivity = 0D;
                index = getIndex(v);
                int h_b = getHeight(index);
                double b_f = ((double)h_b)/(double)ntups;
//                double b_right = w * (index + 1D);
//                double b_part = (b_right - v) / w;
                double contributes = b_f;
                selectivity = 0D;
                for (int i = index + 1; i < numB; i++) {
                    h_b = getHeight(i);
                    selectivity += ((double)h_b)/(double)ntups;
                }
                selectivity += contributes;
                break;
            case GREATER_THAN:
                selectivity = 0D;
                index = getIndex(v);
                h_b = getHeight(index);
                b_f = ((double)h_b)/(double)ntups;
                double b_right = w * (index + 1D);
                double b_part = (b_right - v) / w;
                contributes = b_part * b_f;
                selectivity = 0D;
                for (int i = index + 1; i < numB; i++) {
                    h_b = getHeight(i);
                    selectivity += ((double)h_b)/(double)ntups;
                }
                selectivity += contributes;
                break;
            case LESS_THAN_OR_EQ:
                selectivity = 0D;
                index = getIndex(v);
                h_b = getHeight(index);
                b_f = ((double)h_b)/(double)ntups;
                contributes = b_f;
                selectivity = 0D;
                for (int i = index - 1; i > -1; i--) {
                    h_b = getHeight(i);
                    selectivity += ((double)h_b)/(double)ntups;
                }
                selectivity += contributes;
                break;
            case LESS_THAN:
                selectivity = 0D;
                index = getIndex(v);
                h_b = getHeight(index);
                b_f = ((double)h_b)/(double)ntups;
                double b_left = w * index ;
                b_part = (v - b_left) / w;
                contributes = b_part * b_f;
                selectivity = 0D;
                for (int i = index - 1; i > -1; i--) {
                    h_b = getHeight(i);
                    selectivity += ((double)h_b)/(double)ntups;
                }
                selectivity += contributes;
                break;
            case LIKE:
                break;
            case NOT_EQUALS:
                selectivity = 0D;
                index = getIndex(v);
                h = getHeight(index);
                selectivity = 1-((double)h / w) / (double) ntups;
                break;
                default:
                    break;
        }
        return selectivity > 1.0 ? 1.0 : selectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
//        return histList.toString();
        return "";
    }
}
