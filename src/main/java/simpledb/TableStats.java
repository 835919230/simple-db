package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private DbFile dbFile;
    private int ioCostPerPage;

    private int totalTuples;

    Map<Integer, IntHistogram> intHistogramMap = new HashMap<>();
    Map<Integer, StringHistogram> stringHistogramMap = new HashMap<>();

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
        this.ioCostPerPage = ioCostPerPage;
        TupleDesc tupleDesc = dbFile.getTupleDesc();
        int numFields = tupleDesc.numFields();

        DbFileIterator itr = dbFile.iterator(new TransactionId());
        Map<Integer, List<Integer>> integerListMap = new HashMap<>();
        Map<Integer, Integer> maxIntMap = new HashMap<>();
        Map<Integer, Integer> minIntMap = new HashMap<>();
        for (int i = 0; i < numFields; i++) {
            integerListMap.put(i, new ArrayList<Integer>());
            maxIntMap.put(i, Integer.MIN_VALUE);
            minIntMap.put(i, Integer.MAX_VALUE);
        }
        try {
            itr.open();
            while (itr.hasNext()) {
                Tuple next = itr.next();
                totalTuples++;
                for (int i = 0; i < numFields; i++) {
                    Field field = next.getField(i);
                    Type fieldType = tupleDesc.getFieldType(i);
                    if (fieldType == Type.INT_TYPE) {
                        IntField intField = (IntField) field;
                        int value = intField.getValue();
                        int max = maxIntMap.get(i);
                        int min = minIntMap.get(i);
                        if (value > max)
                            max = value;
                        if (value < min)
                            min = value;
                        maxIntMap.put(i, max);
                        minIntMap.put(i, min);

                        integerListMap.get(i).add(value);
                    } else if (fieldType == Type.STRING_TYPE) {
                        if (stringHistogramMap.get(i) == null) {
                            stringHistogramMap.put(i, new StringHistogram(NUM_HIST_BINS));
                        }
                        StringField stringField = (StringField) field;
                        stringHistogramMap.get(i).addValue(stringField.getValue());
                    }
                }
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < numFields; i++) {
            Type fieldType = tupleDesc.getFieldType(i);
            if (fieldType == Type.INT_TYPE) {
                if (intHistogramMap.get(i) == null) {
                    intHistogramMap.put(i, new IntHistogram(NUM_HIST_BINS, minIntMap.get(i), maxIntMap.get(i)));
                }
                List<Integer> list = integerListMap.get(i);
                for (int j = 0; j < list.size(); j++) {
                    intHistogramMap.get(i).addValue(list.get(j));
                }
            }
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        // Therefore, scancost(t1) is simply the number of pages in t1 x SCALING_FACTOR.
        if (dbFile instanceof HeapFile) {
            HeapFile heapFile = (HeapFile) this.dbFile;
            int numPages = heapFile.numPages();
            return numPages * ioCostPerPage;
        } else if (dbFile instanceof BTreeFile) {
            BTreeFile bTreeFile = (BTreeFile) this.dbFile;
            int numPages = bTreeFile.numPages();
            return numPages * ioCostPerPage;
        }
        return 0;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        Type fieldType = this.dbFile.getTupleDesc().getFieldType(field);
        if (fieldType == Type.INT_TYPE) {
            return intHistogramMap.get(field).avgSelectivity();
        } else if (fieldType == Type.STRING_TYPE) {
            return stringHistogramMap.get(field).avgSelectivity();
        }
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        Type fieldType = this.dbFile.getTupleDesc().getFieldType(field);
        if (fieldType == Type.INT_TYPE) {
            IntField f = (IntField) constant;
            return intHistogramMap.get(field).estimateSelectivity(op, f.getValue());
        } else if (fieldType == Type.STRING_TYPE) {
            StringField f = (StringField) constant;
            return stringHistogramMap.get(field).estimateSelectivity(op, f.getValue());
        }
        return 1.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return totalTuples;
    }

}
