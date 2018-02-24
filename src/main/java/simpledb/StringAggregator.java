package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupFieldIndex;

    private Type groupFieldType;

    private int aggregateFieldIndex;

    private Op operation;

    private Map<Field, Integer> groupMap = new ConcurrentHashMap<>();

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        groupFieldIndex = gbfield;
        groupFieldType = gbfieldtype;
        aggregateFieldIndex = afield;
        if (what != Op.COUNT)
            throw new IllegalArgumentException("StringField Operation must be COUNT!");
        operation = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupField = groupFieldIndex == NO_GROUPING ? null : tup.getField(groupFieldIndex);
        if (!groupMap.containsKey(groupField)) {
            groupMap.put(groupField, 0);
        }
        groupMap.put(groupField, groupMap.get(groupField) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        Set<Map.Entry<Field, Integer>> entries = groupMap.entrySet();
        List<Tuple> tupleList = new ArrayList<>();
        TupleDesc tupleDesc = new TupleDesc(new Type[]{groupFieldType, Type.INT_TYPE}, new String[]{null, null});
        for (Map.Entry<Field, Integer> entry : entries) {
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, entry.getKey());

            tuple.setField(1, new IntField(entry.getValue()));

            tupleList.add(tuple);
        }
        return new TupleIterator(tupleDesc, tupleList);
    }

}
