package simpledb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupFieldIndex;

    private Type groupFieldType;

    private Op operation;

    private int aFieldIndex;

    private Map<Field, ArrayList<IntField>> groupMap = new ConcurrentHashMap<>();

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        groupFieldIndex = gbfield;
        groupFieldType = gbfieldtype;
        aFieldIndex = afield;
        operation = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbField = groupFieldIndex == NO_GROUPING ? null : tup.getField(groupFieldIndex);
        if (!groupMap.containsKey(gbField)) {
            groupMap.put(gbField, new ArrayList<IntField>());
        }
        ArrayList<IntField> list = groupMap.get(gbField);
        list.add((IntField) tup.getField(aFieldIndex));
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        Set<Map.Entry<Field, ArrayList<IntField>>> entrySet = groupMap.entrySet();
        List<Tuple> tupleList = new ArrayList<>();
        TupleDesc tupleDesc = new TupleDesc(new Type[]{groupFieldType, Type.INT_TYPE});
        for (Map.Entry<Field, ArrayList<IntField>> entry : entrySet) {
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, entry.getKey());
            ArrayList<IntField> fieldArrayList = entry.getValue();
            switch (operation) {
                case SUM_COUNT:
                    // TODO: 2018/2/23
                    break;
                case SUM:
                    int sum = 0;
                    for (IntField intField : fieldArrayList) {
                        sum+=intField.getValue();
                    }
                    tuple.setField(1, new IntField(sum));
                    break;
                case SC_AVG:
                    // TODO: 2018/2/23
                    break;
                case MIN:
                    Field minField = null;
                    for (IntField intField : fieldArrayList) {
                        if (minField == null || minField.compare(Predicate.Op.GREATER_THAN, intField))
                            minField = intField;
                    }
                    tuple.setField(1, minField);
                    break;
                case AVG:
                    sum = 0;
                    for (IntField intField : fieldArrayList) {
                        sum += intField.getValue();
                    }
                    tuple.setField(1, new IntField(sum/fieldArrayList.size()));
                    break;
                case COUNT:
                    tuple.setField(1, new IntField(fieldArrayList.size()));
                    break;
                case MAX:
                    Field maxField = null;
                    for (IntField intField : fieldArrayList) {
                        if (maxField == null || maxField.compare(Predicate.Op.LESS_THAN, intField))
                            maxField = intField;
                    }
                    tuple.setField(1, maxField);
                    break;
                    default:
                        break;
            }

            tupleList.add(tuple);
        }

        return new TupleIterator(tupleDesc, tupleList);
    }

}
