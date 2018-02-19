package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tdItemList.iterator();
    }

    private static final long serialVersionUID = 1L;

    private List<TDItem> tdItemList;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (typeAr == null || fieldAr == null || (typeAr.length != fieldAr.length)) {
            throw new IllegalArgumentException("please check typeAr and fieldAr to ensure Arguments right.");
        }
        tdItemList = new ArrayList<>();

        for (int i = 0; i < typeAr.length; i++) {
            TDItem tdItem = new TDItem(typeAr[i], fieldAr[i]);
            tdItemList.add(tdItem);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdItemList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= tdItemList.size() || i < 0) {
            throw new NoSuchElementException("no such element,please check index");
        }
        return tdItemList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= tdItemList.size() || i < 0) {
            throw new NoSuchElementException("no such element,please check index");
        }
        return tdItemList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        int index = -1;
        for (int i =0; i < tdItemList.size();i++) {
            TDItem tdItem = tdItemList.get(i);
            if (tdItem.fieldName != null && tdItem.fieldName.equals(name)) {
                index = i;
                break;
            }
        }
        if (index == -1) {
            throw new NoSuchElementException("no such element: "+name);
        }
        return index;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem item : tdItemList) {
            size += item.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int newNumFields = td1.numFields() + td2.numFields();
        Type[] typeAr = new Type[newNumFields];
        String[] fieldNameAr = new String[newNumFields];
        for (int i = 0; i < td1.tdItemList.size(); i++) {
            typeAr[i] = td1.tdItemList.get(i).fieldType;
            fieldNameAr[i] = td1.tdItemList.get(i).fieldName;
        }
        int offset = td1.tdItemList.size();
        for (int i = 0; i<td2.tdItemList.size();i++) {
            typeAr[i+offset] = td2.tdItemList.get(i).fieldType;
            fieldNameAr[i+offset] = td2.tdItemList.get(i).fieldName;
        }
        TupleDesc tupleDesc = new TupleDesc(typeAr, fieldNameAr);
        return tupleDesc;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o instanceof TupleDesc) {
            TupleDesc tupleDesc = (TupleDesc)o;
            int numFields = tupleDesc.numFields();
            if (numFields != this.numFields())
                return false;
            for (int i = 0; i < numFields; i++) {
                String outsideFieldName = tupleDesc.getFieldName(i);
                String insideFieldName = this.getFieldName(i);
                if (outsideFieldName == null && insideFieldName != null)
                    return false;
                if (outsideFieldName!= null && !outsideFieldName.equals(insideFieldName))
                    return false;

                Type outsideFieldType = tupleDesc.getFieldType(i);
                Type insideFieldType = this.getFieldType(i);
                if (!outsideFieldType.equals(insideFieldType))
                    return false;
                // TODO: 2018/2/16 does order matter? yes
            }
        } else {
            return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (TDItem tdItem : tdItemList) {
            sb.append(tdItem.toString()).append(",");
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }
}
