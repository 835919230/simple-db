package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    
    private Predicate predicate;
    
    private TupleDesc tupleDesc;
    
    private OpIterator[] children;

    private int childrenIndex = 0;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.predicate = p;
        this.children = new OpIterator[]{child};
        this.tupleDesc = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        for (OpIterator iterator : children) {
            iterator.open();
        }
    }

    public void close() {
        // some code goes here
        super.close();
        for (OpIterator iterator : children) {
            iterator.close();
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        for (OpIterator iterator : children) {
            iterator.rewind();
        }
        childrenIndex = 0;
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (childrenIndex >= children.length) {
            return null;
        }
        if (!children[childrenIndex].hasNext()) {
            childrenIndex++;
            return fetchNext();
        }
        Tuple next = children[childrenIndex].next();
        if (!predicate.filter(next)) {
            return fetchNext();
        }
        return next;
    }

//    @Override
//    public boolean hasNext() throws DbException, TransactionAbortedException {
//        return childrenIndex < children.length;
//    }
//
//    @Override
//    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
//        return fetchNext();
//    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return this.children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.children = children;
    }

}
