package simpledb;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    private int field;

    private Op op;

    private Field operand;

    public Predicate(int field, Predicate.Op op, Field operand) {
        this.field = field;
        this.op = op;
        this.operand = operand;
    }

    public int getField() {
        return this.field;
    }

    public Predicate.Op getOp() {
        return this.op;
    }

    public Field getOperand() {
        return this.operand;
    }

    public boolean filter(Tuple t) {
        return (t != null) &&
                (field < t.getTupleDesc().numFields()) &&
                t.getField(field).compare(op, operand);
    }

    public String toString() {
        return "";
    }

    /**
     * Constants used for return codes in Field.compare
     */

    public enum Op implements Serializable

    {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }
}