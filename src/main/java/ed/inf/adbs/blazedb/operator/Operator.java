package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;

import java.io.IOException;

/**
 * The abstract Operator class for the iterator model.
 *
 * Feel free to modify this class, but must keep getNextTuple() and reset()
 */
public abstract class Operator {

    /**
     * Retrieves the next tuple from the iterator.
     * @return A Tuple object representing the row of data, or NULL if EOF reached.
     */
    public abstract Tuple getNextTuple() throws Exception;

    /**
     * Resets the iterator to the start.
     */
    public abstract void reset() throws Exception;
}