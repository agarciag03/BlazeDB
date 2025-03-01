package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DummyOperator;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class JoinOperatorTest {
    private DummyOperator leftChild;
    private DummyOperator rightChild;
    private Expression joinCondition;
    private JoinOperator joinOperator;

    @Before
    public void setUp() {
        Tuple tuple1 = new Tuple("1, 2");
        Tuple tuple2 = new Tuple("3, 4");
        Tuple tuple3 = new Tuple("5, 6");
        Tuple tuple4 = new Tuple("7, 8");


        leftChild = new DummyOperator(tuple1, tuple2);
        rightChild = new DummyOperator(tuple3, tuple4);
        joinCondition = null; // or set an appropriate join condition
        joinOperator = new JoinOperator(leftChild, rightChild, joinCondition);
    }

    @Test
    public void testGetNextTuple() throws Exception {
        Tuple result = joinOperator.getNextTuple();
        assertNotNull(result);
        assertEquals(Arrays.asList(1, 2, 5, 6), result.getValues());

        result = joinOperator.getNextTuple();
        assertNotNull(result);
        assertEquals(Arrays.asList(1, 2, 7, 8), result.getValues());

        result = joinOperator.getNextTuple();
        assertNotNull(result);
        assertEquals(Arrays.asList(3, 4, 5, 6), result.getValues());

        result = joinOperator.getNextTuple();
        assertNotNull(result);
        assertEquals(Arrays.asList(3, 4, 7, 8), result.getValues());

        result = joinOperator.getNextTuple();
        assertNull(result);
    }
}