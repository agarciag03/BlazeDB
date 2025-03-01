// SortOperatorTest.java
package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DummyOperator;
import ed.inf.adbs.blazedb.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SortOperatorTest {
    private DummyOperator dummyOperator;

    @Before
    public void setUp() {
        List<Tuple> tuples = Arrays.asList(
                new Tuple("1, 2, 3"),
                new Tuple("1, 2, 3"),
                new Tuple("4, 5, 6"),
                new Tuple("4, 5, 6"),
                new Tuple("7, 8, 9")
        );
        dummyOperator = new DummyOperator(tuples);

    }
    @Test
    public void testGetAllTuples() throws Exception {

        List<Tuple> tuples = Arrays.asList(
                new Tuple("1, 2, 3"),
                new Tuple("1, 2, 3"),
                new Tuple("4, 5, 6"),
                new Tuple("4, 5, 6"),
                new Tuple("7, 8, 9")
        );

        SortOperator sortOperator = new SortOperator(dummyOperator, Arrays.asList());
        dummyOperator.reset();
        List<Tuple> result = sortOperator.getAllTuples(dummyOperator);

        assertEquals(tuples, result);
    }
}
