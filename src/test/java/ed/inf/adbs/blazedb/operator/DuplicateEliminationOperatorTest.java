package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.DummyOperator;
import ed.inf.adbs.blazedb.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DuplicateEliminationOperatorTest {

    private DummyOperator dummyOperator;
    private DuplicateEliminationOperator duplicateEliminationOperator;

    @Before
    public void setUp() {
        String[] columnNames = {"A", "B", "C"};
        List<Tuple> tuples = Arrays.asList(
                new Tuple("1, 2, 3", "", columnNames),
                new Tuple("1, 2, 3", "", columnNames),
                new Tuple("4, 5, 6", "", columnNames),
                new Tuple("4, 5, 6", "", columnNames),
                new Tuple("7, 8, 9", "", columnNames)
        );
        dummyOperator = new DummyOperator(tuples);
        duplicateEliminationOperator = new DuplicateEliminationOperator(dummyOperator);
    }

    @Test
    public void testGetNextTuple() throws Exception {

        assertEquals("1, 2, 3", duplicateEliminationOperator.getNextTuple().toString());
        assertEquals("4, 5, 6", duplicateEliminationOperator.getNextTuple().toString());
        assertEquals("7, 8, 9", duplicateEliminationOperator.getNextTuple().toString());
        assertNull(duplicateEliminationOperator.getNextTuple());
    }
}