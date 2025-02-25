package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Catalog;
import ed.inf.adbs.blazedb.Tuple;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ScanOperatorTest {

    @Before
    public void setUp() throws Exception {
        Catalog catalog = Catalog.getInstance(); // because of singleton pattern
        catalog.loadSchema("samples/db");
    }

    @Test
    public void testGetNextTupleWithStudentTable() throws Exception {

        ScanOperator scanOperator = new ScanOperator("Student");

        Tuple tuple1 = scanOperator.getNextTuple();
        assertNotNull(tuple1);
        assertEquals("1, 200, 50, 33", tuple1.toString());

        Tuple tuple2 = scanOperator.getNextTuple();
        assertNotNull(tuple2);
        assertEquals("2, 200, 200, 44", tuple2.toString());

        Tuple tuple3 = scanOperator.getNextTuple();
        assertNotNull(tuple3);
        assertEquals("3, 100, 105, 44", tuple3.toString());

        Tuple tuple4 = scanOperator.getNextTuple();
        assertNotNull(tuple4);
        assertEquals("4, 100, 50, 11", tuple4.toString());

        Tuple tuple5 = scanOperator.getNextTuple();
        assertNotNull(tuple5);
        assertEquals("5, 100, 500, 22", tuple5.toString());

        Tuple tuple6 = scanOperator.getNextTuple();
        assertNotNull(tuple6);
        assertEquals("6, 300, 400, 11", tuple6.toString());

        Tuple tuple7 = scanOperator.getNextTuple();
        assertNull(tuple7);
    }

    @Test
    public void testReset() throws Exception {

        ScanOperator scanOperator = new ScanOperator("Student");

        // first tuple
        Tuple tuple1 = scanOperator.getNextTuple();
        assertNotNull(tuple1);
        assertEquals("1, 200, 50, 33", tuple1.toString());

        Tuple tuple2 = scanOperator.getNextTuple();
        assertNotNull(tuple2);
        assertEquals("2, 200, 200, 44", tuple2.toString());

        // reset the operator
        scanOperator.reset();

        // read the first tuple again
        Tuple tupleAfterReset = scanOperator.getNextTuple();
        assertNotNull(tupleAfterReset);
        assertEquals("1, 200, 50, 33", tupleAfterReset.toString());
    }

    @Test
    public void getNextTupleWithNullTable() throws Exception {

        ScanOperator scanOperator = new ScanOperator("Example");

        Tuple tuple = scanOperator.getNextTuple();
        assertNull(tuple);
    }

    @Test
    public void getNextTupleWithNonExistentTable() throws Exception {

        Exception exception = assertThrows(Exception.class, () -> {
            new ScanOperator("NonExistentTable");
        });

        String expectedMessage = "Table file not found";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage));
    }

}
