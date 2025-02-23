package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Catalog;
import ed.inf.adbs.blazedb.Tuple;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;

public class ScanOperatorTest {

    @Test
    public void testGetNextTupleWithStudentTable() throws Exception {
        Catalog catalog = Catalog.getInstance(); // because of singleton pattern
        catalog.loadSchema("samples/db");
        ScanOperator scanOperator = new ScanOperator("Student");

        Tuple tuple1 = scanOperator.getNextTuple();
        assertNotNull(tuple1);
        assertEquals("1, 200, 50, 33", tuple1.toString());

        Tuple tuple2 = scanOperator.getNextTuple();
        assertNotNull(tuple2);
        assertEquals("2, 200, 200, 44", tuple2.toString());

        Tuple tuple3 = scanOperator.getNextTuple();
        assertNotNull(tuple3);
        assertEquals("4, 100, 50, 11", tuple3.toString());

//        Tuple tuple4 = scanOperator.getNextTuple();
//        assertNull(tuple4);
    }
}
