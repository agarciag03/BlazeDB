package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Catalog;
import org.junit.Before;
import org.junit.Test;



public class ProjectOperatorTest {

    @Before
    public void setUp() throws Exception {
        Catalog catalog = Catalog.getInstance();
        catalog.loadSchema("samples/db");
    }

    @Test
    public void testProjectTuple() throws Exception {


    }
}