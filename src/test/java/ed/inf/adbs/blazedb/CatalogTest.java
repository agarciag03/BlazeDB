package ed.inf.adbs.blazedb;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CatalogTest {

    private Catalog catalog;

    @Before
    public void setUp() throws Exception {
        catalog = Catalog.getInstance();
        catalog.loadSchema("samples/db");
    }


}
