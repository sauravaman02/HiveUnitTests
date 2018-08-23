import org.apache.spark.sql.Row;
import org.finra.hiveqlunit.resources.ResourceFolderResource;
import org.finra.hiveqlunit.resources.TextLiteralResource;
import org.finra.hiveqlunit.rules.SetUpHql;
import org.finra.hiveqlunit.rules.TearDownHql;
import org.finra.hiveqlunit.rules.TestDataLoader;
import org.finra.hiveqlunit.rules.TestHiveServer;
import org.finra.hiveqlunit.script.MultiExpressionScript;
import org.finra.hiveqlunit.script.SingleExpressionScript;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public class SrcTableCreateTest {

    @ClassRule
    public static TestHiveServer hiveServer = new TestHiveServer();

    @Rule
    public static TestDataLoader loader = new TestDataLoader(hiveServer);

    @Rule
    public static SetUpHql prepSrc =
            new SetUpHql(
                    hiveServer,
                    new MultiExpressionScript(
                            new TextLiteralResource("CREATE TABLE IF NOT EXISTS src (columnOne String, columnTwo String)\n"
                                    + "ROW FORMAT DELIMITED\n"
                                    + "FIELDS TERMINATED BY '|' ESCAPED BY '\\\\'\n"
                                    + "NULL DEFINED AS ''")
                    )
            );

    @Rule
    public static TearDownHql cleanSrc =
            new TearDownHql(
                    hiveServer,
                    new SingleExpressionScript(
                            new TextLiteralResource("DROP TABLE IF EXISTS src")
                    )
            );

    @Test
    public void testDelimiter() {
        loader.loadDataIntoTable("src", new ResourceFolderResource("/delimiterTest.txt"));

        Row[] results = hiveServer.getHiveContext().sql("SELECT columnOne from src").collect();

        Assert.assertEquals(1, results.length);
        Assert.assertEquals(results[0].get(0), "Lorem|Ipsum");
    }

}