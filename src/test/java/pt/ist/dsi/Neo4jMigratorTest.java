package pt.ist.dsi;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import pt.ist.fenixframework.DomainModelParser;
import pt.ist.fenixframework.dml.DomainModel;

public class Neo4jMigratorTest {

    private static Properties props = new Properties();

    @BeforeClass
    public static void loadProperties() throws IOException {
        props.load(Neo4jMigratorTest.class.getResourceAsStream("/migrator.properties"));
        System.out.println(props);
    }

    @Test
    public void testIt() throws MalformedURLException, SQLException {

        File file = new File("src/test/resources");
        File[] dmls = file.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(".dml");
            }
        });

        List<URL> urls = new ArrayList<>();

        for (File f : dmls) {
            urls.add(f.toURI().toURL());
        }

        Collections.sort(urls, new Comparator<URL>() {
            @Override
            public int compare(URL o1, URL o2) {
                return o1.toExternalForm().compareTo(o2.toExternalForm());
            }
        });

        DomainModel model = DomainModelParser.getDomainModel(urls);

        Neo4jMigrator migrator = new Neo4jMigrator(model, getConnection(), BatchInserters.inserter(props.getProperty("dbPath")));
        migrator.migrate();
    }

    private static Connection getConnection() throws SQLException {
        final String url = "jdbc:mysql:" + props.getProperty("dbAlias");
        final Connection connection =
                DriverManager.getConnection(url, props.getProperty("dbUsername"), props.getProperty("dbPassword"));
        connection.setAutoCommit(false);
        return connection;
    }
}
