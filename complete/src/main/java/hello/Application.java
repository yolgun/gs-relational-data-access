package hello;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SpringBootApplication
public class Application implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String args[]) {
        SpringApplication.run(Application.class, args);
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Override
    public void run(String... strings) throws Exception {

        log.info("Creating tables");

        jdbcTemplate.execute("DROP TABLE IF EXISTS customers");
        jdbcTemplate.execute("CREATE TABLE customers(" +
                "id SERIAL, first_name VARCHAR(255), last_name VARCHAR(255))");

        List<SQLColumn> columns = getColumns();
        System.out.println(columns);


        IntStream.range(0, 100000).parallel().forEach( i -> {
            // Split up the array of whole names into an array of first/last names
            List<Object[]> splitUpNames = Arrays.asList("John Woo", "Jeff Dean", "Josh Bloch", "Josh Long").stream()
                    .map(name -> name.split(" "))
                    .collect(Collectors.toList());

            // Use a Java 8 stream to print out each tuple of the list
            splitUpNames.forEach(name -> log.info(String.format("Inserting customer record for %s %s", name[0], name[1])));

            // Uses JdbcTemplate's batchUpdate operation to bulk load data
            jdbcTemplate.batchUpdate("INSERT INTO customers(first_name, last_name) VALUES (?,?)", splitUpNames);

            log.info("Querying for customer records where first_name = 'Josh':");
            jdbcTemplate.query(
                    "SELECT id, first_name, last_name FROM customers WHERE first_name = ?", new Object[]{"Josh"},
                    (rs, rowNum) -> new Customer(rs.getLong("id"), rs.getString("first_name"), rs.getString("last_name"))
            ).forEach(customer -> log.info(customer.toString()));
        });
    }

    private List<SQLColumn> getColumns() {
        return jdbcTemplate.query("SELECT id, first_name, last_name FROM customers LIMIT 0;", rs -> {
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            List<SQLColumn> columns = new ArrayList<>(columnCount);
            for(int i = 1 ; i <= columnCount ; i++){
                SQLColumn column = new SQLColumn();
                column.setName(rsmd.getColumnName(i));
                column.setAutoIncrement(rsmd.isAutoIncrement(i));
                column.setType(rsmd.getColumnTypeName(i));
                column.setTypeCode(rsmd.getColumnType(i));
                column.setTableName("CUSTOMERS");
                columns.add(column);
            }
            return columns;
        });
    }
}