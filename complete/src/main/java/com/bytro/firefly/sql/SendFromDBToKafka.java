package com.bytro.firefly.sql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import com.bytro.firefly.avro.User;
import com.bytro.firefly.avro.UserGameScoreValue;
import com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootApplication
public class SendFromDBToKafka implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(SendFromDBToKafka.class);
	private static final String SELECT_FROM = "SELECT * FROM %s.%s";
	private static final String SELECT_METADATA_FROM = SELECT_FROM + " LIMIT 0;";
	private static final String SELECT_FROM_WHERE_USER_ID_GREATER_OR_EQUAL = SELECT_FROM + " WHERE userID >= ?";
	private static final List<String> columnRegexWhiteList = ImmutableList.of("unit(\\d+)killed");

	@Value("${mysql.stats_table_name}")
	private String statsTableName;
	@Value("${mysql.stats_db_name}")
	private String statsDbName;

	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private Producer<User, UserGameScoreValue> kafkaProducer;
	@Autowired
	private ColumnFilter columnFilter;


	private Map<Integer, SQLColumn> columns;

	public static void main(String args[]) {
		SpringApplication.run(SendFromDBToKafka.class, args);
	}

	private static int getIntFrom(ResultSet resultSet, String columnName) {
		try {
			return resultSet.getInt(columnName);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void run(String... strings) throws Exception {
		log.info("Starting ");

		columns = getColumns(column -> columnFilter.test(column.getName()));
		log.info("Columns: {}", columns);

		jdbcTemplate.query(getSelectFromWhereUserIdGreaterOrEqual(), new Object[]{5000000}, this::processOneUserEntry);
		kafkaProducer.flush();
	}

	private void processOneUserEntry(ResultSet resultSet) {
		int userID = getIntFrom(resultSet, "userID");
		int gameID = getIntFrom(resultSet, "gameID");
		columns.entrySet()
			   .forEach(entry -> {
				   try {
					   String columnName = entry.getValue()
												.getName();
					   int columnValue = getIntFrom(resultSet, columnName);
					   processOneUserColumn(userID, gameID, columnName, columnValue);
				   } catch (Exception e) {
					   e.printStackTrace();
				   }

			   });
	}

	private void processOneUserColumn(Integer userID, Integer gameID, String columnName, Integer columnValue) {
		if (columnValue != 0) {
			kafkaProducer.send(new ProducerRecord<>("dbTopic",
					new User(userID),
					new UserGameScoreValue(userID, gameID, columnName, columnValue)));
		}
	}

	private Map<Integer, SQLColumn> getColumns() {
		return getColumns(entry -> true);
	}

	private Map<Integer, SQLColumn> getColumns(Predicate<SQLColumn> filter) {
		return jdbcTemplate.query(getSelectMetadataQuery(), resultSet -> {
			ResultSetMetaData metaData = resultSet.getMetaData();
			int columnCount = metaData.getColumnCount();
			Map<Integer, SQLColumn> columns = new HashMap<>(columnCount);
			for (int i = 1; i <= columnCount; i++) {
				SQLColumn column = new SQLColumn().setName(metaData.getColumnName(i))
												  .setAutoIncrement(metaData.isAutoIncrement(i))
												  .setType(metaData.getColumnTypeName(i))
												  .setTypeCode(metaData.getColumnType(i));
				if (filter.test(column)) {
					columns.put(i, column);
				}
			}
			return columns;
		});
	}

	private String getSelectMetadataQuery() {
		return String.format(SELECT_METADATA_FROM, statsDbName, statsTableName);
	}

	private String getSelectFromWhereUserIdGreaterOrEqual() {
		return String.format(SELECT_FROM_WHERE_USER_ID_GREATER_OR_EQUAL, statsDbName, statsTableName);
	}
}