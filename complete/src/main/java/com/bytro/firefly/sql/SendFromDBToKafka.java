package com.bytro.firefly.sql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import com.bytro.firefly.avro.User;
import com.bytro.firefly.avro.UserGameScoreValue;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jooq.lambda.Unchecked;
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
	private static final String SELECT_DATA_RANGE_FROM = SELECT_FROM + " WHERE userID >= ? AND userID < ?";
	private static final String SELECT_MAX_USER_ID = "SELECT MAX(userID) FROM %s.%s";
	private static final String SELECT_MIN_USER_ID = "SELECT MIN(userID) FROM %s.%s";
	private static final String USER_ID = "userID";
	private static final String GAME_ID = "gameID";

	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private Producer<User, UserGameScoreValue> kafkaProducer;
	@Autowired
	private ColumnFilter columnFilter;

	@Value("${mysql.statsTableName}")
	private String statsTableName;
	@Value("${mysql.statsDbName}")
	private String statsDbName;
	@Value("${mysql.chunkSize}")
	private int chunkSize;
	@Value("${kafka.sinkTopic}")
	private String sinkTopic;

	private Map<Integer, SQLColumn> columns;
	private int minUserID;
	private int maxUserID;

	public static void main(String args[]) {
		SpringApplication.run(SendFromDBToKafka.class, args);
	}

	private static int getIntFrom(ResultSet resultSet, String columnName) {
		return Unchecked.toIntBiFunction((ResultSet rs, String cn) -> rs.getInt(cn))
						.applyAsInt(resultSet, columnName);
	}

	@Override
	public void run(String... strings) throws Exception {
		log.info("Starting ");

		columns = getColumns(column -> columnFilter.test(column.getName()));
		log.info("Columns: {}", columns);

		minUserID = jdbcTemplate.queryForObject(getMinUserIdQuery(), Integer.class);
		log.info("Min userID: {}", minUserID);
		maxUserID = jdbcTemplate.queryForObject(getMaxUserIdQuery(), Integer.class);
		log.info("Max userID: {}", maxUserID);

		sendDataToProducer();
		kafkaProducer.flush();
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

	private void sendDataToProducer() {
		int numChunks = (maxUserID - minUserID + 1) / chunkSize;
		IntStream.range(0, numChunks)
				 .forEach(chunkNo -> {
					 int chunkBeg = minUserID + chunkNo * chunkSize;
					 int chunkEndExclusive = chunkBeg + chunkSize;
					 Object[] chunkRange = new Object[]{chunkBeg, chunkEndExclusive};
					 jdbcTemplate.query(getSelectDataQuery(), chunkRange, this::processOneUserRow);
					 log.info("Finished users from {} to {}", chunkBeg, chunkEndExclusive);
				 });
	}

	private void processOneUserRow(ResultSet resultSet) {
		int userID = getIntFrom(resultSet, USER_ID);
		int gameID = getIntFrom(resultSet, GAME_ID);
		columns.values()
			   .forEach(column -> {
				   try {
					   int columnValue = getIntFrom(resultSet, column.getName());
					   processOneUserColumn(userID, gameID, column.getName(), columnValue);
				   } catch (Exception e) {
					   log.error("Error while processing user row", e);
				   }
			   });
	}

	private void processOneUserColumn(Integer userID, Integer gameID, String columnName, Integer columnValue) {
		if (columnValue != 0) {
			kafkaProducer.send(new ProducerRecord<>(sinkTopic,
					new User(userID),
					new UserGameScoreValue(userID, gameID, columnName, columnValue)), (recordMetadata, e) -> {
				if (e != null) {
					log.error("Error while sending message to kafka: ", e);
				}
			});
		}
	}


	private String getSelectMetadataQuery() {
		return String.format(SELECT_METADATA_FROM, statsDbName, statsTableName);
	}

	private String getMaxUserIdQuery() {
		return String.format(SELECT_MAX_USER_ID, statsDbName, statsTableName);
	}

	private String getMinUserIdQuery() {
		return String.format(SELECT_MIN_USER_ID, statsDbName, statsTableName);
	}

	private String getSelectDataQuery() {
		return String.format(SELECT_DATA_RANGE_FROM, statsDbName, statsTableName);
	}
}