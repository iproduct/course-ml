package course.kafka.dao;

import course.kafka.model.StockPrice;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
public class PricesDAO {
    public static final String DB_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    public static final String DB_URL =
            "jdbc:sqlserver://localhost:1433;databaseName=kafka_demo;user=sa;password=sa12X+;";
    public static final String DB_USER = "sa";
    public static final String DB_PASSWORD = "sa12X+";
    public static final String SELECT_ALL_PRICES_SQL =
            "SELECT * FROM [Prices]";
    public static final String INSERT_INTO_PRICES_SQL =
            "INSERT INTO [Prices] ([symbol], [name], [price]) VALUES (?, ?, ?)";
    public static final String SELECT_ALL_OFFSETS_SQL =
            "SELECT * FROM [Offsets]";
    public static final String SELECT_OFFSETS_BY_CONSUMER_SQL =
            "SELECT * FROM [Offsets] WHERE [consumer]=?";
    public static final String SELECT_OFFSETS_COUNT_BY_CONSUMER_TOPIC_PARTITION_SQL =
            "SELECT * FROM [Offsets] WHERE [consumer]=? AND [topic]=? AND [partition]=?";
    public static final String INSERT_OFFSET_SQL =
            "INSERT INTO [Offsets] ([consumer], [topic], [partition], [offset]) VALUES (?, ?, ?, ?)";
    public static final String UPDATE_OFFSET_SQL =
            "UPDATE [Offsets] SET [offset]=? WHERE [consumer]=? AND [topic]=? AND [partition]=?";
    private Connection con;
    private PreparedStatement selectAllStatement;
    private PreparedStatement insertIntoStatement;
    private PreparedStatement selectAllOffsetsStatement;
    private PreparedStatement selectOffsetsByConsumerStatement;
    private PreparedStatement selectOffsetsCountByConsumerTopicPartititonStatement;
    private PreparedStatement insertOffsetStatement;
    private PreparedStatement updateOffsetStatement;

    List<StockPrice> prices = new CopyOnWriteArrayList<>();

    public void init() throws SQLException {
        try {
            Class.forName(DB_DRIVER);
        } catch (ClassNotFoundException ex) {
            log.error("MS SQLServer db driver not found.", ex);
        }
        try {
            con = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            con.setAutoCommit(false);
            selectAllStatement = con.prepareStatement(SELECT_ALL_PRICES_SQL);
            insertIntoStatement = con.prepareStatement(INSERT_INTO_PRICES_SQL);
            selectAllOffsetsStatement = con.prepareStatement(SELECT_ALL_OFFSETS_SQL);
            selectOffsetsByConsumerStatement = con.prepareStatement(SELECT_OFFSETS_BY_CONSUMER_SQL);
            selectOffsetsCountByConsumerTopicPartititonStatement = con.prepareStatement(SELECT_OFFSETS_COUNT_BY_CONSUMER_TOPIC_PARTITION_SQL);
            insertOffsetStatement = con.prepareStatement(INSERT_OFFSET_SQL);
            updateOffsetStatement = con.prepareStatement(UPDATE_OFFSET_SQL);
            log.info("SQL Server connection initialized successfully");
        } catch (SQLException e) {
            log.error("Connection to MS SQLServer URL:{} can not be established.\n{}", DB_URL, e);
            throw e;
        }
    }

    public void commitTransaction() throws SQLException {
        con.commit();
    }

    public void rollbackTransaction() throws SQLException {
        con.rollback();
    }

    public void close(){
        try {
            if (!insertIntoStatement.isClosed()) {
                insertIntoStatement.close();;
            }
            if (!selectAllStatement.isClosed()) {
                selectAllStatement.close();;
            }
            if (!con.isClosed()) {
                con.close();
            }
        } catch (SQLException e) {
            log.error("Error closing connection to SQL Server URL:{}.\n{}", DB_URL, e);
        }
    }

    public void reload() throws SQLException {
        try {
            ResultSet rs = selectAllStatement.executeQuery();
            while(rs.next()) {
                prices.add(new StockPrice(
                        rs.getInt("id"),
                        rs.getString("symbol"),
                        rs.getString("name"),
                        rs.getDouble("price"),
                        rs.getTimestamp("timestamp")
                ));
            }
        } catch (SQLException e) {
            log.error("Error executing SQL statement.", e);
            throw e;
        }
    }

    public int insertPrice(StockPrice price) throws SQLException {
        insertIntoStatement.setString(1, price.getSymbol());
        insertIntoStatement.setString(2, price.getName());
        insertIntoStatement.setDouble(3,price.getPrice());
        int result = insertIntoStatement.executeUpdate();
        log.debug("Successfully inserted StockPrice:{} - {} inserts", price, result);
        return result;

    }

    public int updateOffsets(String consumerGroupId,
                            Map<TopicPartition, OffsetAndMetadata> currentOffsets) throws SQLException {
        int counter = 0;
        for(TopicPartition tp: currentOffsets.keySet()) {
            selectOffsetsCountByConsumerTopicPartititonStatement.setString(1, consumerGroupId);
            selectOffsetsCountByConsumerTopicPartititonStatement.setString(2, tp.topic());
            selectOffsetsCountByConsumerTopicPartititonStatement.setInt(3, tp.partition());
            ResultSet rs = selectOffsetsCountByConsumerTopicPartititonStatement.executeQuery();
            if (rs.next() ) {
                updateOffsetStatement.setLong(1, currentOffsets.get(tp).offset());
                updateOffsetStatement.setString(2, consumerGroupId);
                updateOffsetStatement.setString(3, tp.topic());
                updateOffsetStatement.setInt(4, tp.partition());
                counter += updateOffsetStatement.executeUpdate();
                log.debug("Successfully updated offset:{} for {}",
                        currentOffsets.get(tp).offset(), tp);
            } else {
                insertOffsetStatement.setString(1, consumerGroupId);
                insertOffsetStatement.setString(2, tp.topic());
                insertOffsetStatement.setInt(3, tp.partition());
                insertOffsetStatement.setLong(4, currentOffsets.get(tp).offset());
                counter += insertOffsetStatement.executeUpdate();
                log.debug("Successfully inserted offset:{} for {}",
                        currentOffsets.get(tp).offset(), tp);
            }
        }
        return counter;
    }

    public Map<TopicPartition, OffsetAndMetadata> getOffsetsByConsumerGroupId(String consumerGroupId) throws SQLException {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        try {
            selectOffsetsByConsumerStatement.setString(1, consumerGroupId);
            ResultSet rs = selectOffsetsByConsumerStatement.executeQuery();
            while(rs.next()) {
                offsets.put(
                    new TopicPartition(rs.getString("topic"), rs.getInt("partition")),
                    new OffsetAndMetadata(rs.getLong("offset"))
                );
            }
            return offsets;
        } catch (SQLException e) {
            log.error("Error executing SQL statement.", e);
            throw e;
        }
    }

    public void printData(){
        prices.forEach(price -> {
            System.out.printf(
                "| %10d | %5.5s | %20.20s | %10.2f | %td.%<tm.%<ty %<tH:%<tM:%<tS |\n",
                price.getId(), price.getSymbol(), price.getName(), price.getPrice(),
                    price.getTimestamp());
        });
    }

    public static void main(String[] args) {
        PricesDAO dao = new PricesDAO();
        List<StockPrice> stocks = Arrays.asList(
                new StockPrice("VMW", "VMWare", 215.35),
                new StockPrice("GOOG", "Google", 309.17),
                new StockPrice("CTXS", "Citrix Systems, Inc.", 112.11),
                new StockPrice("DELL", "Dell Inc.", 92.93),
                new StockPrice("MSFT", "Microsoft", 255.19),
                new StockPrice("ORCL", "Oracle", 115.72),
                new StockPrice("RHT", "Red Hat", 111.27)
        );
        try {
            dao.init();
            for(StockPrice sp : stocks) {
                dao.insertPrice(sp);
            }
            dao.reload();
            dao.printData();
        } catch (SQLException e){
            log.error("DB Error:", e);
        } finally {
            dao.close();
        }
    }
}
