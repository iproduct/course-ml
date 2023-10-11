package course.kafka.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class JdbcUtils {
    public static Connection createDbConnection(Properties props) throws IOException, ClassNotFoundException, SQLException {
        return DriverManager.getConnection(props.getProperty("url"), props);
    }

    public static  void closeConnection(Connection connection) throws SQLException {
        if(connection != null && !connection.isClosed()){
            connection.close();
        }
    }
}
