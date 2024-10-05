import java.io.Closeable;
import java.sql.*;

public class DBHandler implements Closeable {
    private final Connection connection;

    public DBHandler(String url, String user, String password) throws SQLException {
        connection = DriverManager.getConnection(url, user, password);
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public ResultSet getQueryResult(String request) throws SQLException {
        return connection.createStatement().executeQuery(request);
    }
}
