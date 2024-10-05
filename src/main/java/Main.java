import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Main {
    private static final String PATH_TO_OUTPUT = "./output/result.txt",
            URL = "jdbc:h2:~/IdeaProjects/TablesParsing/input/DB", USER = "fedor", PASSWORD = "";

    private static FileHandler fileHandler;
    private static DBHandler dbHandler;

    public static void main(String[] args) throws SQLException, IOException {
        final String tablesWithPrimaryKeysTable = "TABLE_LIST",
                tablesCol = "TABLE_NAME",
                primaryKeysCol = "PK";

        initHandlers();

        ResultSet tablesPK = dbHandler.getQueryResult(makeBaseSelectQuery(
                new String[]{tablesCol, primaryKeysCol},
                tablesWithPrimaryKeysTable));

        while (tablesPK.next()) {
            String tableName = tablesPK.getString(tablesCol);

            for (String primaryKeyName : tablesPK.getString(primaryKeysCol).split(", ")) {
                processPrimaryKey(primaryKeyName, tableName);
            }
        }

        closeHandlers();
    }

    private static void initHandlers() {
        try {
            fileHandler = new FileHandler(PATH_TO_OUTPUT);
        } catch (IOException e) {
            System.err.println("Проверьте корректность пути к выходному файлу");
            throw new RuntimeException(e);
        }

        try {
            dbHandler = new DBHandler(URL, USER, PASSWORD);
        } catch (SQLException e) {
            System.err.println("Проверьте корректность параметров подключения к БД");
            throw new RuntimeException(e);
        }
    }

    private static void closeHandlers() {
        fileHandler.close();
        dbHandler.close();
    }

    private static void processPrimaryKey(String primaryKeyName, String tableName) throws SQLException, IOException {
        final String tablesWithColumnsNamesAndTypesTable = "TABLE_COLS",
                tablesCol = "TABLE_NAME",
                columnsNamesCol = "COLUMN_NAME",
                columnsTypesCol = "COLUMN_TYPE";

        ResultSet columnType = dbHandler.getQueryResult(makeSelectQueryWithCondition(
                new String[]{columnsTypesCol},
                tablesWithColumnsNamesAndTypesTable,
                String.format("LOWER(%s)=LOWER('%s') AND %s='%s'",
                        columnsNamesCol, primaryKeyName, tablesCol, tableName)));

        if (columnType.next()) {
            fileHandler.addRow(new String[]{tableName, primaryKeyName, columnType.getString(columnsTypesCol)});

            if (columnType.next()) {
                System.err.printf("В %s продублирована запись для ключа %s таблицы %s\n",
                        tablesWithColumnsNamesAndTypesTable, primaryKeyName, tableName);
            }
        } else {
            System.err.printf("В %s отсутствует запись для ключа %s таблицы %s\n",
                    tablesWithColumnsNamesAndTypesTable, primaryKeyName, tableName);
        }
    }

    private static String makeBaseSelectQuery(String[] columns, String table) {
        return String.format("SELECT %s FROM %s",
                String.join(", ", columns), table);
    }

    private static String makeSelectQueryWithCondition(String[] columns, String table, String condition) {
        return String.format("%s WHERE %s",
                makeBaseSelectQuery(columns, table), condition);
    }
}
