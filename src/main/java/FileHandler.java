import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;

public class FileHandler implements Closeable {
    private final FileWriter writer;

    public FileHandler(String pathToFile) throws IOException {
        writer = new FileWriter(pathToFile);
    }

    @Override
    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void addRow(String[] rowParts) throws IOException {
        writer.append(String.join(", ", rowParts)).append("\n");
    }
}
