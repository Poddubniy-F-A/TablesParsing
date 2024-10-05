import java.io.FileWriter;
import java.io.IOException;

public class FileHandler {
    private final FileWriter writer;

    public FileHandler(String pathToFile) throws IOException {
        writer = new FileWriter(pathToFile);
    }

    public void addRowToFile(String[] rowParts) throws IOException {
        writer.append(String.join(", ", rowParts)).append("\n");
    }

    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
