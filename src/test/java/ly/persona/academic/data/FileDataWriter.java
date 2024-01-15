package ly.persona.academic.data;

import ly.persona.academic.data.etl.TestData;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileDataWriter<T> implements DataWriter<T> {

    private final BufferedWriter writer;

    public FileDataWriter(Path filePath) throws IOException {
        writer = Files.newBufferedWriter(filePath);
    }

    @Override
    public void write(T data) {
        try {
            var row = (TestData) data;
            writer.write(row.getKey() + "," + row.getValue());
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
