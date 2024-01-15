package ly.persona.academic.data.etl;

import lombok.SneakyThrows;
import ly.persona.academic.data.DataReader;
import ly.persona.academic.data.DataWriter;
import ly.persona.academic.data.FileDataWriter;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.stream.Stream;

import static ly.persona.academic.data.etl.SmallDataEtlProcessList.doMap;

// TODO: The task is to process the data like SmallDataEtlProcessList (or SmallDataEtlProcessStream) is processing
// TODO: but keep in the memory only restricted number of objects, for example, not more than 100000 elements,
//
// TODO: It is proposed to implement the data processing using a disk as a storage with linear data writing/reading.
// TODO: An external sorting on files is required for grouping and sorting operation.
public class BigDataEtlProcess implements EtlProcess<TestData>, TestDataEtlOperations {
    private static final int BATCH_SIZE = 100000;

    private static final String FILE_PREFIX = "batch_";

    @SneakyThrows
    @Override
    public void process(DataReader<TestData> reader, DataWriter<TestData> writer) {
        Path tempDir = Files.createTempDirectory("etl");
        List<Path> files = processBatches(reader, tempDir);
        mergeFiles(files, writer);
        cleanupDirectory(tempDir);
    }

    private List<Path> processBatches(DataReader<TestData> reader, Path tempDir) throws IOException {
        List<Path> files = new ArrayList<>();
        List<TestData> batch = new ArrayList<>(BATCH_SIZE);
        int count = 0;
        for (TestData row = reader.read(); row != null; row = reader.read()) {
            batch.add(row);
            count++;
            if (count == BATCH_SIZE) {
                Path file = processBatch(batch, tempDir, files.size());
                files.add(file);
                batch.clear();
                count = 0;
            }
        }
        if (count > 0) {
            Path file = processBatch(batch, tempDir, files.size());
            files.add(file);
        }
        return files;
    }

    private Path processBatch(List<TestData> batch, Path tempDir, int index) throws IOException {
        batch = doMap(batch);
        batch = doReduce(batch);
        String fileName = FILE_PREFIX + index;
        Path filePath = tempDir.resolve(fileName);
        try (DataWriter<TestData> fileWriter = new FileDataWriter<>(filePath)) {
            batch.forEach(fileWriter::write);
        }
        return filePath;
    }

    private List<TestData> doReduce(List<TestData> rows) {
        final Map<TestData, TestData> reduceMap = new TreeMap<>();
        rows.forEach(
                row -> reduceMap.merge(row, row, REDUCE_FUNCTION)
        );
        return new ArrayList<>(reduceMap.keySet());
    }

    private void mergeFiles(List<Path> files, DataWriter<TestData> writer) throws IOException {
        List<BufferedReader> readers = new ArrayList<>();
        for (Path file : files) {
            BufferedReader reader = Files.newBufferedReader(file);
            readers.add(reader);
        }
        // Keep offsets for each first element of file in priority queue
        PriorityQueue<TestData> queue = new PriorityQueue<>(COMPARATOR);
        for (BufferedReader reader : readers) {
            TestData data = readData(reader, readers);
            if (data != null) {
                queue.add(data);
            }
        }
        // Merge data
        while (!queue.isEmpty()) {
            TestData data = queue.poll();
            if (FILTER_FUNCTION.test(data)) {
                writer.write(data);
            }
            BufferedReader reader = readers.get(data.getValue());
            TestData next = readData(reader, readers);
            if (next != null) {
                queue.add(next);
            }
        }
        for (BufferedReader reader : readers) {
            reader.close();
        }
    }

    private TestData readData(BufferedReader reader, List<BufferedReader> readers) throws IOException {
        String line = reader.readLine();
        if (line == null) {
            return null;
        }
        TestData data = TestData.fromCsv(line);
        data.setValue(readers.indexOf(reader));
        return data;
    }

    private void cleanupDirectory(Path tempDir) throws IOException {
        try (Stream<Path> stream = Files.list(tempDir)) {
            stream.forEach(file -> {
                try {
                    Files.delete(file);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        Files.delete(tempDir);
    }
}
