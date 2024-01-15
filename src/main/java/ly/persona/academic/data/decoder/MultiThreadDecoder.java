package ly.persona.academic.data.decoder;

import ly.persona.academic.data.DataReader;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

//
// TODO: It is proposed to implement a Reader that reads and transforms data using several CPU cores to improve the processing speed.
//
public class MultiThreadDecoder<R, V> extends DataDecoder<R, V> {
    public MultiThreadDecoder(DataReader<R> reader, Function<R, V> decoder) {
        super(reader, decoder);
        thread = new Thread(() -> {
            try {
                while (true) {
                    R record = readRecord();
                    CompletableFuture<V> future = CompletableFuture.supplyAsync(() -> decodeRecord(record));
                    queue.put(future);
                    if (record == null) {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();
    }

    private final ArrayBlockingQueue<CompletableFuture<V>> queue = new ArrayBlockingQueue<>(100);
    private Thread thread;

    @Override
    public V read() {
        try {
            CompletableFuture<V> future = queue.take();
            return future.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        thread.interrupt();
        super.close();
    }
}
