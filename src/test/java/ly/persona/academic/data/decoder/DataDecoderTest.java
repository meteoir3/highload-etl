package ly.persona.academic.data.decoder;

import ly.persona.academic.data.DataGenerator;
import ly.persona.academic.data.CountData;
import org.junit.Assert;
import org.junit.Test;

public class DataDecoderTest {

    @Test
    public void testSingleThreadDecoder() {
        doTest(SingleThreadDecoder::new);
    }

    @Test
    public void testMultiThreadDecoder() {
        doTest(MultiThreadDecoder::new);
    }

    private void doTest(DataDecoderFactory factory) {
        try(DataDecoder<String, CountData> decoder = factory.createDecoder(new DataGenerator<>(String::valueOf, 2000), CountData::fromCsv)) {
            long time = System.currentTimeMillis();
            int count = 0;
            for(CountData data = decoder.read(); data != null; data = decoder.read()) {
                // check the order
                Assert.assertEquals(count++, data.getCount());
            }

            time  = System.currentTimeMillis() - time;
            System.out.println("Time = " + time + " millis for " + decoder.getClass().getSimpleName());
        }
    }
}
