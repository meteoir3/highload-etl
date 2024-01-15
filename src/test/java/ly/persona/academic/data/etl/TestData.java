package ly.persona.academic.data.etl;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.Objects;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode(exclude = "value")
public class TestData implements Comparable<TestData> {
    private String key;
    private int value;

    public static TestData fromCsv(String line) {
        String[] fields = line.split(",");
        String key = fields[0];
        int value = Integer.parseInt(fields[1]);
        return new TestData(key, value);
    }

    @Override
    public int compareTo(TestData other) {
        return Objects.compare(this.key, other.key, String::compareTo);
    }
}
