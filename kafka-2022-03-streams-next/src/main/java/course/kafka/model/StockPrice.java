package course.kafka.model;

import lombok.*;

import java.time.LocalDateTime;
import java.util.Date;

@Data
@NoArgsConstructor
@RequiredArgsConstructor
@AllArgsConstructor
public class StockPrice implements Timestamped{
    private Long id;
    @NonNull
    private String symbol;
    @NonNull
    private String name;
    @NonNull
    private Double price;
    @NonNull
    private long timestamp = System.currentTimeMillis();

}
