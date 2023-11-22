package course.dml.model;

import lombok.*;

import java.util.Date;

@Data
@NoArgsConstructor
@RequiredArgsConstructor
@AllArgsConstructor
public class StockPrice {
    private Long id;
    @NonNull
    private String symbol;
    @NonNull
    private String name;
    @NonNull
    private Double price;
    @NonNull
    private Date timestamp = new Date();

}
