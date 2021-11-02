package course.kafka.model;

import lombok.*;

@Data
@NoArgsConstructor
@RequiredArgsConstructor
@AllArgsConstructor
public class Customer {
    private int id;
    @NonNull
    private String name;
    @NonNull
    private String eik;
    @NonNull
    private String address;
}
