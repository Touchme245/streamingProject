package ru.bigdata.flink.dto;

import lombok.*;

import java.io.Serializable;
import java.time.LocalDate;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SaleDto implements Serializable {
    private Integer sale_id;
    private Integer product_id;
    private Integer customer_id;
    private Integer seller_id ;
    private Integer quantity;
    private Float total_price;
    private String date;
}
