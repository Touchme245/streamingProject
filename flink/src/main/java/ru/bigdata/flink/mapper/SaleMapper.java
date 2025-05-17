package ru.bigdata.flink.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.functions.MapFunction;
import org.springframework.stereotype.Service;
import ru.bigdata.flink.dto.SaleDto;

import java.io.IOException;

@Service
public class SaleMapper implements MapFunction<String, SaleDto> {
    private final static ObjectMapper mapper = new ObjectMapper();

//    public SaleMapper() {
//        this.mapper = new ObjectMapper();
//        this.mapper.registerModule(new JavaTimeModule());
//    }

    public SaleDto map(String json) throws IOException {
        return mapper.readValue(json, SaleDto.class);
    }
}
