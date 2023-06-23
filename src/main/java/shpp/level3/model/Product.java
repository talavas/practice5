package shpp.level3.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.util.Map;

public class Product {
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTypeUid() {
        return typeUid;
    }

    public void setTypeUid(String typeUid) {
        this.typeUid = typeUid;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @NotNull
    @Size(min = 5)
    protected String name;
    @NotNull
    protected String typeUid;
    @NotNull
    @Positive
    protected Double price;


    public Product( String name,
                    String typeUid,
                    String price) {
        this.name = name;
        this.typeUid = typeUid;
        this.price = Double.parseDouble(price);
    }

    public Map<String, Object> toMap() {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.convertValue(this, new TypeReference<>() {});
    }

    @Override
    public String toString() {
        return "Product{" +
                "name='" + name + '\'' +
                ", type='" + typeUid + '\'' +
                ", price=" + price +
                '}';
    }
}
