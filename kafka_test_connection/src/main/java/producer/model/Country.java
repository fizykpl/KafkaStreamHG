package producer.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class Country {
    @JsonProperty("country")
    public String country;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Country user = (Country) o;
        return country == user.country;
    }

    @Override
    public int hashCode() {
        return Objects.hash(country);
    }

    @Override
    public String toString() {
        return "Country{" +
                "country=" + country +
                '}';
    }

//        public static void main(String[] args) throws IOException {
//        String jsonString = "{\"country\": \"PL\"}";
//        ObjectMapper mapper = new ObjectMapper();
//        Country country = mapper.readValue(jsonString, Country.class);
//        System.out.println(country);
//    }

}
