package model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Objects;

public class User {
    @JsonProperty("userId")
    public int userId;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        User user = (User) o;
        return userId == user.userId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId);
    }

    @Override
    public String toString() {
        return "User{" +
                "userId=" + userId +
                '}';
    }



//    public static void main(String[] args) throws IOException {
//        String jsonString = "{\"userId\": 1}";
//        ObjectMapper mapper = new ObjectMapper();
//        User user = mapper.readValue(jsonString, User.class);
//        System.out.println(user.);
//    }
}
