package producer.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class User {
    @JsonProperty("userId")
    public int userId;

    public User(int userId) {
        this.userId = userId;
    }

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
}
