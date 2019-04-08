package rest;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import producer.KafkaProducers;

import java.util.concurrent.ExecutionException;


@RestController
public class Controller {


    KafkaProducers kafkaProduce = new KafkaProducers();

    @GetMapping("/")
    public String hello() {
        return "Hello Huuuge Game";
    }

    @PostMapping("/produce/buy")
    public String produceBuy(
            @RequestParam(value="userId") String userid
            , @RequestParam(value="orderId") String orderId
            , @RequestParam(value="amount") int amount)  {

        JsonNode user = kafkaProduce.createUser(userid);
        JsonNode order = kafkaProduce.createOrder(orderId, amount);

        try {
            kafkaProduce.sendToBuys(user,order);
        } catch (ExecutionException|InterruptedException e) {
            e.printStackTrace();
            kafkaProduce.close();
        }

        System.out.println(user + " " + order);
        return user + ":" + order;
    }

    @PostMapping("/produce/login")
    public String produceLogin(
            @RequestParam(value="userId") String userid
            , @RequestParam(value="country") String country)  {

        JsonNode user = kafkaProduce.createUser(userid);
        JsonNode countryJN = kafkaProduce.createCountr(country);
        try {
            kafkaProduce.sendToLogins(user,countryJN);
        } catch (ExecutionException|InterruptedException e) {
            e.printStackTrace();
            kafkaProduce.close();
        }

        return user + ":" + countryJN;
    }
}
