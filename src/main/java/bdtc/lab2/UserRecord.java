package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
public class UserRecord {

    // user1
    private String senderUser;
    // user2
    private String receiverUser;
    // date
    private LocalDateTime date;
    // text
    private String msg;

}
