package personal.ibonny.esstorage.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.UUID;

@Data
@NoArgsConstructor
@ToString
public class ChunkModel {
    private String id;

    private byte[] data;
    private int length;

    public ChunkModel(byte[] data, int length) {
        this.id = UUID.randomUUID().toString();

        this.data = data;
        this.length = length;
    }
}
