package personal.ibonny.esstorage.models;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;
import java.util.UUID;

@Data
@NoArgsConstructor
@ToString
public class FileModel {
    private String id;

    private String bucket;
    private String prefix;
    private String filename;
    private int chunkSize;
    private long filesize;

    private List<String> chunkList;

    public FileModel(String filename, long filesize, int chunkSize, List<String> cl) {
        this.id = UUID.randomUUID().toString();

        this.filename = filename;
        this.filesize = filesize;
        this.chunkSize = chunkSize;

        this.chunkList = cl;
    }
}
