package personal.ibonny.esstorage.repositories;

import personal.ibonny.esstorage.models.ChunkModel;
import personal.ibonny.esstorage.services.ESService;

import java.util.stream.Stream;


public class FileRepository {
    public boolean storeFile(ESService esService, String bucketName, String prefix, String filename, Stream<ChunkModel> chunkStream) {
        System.out.println("Storing data.");

        return true;
    }
}
