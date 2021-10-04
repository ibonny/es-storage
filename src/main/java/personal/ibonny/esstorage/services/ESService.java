package personal.ibonny.esstorage.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import personal.ibonny.esstorage.models.ChunkModel;
import personal.ibonny.esstorage.models.FileModel;
import personal.ibonny.esstorage.repositories.FileRepository;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ESService {
    public static final String FILE_INDEX_NAME = "file_index";
    public static final String CHUNK_INDEX_NAME = "chunk_index";

    private RestHighLevelClient client;

    private FileRepository fileRepository = new FileRepository();

    ObjectMapper mapper = new ObjectMapper();

    public ESService() {
        client = new RestHighLevelClient(
            RestClient.builder(
                new HttpHost("192.168.1.224", 9200, "http")
            )
        );

        createIndex(FILE_INDEX_NAME);

        createIndex(CHUNK_INDEX_NAME);
    }

    public ESService(String host, int port) {
        client = new RestHighLevelClient(
            RestClient.builder(
                new HttpHost(host, port, "http")
            )
        );

        createIndex(FILE_INDEX_NAME);

        createIndex(CHUNK_INDEX_NAME);
    }

    public void closeClient() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean createIndex(String indexName) {
        GetIndexRequest request = new GetIndexRequest(indexName);

        boolean exists;

        try {
            exists = client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();

            return false;
        }

        if (!exists) {
            CreateIndexRequest cur = new CreateIndexRequest(indexName);

            cur.settings(
                Settings.builder().put("index.number_of_shards", 3)
            );

            CreateIndexResponse cir;

            try {
                cir = client.indices().create(cur, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();

                return false;
            }

            if (!cir.isAcknowledged()) {
                System.out.println("ERROR CREATING INDEX " + indexName + "!!!!");

                return false;
            }

            System.out.println("Created Index " + indexName + ".");
        }

        return true;
    }

    public void searchForItems() {
        SearchRequest sr = new SearchRequest("user_index");

        SearchSourceBuilder ssb = new SearchSourceBuilder();

        ssb.query(QueryBuilders.matchAllQuery());

        sr.source(ssb);

        SearchResponse sRes = null;
        try {
            sRes = client.search(sr, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();

            return;
        }

        System.out.println("Response is: " + sRes);
    }

    public List<String> searchForChunks() {
        SearchRequest sr = new SearchRequest(CHUNK_INDEX_NAME);

        SearchSourceBuilder ssb = new SearchSourceBuilder();

        ssb.query(QueryBuilders.matchAllQuery());

        sr.source(ssb);

        SearchResponse sRes;

        try {
            sRes = client.search(sr, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();

            return new ArrayList<>();
        }

        return Arrays.stream(sRes.getHits().getHits())
            .map(SearchHit::getId)
            .collect(Collectors.toList());
    }

    public List<String> searchForFiles() {
        SearchRequest sr = new SearchRequest(FILE_INDEX_NAME);

        SearchSourceBuilder ssb = new SearchSourceBuilder();

        ssb.query(QueryBuilders.matchAllQuery());

        sr.source(ssb);

        SearchResponse sRes;

        try {
            sRes = client.search(sr, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();

            return new ArrayList<>();
        }

        return Arrays.stream(sRes.getHits().getHits())
                .map(SearchHit::getId)
                .collect(Collectors.toList());
    }

    private String getFileModelIndexId(String filename) {
        SearchRequest sr = new SearchRequest(FILE_INDEX_NAME);

        SearchSourceBuilder ssb = new SearchSourceBuilder();

        ssb.query(QueryBuilders.termQuery("filename", filename));

        sr.source(ssb);

        SearchResponse sRes;

        try {
            sRes = client.search(sr, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();

            return null;
        }

        if (sRes.getHits().getTotalHits().value == 0) {
            return null;
        }

        return sRes.getHits().getHits()[0].getId();
    }

    public List<FileModel> searchForFile(String filename) {
        SearchRequest sr = new SearchRequest(FILE_INDEX_NAME);

        SearchSourceBuilder ssb = new SearchSourceBuilder();

        ssb.query(QueryBuilders.termQuery("filename", filename));

        sr.source(ssb);

        SearchResponse sRes;

        try {
            sRes = client.search(sr, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();

            return new ArrayList<>();
        }

        return Arrays.stream(sRes.getHits().getHits())
            .map(x -> {
                try {
                    return mapper.readValue(x.getSourceAsString(), FileModel.class);
                } catch (JsonProcessingException e) {
                    System.out.println("Error processing record: " + e.getMessage());

                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    public IndexResponse storeChunk(ChunkModel chunk) throws IOException {
        IndexRequest request = new IndexRequest(CHUNK_INDEX_NAME);

        request.id(UUID.randomUUID().toString());

        request.source(mapper.writeValueAsString(chunk), XContentType.JSON);

        return client.index(request, RequestOptions.DEFAULT);
    }

    private IndexResponse writeFileIndex(String filename, long size, int chunkSize, List<String> chunks) throws IOException {
        FileModel fileModel = new FileModel(filename, size, chunkSize, chunks);

        IndexRequest request = new IndexRequest(FILE_INDEX_NAME);

        request.id(UUID.randomUUID().toString());

        request.source(mapper.writeValueAsString(fileModel), XContentType.JSON);

        return client.index(request, RequestOptions.DEFAULT);
    }

    public boolean storeFile(String filename, String destination, int bufferSize) {
        byte[] buffer = new byte[bufferSize];

        int numBytes;

        List<FileModel> fm = searchForFile(destination);

        if (fm.size() > 0) {
            System.out.println("\n   File already exists: " + destination + "\n");

            return false;
        }

        List<String> chunkIndexes = new ArrayList<>();

        long size = 0;

        File inputFile = new File(filename);

        if (!inputFile.exists()) {
            System.out.println("\n   Error, cannot find file: " + inputFile.getAbsolutePath() + "\n");
        }

        try (FileInputStream fis = new FileInputStream(inputFile)) {
            while ((numBytes = fis.read(buffer, 0, buffer.length)) != -1) {
                size += numBytes;

                ChunkModel chunk = new ChunkModel(buffer, numBytes);

                IndexResponse indexResponse = storeChunk(chunk);

                chunkIndexes.add(indexResponse.getId());
            }
        } catch (IOException e) {
            e.printStackTrace();

            return false;
        }

        try {
            IndexResponse wfir = writeFileIndex(destination, size, bufferSize, chunkIndexes);

            System.out.println(String.format("File %s written out with index %s, filesize of %d, and %d chunk entries.",
                filename,
                wfir.getId(),
                size,
                chunkIndexes.size()
            ));
        } catch (IOException e) {
            System.out.println("Error writing out file " + filename + ": " + e.getMessage());

            return false;
        }

        return true;
    }

    public void deleteAll() {
        List<String> ids = searchForChunks();

        if (ids.size() == 0) {
            System.out.println("No chunk entries to delete.");
        } else {
            BulkRequest br = new BulkRequest();

            for (String id : ids) {
                br.add(new DeleteRequest("chunk_index", id));
            }

            BulkResponse response;

            try {
                response = client.bulk(br, RequestOptions.DEFAULT);
            } catch (IOException e) {
                System.out.println("ERROR:  " + e.getMessage());

                return;
            }

            System.out.println(response.getItems().length + " chunk entries deleted.");
        }

        ids = searchForFiles();

        if (ids.size() == 0) {
            System.out.println("No entries to delete.");
        } else {
            BulkRequest br = new BulkRequest();

            for (String id : ids) {
                br.add(new DeleteRequest("file_index", id));
            }

            BulkResponse response;

            try {
                response = client.bulk(br, RequestOptions.DEFAULT);
            } catch (IOException e) {
                System.out.println("ERROR:  " + e.getMessage());

                return;
            }

            System.out.println(response.getItems().length + " file entries deleted.");
        }
    }

    public void delete(String filename) {
        List<FileModel> fm = searchForFile(filename);

        if (fm.size() == 0) {
            System.out.println("\n   Cannot find file: " + filename + "\n");

            return;
        }

        BulkRequest br = new BulkRequest();

        for (String id : fm.get(0).getChunkList()) {
            br.add(new DeleteRequest("chunk_index", id));
        }

        BulkResponse response;

        try {
            response = client.bulk(br, RequestOptions.DEFAULT);
        } catch (IOException e) {
            System.out.println("ERROR:  " + e.getMessage());

            return;
        }

        System.out.println(response.getItems().length + " chunk entries deleted.");

        String fileId = getFileModelIndexId(filename);

        DeleteRequest dr = new DeleteRequest(FILE_INDEX_NAME, fileId);

        try {
            client.delete(dr, RequestOptions.DEFAULT);
        } catch (IOException e) {
            System.out.println("ERROR:  " + e.getMessage());

            return;
        }

        System.out.println("File entry deleted for " + filename + ".");
    }

    public List<FileModel> getAllFiles() {
        SearchRequest sr = new SearchRequest();

        sr.indices(FILE_INDEX_NAME);

        SearchSourceBuilder ssb = new SearchSourceBuilder();

        ssb.query(QueryBuilders.matchAllQuery());

        sr.source(ssb);

        SearchResponse searchResponse;

        try {
            searchResponse = client.search(sr, RequestOptions.DEFAULT);
        } catch (IOException e) {
            System.out.println("Error getting list of files: " + e.getMessage());

            return new ArrayList<>();
        }

        return Arrays.stream(searchResponse.getHits().getHits())
            .map(x -> {
                FileModel fm;

                try {
                    fm = mapper.readValue(x.getSourceAsString(), FileModel.class);
                } catch (JsonProcessingException e) {
                    System.out.println("Error reading record: " + e.getMessage());

                    return new FileModel();
                }

                return fm;
            })
            .collect(Collectors.toList());
    }

    private ChunkModel getChunk(String chunkId) {
        SearchRequest sr = new SearchRequest();

        sr.indices(CHUNK_INDEX_NAME);

        SearchSourceBuilder ssb = new SearchSourceBuilder();

        ssb.query(QueryBuilders.termQuery("_id", chunkId));

        sr.source(ssb);

        SearchResponse searchResponse;

        try {
            searchResponse = client.search(sr, RequestOptions.DEFAULT);
        } catch (IOException e) {
            System.out.println("Error getting chunk: " + e.getMessage());

            return null;
        }

        if (searchResponse.getHits().getTotalHits().value == 0) {
            System.out.println("Chunk not found: " + chunkId);

            return null;
        }

        try {
            return mapper.readValue(searchResponse.getHits().getHits()[0].getSourceAsString(), ChunkModel.class);
        } catch (JsonProcessingException e) {
            System.out.println("Error converting chunk: " + e.getMessage());

            return null;
        }
    }

    public ByteArrayOutputStream getFile(String filename) {
        SearchRequest sr = new SearchRequest();

        sr.indices(FILE_INDEX_NAME);

        SearchSourceBuilder ssb = new SearchSourceBuilder();

        ssb.query(QueryBuilders.termQuery("filename", filename));

        sr.source(ssb);

        SearchResponse searchResponse;

        try {
            searchResponse = client.search(sr, RequestOptions.DEFAULT);
        } catch (IOException e) {
            System.out.println("Error getting list of files: " + e.getMessage());

            return new ByteArrayOutputStream();
        }

        if (searchResponse.getHits().getTotalHits().value == 0) {
            System.out.println("File not found.");

            return new ByteArrayOutputStream();
        }

        FileModel fm;

        try {
            fm = mapper.readValue(searchResponse.getHits().getHits()[0].getSourceAsString(), FileModel.class);
        } catch (JsonProcessingException e) {
            System.out.println("Error reading file record: " + e.getMessage());

            return new ByteArrayOutputStream();
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        for (String chunk: fm.getChunkList()) {
            ChunkModel cm = getChunk(chunk);

            if (cm == null) {
                return new ByteArrayOutputStream();
            }

            baos.write(cm.getData(), 0, cm.getLength());
        }

        return baos;
    }

    public ByteArrayOutputStream getFile(File filename) {
        SearchRequest sr = new SearchRequest();

        sr.indices(FILE_INDEX_NAME);

        SearchSourceBuilder ssb = new SearchSourceBuilder();

        ssb.query(QueryBuilders.termQuery("filename", filename.getAbsoluteFile()));

        sr.source(ssb);

        SearchResponse searchResponse;

        try {
            searchResponse = client.search(sr, RequestOptions.DEFAULT);
        } catch (IOException e) {
            System.out.println("Error getting list of files: " + e.getMessage());

            return new ByteArrayOutputStream();
        }

        if (searchResponse.getHits().getTotalHits().value == 0) {
            System.out.println("File not found.");

            return new ByteArrayOutputStream();
        }

        FileModel fm;

        try {
            fm = mapper.readValue(searchResponse.getHits().getHits()[0].getSourceAsString(), FileModel.class);
        } catch (JsonProcessingException e) {
            System.out.println("Error reading file record: " + e.getMessage());

            return new ByteArrayOutputStream();
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        for (String chunk: fm.getChunkList()) {
            ChunkModel cm = getChunk(chunk);

            if (cm == null) {
                return new ByteArrayOutputStream();
            }

            baos.write(cm.getData(), 0, cm.getLength());
        }

        return baos;
    }
}
