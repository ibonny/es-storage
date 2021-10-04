package personal.ibonny.esstorage.services;

import personal.ibonny.esstorage.models.FileModel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class CallOutFunctions {
    public final Map<String, Method> FUNCTIONS;

    private String sortOrder;
    private int bufferSize;
    private String hostPort;
    private boolean optimalBufferSize;

    ESService esService = new ESService();

    public CallOutFunctions() throws Exception {
        FUNCTIONS = Map.of(
            "cat", CallOutFunctions.class.getMethod("catFunction", List.class),
            "list", CallOutFunctions.class.getMethod("listFunction", List.class),
            "store", CallOutFunctions.class.getMethod("storeFunction", List.class)
        );
    }

    public int catFunction(List<String> parameters) {
        if (parameters.size() == 0) {
            System.out.println("\n   Please provide filename to cat.");

            return 1;
        }

        String inputFileName = parameters.get(0);

        ByteArrayOutputStream baos = esService.getFile(inputFileName);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

        int numBytes;

        byte[] buffer = new byte[1024];

        while ((numBytes = bais.read(buffer, 0, 1024)) != -1) {
            System.out.write(buffer, 0, numBytes);
        }

        return 0;
    }

    public int listFunction(List<String> parameters) {
        List<FileModel> files = esService.getAllFiles();

        if (files.size() == 0) {
            System.out.println("\n   No files found.\n");

            esService.closeClient();

            return 0;
        }

        int maxFileLength = 0;

        for (FileModel fm : files) {
            if (fm.getFilename().length() > maxFileLength) {
                maxFileLength = fm.getFilename().length();
            }
        }

        if (this.sortOrder.equals("asc")) {
            files.sort(Comparator.comparing(FileModel::getFilename));
        }

        if (this.sortOrder.equals("desc")) {
            files.sort(Comparator.comparing(FileModel::getFilename).reversed());
        }

        System.out.println(String.format("%-10s %-" + maxFileLength + "s %s",
                "File Size",
                "File Name",
                "Num Chunks"
        ));

        if (maxFileLength < 9) maxFileLength = 9;

        System.out.println(String.format("%-10s %-" + maxFileLength + "s %s",
                "----------",
                "-".repeat(maxFileLength),
                "----------"
        ));

        for (FileModel fm : files) {
            System.out.println(String.format("%10d %-" + maxFileLength + "s %d (%d)",
                    fm.getFilesize(),
                    fm.getFilename(),
                    fm.getChunkList().size(),
                    fm.getChunkSize()
            ));
        }

        return 0;
    }

    private int calculateOptimpalBufferSize(String inputFileName) {
        File file = new File(inputFileName);

        if (file.length() < 4096) {
            System.out.println("Using buffer size of 4096.");

            return 4096;
        }

        long fileSize = file.length();

        int bufferSize = 128;

        while (fileSize / bufferSize > 25 && bufferSize <= 65536) {
            bufferSize *= 2;
        }

        return bufferSize;
    }

    public int storeFunction(List<String> parameters) {
        String inputFileName = "";
        String outputFileName = "";

        if (parameters.size() == 0) {
            System.out.println("\n   Please provide either a filename to use as input and output, " +
                "or a filename to use as input, and one to use as output.");

            return 1;
        }

        if (parameters.size() == 1) {
            inputFileName = parameters.get(0);
            outputFileName = parameters.get(0);
        }

        if (parameters.size() == 2) {
            inputFileName = parameters.get(0);
            outputFileName = parameters.get(1);
        }

        if (optimalBufferSize) {
            bufferSize = calculateOptimpalBufferSize(inputFileName);

            System.out.println("Using calculated buffer size of " + bufferSize + " bytes.");
        }

        System.out.println("Attempting to write " + inputFileName + " to output name: " + outputFileName);

        esService.storeFile(inputFileName, outputFileName, bufferSize);

        return 0;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.bufferSize = (int) parameters.get("bufferSize");
        this.sortOrder = (String) parameters.get("sortOrder");
        this.hostPort = (String) parameters.get("hostPort");
        this.optimalBufferSize = (boolean) parameters.get("optimalBufferSize");
    }
}
