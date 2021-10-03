package personal.ibonny.esstorage;

import personal.ibonny.esstorage.models.FileModel;
import personal.ibonny.esstorage.services.ESService;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;


public class EntryPoint implements Callable<Integer> {
    ESService esService;

    @Parameters(index = "0", description = "Action to run")
    private String action;

    @Parameters(index = "1..*", description = "Action specific parameters")
    private List<String> parameters;

    @Option(names = {"-s", "--size"}, defaultValue = "2048", description = "Buffer size")
    private int bufferSize;

    @Option(names = {"-h", "--host"}, defaultValue = "192.168.1.224:9200", description = "ElasticSearch host/port (e.g. hostname:9200)")
    private String hostPort;

    @Option(names = {"--opt"}, defaultValue = "false", description = "Use optimized buffer size.")
    private boolean optimalBufferSize;

    @Override
    public Integer call() throws Exception {
        String inputFileName = "";
        String outputFileName = "";

        hostPort = hostPort.trim();

        if (! hostPort.contains(":")) {
            System.out.println("Please provide a host and a port.");

            return 1;
        }

        List<String> fields = Arrays.asList(hostPort.split(":"));

        esService = new ESService(fields.get(0), Integer.parseInt(fields.get(1)));

        boolean commandFound = false;

        if (action.equals("cat")) {
            if (parameters.size() == 0) {
                System.out.println("\n   Please provide filename to cat.");

                return 1;
            }

            inputFileName = parameters.get(0);

            ByteArrayOutputStream baos = esService.getFile(inputFileName);

            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

            int numBytes;

            byte[] buffer = new byte[1024];

            while ((numBytes = bais.read(buffer, 0, 1024)) != -1) {
                System.out.write(buffer, 0, numBytes);
            }

            commandFound = true;
        }

        if (action.equals("list")) {
            List<FileModel> files = esService.getAllFiles();

            if (files.size() == 0) {
                System.out.println("\n   No files found.\n");

                esService.closeClient();

                return 0;
            }

            int maxFileLength = 0;

            for (FileModel fm: files) {
                if (fm.getFilename().length() > maxFileLength) {
                    maxFileLength = fm.getFilename().length();
                }
            }

            System.out.println(String.format("%-10s %-" + maxFileLength + "s %s",
                "File Size",
                "File Name",
                "Num Chunks"
            ));

            System.out.println(String.format("%-10s %-" + maxFileLength + "s %s",
                "----------",
                "-".repeat(maxFileLength),
                "----------"
            ));

            for (FileModel fm: files) {
                System.out.println(String.format("%10d %-" + maxFileLength + "s %d (%d)",
                    fm.getFilesize(),
                    fm.getFilename(),
                    fm.getChunkList().size(),
                    fm.getChunkSize()
                ));
            }

            commandFound = true;
        }

        if (action.equals("store") || action.equals("put")) {
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

            commandFound = true;
        }

        if (action.equals("get")) {
            if (parameters.size() == 0) {
                System.out.println("\n   Please provide a filename to get, or a filename to get and a destination filename to write to.\n");

                esService.closeClient();

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

            System.out.println("Using input file name of '" + inputFileName + "' and output filename of '" + outputFileName + "'.");

            ByteArrayOutputStream baos = esService.getFile(inputFileName);

            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

            int numBytes;

            byte[] buffer = new byte[1024];

            long totalBytes = 0;

            try {
                FileOutputStream fos = new FileOutputStream(new File(outputFileName));

                while ((numBytes = bais.read(buffer, 0, 1024)) != -1) {
                    fos.write(buffer, 0, numBytes);

                    totalBytes += numBytes;
                }

                fos.close();
            } catch(IOException ioe) {
                System.out.println("Error writing out to file: " + ioe.getMessage());
            }

            System.out.println(String.format("Wrote out %s to file %s with %d bytes.",
                inputFileName,
                outputFileName,
                totalBytes
            ));

            commandFound = true;
        }

        if (action.equals("delete")) {
            if (parameters.size() == 0) {
                System.out.println("\n   Please provide a filename to delete.\n");

                esService.closeClient();

                return 1;
            }

            esService.delete(parameters.get(0));

            commandFound = true;
        }

        esService.closeClient();

        if (! commandFound) {
            System.out.println("\n   Command not found.\n");

            return 1;
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

    public static void main(String[] args) {
        int exitCode = new CommandLine(new EntryPoint()).execute(args);

        System.exit(exitCode);
    }
}
