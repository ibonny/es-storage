package personal.ibonny.esstorage;

import personal.ibonny.esstorage.services.CallOutFunctions;
import personal.ibonny.esstorage.services.ESService;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.*;
import java.util.*;
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

    @Option(names = {"--sort"}, defaultValue = "none", description = "Sort order for listings. [asc, desc, none]")
    private String sortOrder;

    @Override
    public Integer call() throws Exception {
        CallOutFunctions cof = new CallOutFunctions();

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

        cof.setParameters(Map.of(
            "bufferSize", bufferSize,
            "hostPort", hostPort,
            "optimalBufferSize", optimalBufferSize,
            "sortOrder", sortOrder
        ));

        if (cof.FUNCTIONS.containsKey(action)) {
            if (parameters == null) {
                parameters = new ArrayList<>();
            }

            return (int) cof.FUNCTIONS.get(action).invoke(cof, parameters);
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

        if (action.equals("integrity")) {
            esService.integrityCheck();

            commandFound = true;
        }

        esService.closeClient();

        if (! commandFound) {
            System.out.println("\n   Command not found.\n");

            return 1;
        }

        return 0;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new EntryPoint()).execute(args);

        System.exit(exitCode);
    }
}
