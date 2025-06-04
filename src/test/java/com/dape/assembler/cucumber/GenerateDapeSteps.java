package com.dape.assembler.cucumber;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.dape.assembler.DapeAssemblerApplication;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.pt.Dado;
import io.cucumber.java.pt.Então;
import io.cucumber.java.pt.Quando;
import io.cucumber.spring.CucumberContextConfiguration;
import io.findify.s3mock.S3Mock;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@CucumberContextConfiguration
public class GenerateDapeSteps {
    private static final String BUCKET_NAME = "regulatory-dape";
    private static final String FILE_PATH = "src/test/resources";
    private static final String INPUT_FOLDER = "input";
    private static final String OUTPUT_FOLDER = "output";
    private static final String FOLDER_SEPARATOR = "/";
    private final S3Mock s3Api = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
    private AmazonS3 amazonS3Client;

    @Before
    public void setUp() {
        awsS3Setup();
    }

    @After
    public void tearDown() {
        s3Api.shutdown();
    }

    @Dado("que exista o arquivo de clientes {string}")
    public void queExistaOArquivoDeClientes(String clientFileName) {
        final String inputFilePath = INPUT_FOLDER.concat(FOLDER_SEPARATOR).concat(clientFileName);
        putFileOnS3(inputFilePath, clientFileName);
    }

    @Dado("que exista o arquivo de apostas {string}")
    public void queExistaOArquivoDeApostas(String betFileName) {
        final String inputFilePath = INPUT_FOLDER.concat(FOLDER_SEPARATOR).concat(betFileName);
        putFileOnS3(inputFilePath, betFileName);
    }

    @Dado("que exista o arquivo de bilhetes {string}")
    public void queExistaOArquivoDeBilhetes(String ticketFileName) {
        final String inputFilePath = INPUT_FOLDER.concat(FOLDER_SEPARATOR).concat(ticketFileName);
        putFileOnS3(inputFilePath, ticketFileName);
    }

    @Dado("que exista o arquivo de relacionamento de bilhetes e apostas {string}")
    public void queExistaOArquivoDeRelacionamentoDeBilhetesEApostas(String ticketBetFileName) {
        final String inputFilePath = INPUT_FOLDER.concat(FOLDER_SEPARATOR).concat(ticketBetFileName);
        putFileOnS3(inputFilePath, ticketBetFileName);
    }

    @Quando("a aplicação for executada sem parâmetros")
    public void aAplicaçãoForExecutadaSemParâmetros() throws IOException {
        String[] args = new String[]{"--parameters", "{}"};
        DapeAssemblerApplication.main(args);
    }

    @Então("deve ser gerado o arquivo {string} com o conteúdo exatamente igual ao arquivo {string}")
    public void deveSerGeradoOArquivoComOConteúdoExatamenteIgualAoArquivo(String actualFileName, String expectedFileName) throws IOException {
        List<String> expectedLines;
        List<String> actualLines;

        try (FileReader compareFile = new FileReader(FILE_PATH.concat(FOLDER_SEPARATOR).concat(expectedFileName))) {
            expectedLines = getExpectedLines(compareFile);

            final S3Object s3Object = amazonS3Client.getObject(BUCKET_NAME, OUTPUT_FOLDER.concat(FOLDER_SEPARATOR).concat(actualFileName));
            actualLines = getS3FileLines(s3Object);
        }

        compareFilesAndShowDifferencesInLog(actualFileName, expectedFileName, actualLines, expectedLines);

        assertEquals(expectedLines, actualLines);
    }

    private List<String> getS3FileLines(S3Object s3Object) {
        try (BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(s3Object.getObjectContent()))) {
            return reader.lines().toList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Quando("a aplicação for executada com o parâmetro investigationClientIdt {string}")
    public void aAplicaçãoForExecutadaComOParâmetro(String investigationClientIdt) throws IOException {
        String[] args = new String[]{"--parameters", "{\"investigationClientIdt\": \"" + investigationClientIdt + "\"}"};

        DapeAssemblerApplication.main(args);
    }

    private void awsS3Setup() {
        s3Api.start();

        AwsClientBuilder.EndpointConfiguration endpoint = new AwsClientBuilder.EndpointConfiguration("http://localhost:8001", "us-east-1");
        this.amazonS3Client = AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(endpoint)
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .build();

        this.amazonS3Client.createBucket(BUCKET_NAME);
    }

    private void putFileOnS3(String key, String fileName) {
        try {
            final File file = new File(FILE_PATH.concat(FOLDER_SEPARATOR).concat(fileName));
            amazonS3Client.putObject(BUCKET_NAME, key, file);
            amazonS3Client.listObjectsV2(BUCKET_NAME);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> getExpectedLines(FileReader compareFile) {
        return new BufferedReader(compareFile).lines().toList();
    }

    private static void compareFilesAndShowDifferencesInLog(
            String actualFileName,
            String expectedFileName,
            List<String> actualLines,
            List<String> expectedLines
    ) {
        final Set<String> actualSet = new LinkedHashSet<>(actualLines);
        final Set<String> expectedSet = new LinkedHashSet<>(expectedLines);

        final List<String> missingInExpected = actualLines.stream().filter(line -> !expectedSet.contains(line)).toList();
        final List<String> missingInActual = expectedLines.stream().filter(line -> !actualSet.contains(line)).toList();

        if (missingInExpected.isEmpty() && missingInActual.isEmpty()) {
            System.out.println("\nOs arquivos são equivalentes!!!");
        } else {
            System.out.println("\nOs arquivos possuem diferenças!!!");

            if (!missingInExpected.isEmpty()) {
                System.out.println("\nLinhas do arquivo gerado[" + actualFileName + "] que NÃO estão no arquivo esperado[" + expectedFileName + "]:");
                for (int i = 0; i < actualLines.size(); i++) {
                    final String line = actualLines.get(i);
                    if (missingInExpected.contains(line)) {
                        System.out.println("Linha " + (i + 1) + ": " + line);
                    }
                }
            }

            if (!missingInActual.isEmpty()) {
                System.out.println("\nLinhas do arquivo esperado[" + expectedFileName + "] que NÃO estão no arquivo gerado[" + actualFileName + "]:");
                for (int i = 0; i < expectedLines.size(); i++) {
                    final String line = expectedLines.get(i);
                    if (missingInActual.contains(line)) {
                        System.out.println("Linha " + (i + 1) + ": " + line);
                    }
                }
            }
        }
    }
}
