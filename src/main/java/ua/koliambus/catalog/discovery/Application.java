package ua.koliambus.catalog.discovery;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MapperService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootApplication
public class Application implements CommandLineRunner {

    private final AmazonSQS amazonSQS;

    private final ApplicationContext applicationContext;

    private final RestHighLevelClient restHighLevelClient;

    private static final int PARALLEL_WORKER_THREADS = 10;

    private static final int LONG_POLLING_INTERVAL_SECONDS = 10;

    private static final String QUEUE_NAME = "published_songs_queue";

    public Application(AmazonSQS amazonSQS, ApplicationContext applicationContext, RestHighLevelClient restHighLevelClient) {
        this.amazonSQS = amazonSQS;
        this.applicationContext = applicationContext;
        this.restHighLevelClient = restHighLevelClient;
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args).close();
    }

    private void consumeMessagesFromQueue(final String queueUrl){
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withWaitTimeSeconds(LONG_POLLING_INTERVAL_SECONDS)
                .withVisibilityTimeout(40);

        List<Message> messages = amazonSQS.receiveMessage(receiveMessageRequest).getMessages();
        System.out.println("Messages received: " + messages);

        if (messages.isEmpty()) {
            return;
        }

        BulkRequest bulkRequest = new BulkRequest("published-songs", MapperService.SINGLE_MAPPING_NAME);

        messages.forEach(message -> {
            try {
                System.out.println("Message: " + message.getBody());
                ObjectMapper mapper = new ObjectMapper();
                TypeReference<HashMap<String, String>> typeRef
                        = new TypeReference<HashMap<String, String>>() {};
                Map<String, String> map = mapper.readValue(message.getBody(), typeRef);
                final String songID = map.get("id");

                bulkRequest.add(
                        new IndexRequest()
                                .opType(DocWriteRequest.OpType.CREATE)
                                .id(songID)
                                .source(message.getBody().getBytes(), XContentType.JSON));

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        });


        restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                List<DeleteMessageBatchRequestEntry> deleteList = new ArrayList<>(messages.size());
                for (BulkItemResponse itemResponse : bulkItemResponses.getItems()) {
                    if (itemResponse.isFailed()) {
                        System.out.println("Create in elasticsearch failed. " + itemResponse.getFailureMessage());
                    } else {
                        Message message = messages.get(itemResponse.getItemId());
                        deleteList.add(new DeleteMessageBatchRequestEntry(message.getMessageId(), message.getReceiptHandle()));
                    }
                }

                if (!deleteList.isEmpty()) {
                    DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest(queueUrl);
                    deleteMessageBatchRequest.withEntries(deleteList);
                    amazonSQS.deleteMessageBatch(deleteMessageBatchRequest);
                }
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println("Send to elasticsearch failed. " + e.getMessage());
            }
        });
    }

    @Override
    public void run(String... args) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(PARALLEL_WORKER_THREADS);

        final String queueUrl = amazonSQS.getQueueUrl(QUEUE_NAME).getQueueUrl();
        System.out.println("Start reading messages");

        try {
            executorService.submit(() -> {
//                while (true) {
//                    Thread.sleep(60);
                    consumeMessagesFromQueue(queueUrl);
//                }
            }).get();
        } finally {
            System.out.println("Stopped reading messages");
//            SpringApplication.exit(applicationContext);
        }
    }
}
