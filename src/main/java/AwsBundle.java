import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import java.io.*;
import java.util.*;


public class AwsBundle {


    private static final int MAX_INSTANCES = 19;
    private final Ec2Client ec2;
    private final S3Client s3;
    private final SqsClient sqs;


    public final String requestsAppsQueueName = "requestsAppsQueue";
    public final String resultsAppsQueueName = "resultsAppsQueue";
    public final String requestsWorkersQueueName = "requestsWorkersQueue";
    public final String resultsWorkersQueueName = "resultsWorkersQueue";
    public final String localManagerConnectionQueue = "locManConQueue";

    public static final String bucketName = "razalmog1122";

    public static final String inputFolder = "input-files";
    public static final String outputFolder = "output-files";
    public static final String resultQueuePrefix = "resultQueue_";


    public static final String ami = "ami-00e95a9222311e8ed";
    static final String Delimiter = "#";

    private static final AwsBundle instance = new AwsBundle();

    private AwsBundle() {

        ec2 = Ec2Client.builder()
                .region(Region.US_EAST_1)
                .build();
        s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();
        sqs = SqsClient.builder()
                .region(Region.US_EAST_1)
                .build();
    }

    public static AwsBundle getInstance() {
        return instance;
    }

    // EC2:

    /**
     * Create an ec2 instance.
     *
     * @param name:     The name of the ec2 instance to instantiate.
     * @param amiId:    Ami-id of the instance.
     * @param userData: Initial script (string) for the instance.
     * @return: Id of the instance.
     */
    public String createEC2Instance(String name, String amiId, String userData) {

        IamInstanceProfileSpecification role = IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build();
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .userData(Base64.getEncoder().encodeToString(userData.getBytes()))
                .iamInstanceProfile(role)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(1)
                .minCount(1)
                .build();


        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("Name")
                .value(name)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "Successfully started EC2 Instance %s based on AMI %s\n",
                    instanceId, amiId);

            return instanceId;

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

        return "";
    }

    /**
     * Check if an instance exists, by tag name.
     *
     * @param name: Name of the instance (tag-name).
     * @return: True if the instance exists (running), else otherwise.
     */
    public boolean checkIfInstanceExist(String name) {

        String nextToken = null;

        DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(1000).nextToken(nextToken).build();

        DescribeInstancesResponse response = this.ec2.describeInstances(request);

        for (Reservation reservation : response.reservations()) {
            for (Instance i : reservation.instances()) {
                if (!i.state().name().equals(InstanceStateName.RUNNING))
                    continue;
                for (Tag t : i.tags()) {
                    if (t.key().equals("Name") && t.value().equals(name)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Get the amount of currently running instances.
     *
     * @return: The number of instances running.
     */
    public int getAmountOfRunningInstances() {

        int counter = 0;
        String nextToken = null;

        DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(1000).nextToken(nextToken).build();

        DescribeInstancesResponse response = this.ec2.describeInstances(request);

        for (Reservation reservation : response.reservations()) {
            for (Instance i : reservation.instances()) {
                if (!i.state().name().equals(InstanceStateName.RUNNING))
                    continue;
                //for (Tag t : i.tags())
                counter++;
            }
        }
        return counter;
    }

    /**
     * Terminate currernt instance.
     */
    public void terminateCurrentInstance() {
        String instanceId = EC2MetadataUtils.getInstanceId();
        List<String> instanceIds = new ArrayList<>();
        instanceIds.add(instanceId);
        TerminateInstancesRequest request = TerminateInstancesRequest.builder().instanceIds(instanceIds).build();
        System.out.println(instanceId + " is terminating itself.\n");
        this.ec2.terminateInstances(request);
    }

    public void terminateEC2(String instanceID) {

        System.out.println("Terminating instaceId: " + instanceID + "\n");

        try{
            TerminateInstancesRequest ti = TerminateInstancesRequest.builder()
                    .instanceIds(instanceID)
                    .build();

            TerminateInstancesResponse response = ec2.terminateInstances(ti);
            List<InstanceStateChange> list = response.terminatingInstances();

            for (int i = 0; i < list.size(); i++) {
                InstanceStateChange sc = (list.get(i));
                System.out.println("The ID of the terminated instance is "+sc.instanceId() + "\n");
            }
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public void terminateAllIntancesButManager(){

        String nextToken = null;

        DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(1000).nextToken(nextToken).build();

        DescribeInstancesResponse response = this.ec2.describeInstances(request);

        for (Reservation reservation : response.reservations()) {
            for (Instance i : reservation.instances()) {
                if (!i.state().name().equals(InstanceStateName.RUNNING))
                    continue;
                for (Tag t : i.tags()){
                    if (!(t.key().equals("Name") && t.value().equals("Manager"))) {
                        terminateEC2(i.instanceId());
                    }
                }
            }
        }
    }


    // S3:

    /**
     * Create a bucket by using a S3Waiter object
     *
     * @param bucketName: The name of the bucket we wish to create.
     */
    public void createBucketIfNotExists(String bucketName) {

        try {
            S3Waiter s3Waiter = s3.waiter();
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            s3.createBucket(bucketRequest);
            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            // Wait until the bucket is created and print out the response
            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            // waiterResponse.matched().response().ifPresent(System.out::println);
            System.out.println("Bucket " + bucketName + " is ready\n");

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    /**
     * Put a file on the S3 memory.
     *
     * @param bucketName: Name of bucket.
     * @param objectKey:  key-name of the object we wish to download.
     * @param objectPath: The path of the file in S3.
     * @return: eTag of response.
     */
    public String putS3Object(String bucketName,
                              String objectKey,
                              String objectPath) {

        try {

            Map<String, String> metadata = new HashMap<>();
            metadata.put("x-amz-meta-myVal", "test");

            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .metadata(metadata)
                    .build();

            PutObjectResponse response = s3.putObject(putOb,
                    RequestBody.fromBytes(getObjectFile(objectPath)));

            System.out.println("Successfully uploaded a new object to the S3 memory.\n");

            return response.eTag();

        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return "";
    }

    /**
     * Return a byte array, from a file. (Used to send the file on the computer to S3!)
     *
     * @param filePath: Path of the to upload to S3.
     * @return: Byte array of the file.
     */
    private byte[] getObjectFile(String filePath) {

        FileInputStream fileInputStream = null;
        byte[] bytesArray = null;

        try {
            File file = new File(filePath);
            bytesArray = new byte[(int) file.length()];
            fileInputStream = new FileInputStream(file);
            fileInputStream.read(bytesArray);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return bytesArray;
    }

    /**
     * Get a file from the S3 memory, save it in path. (Not to be confused with the 'GetObjectFile').
     *
     * @param bucketName: Bucket name.
     * @param keyName:    Key name.
     * @param path:       Path to save the downloaded file in.
     */
    public void getS3Object(String bucketName, String keyName, String path) {

        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
            byte[] data = objectBytes.asByteArray();

            // Write the data to a local file
            File myFile = new File(path);
            if (!myFile.createNewFile())
                return;
            OutputStream os = new FileOutputStream(myFile);
            os.write(data);
            System.out.println("Successfully obtained bytes from S3 object: bucketName: " + bucketName + " KeyName: " + keyName + "\n");
            os.close();

        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    /**
     * Delete an object from S3 memory.
     *
     * @param bucketName: Bucket in which the file is in.
     * @param objectName: Name of object to delete.
     */
    public void deleteBucketObjects(String bucketName, String objectName) {

        ArrayList<ObjectIdentifier> toDelete = new ArrayList<ObjectIdentifier>();
        toDelete.add(ObjectIdentifier.builder().key(objectName).build());

        try {
            DeleteObjectsRequest dor = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(Delete.builder().objects(toDelete).build())
                    .build();
            s3.deleteObjects(dor);
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        System.out.println(objectName + " deleted from bucket: " + bucketName + "\n");
    }

    // SQS:

    /**
     * Create a Queue. Return Queue url.
     * If already exists --> Only return the queue url.
     *
     * @param queueName: Name of queue.
     * @return: QueueUrl of the queue.
     */
    public String createQueue(String queueName) {
        try {
            System.out.println("Creating Queue: " + queueName + "\n");

            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();

            this.sqs.createQueue(createQueueRequest);

            GetQueueUrlResponse getQueueUrlResponse =
                    sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            String queueUrl = getQueueUrlResponse.queueUrl();

            System.out.println("Queue created successfully\n");

            return queueUrl;

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    /**
     * Send a single message
     *
     * @param queueUrl: Queue url.
     */
    public void sendMessage(String queueUrl, String message) {

        System.out.println("Sending message\n");

        try {
            SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    // Can use the id field of the message in the future if we want.
                    .entries(SendMessageBatchRequestEntry.builder().id("id").messageBody(message).build())
                    .build();
            sqs.sendMessageBatch(sendMessageBatchRequest);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    /**
     * Send a batch (several) messages to the queue with url 'queueUrl'.
     *
     * @param queueUrl: Queue url.
     */
    public void sendBatchMessages(String queueUrl) {

        System.out.println("Sending messages\n");

        try {
            SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(SendMessageBatchRequestEntry.builder().id("id1").messageBody("Hello from msg 1").build(),
                            SendMessageBatchRequestEntry.builder().id("id2").messageBody("msg 2").delaySeconds(10).build())
                    .build();
            sqs.sendMessageBatch(sendMessageBatchRequest);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    /**
     * Recieve 'numOfMessages' messages from the queue with url 'queueUrl'.
     *
     * @param queueUrl:      Queue url.
     * @param numOfMessages: Number of messages to receive.
     * @return: List of Messages, received from the queue.
     * Notice: that these messages are NOT deleted from the queue. They must be actively deleted.
     * Notice: Visibility timeout is 60 seconds.
     */
    public List<Message> receiveMessages(String queueUrl, int numOfMessages) {

        System.out.println("Receiving messages\n");

        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(numOfMessages)
                    .waitTimeSeconds(20)
                    .visibilityTimeout(60)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
            return messages;
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    /**
     * Delete specific messages from the queue with url 'queueUrl'.
     *
     * @param queueUrl: Queue url.
     * @param messages: Messages to delete.
     */
    public void deleteMessages(String queueUrl, List<Message> messages) {
        System.out.println("Deleting Messages: ");
        for (Message m : messages) {
            System.out.println(m.messageId() + "\n");
        }
        System.out.println("\n");

        try {
            for (Message message : messages) {
                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build();
                sqs.deleteMessage(deleteMessageRequest);
            }

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }


// Add cleanQueue function.

    /**
     * Delete sqs queue.
     *
     * @param queueName: Queue name.
     */
    public void deleteSQSQueueByName(String queueName) {

        try {

            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();

            String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            sqs.deleteQueue(deleteQueueRequest);

            System.out.println(queueName + " deleted.\n");

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    /**
     * Delete sqs queue.
     *
     * @param queueUrl: Queue url.
     */
    public void deleteSQSQueueByUrl(String queueUrl) {

        try {

            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            sqs.deleteQueue(deleteQueueRequest);

            System.out.println("Queue deleted.\n");

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    /**
     * Get queue url.
     *
     * @param queueName: Queue name.
     * @return: Queue url.
     */
    public String getQueueUrl(String queueName) {
        try {
            GetQueueUrlResponse getQueueUrlResponse =
                    sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            String queueUrl = getQueueUrlResponse.queueUrl();
            return queueUrl;
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    /**
     * Clean the queue from all messages.
     *
     * @param queueUrl: Queue url.
     */
    public void cleanQueue(String queueUrl) {
        PurgeQueueRequest request = PurgeQueueRequest.builder().queueUrl(queueUrl).build();
        this.sqs.purgeQueue(request);
        System.out.println("\nQueue cleaned\n");
    }

}
