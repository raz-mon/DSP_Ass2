import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.model.*;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.s3.S3Client;


public class Main {
    public static S3Client S3;
    public static EmrClient emr;

    public static void main(String[] args){

        // Bucket name:
        String bucketName = "razalmog1122";

        S3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();

        System.out.println("Instantiating EMR instance!");
        emr = EmrClient.builder()
                .region(Region.US_EAST_1)
                .build();

		/*
        step 1
        1-gram + amount of words in the corpus.
		 */


        HadoopJarStepConfig step1 = HadoopJarStepConfig.builder()
                .jar("s3://" + bucketName + "/step1.jar")
                // .args("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data" , "s3://" + bucketName + "/output/1gram")
                .args("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data")
                .mainClass("step1")
                .build();

        StepConfig stepOne = StepConfig.builder()
                .hadoopJarStep(step1)
                .name("step1")
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

		/*
        step 2
        2-gram
		 */
        HadoopJarStepConfig step2 = HadoopJarStepConfig.builder()
                .jar("s3://" + bucketName + "/step2.jar")
                // .args("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data" , "s3://" + bucketName + "/output/2gram")
                .args("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data")
                .mainClass("step2")
                .build();

        StepConfig stepTwo = StepConfig.builder()
                .name("step2")
                .hadoopJarStep(step2)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
		/*
        step 3
        3-gram.
		 */
        HadoopJarStepConfig step3 = HadoopJarStepConfig.builder()
                .jar("s3://" + bucketName + "/step3.jar")
                // .args("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data" , "s3://" + bucketName + "/output/3gram")
                .args("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data")
                .mainClass("step3")
                .build();

        StepConfig stepThree = StepConfig.builder()
                .name("step3")
                .hadoopJarStep(step3)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
		/*
        step 4
        Join data from step 2 and 3, so that one key (which will be a threesome of
        words) will have the following data:
        1. The amount of times (w1, w2) appeared in the corpus.
        2. The amount of times (w2, w3) appeared in the corpus.
        3. The amount of times (w1, w2, w3) appeared in the corpus.
		 */
        HadoopJarStepConfig step4 = HadoopJarStepConfig.builder()
                .jar("s3://" + bucketName + "/step4.jar")
                .args("step4")
                .mainClass("step4")
                .build();

        StepConfig stepFour = StepConfig.builder()
                .name("step4")
                .hadoopJarStep(step4)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
		/*
        step 5
        Calculate the desired probability
        (all needed data is supplied in this step, using a local hashmap for 1-gram).
		 */
        HadoopJarStepConfig step5 = HadoopJarStepConfig.builder()
                .jar("s3://" + bucketName + "/step5.jar")
                .args("step5")
                .mainClass("step5")
                .build();

        StepConfig stepFive = StepConfig.builder()
                .name("step5")
                .hadoopJarStep(step5)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
		/*
        step 6
        Sort the order of appearance of the output file, as requested.
		 */
        HadoopJarStepConfig step6 = HadoopJarStepConfig.builder()
                .jar("s3://" + bucketName + "/step6.jar")
                .args("s3n://" + bucketName + "//outputAssignment2")
                .mainClass("step6")
                .build();

        StepConfig stepSix = StepConfig.builder()
                .name("step6")
                .hadoopJarStep(step6)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        JobFlowInstancesConfig instances = JobFlowInstancesConfig.builder()
                .instanceCount(3)
                .masterInstanceType(InstanceType.M4_LARGE.toString())
                .slaveInstanceType(InstanceType.M4_LARGE.toString())
                .hadoopVersion("3.1.3")
                .ec2KeyName("kp1")
                .placement(PlacementType.builder().availabilityZone("us-east-1a").build())
                .keepJobFlowAliveWhenNoSteps(false)
                .build();


        RunJobFlowRequest request = RunJobFlowRequest.builder()
                .name("DSP_Ass_2")
                .instances(instances)
                .steps(stepOne,stepTwo,stepThree,stepFour,stepFive,stepSix)
                .logUri("s3n://" + bucketName + "/logs/")
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .releaseLabel("emr-5.11.0")
                .build();

        System.out.println("Sending the job...");

        RunJobFlowResponse res = emr.runJobFlow(request);
        String jobFlowId = res.jobFlowId();
        System.out.println("Ran JobFlow with id: " + jobFlowId);
    }

}