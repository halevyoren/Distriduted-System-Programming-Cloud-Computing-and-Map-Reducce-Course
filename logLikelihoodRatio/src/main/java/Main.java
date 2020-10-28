
import java.io.IOException;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.HadoopJarStepConfig;
import software.amazon.awssdk.services.emr.model.JobFlowInstancesConfig;
import software.amazon.awssdk.services.emr.model.RunJobFlowRequest;
import software.amazon.awssdk.services.emr.model.RunJobFlowResponse;
import software.amazon.awssdk.services.emr.model.StepConfig;


public class Hw2 {
	private static final Region region = Region.US_EAST_1;
    public static void main(String[] args) throws IOException {	
    	  	 	
    	String gram1;
    	String gram2;
    	if(args[1].equals("eng")) {
    		gram1 = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data";
    		gram2 = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data";
    	}else {
    		gram1 = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data";
    		gram2 = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
    	}
    	EmrClient emr = EmrClient.builder().region(region).build();
        //Step 1
        HadoopJarStepConfig step1cfg = HadoopJarStepConfig.builder()
        		.jar("s3://top-100-collocations-688622487960/Step1.jar")
                .args(gram1,"s3://top-100-collocations-688622487960/output/step1",args[1])
                .build();
        StepConfig step1 = StepConfig.builder() 
                .name("Step1")
                .actionOnFailure("CONTINUE")
                .hadoopJarStep(step1cfg)
                .build(); 
        //Step 2
        HadoopJarStepConfig step2cfg = HadoopJarStepConfig.builder()
        		.jar("s3://top-100-collocations-688622487960/Step2.jar")
                .args(gram2,"s3://top-100-collocations-688622487960/output/step2",args[1])
                .build();
        StepConfig step2 = StepConfig.builder() 
                .name("Step2")
                .actionOnFailure("CONTINUE")
                .hadoopJarStep(step2cfg)
                .build();         
        //Step 3
        HadoopJarStepConfig step3cfg = HadoopJarStepConfig.builder()
        		.jar("s3://top-100-collocations-688622487960/Step3.jar")
                .args("s3://top-100-collocations-688622487960/output/step1/*","s3://top-100-collocations-688622487960/output/step3")
                .build();
        StepConfig step3 = StepConfig.builder() 
                .name("Step3")
                .actionOnFailure("CONTINUE")
                .hadoopJarStep(step3cfg)
                .build();        
        //Step 4
        HadoopJarStepConfig step4cfg = HadoopJarStepConfig.builder()
        		.jar("s3://top-100-collocations-688622487960/Step4.jar")
                .args("s3://top-100-collocations-688622487960/output/step2/*","s3://top-100-collocations-688622487960/output/step1/*","s3://top-100-collocations-688622487960/output/step4")
                .build();
        StepConfig step4 = StepConfig.builder() 
                .name("Step4")
                .actionOnFailure("CONTINUE")
                .hadoopJarStep(step4cfg)
                .build();  
        //Step 5
        HadoopJarStepConfig step5cfg = HadoopJarStepConfig.builder()
        		.jar("s3://top-100-collocations-688622487960/Step5.jar")
                .args("s3://top-100-collocations-688622487960/output/step4/*","s3://top-100-collocations-688622487960/output/step1/*","s3://top-100-collocations-688622487960/output/step5")
                .build();
        StepConfig step5 = StepConfig.builder() 
                .name("Step5")
                .actionOnFailure("CONTINUE")
                .hadoopJarStep(step5cfg)
                .build();   
        
        //Step 6
        HadoopJarStepConfig step6cfg = HadoopJarStepConfig.builder()
        		.jar("s3://top-100-collocations-688622487960/Step6.jar")
                .args("s3://top-100-collocations-688622487960/output/step5/*","s3://top-100-collocations-688622487960/output/step3/*","s3://top-100-collocations-688622487960/output/step6")
                .build();
        StepConfig step6 = StepConfig.builder() 
                .name("Step6")
                .actionOnFailure("CONTINUE")
                .hadoopJarStep(step6cfg)
                .build();
      //Step 7
        HadoopJarStepConfig step7cfg = HadoopJarStepConfig.builder()
        		.jar("s3://top-100-collocations-688622487960/Step7.jar")
                .args("s3://top-100-collocations-688622487960/output/step6/*","s3://top-100-collocations-688622487960/output/step7")
                .build();
        StepConfig step7 = StepConfig.builder() 
                .name("Step7")
                .actionOnFailure("CONTINUE")
                .hadoopJarStep(step7cfg)
                .build(); 
      //Step 8
        HadoopJarStepConfig step8cfg = HadoopJarStepConfig.builder()
        		.jar("s3://top-100-collocations-688622487960/Step8.jar")
                .args("s3://top-100-collocations-688622487960/output/step7/*","s3://top-100-collocations-688622487960/output/step8")
                .build();
        StepConfig step8 = StepConfig.builder() 
                .name("Step8")
                .actionOnFailure("CONTINUE")
                .hadoopJarStep(step8cfg)
                .build();  
        
        RunJobFlowRequest flowRequest = RunJobFlowRequest.builder()
        		.name("emr-spot-example")
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .logUri("s3://aws-logs-688622487960/elasticmapreduce/")
                .steps(step1,step2,step3,step4,step5,step6,step7,step8)
                .visibleToAllUsers(true)
                .releaseLabel("emr-5.0.0")
                .instances(JobFlowInstancesConfig.builder()
                		.ec2KeyName("abcd")
                        .instanceCount(6) // CLUSTER SIZE
                        .keepJobFlowAliveWhenNoSteps(false)
                        .masterInstanceType("m4.large")
                        .slaveInstanceType("m4.large")
                        .build())
                .build();
        RunJobFlowResponse response = emr.runJobFlow(flowRequest);  
        System.out.println("JobFlow id: "+response.jobFlowId());
        System.out.println("*****************************************************************************************");
        System.out.println("Your request is in progress. Your output can be found in: " +
                "top-100-collocations-688622487960" + " once it's finished");
        System.out.println("*****************************************************************************************");  
    }
}
