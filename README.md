## Top-100 collocations
submitters:
1. Oren Halevy 
2. Vladislav Yegorov 

## The assignment
This a map-reduce program produces the list of top-100 collocations for each decade (1990-1999, 2000-2009, etc.) for English and Hebrew, with their log likelihood ratios (in descending order).
The log likelihood ratios are calculated with refer to Google bigrmas:

* English: s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data
* Hebrew: s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data

If you want to get familiar with the n-grams dataset, you can read this [overview](https://aws.amazon.com/datasets/google-books-ngrams/).

In this assignment we are automatically extracting collocations from the Google 2-grams dataset using Amazon Elastic Map Reduce.
## Statistics
We also provided the number of key-value pairs that were sent from the mappers to the reducers in our map-reduce runs, and their size, with and without local aggregation.
## Stop Words
In this assignment, we removed all bigram that contain stop words and did not include them in our counts.
* A list of Hebrew stop words can be found [here](https://www.cs.bgu.ac.il/~dsp112/Forum?action=show-thread&id=0304f844911ab448f0282b0478a05547).
* A list of English stop words can be found [here](http://web.archive.org/web/20080501010608/http://www.dcs.gla.ac.uk/idom/ir_resources/linguistic_utils/stop_words).

## How to run program from the source file:

1. At / run:
		mvn clean
2. At / run:
		mvn package
3. At / run:
		cd target
4. At /target/ run:
		java -jar Hw2-0.0.1-SNAPSHOT-jar-with-dependencies.jar ExtractCollations heb/eng

You will then need to get an address in s3, in which the output files will appear once the task has been completed.
## Output
At the end of the run, the output could be found on the S3 link: s3://top-100-collocations-688622487960/output/step8

## Job Flow:
Step1:
* Objective: finding all word by decade in 1 gram.
* Input: 1gram
* Output: <w1,decade,c1> 
	(w1 is the word, decade is the without the the single digits of the year, for example 1993->199, 1998->199, c1 is the counter for the number of times the word was used)  

Step2:
* Objective: finding all word by decade in 2 gram.
* Input: 2gram
* Output: <w1 w2,decade,c12> 
		(w1 and w2 are the words used, decade is the without the the single digits of the year, c12 is the counter for the number of times the words were used when w1 was follwed closely by w2)

Step3:
* Objective: finding N for each decade (sum of counters for every word for the specific decade) 
* Input: step1 - <w1,decade,c1> 
* Output: <decade,N> 

Step4:
* Objective: joining w1w2 count with w1 count
* First Input: step2 <w1 w2,decade,c12> 
* Second Input: step1 <w1,decade,c1>	
* Output: <w1 w2,decade,c12 c1> 
	
Step5:
* Objective: joining w1w2_w1 (output of step 4) count with w2 count (output of step 1 for the word w2 of step 4)
* First Input: step4 - <w1 w2,decade,c12 c1> 
* Second Input: step1 - <w2,decade,c2>	
* Output: <w1 w2,decade,c12 c1 c2>  
	
Step6:
* Objective: joining w1w2_w1_w2 (output of step 5) count with N count (output of step 3, by decade)
* First Input: step5 - <w1 w2,decade,c12 c1 c2> 
* Second Input: step3 - <decade,N>  	
* Output: <w1 w2,decade,c12 c1 c2 N>  
	
Step7:
* Objective: joining w1w2_w1 count with w2 count (calculating the "Log likelihood ratio" as seen in the image "logLikelihoodRatio.png")
* Input: step6 - <w1 w2,decade,c12 c1 c2 N>
* Output: <w1 w2,decade,likelihood ratio> 

Step8
* Objective: take top-100 collocations for each decade (select the top 100 words from each decade)
* First Input: step7 - <w1 w2,decade,likelihood ratio>	
* Output: <w1 w2,decade,likelihood ratio>  

other files:
* GramByDecade : 	class GramByDeacade stores the word and decade.
* sort <w,decade> by decade, then by w (lex).
* StopWords : 	class StopWords create a list according to the language (English or Hebrew).
