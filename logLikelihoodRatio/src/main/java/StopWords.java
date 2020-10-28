import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class StopWords {
	List<String> StopWords;
	String lang;
	
	StopWords(String lang) throws IOException{
		StopWords = new ArrayList<String>();
		this.lang = lang;
		URL url;
		if(lang.equals("eng")) 
			url = new URL("https://top-100-collocations-688622487960.s3.amazonaws.com/stopWordsEng");
		else
			url = new URL("https://top-100-collocations-688622487960.s3.amazonaws.com/stopWordsHeb");
		BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
		String inputLine;
		while ((inputLine = in.readLine()) != null)
			StopWords.add(inputLine.trim());
	 	in.close();
	}
	
	public List<String> getAllStopWords(){
		return StopWords;
	}
	
	public boolean isStopWord(String word) {
		return StopWords.contains(word);
	}
}
	

