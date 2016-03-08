package Dictionary;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class DictionaryMapper  extends Mapper<Text, Text, Text, Text> {
      
    //<EnglishWord>_<PartsOfSpeech> : <language>_<translatedWord>
    String language;
    public void setup(Context context) {
        String fileName =  ((FileSplit)context.getInputSplit()).getPath().getName();
        language = fileName.substring(0,fileName.length()-4);
    }
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        if(!(key.toString().charAt(0)=='#') && (value.toString().indexOf('['))>0){
            String partsOfSpeech = value.toString().substring(value.toString().lastIndexOf('[')+1,value.toString().length()-1);
            String translations= value.toString().substring(0,value.toString().lastIndexOf('[') );
            if(valid(partsOfSpeech)){
                System.out.println("valid POS found");
                String keyMap= key + " : ["+partsOfSpeech+"]";
                String valueMap = language+ ":"+translations;
                System.out.println("key: "+keyMap);
                System.out.println("Value: "+ valueMap);
                context.write(new Text(keyMap), new Text(valueMap));
            }
        }        
    }
    private boolean valid(String partsOfSpeech) {
        String[] words = {"Noun", "Pronoun", "Verb", "Adverb", "Adjective", "Preposition", "Conjunction", "interjection"};  
        return (Arrays.asList(words).contains(partsOfSpeech));
    }
    
}
