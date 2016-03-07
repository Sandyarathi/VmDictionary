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
                    /*String[] splitValue=value.toString().split("\\[");
                    if(splitValue.length!=0 && splitValue.length!=1 ){
                        String partsOfSpeech = splitValue[1].substring(0,splitValue[1].length()-1);
                        if(valid(partsOfSpeech)){
                            String keyMap= key + " : ["+partsOfSpeech+"]";
                            String valueMap = language+ ":"+splitValue[0];
                            context.write(new Text(keyMap), new Text(valueMap));
                        }
                        
                    }*/
                String partsOfSpeech = key.toString().substring(key.toString().lastIndexOf('['),key.toString().length());
                String translations= key.toString().substring(0,key.toString().lastIndexOf('[')-1 );
                if(valid(partsOfSpeech)){
                    String keyMap= key + " : ["+partsOfSpeech+"]";
                    String valueMap = language+ ":"+translations;
                    System.out.println("key: "+keyMap);
                    context.write(new Text(keyMap), new Text(valueMap));
                }
            }
        
    }
    private boolean valid(String partsOfSpeech) {
        String[] words = {"Noun", "Pronoun", "Verb", "Adverb", "Adjective", "Preposition", "Conjunction"};  
        return (Arrays.asList(words).contains(partsOfSpeech));
    }
    
}
