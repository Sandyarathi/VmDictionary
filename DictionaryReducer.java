package Dictionary;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class DictionaryReducer extends Reducer<Text,Text,Text,Text> {
   
    //English-word: [part of speech] french:french-translation|german:german-translation|
    //italian:italian-translation|portuguese:portuguese-translation|spanish:spanish-translation
    private Text result = new Text();
    public void reduce(Text word, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
        boolean french=false;
        boolean german=false;
        boolean italian=false;
        boolean portuguese=false;
        boolean spanish=false;
        String frenchWord="";
        String germanWord="";
        String italianWord="";
        String portugueseWord="";
        String spanishWord="";
        String translations = "";
        for (Text val : values){
            System.out.println("Full Value :"+val);
            String[] language = val.toString().split(":");
            System.out.println("language:"+language[0]);
            System.out.println("values:"+language[1]);            
            if(language[0].matches("french")){
                french=true;
                frenchWord=language[0]+":"+listValues(language[1]);
            }
            else if(language[0].matches("german")){
                german=true;
                germanWord=language[0]+":"+listValues(language[1]);
            }
            else if(language[0].matches("italian")){
                italian=true;
                italianWord=language[0]+":"+listValues(language[1]);
            }
            else if(language[0].matches("portuguese*")){
                portuguese=true;
                portugueseWord=language[0]+":"+listValues(language[1]);
            }
            else if(language[0].matches("spanish*")){
                spanish=true;
                spanishWord=language[0]+":"+listValues(language[1]);
            }                
        }
        if(german==false){
            germanWord= "german:N/A";
        }
        if(italian == false) {
            italianWord= "italian:N/A";
        }
        if(french == false) {
            frenchWord= "french:N/A";
        }
        if(portuguese == false) {
            portugueseWord= "portuguese:N/A";
        }
        if(spanish == false) {
            spanishWord= "spanish:N/A";
        }        
        translations += frenchWord +" | "+germanWord+" | "+italianWord+" | "+portugueseWord+" | "+spanishWord;
        result.set(translations);
        context.write(word, result);
   }

   private String listValues(String word){
        String result = "";
        String[]tokens = word.split(",|;");
        if(tokens.length >1){
            for(int i=0;i<tokens.length;i++){
                result+=tokens[i]+',';
            }
            result = result.substring(0,result.length()-1);
        }
        else
            result=word;
        return result;

   }
    
}
  
