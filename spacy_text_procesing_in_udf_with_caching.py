### Assumptions:
# You have en_core_web_sm.zip downloaded locally
# You have the necessary packages installed including cachetools, spacy, beautifulsoup4

### Results:
# Calling UDF process_text() in a DataFrame with 1000 rows -- once per row
# ~8 MINUTES -- WITHOUT caching by not adding @cached(cache={}) decorator on function load_en_core_web_sm()
# ~5 SECONDS -- WITH caching by adding @cached(cache={}) decorator on function load_en_core_web_sm(): 

# Create staging area 
session.sql("create or replace stage dash_udf_imports").collect()
# Upload file to the staging area
session.file.put("file:///Users/ddesai/en_core_web_sm.zip", "@dash_udf_imports/")

# Clear imports
session.clear_imports()
# Add file dependency
session.add_import('@dash_udf_imports/en_core_web_sm.zip.gz')

# Create function that caches results after downloading and extracting a file
from cachetools import cached

@cached(cache={})
def load_en_core_web_sm(input_file: str, output_dir: str)-> object:
    import zipfile
    import spacy
            
    with zipfile.ZipFile(input_file, 'r') as zip_ref:
        zip_ref.extractall(output_dir)
        
    # Load and return spacy's optimized "English" pipeline (en_core_web_sm)
    nlp = spacy.load(output_dir + "/en_core_web_sm/en_core_web_sm-2.3.0")
    return nlp 


# Create UDF that leverages the cached version of spacy's optimized "English" pipeline (en_core_web_sm) and process the passing in text 
@udf(session=session,packages=['spacy==2.3.5','beautifulsoup4','cachetools==4.2.2'],replace=True)
def process_text(text: str) -> str:
    import os
    import sys
    import spacy
    from bs4 import BeautifulSoup 
    from spacy.tokenizer import Tokenizer
                       
    IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
    import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
    
    input_file = import_dir + 'en_core_web_sm.zip'
    output_dir = '/tmp/en_core_web_sm' + str(os.getpid())
    
    nlp = load_en_core_web_sm(input_file,output_dir)
    stop_words = nlp.Defaults.stop_words
    tokenizer = Tokenizer(nlp.vocab)
    
    # strip html
    text = BeautifulSoup(text, "html.parser").get_text()
    
    # tokenize
    tokens = tokenizer(text)
    
    # lemmatize verbs and remove stop words
    text = [str(t.lemma_) for t in tokens if (t.orth_) not in stop_words] 

    return text


### Test code
# Call UDF process_text() in a DataFrame with 1000 rows -- once per row
%%time
df = session.createDataFrame([[1, 2, 3, 4]] * 1000, schema=["a", "b", "c", "d"])
df = df.with_columns(['hw'],[process_text(lit("He determined to drop his litigation with the monastry, and relinguish his claims to the wood-cuting and fishery rihgts at once. He was the more ready to do this becuase the rights had become much less valuable, and he had indeed the vaguest idea where the wood and river in question were."))])
df.collect()

### Results:
# Calling UDF process_text() in a DataFrame with 1000 rows
# ~8 MINUTES -- WITHOUT caching by not adding @cached(cache={}) decorator on function load_en_core_web_sm()
# ~5 SECONDS -- WITH caching by adding @cached(cache={}) decorator on function load_en_core_web_sm(): 
