### Script Descriptions

- `dataset_cleaned_headers_body.py`:  
  Reads dataset and parses it to create columns: `headers`, `body`.

- `dataset_groupby_subject.py`:  
  Groups the emails in the All Documents folder by subject. 

- `dataset_Confidential_cleaned_header_body.py`:  
  Filters the dataset by confidential in the subject

- `pii_entities.py`:  
  Runs the Hugging Face PII model. Uses NER to identify entities and provides a confidence score.

- `dataset.py`:  
  Creates a dataset by randomly picking `n` rows from the full dataset.  
  >  Note: `n` is currently hardcoded in the file.  

