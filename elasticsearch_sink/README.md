# Elasticsearch Sink
Optional: setup a template in Elasticsearch  
```
PUT _template/test_template
{
  "index_patterns": ["test-*"],
  "mappings": {
    "properties": {
      "name": {
        "type":"keyword"
      },
      "age":{
        "type":"integer"
      },
      "location":{
        "type":"geo_point"
      },
      "birthday":{
        "type":"date",
        "format": "yyyy-MM-dd HH:mm:ss"
      }
    }
  }
}
```
