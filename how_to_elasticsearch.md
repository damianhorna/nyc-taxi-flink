To run:
```
cd elasticsearch-1.7.3
./bin/elasticsearch
```

Creating an index:
```
curl -XPUT "http://localhost:9200/nyc-idx"
```

Creating index mapping:
```
curl -XPUT "http://localhost:9200/nyc-idx/_mapping/popular-locations" -d'
{
 "popular-locations" : {
   "properties" : {
      "cnt": {"type": "integer"},
      "location": {"type": "geo_point"},
      "time": {"type": "date"}
    }
 } 
}'
```

To clear the nyc-idx index just drop the mapping:
```
curl -XDELETE 'http://localhost:9200/nyc-idx/popular-locations'
```

and recreate it.

In case of version 7.6.2:
```
curl -XPUT "http://localhost:9200/nyc-idx/_mapping/popular-locations?include_type_name=true" -d'
{                       
 "popular-locations" : {
   "properties" : {
      "cnt": {"type": "integer"},
      "location": {"type": "geo_point"},
      "time": {"type": "date"}
    }
 }                                   
}' -H'Content-Type: application/json'
```