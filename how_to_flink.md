### Flink
1. Download any binary you want from https://flink.apache.org/downloads.html
```
cd ~/Downloads 
tar xzf flink-*.tgz
cd flink-1.10.0
```

Then 
```
./bin/start-cluster.sh
```

And here you go, visit http://localhost:8081

To stop the cluster:
```
./bin/stop-cluster.sh
```