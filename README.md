# zk-tools
ops tools for zookeeper

### backup
	dump zookeeper data to file
```
./backup -host 1.1.1.1 -port 2181 -dir ./
```

### backup
	restore data to zookeeper from backup file
```
./restore -backupFile ../backup/20170704-0945.txt -host 2.2.2.2 -port 2181
```
