# EMR 
create EMR cluster using AWS console and get .pem file
update security group to open up ssh on master node
load cfg file and etl.py file to masternode node directory

# Execute spark job
```bash
spark-submit etl.py --master yarn --deploy-mode client --driver-memory 4g --num-executors 2 --executor-memory 2g --executor-core 2
```

# validate results
verify results on the output folder