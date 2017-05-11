#!/usr/bin/sh
aws emr create-cluster --auto-scaling-role EMR_AutoScaling_DefaultRole --termination-protected --applications Name=Hadoop Name=Zeppelin Name=Spark Name=Hive --ec2-attributes '{"KeyName":"gu5sfdejcadmin","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-5406260f","EmrManagedSlaveSecurityGroup":"sg-5ae2a225","EmrManagedMasterSecurityGroup":"sg-0ddf9f72"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-5.5.0 --log-uri 's3n://aws-logs-105505458701-us-east-1/elasticmapreduce/spotify/' --steps '[{"Args":["spark-submit","--deploy-mode","cluster","s3://labscripts/reducer.py"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark application"}]' --name 'spotifysimpleemr' --instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"MASTER","InstanceType":"m4.xlarge","Name":"Master - 1"},{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"CORE","InstanceType":"m4.xlarge","Name":"Core - 2"}]' --scale-down-behavior TERMINATE_AT_INSTANCE_HOUR --region us-east-1
#aws emr list-instances --cluster-id j-C1G2OT9QY4PL
#server=ec2-52-90-235-236.compute-1.amazonaws.com
scp -i ~/.ssh/gu5sfdejcadmin.pem ~/.ssh/gu5sfdejcadmin.pem hadoop@$server:/home/hadoop/.ssh/
scp -i ~/.ssh/gu5sfdejcadmin.pem Predict_genre.py hadoop@$server:/home/hadoop/

# On EC2
# spark-submit Lab71BatchSparkML.py > Lab71BatchSparkMLoutput.log
# ('# of rows: ', 58525)
# ('AUC for Random Forest:', 0.9110919373915368)