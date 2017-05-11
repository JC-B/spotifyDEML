vagrant up
server=`vagrant awsinfo -k host`
scp -i ~/.ssh/gu5sfdejcadmin.pem ~/.ssh/gu5sfdejcadmin.pem ubuntu@$server:/home/ubuntu/.ssh/
scp -i ~/.ssh/gu5sfdejcadmin.pem constants.cfg ubuntu@$server:/home/ubuntu/
scp -i ~/.ssh/gu5sfdejcadmin.pem offset.cfg ubuntu@$server:/home/ubuntu/
scp -i ~/.ssh/gu5sfdejcadmin.pem project_utils.py ubuntu@$server:/home/ubuntu/
scp -i ~/.ssh/gu5sfdejcadmin.pem aws_operations.py ubuntu@$server:/home/ubuntu/
scp -i ~/.ssh/gu5sfdejcadmin.pem spotify_stream.py ubuntu@$server:/home/ubuntu/
scp -i ~/.ssh/gu5sfdejcadmin.pem run_me.py ubuntu@$server:/home/ubuntu/
ssh -i ~/.ssh/gu5sfdejcadmin.pem ubuntu@$server chmod +x /home/ubuntu/*.py
ssh -i ~/.ssh/gu5sfdejcadmin.pem ubuntu@$server mkdir /home/ubuntu/tmp
ssh -i ~/.ssh/gu5sfdejcadmin.pem ubuntu@$server mkdir /home/ubuntu/log
#grep -o "," /home/ubuntu/tmp/atrack | wc -l