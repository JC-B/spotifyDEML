#On EC2
sudo yum update -y
sudo yum install -y python34-setuptools
sudo easy_install-3.4 pip
sudo python3 -m pip install pip awscli pyyaml ipython jupyter pandas boto3 -U
#aws configure
export PYSPARK_DRIVER_PYTHON=`which jupyter`
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.open_browser=False --NotebookApp.ip='*' --NotebookApp.port=8888"
#source .bashrc
#PYSPARK_DRIVER_PYTHON=ipython spark-submit spotify_make_parquets.py