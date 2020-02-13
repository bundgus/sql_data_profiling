# sql_data_profiling
data profiling with sql and python

RedHat Linux Environment

sudo yum install rh-python36-python-devel.x86_64
sudo yum install devtoolset-8

mkdir ~/sql_data_profiling
cd ~/sql_data_profiling
/opt/rh/rh-python36/root/bin/virtualenv --python=/opt/rh/rh-python36/root/bin/python3.6 python36
source python36/bin/activate
pip install --upgrade pip