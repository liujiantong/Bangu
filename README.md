# `Bangu（班固）` - Spark任务REST服务

Bangu（班固）是一个REST服务，用于从远端运行Spark ETL脚本，目前只支持python脚本。

**注意：** Bangu（班固）必须安装在`spark-submit`命令能正常运行的机器上。

## 安装`Bangu（班固）`
```
scp bangu.py root@172.172.172.165:bangu
scp bangu.service root@172.172.172.165:/usr/lib/systemd/system

systemctl enable bangu
systemctl start bangu
systemctl status bangu
```

## 如何打包python依赖库

可以使用两种方式安装Spark任务中使用python依赖。
1. 在Spark节点上安装 `pip install -r requirements.txt`
2. 执行脚本前使用一下方法打包成zip文件，然后用`py-files`参数上传

```
mkdir dependencies
pip install -t dependencies -r requirements.txt
cd dependencies
zip -r ../dependencies.zip .
```

## `BanguClient` 例子

可参考`test_bangu.py`

```python
import time
from bangu.banbu_client import BanguClient

# spark-submit命令参数设置
params = {
    "name": "pi computer",
    "executor-cores": "2",
    "num-executors": 2
}

client = BanguClient('http://172.172.172.165:8998')

py = '/Users/liutao/workspace/spark-solr-etl/pi.py'
ret_json = client.submit(py, params=params)
print('submit response: %s' % ret_json)

job_id = ret_json.get('job_id', None)
# job_id = '7ef71c08-f195-43df-b644-cb27d759a21e'

while True:
    job_json = client.get_job(job_id)
    print('job response: %s' % job_json)

    status = job_json.get('status', None)
    print('job:%s %s' % (job_id, status))

    if status == u'finished' or status == u'failed':
        break
    else:
        time.sleep(5)
```
