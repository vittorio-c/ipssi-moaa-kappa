import os

import hdfs

client = hdfs.InsecureClient("http://namenode:9870")

for year in range(2007, 2023):
    hdfs_path = os.path.join(os.sep, 'lambda-user', 'moaa_data', str(year))
    local_path = os.getcwd() + f'/data/{str(year)}'
    print(f'Uploading {local_path} to {hdfs_path}')
    client.upload(
        hdfs_path,
        local_path,
        overwrite=True,
        temp_dir='/tmp/hadoop-upload',
        n_threads=4
    )
