import os

import hdfs

client = hdfs.InsecureClient("http://namenode:9870")


data_directory = os.getcwd() + '/data'

for sub_directory in [x[0] for x in os.walk(data_directory)][1:]:
    year = sub_directory.split('/')[-1]
    hdfs_path = os.path.join(os.sep, 'lambda-user', 'moaa_data', year)
    print(f'Uploading {sub_directory} to {hdfs_path}')
    client.upload(
        hdfs_path,
        sub_directory,
        overwrite=True,
        temp_dir='/tmp/hadoop-upload',
        n_threads=4
    )
