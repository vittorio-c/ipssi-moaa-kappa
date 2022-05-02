import hdfs


client = hdfs.InsecureClient("http://namenode:9870")
client.list("/")
