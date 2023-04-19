# -*- coding=utf-8
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
from qcloud_cos import CosServiceError
from qcloud_cos.cos_threadpool import SimpleThreadPool
import os
#import sys
#import logging
import hashlib
#from logging_init import logger

#logging.basicConfig(level=logging.INFO, stream=sys.stdout)

class Cos_helper:
    secret_id = ""
    secret_key = ""
    region = ""
    token = None
    config = None
    client = None
    bucket = None
    file_path = None
    cos_path = ''
    
    def __init__(self, secret_id, secret_key, region, bucket, domain):
        # 腾讯云COSV5Python SDK, 目前可以支持Python2.6与Python2.7以及Python3.x

        # pip安装指南:pip install -U cos-python-sdk-v5

        # cos最新可用地域,参照https://www.qcloud.com/document/product/436/6224

        # 设置用户属性, 包括secret_id, secret_key, region
        # appid已在配置中移除,请在参数Bucket中带上appid。Bucket由bucketname-appid组成
        self.secret_id = secret_id  # 替换为用户的secret_id
        self.secret_key = secret_key  # 替换为用户的secret_key
        self.region = region  # 替换为用户的region
        self.bucket = bucket # 存储桶名
        self.token = None  # 使用临时密钥需要传入Token，默认为空,可不填
        self.config = CosConfig(Region=self.region, SecretId=self.secret_id, SecretKey=self.secret_key, Token=self.token)  # 获取配置对象
        self.client = CosS3Client(self.config)
        self.cos_path = 'https://'+bucket+'.cos.'+region+'.'+domain+'/'

    def reset(self, secret_id, secret_key, region, bucket, domain):
        self.secret_id = secret_id  # 替换为用户的secret_id
        self.secret_key = secret_key  # 替换为用户的secret_key
        self.region = region  # 替换为用户的region
        self.bucket = bucket # 存储桶名
        self.token = None  # 使用临时密钥需要传入Token，默认为空,可不填
        self.config = CosConfig(Region=self.region, SecretId=self.secret_id, SecretKey=self.secret_key, Token=self.token)  # 获取配置对象
        self.client = CosS3Client(self.config)
        self.cos_path = 'https://'+bucket+'.cos.'+region+'.'+domain+'/'

    def get_cos_files_list(self, marker_key, maxsize, cos_path):
        print('get_cos_files_list path: '+ cos_path + '\n')
        marker = 0
        maxKeys = 1000
        if marker_key != None:
            marker = marker_key
        if maxsize != None:
            maxKeys = maxsize
        response = self.client.list_objects(
            Bucket = self.bucket,
            Prefix = cos_path,
            Marker = marker,
            MaxKeys = maxKeys,
        )
        #logger.debug(response)
        if 'Contents' not in response:
            return None
        print(response['Contents']) 
        return response['Contents']
    
    def get_local_files_list(self, files_path):
        print('get_local_files_list\n')
        file_name_list = os.listdir(files_path)
        return file_name_list
        
    def get_local_file_md5(self, file_name):
        md5_str = None
        file_object = open(file_name, 'rb')
        file_content = file_object.read()
        file_object.close()
        md5 = hashlib.md5()
        md5.update(file_content)
        md5_str = md5.hexdigest()
        print('get_local_file_md5 file: %s, md5: %s\n' % (file_name, md5_str))
        return md5_str

    def delete_file_from_cos(self, cos_path):
        print('delete_file_from_cos file: %s\n' % cos_path)
        response = self.client.delete_object(
            Bucket=self.bucket,
            Key=cos_path
        )
        print(response)

    def push_file_to_cos(self, local_path, cos_path):
        print('push_file_to_cos file: %s\n' % (cos_path))
        # md5_str = self.get_local_file_md5(local_path)
        # 高级上传接口(推荐)
        response = self.client.upload_file(
            Bucket=self.bucket,
            LocalFilePath=local_path,
            Key=cos_path,
            PartSize=10,
            MAXThread=10,
            EnableMD5=False,
            # Metadata={
            #     'x-cos-meta-md5' : md5_str      #设置自定义参数，设置为 MD5 校验值
            # }
        )
        print(response['ETag'])

    def pull_file_from_cos(self, local_path, cos_path):
        print('pull_file_from_cos file: %s\n' % cos_path)
        # 文件下载 获取文件到本地
        response = self.client.download_file(
            Bucket=self.bucket,
            Key=cos_path,
            DestFilePath=local_path,
            PartSize=10,
            MAXThread=10
        )
        

    def sync_file_to_cos(self, local_path, cos_path):
        file_exist = False
        maxsize = 100
        total_cos_file_list = None
        if not os.path.exists(local_path):
           os.makedirs(local_path)
        local_file_name_list = self.get_local_files_list(local_path)
        print(local_file_name_list)
        # 获取cos上指定路径下的所有文件
        cos_file_list = self.get_cos_files_list("", maxsize, cos_path)
        total_cos_file_list = cos_file_list
        while cos_file_list and len(cos_file_list) == maxsize:
            print("key: " + total_cos_file_list[-1]['Key'])
            cos_file_list = self.get_cos_files_list(total_cos_file_list[-1]['Key'], maxsize, cos_path)
            if cos_file_list == None:
                break
            total_cos_file_list += cos_file_list

        if total_cos_file_list != None:
            print(total_cos_file_list)
        
        # cos没有该文件，直接上传，有则比对MD5是否不一样，再上传
        for local_file_name in local_file_name_list:
            file_exist = False
            if total_cos_file_list != None:
                for cos_file in total_cos_file_list:
                    if (cos_path + local_file_name) == cos_file['Key']:
                        file_exist = True
                        md5_str = self.get_local_file_md5(local_path + local_file_name)
                        print(cos_file['ETag'])
                        print(md5_str != cos_file['ETag'][1:-1])
                        if md5_str != cos_file['ETag'][1:-1]:
                            print(local_path + local_file_name)
                            self.push_file_to_cos(local_path + local_file_name, cos_file['Key'])
                        break
            if file_exist == False:
                md5_str = self.get_local_file_md5(local_path + local_file_name)
                self.push_file_to_cos(local_path + local_file_name, cos_path + local_file_name)
        # 删除cos上本地没有的文件
        for i in range(len(local_file_name_list)):
           local_file_name_list[i] = cos_path + local_file_name_list[i]

        if total_cos_file_list != None:
            for cos_file in total_cos_file_list:
                if cos_file['Key'] == local_path:
                    continue
                if cos_file['Key'] not in local_file_name_list:
                    self.delete_file_from_cos(cos_file['Key'])

    def sync_file_from_cos(self, local_path, cos_path):
        file_exist = False
        maxsize = 100
        total_cos_file_list = None
        total_cos_file_path_list = []
        if not os.path.exists(local_path):
           os.makedirs(local_path)
        local_file_name_list = self.get_local_files_list(local_path)
        print(local_file_name_list)
        # 获取cos上指定路径下的所有文件
        cos_file_list = self.get_cos_files_list("", maxsize, cos_path)
        total_cos_file_list = cos_file_list
        while cos_file_list and len(cos_file_list) == maxsize:
            print("key: " + total_cos_file_list[-1]['Key'])
            cos_file_list = self.get_cos_files_list(total_cos_file_list[-1]['Key'], maxsize, cos_path)
            if cos_file_list == None:
                break
            total_cos_file_list += cos_file_list

        if total_cos_file_list != None:
            print(total_cos_file_list)
        else:
            print("cos path:%s is not exist\n", cos_path)
            return
        # 本地没有该文件，直接下载，有则比对MD5是否不一样，再下载
        for cos_file in total_cos_file_list:
            if cos_file['Key'] == local_path:
                continue
            total_cos_file_path_list.append(cos_file['Key'])
            file_exist = False
            if local_file_name_list != None and len(local_file_name_list):
                for local_file_name in local_file_name_list:
                    if (cos_path + local_file_name) == cos_file['Key']:
                        file_exist = True
                        md5_str = self.get_local_file_md5(local_path + local_file_name)
                        print(cos_file['ETag'])
                        print(md5_str != cos_file['ETag'][1:-1])
                        if md5_str != cos_file['ETag'][1:-1]:
                            print(local_path + local_file_name)
                            self.pull_file_from_cos(local_path + local_file_name, cos_file['Key'])
                        break
            if file_exist == False:
                self.pull_file_from_cos(local_path + cos_file['Key'][len(cos_path):], cos_file['Key'])
        
        # 删除cos上没有的本地文件
    
        if local_file_name_list != None and len(local_file_name_list):
            for local_file in local_file_name_list:
                if total_cos_file_path_list != None:
                    if cos_path + local_file not in total_cos_file_path_list:
                        os.remove(local_path + local_file)
