"""
SFTP Util
reference : http://docs.paramiko.org/en/2.7/api/sftp.html
"""

# !/usr/bin/python3
# coding: utf-8

import os
import stat
import traceback

import paramiko


class SFTP(object):
    """
    connect to SFTP server, download or upload file, disconnect
    """
    host = '127.0.0.1'
    port = 22
    user = 'fred'
    pwd = 'fish'

    manager = None
    client = None

    def __init__(self, host, port, user, pwd):
        self.host = host
        self.port = port
        self.user = user
        self.pwd = pwd

        self.manager = None
        self.client = None

    def connect(self):
        """
        connect to sftp server
        :return:
        """
        manager = paramiko.Transport((self.host, self.port))
        manager.connect(username=self.user, password=self.pwd)
        self.manager = manager

        client = paramiko.SFTPClient.from_transport(manager)
        self.client = client

    def quit(self):
        """
        disconnect to sftp server
        :return:
        """
        if not self.client:
            self.client.close()

        if not self.manager:
            self.manager.close()

    def exists(self, remote_path):
        """
        remote file already exists
        :param remote_path:
        :return:
        """
        try:
            self.client.stat(remote_path)
            return True
        except FileNotFoundError:
            return False

    def isdir(self, remote_dir):
        """
        remote path is folder
        :param remote_dir: remote folder path
        :return:
        """
        if not self.exists(remote_dir):
            return False

        if stat.S_ISDIR(self.client.stat(remote_dir).st_mode):
            return True
        else:
            return False

    def isfile(self, remote_path):
        """
        remote path is file
        :param remote_path: remote file path
        :return:
        """
        if not self.exists(remote_path):
            return False

        if stat.S_ISREG(self.client.stat(remote_path).st_mode):
            return True
        else:
            return False

    def listdir(self, remote_dir):
        """
        list remote file paths by sftp server
        :param remote_dir: remote folder path
        :return:
        """
        paths = []
        if not self.isdir(remote_dir):
            paths.append(remote_dir)
            return paths

        remote_names = self.client.listdir(remote_dir)
        # Sorry, I can't resolve issues like
        # UnicodeDecodeError: 'utf-8' codec can't decode bytes in position ?: invalid continuation byte

        for remote_name in remote_names:
            remote_path = os.path.join(remote_dir, remote_name)
            if self.isdir(remote_path):
                paths2 = self.listdir(remote_path)
                if paths2:
                    paths.extend(paths2)
            else:
                paths.append(remote_path)
        return paths

    def mkdir(self, remote_dir):
        self.client.mkdir(remote_dir)

    def remove(self, remote_path):
        """
        delete remote file in sftp server
        :param remote_path: remote file path
        :return:
        """
        if self.isfile(remote_path):
            self.client.remove(remote_path)
        else:
            self.client.rmdir(remote_path)

    def download(self, remote_path, local_dir):
        """
        download file from sftp server
        :param remote_path: remote file path
        :param local_dir: local folder path
        :return:
        """
        # if not os.path.exists(local_dir):
        #     os.makedirs(local_dir)

        remote_name = os.path.basename(remote_path)
        local_path = os.path.join(local_dir, remote_name)

        self.client.get(remote_path, local_dir)

    def upload(self, local_path, remote_path):
        """
        upload file to sftp server
        :param local_path: local file path
        :param remote_dir: remote folder path
        :return:
        """
        # if not os.path.isfile(local_path):
        #     raise FileNotFoundError('File %s is not found' % local_path)
        #
        # if not self.exists(remote_dir):
        #     self.mkdir(remote_dir)
        #
        # local_name = os.path.basename(local_path)
        # remote_path = os.path.join(remote_dir, local_name)

        self.client.put(local_path, remote_path)


if __name__ == '__main__':
    sftp = None
    try:
        sftp = SFTP('127.0.0.1', 22, 'admin', '123456')
        sftp.connect()
        lns = os.listdir('D:/本地的')
        for ln in lns:
            lp = os.path.join('D:/本地的', ln)
            sftp.upload(lp, '/远程的/')
    except:
        traceback.print_exc()
    finally:
        if sftp:
            sftp.quit()