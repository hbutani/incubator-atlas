#!/usr/bin/env python

'''
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
import sys
from os import environ
from mock import patch
import unittest
import logging
import atlas_config as mc
import atlas_start as atlas
import platform

IS_WINDOWS = platform.system() == "Windows"
logger = logging.getLogger()

class TestMetadata(unittest.TestCase):
  @patch.object(mc,"win_exist_pid") 
  @patch.object(mc,"unix_exist_pid") 
  @patch.object(mc,"writePid")
  @patch.object(mc, "executeEnvSh")
  @patch.object(mc,"atlasDir")
  @patch.object(mc, "expandWebApp")
  @patch("os.path.exists")
  @patch.object(mc, "java")

  def test_main(self, java_mock, exists_mock, expandWebApp_mock, atlasDir_mock, executeEnvSh_mock, writePid_mock, unix_exist_pid_mock, win_exist_pid_mock):
    sys.argv = []
    exists_mock.return_value = True
    expandWebApp_mock.return_value = "webapp"
    atlasDir_mock.return_value = "atlas_home"

    win_exist_pid_mock("789")
    win_exist_pid_mock.assert_called_with((str)(789))
    unix_exist_pid_mock(789)
    unix_exist_pid_mock.assert_called_with(789)
    atlas.main()
    self.assertTrue(java_mock.called)
    if IS_WINDOWS:
      
      java_mock.assert_called_with(
        'org.apache.atlas.Atlas',
        ['-app', 'atlas_home\\server\\webapp\\atlas'],
        'atlas_home\\conf;atlas_home\\server\\webapp\\atlas\\WEB-INF\\classes;atlas_home\\server\\webapp\\atlas\\WEB-INF\\lib\\atlas-titan-${project.version}.jar;atlas_home\\server\\webapp\\atlas\\WEB-INF\\lib\\*;atlas_home\\libext\\*;atlas_home\\hbase\\conf',
        ['-Datlas.log.dir=atlas_home\\logs', '-Datlas.log.file=application.log', '-Datlas.home=atlas_home', '-Datlas.conf=atlas_home\\conf', '-Xmx1024m', '-XX:MaxPermSize=512m', '-Dlog4j.configuration=atlas-log4j.xml', '-Djava.net.preferIPv4Stack=true'], 'atlas_home\\logs')
      
    else:
      java_mock.assert_called_with(
        'org.apache.atlas.Atlas',
        ['-app', 'atlas_home/server/webapp/atlas'],
        'atlas_home/conf:atlas_home/server/webapp/atlas/WEB-INF/classes:atlas_home/server/webapp/atlas/WEB-INF/lib/atlas-titan-${project.version}.jar:atlas_home/server/webapp/atlas/WEB-INF/lib/*:atlas_home/libext/*:atlas_home/hbase/conf',
        ['-Datlas.log.dir=atlas_home/logs', '-Datlas.log.file=application.log', '-Datlas.home=atlas_home', '-Datlas.conf=atlas_home/conf', '-Xmx1024m', '-XX:MaxPermSize=512m', '-Dlog4j.configuration=atlas-log4j.xml', '-Djava.net.preferIPv4Stack=true'],  'atlas_home/logs')

    pass

  def test_jar_java_lookups_fail(self):
    java_home = environ['JAVA_HOME']
    del environ['JAVA_HOME']
    orig_path = environ['PATH']
    environ['PATH'] = "/dev/null"

    self.assertRaises(EnvironmentError, mc.jar, "foo")
    self.assertRaises(EnvironmentError, mc.java, "empty", "empty", "empty", "empty")

    environ['JAVA_HOME'] = java_home
    environ['PATH'] = orig_path

  @patch.object(mc, "runProcess")
  @patch.object(mc, "which", return_value="foo")
  def test_jar_java_lookups_succeed_from_path(self, which_mock, runProcess_mock):
    java_home = environ['JAVA_HOME']
    del environ['JAVA_HOME']

    mc.jar("foo")
    mc.java("empty", "empty", "empty", "empty")

    environ['JAVA_HOME'] = java_home

if __name__ == "__main__":
  logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
  unittest.main()
