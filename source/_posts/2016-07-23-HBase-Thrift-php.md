---
title: PHP通过Thrift接口访问HBase
date: 2016-07-23 21:43:45
categories: HBase
tags: [HBase, Thrift, PHP]
---
本文介绍使用Thrift接口实现PHP访问HBase。
<!-- more -->
## 安装Thrift
下载Thrift源码包thrift-0.8.0.tar.gz，解压后编译安装：
```sh
#首先安装依赖包
yum install automake libtool flex bison pkgconfig gcc-c++ boost-devel libevent-devel zlib-devel python-devel ruby-devel
#然后编译安装thrift
./configure
make
make install
```
## 生成php和HBase的接口文件
此处需要Hbase.thrift文件，HBase源码包中有该文件。
```sh
thrift -gen php hbase-version/src/main/resource/org/apache/hadoop/hbase/thrift/Hbase.thrift
```
执行完生成Hbase.php和Hbase_type.php两个文件，在gen-php/Hbase目录下。
## 将thrift php库和生成的接口文件提供给php程序调用
```sh
#将thrift/lib/php/src目录文件复制到web目录下
cp -a thrift-0.8.0/lib/php/src /var/www/html/hbasethrift/thrift
#将接口文件也复制过去
cp -a gen-php/Hbase /var/www/html/hbasethrift/thrift/src/packages
```
注：thrift php库和接口文件放置的位置不是绝对的，请灵活调整，确保php程序可以正确的引用到。
测试程序client.php ：
```php
<?php
$GLOBALS['THRIFT_ROOT']='/var/www/html/hbaseclient/thrift/src';
require_once( $GLOBALS['THRIFT_ROOT'] . '/Thrift.php' );
require_once( $GLOBALS['THRIFT_ROOT'] . '/transport/TSocket.php' );
require_once( $GLOBALS['THRIFT_ROOT'] . '/transport/TBufferedTransport.php' );
require_once( $GLOBALS['THRIFT_ROOT'] . '/protocol/TBinaryProtocol.php' );
require_once( $GLOBALS['THRIFT_ROOT'] . '/packages/Hbase/Hbase.php' );

$socket = new TSocket('localhost',9090);
$socket->setSendTimeout(10000);
$socket->setRecvTimeout(20000);

$transport = new TBufferedTransport($socket);
$protocol = new TBinaryProtocol($transport);
$client = new Hbase_HbaseClient($protocol);
$transport->open();

echo( "listing tables...\n" );
$tables = $client->getTableNames();
sort( $tables );
foreach ( $tables as $name ) {
    echo("  found: {$name}\n");
}
$transport->close();
?>
```
运行测试：
```sh
[root@hadoop04 hbaseclient]# php client.php    
listing tables...
  found: wanglei:20160628
  found: wanglei:20160706
  found: wanglei:20160707
[root@hadoop04 hbaseclient]# 
```
## 遇到的问题
网上很多例子中通过** $client = new HbaseClient($protocol) ** 创建客户端，我在运行测试程序时报错 PHP Fatal error:  Class 'HbaseClient' not found。程序找不HbaseClient类，查看生成的接口文件Hbase.php，里面定义的是Hbase_HbaseClient类，改成该类后测试成功。
```php
class Hbase_HbaseClient implements Hbase_HbaseIf {
  protected $input_ = null;
  protected $output_ = null;

  protected $seqid_ = 0;

  public function __construct($input, $output=null) {
    $this->input_ = $input;
    $this->output_ = $output ? $output : $input;
  }
  ……
}
```

