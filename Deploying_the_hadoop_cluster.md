# Introduction #
This would help you deploy the hadoop cluster, namenode and datanodes included, in a linux-like system. We have done a very good test in ubuntu 8.04, and we are pretty sure that other os could also be available.

Now we've updated to hadoop 0.21.0 deployed in ubuntu 10.04 or higher version


# Details #

一：前期环境准备
（1）安装sun的JDK
sudo apt-get install sun-java6-jdk，如果你不确定是不是已经安装过了JDK，可以用命令：java -version来看看。
如果说，java version不是sun的，或者是说java不是内部命令，那么就需要安装了。
（2）配置JDK环境变量
JDK一般默认安装到/usr/lib/jvm/java-6-sun下面，包括可执行程序以及类库都在这下面，可以用cd /usr/lib/jvm/java-6-sun命令查看一下。
配置路径，一个是/etc/environment文件，一个是~/.bashrc文件，分别是这样的：
/etc/environment文件：
CLASSPATH=/usr/lib/jvm/java-6-sun/lib
JAVA\_HOME=/usr/lib/jvm/java-6-sun
~/.bashrc的最末行加上
export JAVA\_HOME=/usr/lib/jvm/java-6-sun
export CLASSPATH=.:$JAVA\_HOME/lib/dt.jar:$JAVA\_HOME/lib/tools.jar
export PATH=.:$PATH:$JAVA\_HOME/bin:$JAVA\_HOME/jre/bin
（3）安装SSH Server用以下命令来安装：
sudo apt-get install ssh
sudo apt-get install rsync
如果出现版本不一样的问题，先将已经装载的server或client卸载，然后重装。
（4）免密码ssh配置
生成密钥，命令如下：
sudo ssh-keygen -t dsa -P '' -f ~/.ssh/id\_dsa
sudo cat ~/.ssh/id\_dsa.pub >> ~/.ssh/authorized\_keys
配置完成后，执行ssh localhsot,确认每台机器都可以使用ssh
（5）在所有机器修改 /etc/hosts文件
192.168.XXX.XXX     namenode
192.168.XXX.XXX     datanode
（6）将namenode服务器上的authorized\_keys的内容加到datanode台机器的authorized\_keys文件中。让namenode可以不需要密码访问datanode服务器
sudo scp authorized\_keys XXX:/home/XXX/.ssh/	// cis:/home/xuwq/.ssh/ 或者直接用移动介质拷贝
ssh XXX 					// ssh cis
确认namenode可以免密码访问每台datanode

（7）搭建nfs文件系统（具体参考鸟哥私房菜http://linux.vbird.org/linux\_server/0330nfs.php#nfsserver\_exports）

在namenode上安装nfs server 和client
sudo apt-get install nfs-kernel-server nfs-common portmap(很对老版本ubuntu，新版本默认集成)
在每个datanode上安装nfs client
sudo apt-get install nfs-common protmap
然后在每个节点上创建nfs共享目录 nfs\_home，保证每个节点都有相同的绝对路径
mkdir nfs\_home

在这里我们用$NFS\_HOME表示nfs文件的绝对路径

在namenode上运行sudo /etc/init.d/nfs-kernel-server start启动nfs-server
修改/etc/exports添加如下信息：
$NFS\_HOME datanode1(rw,sync) #后面表示数据节点访问共享文件夹的访问权限，/etc/exports有介绍，主要的权限有rw, ro, wo, sync, no\_subtree\_check等，注意各个权限用“,”分隔，不能有空格；
修改好之后，在namenode上运行sudo exports -rv，这步没做则各节点无法mount

随后在每个datanode上运行如下命令挂在共享文件(括号中为注释内容)
sudo mount namenode:$NFS\_HOME(@namenode) $NFS\_HOME(@each datanode's local)


二：安装配置Hadoop分布式系统:
（1）配置Hadoop环境变量
下载hadoop安装包以后
tar xvf hadoop-xxx.tat.gz $NFS\_HOME/hadoop （注意拷贝到nfs共享文件夹底下）
在/etc/environment文件尾添加：
HADOOP\_HOME= $NFS\_HOME/hadoop
HADOOP\_CONF\_DIR=$HADOOP\_HOME/conf
HADOOP\_LOG\_DIR=$HADOOP\_HOME/log
在~/.bashrc文件尾添加:
HADOOP\_HOME= $NFS\_HOME/hadoop#hadoop的主目录
export HADOOP\_HOME
HADOOP\_CONF\_DIR=$HADOOP\_HOME/conf   #hadoop的配置文件目录
export HADOOP\_CONF\_DIR
HADOOP\_LOG\_DIR= $NFS\_HOME/hadoop/log    #存放运行日志目录
export HADOOP\_LOG\_DIR
export PATH=$PATH:$HADOOP\_HOME/bin   //这一句可以由下面一句替代，将JDK和Hadoop的路径写在一起
export PATH=.:$PATH:$JAVA\_HOME/bin:$JAVA\_HOME/jre/bin:$HADOOP\_HOME/bin
修改完毕后需要重启。
（2）修改hadoop的conf/masters和conf/slaves文件
conf/masters	内容为/etc/hosts里配置的namenode名，namenode
conf/slaves	内容为/etc/hosts里配置的每个datanode名，datanode1, ....
（3）修改conf/hadoop-env.sh文件
添加jdk路径	export JAVA\_HOME=/usr/lib/jvm/java-6-sun
（4）修改conf/hadoop-site.xml (hadoop-0.18.3)


&lt;configuration&gt;


> 

&lt;property&gt;


> > 

&lt;name&gt;

fs.default.name

&lt;/name&gt;

	//namenode的配置，机器名加端口
> > 

&lt;value&gt;

hdfs://XXX:54310/

&lt;/value&gt;



> 

&lt;/property&gt;


> 

&lt;property&gt;


> > 

&lt;name&gt;

mapred.job.tracker

&lt;/name&gt;

	//JobTracker的配置，机器名加端口
> > > 

&lt;value&gt;

hdfs://XXX:54311

&lt;/value&gt;



> 

&lt;/property&gt;


> 

&lt;property&gt;


> > 

&lt;name&gt;

dfs.replication

&lt;/name&gt;

	//数据备份的数量，默认是3
> > 

&lt;value&gt;

1

&lt;/value&gt;

			//这里是1台namenode，1台datanode，所以value=1，建议value值小于等于datanode的数量

> 

&lt;/property&gt;


> 

&lt;property&gt;




Unknown end tag for &lt;/configuration&gt;



注意从hadoop-0.20开始分成几个文件分别配置
##core-site.xml### 配置hdfs


&lt;configuration&gt;


> 

&lt;property&gt;


> > 

&lt;name&gt;

fs.default.name

&lt;/name&gt;

	//namenode的配置，机器名加端口  or hadoop.defaultFS
> > 

&lt;value&gt;

hdfs://namenode:9000/

&lt;/value&gt;



> 

&lt;/property&gt;


> 

&lt;property&gt;



##hdfs-site.xml### 配置hdfs的基本参数，配置在core-site.xml应该也可


&lt;configuration&gt;




&lt;property&gt;




&lt;name&gt;

dfs.replication

&lt;/name&gt;




&lt;value&gt;

1

&lt;/value&gt;




&lt;/property&gt;




&lt;/configuration&gt;



##mapred-site.xml### map reduce相关的最好配置在这个文件底下


&lt;configuration&gt;





&lt;property&gt;




&lt;name&gt;

mapreduce.jobtracker.address

&lt;/name&gt;




&lt;value&gt;

hdfs://namenode:9001/

&lt;/value&gt;




&lt;/property&gt;





&lt;/configuration&gt;


~



> （5）启动集群
先要格式化namenode, 执行hadoop namenode -format
如果之前已经运行过然后停止系统的话，最后在每个节点上将存放的上次运行的data删除，如果没有配置过，应该是在/tmp/hadoop\*文件夹底下
rm -rf /tmp/hadoop

然后启动hdfs文件系统，执行start-all.sh，启动后可以用jps查看进程
conf/masters文件里的机器运行的进程有NameNode, SecondaryNameNode, JobTracker
conf/slaves文件里的机器运行的进程有DataNode, TaskTracker

在hadoop-0.20以后，用两个命令代替上述命令：
start-dfs.sh
start-mapred.sh
（6）停止集群
stop-all.sh
相应的也替代为
stop-dfs.sh
stop-mapred.sh

例子:
例1：
# 先将待测的文件放到本地文件系统的/home/test-in目录
$ mkdir test-in
$ cd test-in
$ echo "hello world bye world cdh" >file1.txt
$ echo "hello hadoop goodbye hadoop" >file2.txt
# 将本地文件系统上的 /home/test-in 目录拷到 HDFS 的根目录上，目录名改为 input
$ hadoop fs –put test-in input
#查看执行结果:
$ hadoop jar $HADOOP\_HOME/hadoop-xxx-examples.jar wordcount input output
# 将文件从 HDFS 拷到本地文件系统中再查看：
$ hadoop fs -get output output
$ cat output/**# 也可以直接查看
$ hadoop fs -cat output/**

例2：（文档中例）
将输入文件拷贝到分布式文件系统：
$ hadoop fs -put $HADOOP\_HOME/conf input
运行发行版提供的示例程序：
$ hadoop jar $HADOOP\_HOME/hadoop-xxx-examples.jar grep input output 'dfs[a-z.]+'
查看输出文件：
将输出文件从分布式文件系统拷贝到本地文件系统查看：
$ hadoop fs -get output output
$ cat output/**或者
在分布式文件系统上查看输出文件：
$ hadoop fs -cat output/**

注意问题：
（1）所有机器的机器名必须不同，但是要保证每台机器上均有一位相同的用户
（2）可能会遇到HADOOP-1212的bug
org.apache.hadoop.dfs.DataNode: java.io.IOException: Incompatible namespaceIDs in the logs of a datanode..............
原因如下：
hadoop格式化时重新创建namenodeID而在datanode的tmp/dfs/data中包含上次format留下的ID,格式化清空namenode的数据但是保留了datanode的数据
解决方案是这样的：
(1) stop-all.sh停止所有进程；
(2）删除filesystem/data这个文件夹；	// 默认为tmp/dfs/data
(3）hadoop namenode –format重新格式化namenode；
(4) 重新启动