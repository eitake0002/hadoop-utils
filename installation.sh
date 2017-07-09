
# Description:
#   Install JDK1.8-devel
function install-jdk-8()
{
  yum -y install java-1.8.0-openjdk-devel.x86_64
}

# Description:
#   Check Java version.
function java-version()
{
  java -version
}

# Description:
#   Set JAVA_HOME on ~/.bash_profile
function set-java-home()
{
  echo 'export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.131-0.b11.el6_9.x86_64' >> ~/.bash_profile
}

# Description:
#   Install requiement tools for install.
function install-requirement-tools()
{
  yum -y install openssh-clients rsync wget
}

# Description:
#   Download hadoop2.8.0 package.
function download-hadoop-pkg()
{
  wget http://ftp.riken.jp/net/apache/hadoop/common/hadoop-2.8.0/hadoop-2.8.0.tar.gz
}

# Description:
#   Uncompress download file.
function uncompress-tar-gz()
{
  tar -zxvf hadoop-2.8.0.tar.gz
}
