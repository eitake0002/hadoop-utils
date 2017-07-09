
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
#   Install requiement tools for install.
function install-requirement-tools()
{
  yum -y install openssh-clients rsync wget
}
