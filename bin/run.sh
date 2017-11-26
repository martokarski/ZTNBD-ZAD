if [[ -z "$1" ]]; then
	echo "Syntax:"
	echo "run <server_user> [silent]"
	exit
fi

SPARK_STREAM=
if [ "$2" == "silent" ]; then
    SPARK_STREAM="2>/dev/null"
fi

# Configuration variables
MAIN_SCRIPT="run.py"
SERVER_USER=$1
SERVER_HOST=153.19.52.196

# Script variables
REL_DIR="`dirname \"$0\"`/.."
ABS_DIR="`( cd \"$REL_DIR\" && pwd )`"
SERVER_ADDR=$SERVER_USER@$SERVER_HOST

ZTNBD_FILES="$ABS_DIR/Pipfile $ABS_DIR/Pipfile.lock"
ZTNBD_DIRS="$ABS_DIR/src $ABS_DIR/modules $ABS_DIR/resources"

ctl=~/tmpconn
ssh -fNMS $ctl $SERVER_ADDR # open persistent ssh connection
ssh -S $ctl $SERVER_ADDR "rm -rf ~/ztnbd && mkdir ~/ztnbd 2>/dev/null"
scp -o ControlPath=$ctl $ZTNBD_FILES $SERVER_ADDR:~/ztnbd/
scp -o ControlPath=$ctl -r $ZTNBD_DIRS $SERVER_ADDR:~/ztnbd/

ssh -S $ctl $SERVER_ADDR << SSH_SESS
	cd ztnbd
	pipenv install
	hdfs dfs -mkdir -p /user/TZ/$SERVER_USER/ztnbd
	hdfs dfs -copyFromLocal -f resources/pages/* /user/TZ/$SERVER_USER/ztnbd/
	echo "===================== $MAIN_SCRIPT ====================="
	pipenv run spark-submit --conf spark.ui.enabled=true "src/$MAIN_SCRIPT" $SERVER_USER $SPARK_STREAM
SSH_SESS

ssh -S $ctl -O exit $SERVER_ADDR # close persistent connection
