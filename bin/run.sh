if [[ -z "$1" ]]; then
	echo "Syntax:"
	echo "run <server_user>"
	exit
fi

# Configuration variables
MAIN_SCRIPT="MLPipeline.py"
SERVER_USER=$1
SERVER_HOST=153.19.52.196

# Script variables
REL_DIR="`dirname \"$0\"`/.."
ABS_DIR="`( cd \"$REL_DIR\" && pwd )`"
SERVER_ADDR=$SERVER_USER@$SERVER_HOST

ssh $SERVER_ADDR "mkdir ~/ztnbd 2>/dev/null"
scp "$ABS_DIR/Pipfile" "$SERVER_ADDR:~/ztnbd/Pipfile"
scp "$ABS_DIR/Pipfile.lock" "$SERVER_ADDR:~/ztnbd/Pipfile.lock"
scp -r "$ABS_DIR/src" "$SERVER_ADDR:~/ztnbd/src"

ssh $SERVER_ADDR << SSH_SESS
	cd ztnbd
	pipenv install
	#hdfs dfs -copyFromLocal src/pages/page1.html /input/page1.html
	pipenv run spark-submit "src/$MAIN_SCRIPT"
SSH_SESS