if [[ -z "$1" ]]; then
	echo "Syntax:"
	echo "ssh <server_user>"
	exit
fi

SERVER_USER=$1
SERVER_HOST=153.19.52.196

ssh -L 4040:127.0.0.1:4040 $SERVER_USER@$SERVER_HOST