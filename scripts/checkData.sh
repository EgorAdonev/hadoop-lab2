if [[ $# -eq 0 ]] ; then
    echo 'You should specify output file!'
    exit 1
fi

#GROUPPREFIX="group"
#for j in {1..8}
#  do
#    GROUP=$((1+RANDOM % 8))
#    sudo -u postgres -H -- psql -d $1 -c 'INSERT INTO groupMap (user, group) values ('"${USERPREFIX$GROUP}"','"${GROUPPREFIX$GROUP}"');'
#  done
rm -rf input
mkdir input
POSTFIX=("what's up,guys:,link"
        "Hey,did yo see that:,news")
USERPREFIX="user"
for i in {1..200}
	do
	    USER=$((1+RANDOM % 8))
	    HOUR=$((RANDOM % 24))
	    if [ $HOUR -le 9 ]; then
	        TWO_DIGIT_HOUR="0$HOUR"
	    else
	        TWO_DIGIT_HOUR="$HOUR"
	    fi
	    RESULT=${USERPREFIX}${USER}+${USERPREFIX}${USER}+'\''May 03 '"$TWO_DIGIT_HOUR"':13:56''\'+"${POSTFIX[$((RANDOM % ${#POSTFIX[*]}))]}"
	    echo $RESULT >> input/$1.1
	done