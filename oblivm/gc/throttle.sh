tc qdisc del dev eth0 root
tc qdisc add dev eth0 handle 1: root htb default 11
tc class add dev eth0 parent 1: classid 1:1 htb rate $1mbit
tc class add dev eth0 parent 1:1 classid 1:11 htb rate $1mbit
