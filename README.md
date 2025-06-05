this is my way to learn go-lang.
build a mimic of a redis-server trying go for the first time so go easy on me.
What is supported so far:
1) RESP Parser.
2) support many commands including but not limited: get, set, info, wait, keys...
3) supporting expiring keys.
4) adding a replica(full handshake supported)
5) propgating write commands from master to replicas.

currently working on synchronization of dataset between master to replicas(keep track of offset)
