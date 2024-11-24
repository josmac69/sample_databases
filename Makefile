start:
	docker compose up -d

stop:
	docker compose down

logs:
	docker logs mssql-db

bash:
	docker exec -it mssql-db /bin/bash

cli:
	docker exec -it mssql-db /opt/mssql-tools18/bin/sqlcmd -C -d master -H localhost -U sa -P TeST@+$-

# docker run -it \
# --net mssql_network \
# mcr.microsoft.com/mssql-tools \
# /opt/mssql-tools/bin/sqlcmd -H mssql-db -d master -U sa -P TeST@+$-

tools:
	docker run -it \
	--net mssql_network \
	mcr.microsoft.com/mssql-tools

.PHONY: start stop logs bash cli tools

