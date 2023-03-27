build:
	docker build -t db_script .

run: build
	docker run --network=host --env-file .env db_script
 