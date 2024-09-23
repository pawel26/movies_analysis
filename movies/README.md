Welcome to your new dbt project!

### Using the starter project

install requirements from requirements.txt
To run transformations you need to have database - this project uses PostgresQL
To create local instance of database pleas run: 

`docker-compose up -d`
you need to create database schema inside postgres container - pleas execute

``docker exec -it dbt-movies bin/bash``

and inside container do the following steps:
- ``psql -U postgres``
- ``CREATE DATABASE movies_wh;`` - only of not created by default inside docker compose
- ``CREATE SCHEMA dev;``
- verify connection: ```psql -U postgres -d  movies_wh```

outside the container run:
``dbt debug`` to make sure that everything is ok

Put input csv file into `data_sources` folder
run load service to fill source table with data -> ``python load_movies_service.py``
run dbt run to create models

Try running the following commands:
- ``dbt test``
- ``dbt run``

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
