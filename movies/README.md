Welcome to your new dbt project!

### Using the starter project

To run transformations you need to have database - this project uses PostgresQL
To create local instance of database pleas run: 

`docker run -d --name dbt-movies -p 5432:5432 -e POSTGRES_USER=YOUR_USER -e POSTGRES_PASSWORD=YOUR_PASS postgres:15-alpine`

you need to create database inside postgres container - pleas execute

``docker exec -it dbt-movies bin/bash``

and inside container do the following steps:
- ``psql -U YOUR_USER``
- ``CREATE DATABASE movies_wh;``
- ``CREATE SCHEMA dev;``
- verify connection: ```psql -U YOUR_USER -d  movies_wh```

outside the container run:
``dbt debug`` to make sure that everything is ok

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
