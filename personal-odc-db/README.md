# Running a personal ODC Database

Normally, you don't have write access to the ODC Database, so you can't create new Products and Datasets.

This can make working with large amounts of new data, or working with data you're going to distribute rather challenging.

To save messing around with permissions on a shared system, lets run our own private PostgreSQL server for ODC.

To make this work, we're going to new PostgreSQL installed.

On Linux, this options are:
- Installed using the system package manager.
- Compiled from source.
- Run within a Docker container.
- Installed using an alternative package manager like conda/mamba/micromamba


Steps:
- Install from conda-forge using micromamba
- Setup


## TLDR

1. [Install micromamba](https://mamba.readthedocs.io/en/latest/installation/micromamba-installation.html#automatic-install)

       "${SHELL}" <(curl -L micro.mamba.pm/install.sh)

       # Accept the installation defaults, then:
       source ~/.bashrc

2. Download and install PostgreSQL into ~/postgresql.

       micromamba create --prefix ~/postgresql --yes postgresql

3. Initialise a Postgres Data Dir, Launch Postgres locally, and create an empty datacube DB.

       ./db.sh init

4. Set ODC environment variables and initialise an ODC Database.

       eval "$(./db.sh env)"
       datacube system init

