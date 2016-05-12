# Human Trafficking ETL

This project will hold all the ETL code required to transform the raw HTML
web pages into clean, normalized data that can be used for analysis.

## Getting started
You will need the following installed:

* Python 2.7.x (where `x` mean any number) [download here](https://www.python.org/downloads)

Then click on the 'Fork' button above to make your own copy of the project,
so that you run

```
git clone https://gitlab.com/yourUsername/ht-etl.git
```

where `yourUsername` is your actual user name (e.g. for Bryant Menn it would
be `bmenn`, yours may vary). The SSH protocol currently does *NOT* work with
`git lfs` with these instructions.

Do *NOT* run the following:

```
git clone https://gitlab.com/anidata/ht-etl.git
```

We are going to use what is called a fork-merge model for git.

### Getting the raw data
A sample of the raw can be accessing by using the `lfs` plugin for `git`.
Instructions to install the `lfs` plugin can be found
[here](https://git-lfs.github.com/).

After installing the `lfs` plugin, setup the plugin by running

```
git lfs install
```

and get the file with

```
git lfs fetch
```

### Hacking

Pick an issue off the issue list and get started! If you need help just ping
the `anidata1_1` slack channel for help.

When you're done hacking, run

```
git add .
git commit -m "An explanation of what you did goes here"
git push origin
```

And then open a merge request and make sure it's mentioned in the issue's
comments.

## Module Installation

```
pip install -e .
```

## Running ETL

ETL batch uses Luigi (http://luigi.readthedocs.io/en/stable/index.html) under the hood.  

To configure Luigi, rename `luigi.cfg.example` to `luigi.cfg` and add the password to that file.

To run all the jobs excute:

```
luigi --module htetl.all_jobs HtTasks --local-scheduler
```
