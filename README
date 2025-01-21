# Overview

This is a tutorial to try using the basics of pyspark to read a csv file into a dataframe and clean the data.

This demo is designed to be run on Databricks. A community edition account will allow you to access all the necessary functionality.

## Create account

Complete the sign up process at https://www.databricks.com/try-databricks#account
Chose the Community edition when signing up. You DO NOT need to set up a cloud account.

## Setup Cluster

A cluster is a set of compute resources which will run your code

Once you are in the databricks environment, navigate to the `Compute` page on the left and click `Create Compute`

Select the runtime `13.3` LTS from the dropdown then create the compute.
It will take a few minutes to provision your resources

## Upload Databricks Notebooks

To run the notebooks in this repository you will need to upload them to databricks
Navigate to the `Workspace` page on the left then go to your `Home`
Click the 3 dots in the top right and select `import`
This will allow you to drag and drop or navigate to the file you want to upload. Please choose the file located in this repo in `task1/Pyspark Intro - Task 1.py`

## Upload CSV file

To upload the data we are using you will need the `Catalog`

Select `Create Table` and leave the `Target Directory` blank
Select `Upload file` and upload the data.csv file found in the task1 directory.Select `Create Table with UI`

Choose the cluster you created earlier from the dropdown and select `Preview Table`
Give your table a name. The name used in the example was `task1_input`. You can use any name.

Tick the boxes for:

- First row is header
- Infer schema

Select `Create Table`

## Run your notebook

Go back to the notebook we uploaded to your Home workspace and click `Connect` in the top right to attach the notebook to a cluster. Chose the cluster you created and wait for this to start. You can then follow the instructions in the notebook to process your data

## Extension task

You will need to follow the same instructions as part 1 to upload the databricks notebook and CSV files. In this case there are 2 CSV files containing planet data which we will combine.
