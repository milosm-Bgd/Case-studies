# Databricks notebook source
raw_folder_path = '/mnt/getcase1dl/raw'
processed_folder_path = '/mnt/getcase1dl/processed'
presentation_folder_path = '/mnt/getcase1dl/presentation'

# COMMAND ----------

#commands we use to link two notebooks together
# while we are in another notebook, we insert thecommand: 
# %run "../fodler_name/notebook_name" , for ex:  %run "../includes/configuration"