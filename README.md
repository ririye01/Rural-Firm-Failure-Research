# **Identifying Determinants of Firm Failure in Rural Texas: A Multi-Model Machine Learning Approach for Investigating Successful Economic Development in Rural Texas**
##### *Contributors: Reece Iriye (ririye@smu.edu), Mark Hand (Ph.D)*

## **Repository Overview**

This repository contains the data, code, and other necessary materials that have been used in writing our co-authored research paper: "Identifying Determinants of Rural Economic Resiliency via COVID-19 Firm Closures: A Multi-Model Machine Learning Approach for Investigating Successful Economic Development in Rural Texas". 

## **How to Navigate this Repository**

This repository is meant to maintain the code and data necessary for this project in 1 consistent location that can be updated repeatedly. We decided that this repository should be public for both transparency and reproducibility purposes. The file directory structure is as follows:

```
code/
│   ├──ETL_Pipeline/
│   │   ├──bronze_to_silver/
│   │   │   ├── ...
│   │   ├──silver_to_gold/
│   │   │   ├── ...
│   │   ├──pipeline.py
│   ├──EDA/
│   │   ├── ...
│   ├──models/
│       ├── ...
data/
│   ├──bronze/
│   │   ├── ...
│   ├──silver/
│   │   ├── ...
│   ├──gold/
│       ├── ...
resources/
│   ├──"source_1"/
│   │   ├── ...
│   ├── ...
│   ├──"source_n"/
│       ├── ...
```
### **`code/`**

The `code` directory contains all relevant Python scripts and Jupyter notebooks used to transform, explore, and model data. It contains 3 sub-directories (`ETL_Pipeline`, `EDA`, `models`) to categorize exactly what part of the process the code is responsible for executing.

 The `ETL_Pipeline` sub-directory contains standard Python scripts meant to mimic a multitide of "Explore-Transform-Load" (ETL) pipelines by taking in raw `.csv` or `.xlsx` files from reliable sources then outlining the entire processes for feature engineering variables that can be used for modeling purposes. By executing `pipeline.py` within this folder, all processes in the ETL pipeline will be performed, and datasets will be saved to Disk Storage. If any changes to the code are made, the changes will be reflected in the `data` directory. The folders inside the `ETL_Pipeline` sub-directory follow Databricks' medallion architecture, which can be seen <a href="https://www.databricks.com/glossary/medallion-architecture">here</a>.
 
 The `EDA` sub-directory contains Jupyter notebooks that outline important understandings that we get from the exploratory data analysis phase. Code for plots, as well as conclusions, are featured in that section of the codebase. 
 
 The `models` sub-directory contains both Python scripts and Jupyter notebooks for actually computing models. Results obtained from these models, as well as the methodology used for computing them, are implemented here. 
 
 We hope that incorporating all of the code here will help alleviate "black-box algorithm" concerns and provide transparency into how we are making these conclusions here that we are making.

 ### **`data/`**

The `data` directory contains the `.csv` and `.xlsx` files at all stages of the data science process, from preprocessing to model-building. The sub-directory within this folder follow Databricks` medallion architecture, where information can be found <a href="https://www.databricks.com/glossary/medallion-architecture">here</a>.
