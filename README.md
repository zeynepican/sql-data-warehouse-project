# sql-data-warehouse-project
Building a modern data warehouse with SQL Server, including ETL processes, data modeling and analytics. 
# Data Warehouse and Analytics Project

Welcome to the **Data Warehouse and Analytics Project** repository! 

This project is developed by following the YouTube series of **Data With Baraa** and aims to reinforce practical knowledge in data engineering and data analytics through a hands-on implementation.

Rather than being an original project from scratch, this repository represents a **guided learning process**, where the concepts are actively implemented, tested, and understood step by step.

---

##  Data Architecture

The data architecture in this project follows the **Medallion Architecture** approach with **Bronze**, **Silver**, and **Gold** layers:


1. **Bronze Layer**: Raw data is ingested from CSV files into the SQL Server database without transformation.
2. **Silver Layer**: Data is cleaned, standardized, and normalized to ensure consistency and quality.
3. **Gold Layer**: Data is transformed into a **star schema** (fact and dimension tables) optimized for analytical queries.

---

##  Project Overview

This project includes the following components:

1. **Data Architecture**
   Implementation of a modern data warehouse using Medallion Architecture principles.

2. **ETL Pipelines**
   Extracting data from source systems and applying transformation logic before loading into analytical layers.

3. **Data Modeling**
   Designing fact and dimension tables to support efficient querying and reporting.

4. **Analytics & Reporting**
   Writing SQL queries to generate insights related to business performance.

---

##  Learning Objectives

This project focuses on building practical understanding in:

* SQL Development
* Data Warehousing Concepts
* ETL Pipeline Design
* Dimensional Modeling (Fact & Dimension Tables)
* Data Analytics

---

##  Disclaimer

This is a **tutorial-based project** inspired by the content from *Data With Baraa*.
The core structure, dataset, and overall workflow are based on the original series.

This repository is created as part of a **learning process**, with the goal of understanding industry practices by implementing them hands-on.

---

##  What I Gained from This Project

* Hands-on experience in designing a data warehouse
* Understanding Medallion Architecture (Bronze, Silver, Gold)
* Practical implementation of ETL pipelines
* Experience with dimensional modeling (star schema)
* Writing analytical SQL queries for business insights

---

##  Tools & Technologies

* SQL Server
* SQL Server Management Studio (SSMS)
* CSV Data Sources
* DrawIO (for architecture diagrams)

---

##  Future Improvements

As a next step, this project may be extended by:

* Migrating the implementation to PostgreSQL
* Enhancing the data model with advanced techniques (e.g., SCD handling)
* Adding more analytical queries and reporting scenarios
* Integrating with BI tools for visualization

---

## 📌 Source

Original project and guidance: **Data With Baraa (YouTube)**

