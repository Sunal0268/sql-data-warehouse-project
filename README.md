
# Data Warehouse and Analytics Project 

Welcome to my **Data Warehouse and Analytics Project** 
This repository showcases an end-to-end **modern data warehouse implementation** using Medallion Architecture (Bronze → Silver → Gold). The project is built as a **portfolio project** to demonstrate my hands-on skills in **data engineering, SQL, data modeling, and analytics**.

---

## 🏗️ Data Architecture

This project follows the **Medallion Architecture** approach:

**Bronze → Silver → Gold**

1. **Bronze Layer**

   * Stores raw data exactly as received from source systems (ERP & CRM CSV files).
   * No transformations are applied at this stage.
   * Purpose: traceability, auditability, and reproducibility.

2. **Silver Layer**

   * Cleansed and standardized data.
   * Handles deduplication, data type corrections, business rule standardization and data quality fixes.
   * Designed to be analytics-ready but still normalized.

3. **Gold Layer**

   * Business-ready data modeled into a **Star Schema** (Facts & Dimensions).
   * Optimized for reporting, analytics and BI use cases.

---

## 📖 Project Overview

This project demonstrates:

1. **Data Architecture Design**

   * Implementation of a scalable warehouse using Bronze, Silver and Gold layers.

2. **ETL / ELT Pipelines**

   * Data ingestion from CSV sources into Bronze.
   * Transformations using SQL procedures and views for Silver and Gold layers.

3. **Data Modeling**

   * Creation of dimension and fact views following Star Schema principles.
   * Use of surrogate keys, conformed dimensions and clean grain definition.

4. **Analytics & Reporting Readiness**

   * Gold layer views can be directly consumed by BI tools or analytical queries.

---


## 🚀 Project Requirements

### Data Warehouse 

**Objective**
Build a modern data warehouse to consolidate sales, customer and product data for analytical reporting.

**Specifications**

* **Data Sources**: ERP and CRM systems provided as CSV files
* **Data Quality**: Handle duplicates, invalid dates, missing values and inconsistent codes
* **Integration**: Merge multiple sources into a unified analytical model
* **Scope**: Focus on the latest data (no historical versioning)
* **Documentation**: Clear schema and transformation logic documentation

---

### BI & Analytics (Data Analysis)

**Objective**
Enable analytics for:

* Customer behavior analysis
* Product performance tracking
* Sales trend analysis

The Gold layer is designed so that business users and analysts can query it directly without additional transformations.

---

## 🧪 Data Quality & Validation

The project includes **data quality checks** to ensure:

* Uniqueness of surrogate keys in dimension views
* Referential integrity between fact and dimension views
* Reliable joins for analytics and reporting

These checks help prevent silent data issues that can impact business decisions.

---

## 🛠️ Tools & Technologies

All tools used in this project are **free and open-source**:

* **PostgreSQL** – Primary data warehouse engine
* **pgAdmin** – Database management and development
* **CSV Files** – Source datasets (ERP & CRM)
* **GitHub** – Version control and project hosting

---

## 🎯 This project highlights my skills in:

* SQL 
* Data Warehousing Concepts
* Data Engineering
* ETL / ELT Design
* Data Modeling (Star Schema)
* Analytical Thinking

---

## 🧠 What I Learned From This Project

* How real-world data quality issues appear in raw datasets
* Why layered data architecture improves scalability and trust
* How to design analytical models aligned with business questions
* How SQL can be used not just for querying, but for **data engineering workflows**

---

## 📌 About Me

Hi 👋
I am **Sunal Singh**, an aspiring **Data Analyst** with having strong interest in **data warehousing, SQL and analytics engineering**.

This project reflects my hands-on learning and practical understanding of building production-style data warehouses using PostgreSQL.

📫 *Feel free to explore the repository and review the SQL logic and architecture.*
