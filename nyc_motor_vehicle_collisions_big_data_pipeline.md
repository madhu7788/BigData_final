# NYC Motor Vehicle Collisions – Big Data Pipeline

## 📌 Project Overview
This project demonstrates an **end-to-end Big Data pipeline** using the **New York City Motor Vehicle Collisions dataset**. The objective is to show how large, semi-structured real-world data can be ingested, processed, validated, aggregated, and analyzed using modern Big Data tools.

The pipeline is implemented using **Databricks Serverless** for scalable processing, **MongoDB Atlas** as a distributed NoSQL data platform, and **Python** for data cleaning, aggregation, and visualization. The design follows a **Medallion Architecture (Raw → Silver → Gold)**.

---

## 🧱 System Architecture

**Data Flow:**
1. NYC Open Data REST API (JSON)
2. Databricks Serverless (Python + Pandas)
3. MongoDB Atlas (Distributed NoSQL Database)
   - Raw Layer
   - Silver Layer
   - Gold Layer
4. Visualization using Python (Pandas + Matplotlib)

MongoDB Atlas acts as the central Big Data system, storing raw data, cleaned data, and analytics-ready aggregates.

---

## 📊 Dataset Details

- **Source:** NYC Open Data – Motor Vehicle Collisions
- **Access Method:** REST API
- **Data Format:** Semi-structured JSON
- **Current Ingested Volume:** **100,000 records** (verified in MongoDB Atlas)
- **Scalability:** Pipeline is designed to support **1,000,000+ records** using the same architecture
- **Key Attributes:** Crash date, crash time, borough, location, injuries, fatalities, and related metadata

The dataset’s size and JSON structure make it suitable for a distributed NoSQL system rather than a traditional relational database.

---

## 🗄️ MongoDB Data Model

### 🔹 Raw Layer
**Collection:** `MVCollisions`
- Stores raw collision data ingested directly from the NYC Open Data API
- No transformations applied
- Preserves original source records
- **Document count:** 100,000

### 🔹 Silver Layer
**Collection:** `MVCollisions_silver`
- Cleaned and validated version of the raw data
- Missing values handled
- Text fields normalized
- Dates and timestamps standardized
- Duplicate records removed
- Schema validated using **Pydantic**
- **Document count:** 100,000

### 🔹 Gold Layer (Aggregated Collections)

- **`MVCollisions_gold_borough`**
  - Aggregates total accidents by borough
  - **Document count:** 6

- **`MVCollisions_gold_injuries`**
  - Aggregates injuries and fatalities by borough
  - **Document count:** 6

- **`MVCollisions_gold_daily`**
  - Tracks daily accident trends over time
  - **Document count:** 752

Gold collections are optimized for analytical queries and visualization.

---

## ⚙️ Data Processing & Validation

- **Language:** Python
- **Libraries:** Pandas, PyMongo, Pydantic
- **Schema Validation:** Pydantic models used before Silver layer insertion
- **Indexes:** Created on frequently queried fields such as `crash_date`, `borough`, and `collision_id` to improve query performance

---

## 📈 Visualizations

Visualizations are generated **directly from MongoDB Atlas** using Python and Pandas (no flat files):

- Total accidents by borough
- Injuries vs fatalities by borough
- Daily accident trends

Matplotlib is used to render charts for analysis and insights.

---

## 🎥 Video Demonstration

A short video presentation (≤ 6 minutes) accompanies this project and demonstrates:
- System architecture
- MongoDB Atlas setup
- Raw, Silver, and Gold collections
- Document counts and indexes
- Aggregations and visualizations
- Key project learnings

---

## 📁 Repository Structure

```
.
├── src/
│   ├── ingestion.py
│   ├── cleaning.py
│   ├── aggregation.py
│   └── visualization.py
├── diagrams/
│   └── architecture.png
├── requirements.txt
├── README.md
```

---

## 🔐 Security Considerations

- No database credentials are hardcoded
- MongoDB connection strings are managed using environment variables

---

## ✅ Key Learnings

- Designing a production-style Big Data pipeline
- Handling real-world data quality issues
- Applying schema validation in a NoSQL system
- Using MongoDB Atlas for scalable analytics

---

## 🏁 Conclusion

This project applies Big Data concepts to a real-world dataset using a scalable and modular architecture. The combination of Databricks Serverless, MongoDB Atlas, and Python demonstrates how modern Big Data systems are built and analyzed in practice.

---

**Author:** Madhusudhan Reddy  
**Course:** Big Data

