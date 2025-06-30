# 📦 Supplychain Disruption Prediction

In an increasingly interconnected global economy, supply chains are more vulnerable than ever to disruptions caused by weather events, 
geopolitical instability, port congestion, and transportation delays. This project aims to build an intelligent, real-time platform that empowers 
logistics managers and supply chain analysts to make proactive decisions, reducing risk, improving efficiency, and ensuring timely delivery. 
The end goal is to bridge raw data to actionable insights and deliver them via a simple, accessible web application that anyone in the logistics chain can use.

### Core Objectives
1. Predict supply chain disruptions based on real-time and historical data inputs.
2. Deliver disruption risk scores through a responsive and user-friendly web interface.
3. Integrate external data sources (weather, geopolitical events, shipment routes).
4. Provide actionable alerts and recommendations (e.g., rerouting, delaying shipments).
5. Implement a scalable and maintainable ML lifecycle, including MLOps for monitoring, retraining, and versioning.

## 1.  Data Ingestion Pipeline 🔄
Built a robust data ingestion pipeline using Python and MongoDB. Developed and debugged the spatial-temporal merging logic with vectorized Haversine distance for optimization.

#### 🧩 Datasets Used

| Dataset | Description | Source |
| ------------- | ------------- | ------------- |
| Weather_Data.csv  | Contains historical weather event data (location, severity, azimuth, etc.) across 2023–2025  |    NOAA / Government open data           |
| Shipment_Route_Data.csv  | Simulated or real shipment records (origin, destination, freight cost, date, etc.)  |      Enterprise logistics simulation         |
|Geopolitical_Data.csv|Conflict events with time, location, type, and severity of disruption| ACLED / public dataset |

All datasets were stored in MongoDB Atlas. 

#### 🌍 Spatial and Temporal Matching Strategy
The three datasets (weather, shipments, and conflicts) have no common keys to join them directly. Instead, a spatial-temporal proximity strategy was applied:

  - **Spatial Matching:** For each shipment, match conflict/weather events within a 50 km radius using the Haversine distance.
  - **Temporal Matching:** Match conflict events that occurred within ±3 days of the shipment date and weather events within the same month.
