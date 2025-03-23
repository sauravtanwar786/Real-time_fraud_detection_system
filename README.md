


### **Real-Time Data Replication using Change Data Capture**  

This project implements a **real-time credit card fraud detection pipeline** using **Debezium, Kafka, Spark Streaming, and MinIO**. A **real-time data replication service** captures and replicates **transaction data** using **Debezium and Kafka**, while **Spark Streaming** processes the data. The **classified transaction records** are then displayed on a **dashboard for visualization and monitoring.**  

---

### **Project Design & Prerequisites**  

#### **Prerequisites:**  
1. **Docker** (Version ≥ 20.10.17) – Ensure Docker is running (`docker ps`).  
2. **For Windows Users:**  
   - Set up **WSL (Windows Subsystem for Linux)** and install **Ubuntu VM** following [this guide](#).  
   - Install the necessary tools using:  
     ```bash
     sudo apt install make -y
     ```
   - Follow [these steps](#) to install **Docker** (only Step 1 required).  

---

### **Setup & Deployment**  

All commands should be executed in the **terminal (Ubuntu for WSL users)**. The setup process uses **Docker** for containerized environments.  

#### **Steps:**  
1. Clone the repository and navigate to it.  
2. Use **make commands** for an easier workflow:  

| **Command** | **Description** |
|------------|----------------|
| `make up` | Starts all required containers |
| `make load_initial_data` | Loads initial dataset for transactions |
| `make connectors` | Sets up **Kafka connectors** for CDC |
| `make job3` | Starts the **Kafka transactions stream** |
| `make fraud_detection` | Runs the **fraud detection model** |
| `make pg` | Adds a new transaction entry to PostgreSQL |

#### **Demo Execution:**  
To demonstrate the fraud detection system, run:  
```bash
make job3
make fraud_detection
```
Then, add a **new transaction entry** in the PostgreSQL **transactions table** to trigger the pipeline.

---

### **Architecture Overview**  
- **Debezium + Kafka** → Captures **real-time database changes**  
- **Spark Streaming** → Processes transactions & applies **fraud detection model**  
- **MinIO** → Provides **object storage** for data persistence  
- **Dremio + Apache Superset** → Visualizes classified transactions  

This **scalable and real-time solution** ensures **instant fraud detection**, **seamless data replication**, and **interactive monitoring dashboards**. 🚀  

Would you like any **further refinements** or **additional details**? 😊
