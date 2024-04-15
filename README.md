# Disruptive Technologies Sensor Data Backup

## Overview

This repository contains the code to fetch sensor data and store it in a MySQL database. It uses Node.js for backend operations, cron for scheduling data fetching, and environment variables for configuration.

## Prerequisites

Node.js and npm: Install from [nodejs.org](https://nodejs.org/en/download/).

MySQL Database: Install from [MySQL official site](https://nodejs.org/en/download/).

## Installation

Clone the repository or download the source code.

Navigate to the project directory and install the necessary packages:

```
npm install mysql2 node-fetch node-cron dotenv
```

## Configuration

Create a .env.local file in the root directory and configure the database and API credentials as shown in the sample configuration section below:

```
DB_HOST = localhost
DB_USER = root
DB_PASS = yourpassword
DB_NAME = DT_sensor_data

API_URL = https://api.disruptive-technologies.com/v2/
API_KEY = yourapikey
API_SECRET = yoursecret
PROJECT_ID = yourprojectid
```

Replace yourpassword, yourapikey, yoursecret, and yourprojectid with your actual database credentials and API credentials.

## Running the Application

To start the application, run:

```
node backup-DT-sensor-data.js
```

This will start the data collection process, which fetches sensor data every 3 minutes and stores it in your MySQL database.

## Functionality

Database Setup: The script checks if the specified database exists and creates it if it does not.

Data Fetching: Sensor data is fetched from a configured API.

Data Insertion: Data is inserted into dynamically created tables based on the device ID and data type.