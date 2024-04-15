/*
Required: Node.js and npm https://nodejs.org/en/download
Required: mySQL database https://dev.mysql.com/downloads/mysql/
Install the required packages using the following command:
npm install mysql2 node-fetch node-cron dotenv
*/

const mysql2 = require('mysql2');
const cron = require('node-cron');
require('dotenv').config({ path: '.env.local' });

/* *********** Database configuration ******** */
// For security reasons, it is recommended to store the database credentials in a '.env.local' file
const DB_HOST     = process.env.DB_HOST;
const DB_USER     = process.env.DB_USER;
const DB_PASS     = process.env.DB_PASS;
const DB_NAME     = process.env.DB_NAME;

const API_URL     = process.env.API_URL;
const API_KEY     = process.env.API_KEY;
const API_SECRET  = process.env.API_SECRET;
const PROJECT_ID  = process.env.PROJECT_ID;

console.log('DB_HOST:', DB_HOST);
console.log('DB_USER:', DB_USER);
console.log('DB_PASS:', DB_PASS);
console.log('DB_NAME:', DB_NAME);

/* 
*************************************************
// Sample '.env.local' configuration
DB_HOST = localhost
DB_USER = root
DB_PASS = abcdefg
DB_NAME = DT_sensor_data

API_URL     = https://api.disruptive-technologies.com/v2/
API_KEY     = abcdefg
API_SECRET  = abcdefg
PROJECT_ID  = abcdefg
*************************************************
*/

// Use dynamic import for fetch due to ESM compatibility
let fetch;
import('node-fetch').then(({ default: nodeFetch }) => {
  fetch = nodeFetch;
  main();
});

async function ensureDatabaseExists() {
  const testConnection = await mysql2.createConnection({
    host: DB_HOST,
    user: DB_USER,
    password: DB_PASS
  });

  try {
    // Check if the database exists
    const [databases] = await testConnection.promise().query(`SHOW DATABASES LIKE '${DB_NAME}'`);
    if (databases.length === 0) {
      testConnection.execute(`CREATE DATABASE ${DB_NAME}`);
      console.log('Database does not exist. Database created.');
    }
  } catch (error) {
    console.error('Error working with the database:', error);
  }

  // Close the test connection
  await testConnection.end();
  // Return a new connection to the specific database
  return mysql2.createConnection({
    host: DB_HOST,
    user: DB_USER,
    password: DB_PASS,
    database: DB_NAME
  });
}

// Function to insert data into the database
async function insertIntoDatabase(data) {
  console.log('*--------------------*');
  console.log('Fetching sensor data...');
  const connection = await ensureDatabaseExists();
  await connection.connect();

  for (const device of data.devices) {
    // Use device ID as table names
    const deviceId = device.name.split('/').pop();
    const tableName = `\`${deviceId}\``;

    // Create table and insert data based on device type
    let values = [];
    insertQuery = '';
    createTableQuery = '';

    switch (device.type) {
      case 'humidity':
        createTableQuery = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
          id INT AUTO_INCREMENT PRIMARY KEY,
          capture_timestamp VARCHAR(255) NOT NULL,
          recorded_timestamp VARCHAR(255) NOT NULL,
          name VARCHAR(255),
          type VARCHAR(255) NOT NULL,
          kit VARCHAR(255) NOT NULL,
          temperature FLOAT,
          relativeHumidity FLOAT
        )`;

        insertQuery = `
           INSERT INTO ${tableName} (
           capture_timestamp, recorded_timestamp, name, type, kit, temperature, relativeHumidity) 
           VALUES (?, ?, ?, ?, ?, ?, ?)
          `;

        values = [
          device.reported.humidity.updateTime,
          new Date().toISOString(),
          device.name,
          device.type,
          device.labels.kit,
          device.reported.humidity.temperature,
          device.reported.humidity.relativeHumidity
        ];
        break;

      case 'co2':
        createTableQuery = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
          id INT AUTO_INCREMENT PRIMARY KEY,
          capture_timestamp VARCHAR(255) NOT NULL,
          recorded_timestamp VARCHAR(255) NOT NULL,
          name VARCHAR(255),
          type VARCHAR(255) NOT NULL,
          kit VARCHAR(255) NOT NULL,
          ppm FLOAT
        )`;

        insertQuery = `
           INSERT INTO ${tableName} (
           capture_timestamp, recorded_timestamp, name, type, kit, ppm) 
           VALUES (?, ?, ?, ?, ?, ?)
          `;

        values = [
          device.reported.humidity.updateTime,
          new Date().toISOString(),
          device.name,
          device.type,
          device.labels.kit,
          device.reported.co2.ppm
        ];
        break;
    }

    // Execute the queries
    if (createTableQuery != '') {
      try {
        await connection.promise().query(createTableQuery);
        connection.execute(insertQuery, values);
      } catch (error) {
        console.error('Error working with the database:', error);
      }
    }
  }
  console.log("Insert into databse successfull.");
}

// Function to fetch sensor data
async function fetchSensorData() {
  const apiUrl = API_URL + 'projects/' + PROJECT_ID + '/devices';
  const apiKey = API_KEY;
  const apiSecret = API_SECRET

  try {
    const authBuffer = Buffer.from(`${apiKey}:${apiSecret}`);
    const base64Auth = authBuffer.toString('base64');
    const response = await fetch(apiUrl, {
      method: 'GET',
      headers: {
        'Authorization': `Basic ${base64Auth}`,
        'Content-Type': 'application/json',
      },
    });
    if (!response.ok) { throw new Error(`Error: ${response.statusText}`); }
    const data = await response.json();

    // Insert the fetched data into the database
    insertIntoDatabase(data);
  } catch (error) {
    console.error('Error fetching sensor data:', error);
  }
}

function main() {
  fetchSensorData();
  // Run the fetchSensorData function every 3 minutes
  cron.schedule('*/3 * * * *', () => {
    fetchSensorData();
  });

  // // DEBUG: Run the fetchSensorData function every 10 seconds
  // cron.schedule('*/10 * * * * *', () => {
  //   fetchSensorData();
  // });
}