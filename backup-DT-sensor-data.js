// Created by Yifan Wang
// Date: 2024-04-15

/*
Required: Node.js and npm https://nodejs.org/en/download
Required: mySQL database https://dev.mysql.com/downloads/mysql/
Install the required packages using the following command:
npm install mysql2 node-fetch node-cron dotenv eventsource
*/

const mysql2 = require('mysql2');
const cron = require('node-cron');
const EventSource = require("eventsource")
require('dotenv').config({ path: '.env.local' });

/* ********** Database configuration ********** */
// For security reasons, it is recommended to store the database credentials in a '.env.local' file
const DB_HOST = process.env.DB_HOST;
const DB_USER = process.env.DB_USER;
const DB_PASS = process.env.DB_PASS;
const DB_NAME = process.env.DB_NAME;

// Service Account credentials
const API_URL = process.env.API_URL;
const API_KEY = process.env.API_KEY;
const API_SECRET = process.env.API_SECRET;
const PROJECT_ID = process.env.PROJECT_ID;

console.log('DB_HOST:', DB_HOST);
console.log('DB_USER:', DB_USER);
console.log('DB_PASS:', DB_PASS);
console.log('DB_NAME:', DB_NAME);

/* 
*************************************************
// Sample '.env.local' configuration
DB_HOST = localhost
DB_USER = root
DB_PASS = yourpassword
DB_NAME = DT_sensor_data

API_URL     = https://api.disruptive-technologies.com/v2/
API_KEY     = yourapikey
API_SECRET  = yoursecret
PROJECT_ID  = yourprojectid
*************************************************
*/

// Constants
const MAX_CONNECTION_RETRIES = 5  // Max retries without any received messages
const PING_INTERVAL         = 10  // Expected interval between pings in seconds
const PING_JITTER           = 2   // Expected ping jitter in seconds

// Construct API URL
const DEVICES_STREAM_URL = API_URL + `projects/${PROJECT_ID}/devices:stream`

// Use dynamic import for fetch due to ESM compatibility
let fetch;
import('node-fetch').then(({ default: nodeFetch }) => {
  fetch = nodeFetch;
  main();
});

// Function to ensure the database exists and establish a connection
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
  // Return a new connection to the database
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

  // Connect to the database
  const connection = await ensureDatabaseExists();
  await connection.connect();

  for (const device of data.devices) {
    // Use device ID as table names
    let values = [];
    insertQuery = '';
    createTableQuery = '';
    const deviceId = device.name.split('/').pop();
    const tableName = `\`${deviceId}\``;

    // Create the table and insert query based on the device type
    switch (device.type) {
      case 'humidity':
        createTableQuery = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            id                                      INT AUTO_INCREMENT PRIMARY KEY,
            recorded_timestamp                      VARCHAR(255) NOT NULL,

            project_id                              VARCHAR(255) NOT NULL,
            device_id                               VARCHAR(255) NOT NULL,
            type                                    VARCHAR(255) NOT NULL,
            product_number                          VARCHAR(255) NOT NULL,

            labels_kit                              VARCHAR(255),
            labels_name                             VARCHAR(255),

            reported_humidity_temperature           FLOAT,
            reported_humidity_relativehumidity      FLOAT,
            reported_humidity_updatetime            VARCHAR(255),

            reported_touch_updatetime               VARCHAR(255),

            reported_networkstatus_signalstrength   INT,
            reported_networkstatus_rssi             INT,
            reported_networkstatus_updatetime       VARCHAR(255),
            reported_networkstatus_transmissionmode VARCHAR(255),

            reported_batterystatus_percentage       INT,
            reported_batterystatus_updatetime       VARCHAR(255),

            event_eventid                           VARCHAR(255),
            event_timestamp                         VARCHAR(255)
        )`;

        insertQuery = `
           INSERT INTO ${tableName} (
            recorded_timestamp,

            project_id,
            device_id,
            type,
            product_number,

            labels_kit,
            labels_name,

            reported_humidity_temperature,
            reported_humidity_relativehumidity,
            reported_humidity_updatetime,

            reported_touch_updatetime,

            reported_networkstatus_signalstrength,
            reported_networkstatus_rssi,
            reported_networkstatus_updatetime,
            reported_networkstatus_transmissionmode,

            reported_batterystatus_percentage,
            reported_batterystatus_updatetime
          ) 
           VALUES (
            ?,
            ?, ?, ?, ?,
            ?, ?,
            ?, ?, ?,
            ?,
            ?, ?, ?, ?,
            ?, ?
            )
          `;

        values = [
          new Date().toISOString(),

          device.name.split('/')[1],
          device.name.split('/').pop(),
          device.type,
          device.productNumber,

          device.labels.kit,
          device.labels.name || null,

          device.reported.humidity.temperature,
          device.reported.humidity.relativeHumidity,
          device.reported.humidity.updateTime,

          device.reported.touch.updateTime,

          device.reported.networkStatus.signalStrength,
          device.reported.networkStatus.rssi,
          device.reported.networkStatus.updateTime,
          device.reported.networkStatus.transmissionMode,

          device.reported.batteryStatus.percentage,
          device.reported.batteryStatus.updateTime
        ];
        break;

      case 'co2':
        createTableQuery = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            id                                      INT AUTO_INCREMENT PRIMARY KEY,
            recorded_timestamp                      VARCHAR(255) NOT NULL,

            project_id                              VARCHAR(255) NOT NULL,
            device_id                               VARCHAR(255) NOT NULL,
            type                                    VARCHAR(255) NOT NULL,
            product_number                          VARCHAR(255) NOT NULL,

            labels_kit                              VARCHAR(255),
            labels_name                             VARCHAR(255),

            reported_co2_ppm                        INT,
            reported_co2_updatetime                 VARCHAR(255),

            reported_pressure_pascal                INT,
            reported_pressure_updatetime            VARCHAR(255),

            reported_humidity_temperature           FLOAT,
            reported_humidity_relativehumidity      FLOAT,
            reported_humidity_updatetime            VARCHAR(255),

            reported_networkstatus_signalstrength   INT,
            reported_networkstatus_rssi             INT,
            reported_networkstatus_updatetime       VARCHAR(255),
            reported_networkstatus_transmissionmode VARCHAR(255),

            reported_batterystatus_percentage       INT,
            reported_batterystatus_updatetime       VARCHAR(255),

            event_eventid                           VARCHAR(255),
            event_timestamp                         VARCHAR(255)
        )`;

        insertQuery = `
           INSERT INTO ${tableName} (
            recorded_timestamp,

            project_id,
            device_id,
            type,
            product_number,

            labels_kit,
            labels_name,

            reported_co2_ppm,
            reported_co2_updatetime,

            reported_pressure_pascal,
            reported_pressure_updatetime,

            reported_humidity_temperature,
            reported_humidity_relativehumidity,
            reported_humidity_updatetime,

            reported_networkstatus_signalstrength,
            reported_networkstatus_rssi,
            reported_networkstatus_updatetime,
            reported_networkstatus_transmissionmode,

            reported_batterystatus_percentage,
            reported_batterystatus_updatetime
          ) 
           VALUES (
            ?,
            ?, ?, ?, ?,
            ?, ?,
            ?, ?,
            ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?
            )
          `;

        values = [
          new Date().toISOString(),

          device.name.split('/')[1],
          device.name.split('/').pop(),
          device.type,
          device.productNumber,

          device.labels.kit,
          device.labels.name || null,

          device.reported.co2.ppm,
          device.reported.co2.updateTime,

          device.reported.pressure.pascal,
          device.reported.pressure.updateTime,

          device.reported.humidity.temperature,
          device.reported.humidity.relativeHumidity,
          device.reported.humidity.updateTime,

          device.reported.networkStatus.signalStrength,
          device.reported.networkStatus.rssi,
          device.reported.networkStatus.updateTime,
          device.reported.networkStatus.transmissionMode,

          device.reported.batteryStatus.percentage,
          device.reported.batteryStatus.updateTime
        ];
        break;

      case 'proximity':
        createTableQuery = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            id                                      INT AUTO_INCREMENT PRIMARY KEY,
            recorded_timestamp                      VARCHAR(255) NOT NULL,

            project_id                              VARCHAR(255) NOT NULL,
            device_id                               VARCHAR(255) NOT NULL,
            type                                    VARCHAR(255) NOT NULL,
            product_number                          VARCHAR(255) NOT NULL,

            labels_kit                              VARCHAR(255),
            labels_name                             VARCHAR(255),

            reported_objectpresent_state            VARCHAR(255),
            reported_objectpresent_updatetime       VARCHAR(255),

            reported_touch_updatetime               VARCHAR(255),

            reported_networkstatus_signalstrength   INT,
            reported_networkstatus_rssi             INT,
            reported_networkstatus_updatetime       VARCHAR(255),
            reported_networkstatus_transmissionmode VARCHAR(255),

            reported_batterystatus_percentage       INT,
            reported_batterystatus_updatetime       VARCHAR(255),

            event_eventid                           VARCHAR(255),
            event_timestamp                         VARCHAR(255)
        )`;

        insertQuery = `
           INSERT INTO ${tableName} (
            recorded_timestamp,

            project_id,
            device_id,
            type,
            product_number,

            labels_kit,
            labels_name,

            reported_objectpresent_state,
            reported_objectpresent_updatetime,

            reported_touch_updateTime,

            reported_networkstatus_signalstrength,
            reported_networkstatus_rssi,
            reported_networkstatus_updatetime,
            reported_networkstatus_transmissionmode,

            reported_batterystatus_percentage,
            reported_batterystatus_updatetime
          ) 
           VALUES (
            ?,
            ?, ?, ?, ?,
            ?, ?,
            ?, ?,
            ?,
            ?, ?, ?, ?,
            ?, ?
            )
          `;

        values = [
          new Date().toISOString(),

          device.name.split('/')[1],
          device.name.split('/').pop(),
          device.type,
          device.productNumber,

          device.labels.kit,
          device.labels.name || null,

          device.reported.objectPresent.state,
          device.reported.objectPresent.updateTime,

          device.reported.touch.updateTime,

          device.reported.networkStatus.signalStrength,
          device.reported.networkStatus.rssi,
          device.reported.networkStatus.updateTime,
          device.reported.networkStatus.transmissionMode,

          device.reported.batteryStatus.percentage,
          device.reported.batteryStatus.updateTime
        ];
        break;

      case 'motion':
        createTableQuery = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            id                                      INT AUTO_INCREMENT PRIMARY KEY,
            recorded_timestamp                      VARCHAR(255) NOT NULL,

            project_id                              VARCHAR(255) NOT NULL,
            device_id                               VARCHAR(255) NOT NULL,
            type                                    VARCHAR(255) NOT NULL,
            product_number                          VARCHAR(255) NOT NULL,

            labels_kit                              VARCHAR(255),
            labels_name                             VARCHAR(255),

            reported_motion_state                   VARCHAR(255),
            reported_motion_updatetime              VARCHAR(255),

            reported_networkstatus_signalstrength   INT,
            reported_networkstatus_rssi             INT,
            reported_networkstatus_updatetime       VARCHAR(255),
            reported_networkstatus_transmissionmode VARCHAR(255),

            reported_batterystatus_percentage       INT,
            reported_batterystatus_updatetime       VARCHAR(255),

            event_eventid                           VARCHAR(255),
            event_timestamp                         VARCHAR(255)
        )`;

        insertQuery = `
           INSERT INTO ${tableName} (
            recorded_timestamp,

            project_id,
            device_id,
            type,
            product_number,

            labels_kit,
            labels_name,

            reported_motion_state,
            reported_motion_updatetime,

            reported_networkstatus_signalstrength,
            reported_networkstatus_rssi,
            reported_networkstatus_updatetime,
            reported_networkstatus_transmissionmode,

            reported_batterystatus_percentage,
            reported_batterystatus_updatetime
          ) 
           VALUES (
            ?,
            ?, ?, ?, ?,
            ?, ?,
            ?, ?,
            ?, ?, ?, ?,
            ?, ?
            )
          `;

        values = [
          new Date().toISOString(),

          device.name.split('/')[1],
          device.name.split('/').pop(),
          device.type,
          device.productNumber,

          device.labels.kit,
          device.labels.name || null,

          device.reported.motion.state,
          device.reported.motion.updateTime,

          device.reported.networkStatus.signalStrength,
          device.reported.networkStatus.rssi,
          device.reported.networkStatus.updateTime,
          device.reported.networkStatus.transmissionMode,

          device.reported.batteryStatus.percentage,
          device.reported.batteryStatus.updateTime
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

async function streamEvents() {
  let retryCount = 0
  let stream
  setupStream()

  // Sets up a timer that will restart the stream if there has passed too 
  // much time between ping events. This timer is reset every time we 
  // receive a ping.
  const pingTimer = setTimeout(() => {
      console.log("Too long between pings. Reconnecting...")
      clearTimeout(pingTimer)
      setTimeout(() => { // Wait a second before reconnecting
          setupStream()
      }, 1000)
  }, (PING_INTERVAL + PING_JITTER) * 1000)

  async function setupStream() {
      // If we've retried too many times without getting any messages, exit
      if (retryCount >= MAX_CONNECTION_RETRIES) {
          console.log("Retried too many times. Exiting")
          process.exit(1)
      }
      retryCount += 1
      
      // Close the existing stream if we have one
      if (stream) {
          stream.close()
      }

      console.log('Streaming... Press CTRL+C to exit.')

      // Add query parameters to the URL
      let url = DEVICES_STREAM_URL
      url += `?ping_interval=${PING_INTERVAL}s` // Specifies ping interval

      // Prepare the "Authorization" header with basic auth.
      // NOTE: This should be implemented using OAuth2 in a production environment.
      const basicAuthStr = `${API_KEY}:${API_SECRET}`
      const headers = {
          Authorization: "Basic " + Buffer.from(basicAuthStr).toString("base64")
      }
      
      // Set up a new stream with callback functions for messages and errors
      // Using headers only works external "eventsource" package. In a browser
      // environment, either use polyfill or the "token" query parameter with
      // an access token from OAuth2.
      stream = new EventSource(url, { headers })
      stream.onmessage = handleStreamMessage
      stream.onerror = handleStreamError
  }
  
  function handleStreamError(err) {
      console.error("Got error from stream:")
      console.error(err)
      console.log("Reconnecting...")
      
      clearTimeout(pingTimer)
      setTimeout(() => { // Wait a second before reconnecting
          setupStream()
      }, 1000)
  }
  
  function handleStreamMessage(message) {
      // Parse the payload as JSON
      const data = JSON.parse(message.data)

      // Check if we got an error
      if (data?.error) {
          handleStreamError(data.error)
          return
      }

      // Reset the retry counter now that we've got an event
      retryCount = 0

      // Parse the event object
      const event = data.result.event
      if (event.eventType === "ping") {
          // We got a ping event. Reset the ping timer
          pingTimer.refresh()
      } else {
          // We got an event
          console.log(`Got ${event.eventType} event.`)
      }
  }
}

function main() {
  
  // Backup every 3 minutes
  fetchSensorData();
  cron.schedule('*/3 * * * *', () => {
    fetchSensorData();
  });

  // // DEBUG: Run the fetchSensorData function every 10 seconds
  // cron.schedule('*/10 * * * * *', () => {
  //   fetchSensorData();
  // });

  // // EXPERIMENTAL: Stream events from the API
  // streamEvents();
}