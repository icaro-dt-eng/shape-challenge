# Equipment Failure Analysis

## Description

This project is designed to analyze equipment failure data to uncover patterns and trends. It involves processing raw sensor data, conducting in-depth data analysis, and storing the insights in a PostgreSQL database for further examination and reporting. The following diagram describes the architecture of this project:


<p align="center">
  <img src="/diagram.svg" class="center" alt="image" width="350" height="auto">
</p>

## Features
- Parses failure logs to extract relevant sensor data.
- Creates tables in a PostgreSQL database to store equipment, sensor, and failure data.
- Executes SQL queries to answer the questions:
  - Total equipment failures that happened?
  - Which equipment name had most failures?
  - Average amount of failures across equipment group, ordered by the number of failures in ascending order?
  - Rank the sensors which present the most number of errors by equipment name in an equipment group.
- Generates visualizations based on the analysis results.

## Installation
To set up the project environment, follow these steps:
1. Clone the repository to your local machine:
   `git clone <repository_url>`
   <br />

2. Navigate to the root directory of the project:
   `cd <project_directory>`
   <br />

3. Ensure that you have Docker installed on your system. If not, you can download and install it from the [official Docker website](https://docs.docker.com/engine/install/).
    <br />

4. Once Docker is installed, proceed to the usage steps.

## Usage
1. Unpack the equipment_failure_sensors.rar file into the data folder located in the root directory of the project. This step is crucial for the project to function correctly.
    <br />

2. In the root directory of the project, create a .env file if it doesn't already exist. This file should contain environment variables required for the project. Here's an example of what the .env file might look like:
    ```
    DB_HOST=your_database_host
    DB_NAME=your_database_name
    DB_USER=your_database_user
    DB_PASSWORD=your_database_password
    DB_PORT=your_database_port
    ```
    Replace your_database_host, your_database_name, your_database_user, your_database_password, and your_database_port with the appropriate values for your database setup.
    <br />

    A .env.example file is available in the root directory.
    <br />



3. Run the following command to build and start the containers:
    `docker-compose up --build`
    <br />

4. Wait for the containers to build and start. Once the project is executed, the data will be processed and inserted into the database, and the answers to the previously mentioned questions will be printed."