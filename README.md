# Task 2: Main Image Selection Pipeline Implementation

## Overview

This project implements a main image selection pipeline for hotel images using Apache Spark and Python. The pipeline reads image data, calculates scores based on various criteria, and selects the best main image for each hotel.

## Prerequisites

- Docker
- Make

Please note that Python and Spark are being installed by Docker.

## How to Execute the Code

To run this project on your local machine, follow these steps:

1. **Build the Docker Image**:
   Navigate to the directory where the Dockerfile is located and build the Docker image.

   ```sh
   cd path/to/dockerfile
   ````

    ```sh
   make build
   ````
   
2. **Run the Docker Container**:
   Navigate to the directory where the Dockerfile is located and build the Docker image.
   
     ```sh
   make run
   ````
   
   
   