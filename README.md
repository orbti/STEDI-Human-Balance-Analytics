# Project: STEDI Human Balance Analytics

### Introduction

In this project, you'll act as a data engineer for the STEDI team to build a data lakehouse solution for sensor data that trains a machine learning model.

### Project Details

The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

* trains the user to do a STEDI balance exercise;
* and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
* has a companion mobile app that collects customer data and interacts with the device sensors.

STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

### Project Summary

As a data engineer on the STEDI Step Trainer team, you'll need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

## Implementation

### Landing Zone

[Customer Landing SQL](https://github.com/orbti/STEDI-Human-Balance-Analytics/blob/main/scripts/customer_landing.sql)

[Accelerometer Landing SQL](https://github.com/orbti/STEDI-Human-Balance-Analytics/blob/main/scripts/acceleromeiter_landing.sql)

![alt text](https://github.com/orbti/STEDI-Human-Balance-Analytics/blob/main/images/customer_landing.PNG "Customer Landing Table")

![alt text](https://github.com/orbti/STEDI-Human-Balance-Analytics/blob/main/images/accelerometer_landing.PNG "Accelerometer Landing Table")

### Trusted Zone

[Customer Landing to Trusted Script](https://github.com/orbti/STEDI-Human-Balance-Analytics/blob/main/scripts/customer_landing_to_trusted.py)

[Accelerometer Landing to Trusted Script](https://github.com/orbti/STEDI-Human-Balance-Analytics/blob/main/scripts/accelerometer_landing_to_trusted.py)

![alt text](https://github.com/orbti/STEDI-Human-Balance-Analytics/blob/main/images/customer_trusted.PNG "Customer Trusted Table")

![alt text](https://github.com/orbti/STEDI-Human-Balance-Analytics/blob/main/images/customer_trusted.PNG "Accelerometer Trusted Table")

### Curated Zone

[Customer Trusted to Curated Script](https://github.com/orbti/STEDI-Human-Balance-Analytics/blob/main/scripts/customer_trusted_to_curated.py)

### Machine Learning Zone

[Step Trainer Trusted Script](https://github.com/orbti/STEDI-Human-Balance-Analytics/blob/main/scripts/step_trainer_to_trusted.py)

ADD SPARK SCRIPTS HERE FOR MACHINE LEARNING
