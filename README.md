### MyDrive Data Challenge

MyDrive Data Challenge

## Requirements

- Python
- PySpark

## How to run

- unzip challenge.zip, place unzipped folder into your working directory.

- `cd your_working_directory/challenge/data-my-drive/src/`

- `python main.py path_to_unzipped_data_directory/interview-data/trips/`

- Example: `python /home/ec2-user/challenge/data-my-drive/src/main.py /home/ec2-user/data/interview-data/trips/`

## Task

1. Sample 1% of data knowing that you will be asked to provide some per driver/per trip
summary statistics.
2. Total distance in km, and total time in seconds for the sample data set.
3. From the sample data set, generate two reports:
a. The total distance in km, total time in hours and average speed in kph per trip
b. The total distance in km, total time in hours and average speed in kph per driver
4. What other summary statistics do you think would be interesting?
5. Optional Task : Visualise data present in reports generated in pt 3. Please provide
screenshot, method / script etc.
You may use whatever tools and resources you want.

## Assumptions

- Analysis carried out on all .processed.json files.

## Conventions

- Visualisation file names are in column1_vs_column2_driver/trip_linear.png for linear plots and column1_vs_column2_driver/trip_scattered.png for scattered plots.

- Plot figures will be created in the current working directory.

## Other Summary Statistics (Question 4)

- It would great to know the max speed of driver and also per trip.

- The number of trips per driver can also give some useful insights.

- One more statistics which can be useful is that how many times a driver crosses a speed threshold.
