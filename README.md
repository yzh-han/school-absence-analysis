# School Absence Analysis Application

## CS5052 Practical 1: School's Out

This is a console-based application, which analyzes pupil absence data in schools in England from 2006-2018 using Apache Spark and Python.

## Prerequisites

Before running the application, ensure you have the following installed:

- java@21
- python3.12
- PySpark
- Matplotlib

using `pip install -r requirements.txt` to install dependency. 

## Dataset

The application is designed to work with the UK government's pupil absence dataset (2006-2018). The original dataset is not included, but the cached version is included.

The dataset should be a CSV file.

## Running the Application

To run the application, use the following command from the project's root directory `src/`:

```bash
python school_absence_entry.py [path_to_dataset.csv]
```

Or you can run without the dataset path, it will to use the stored version:

```bash
python school_absence_entry.py
```

## Usage

Once the application starts, a command prompt will appear with available commands:

- `:search_la` - Search by local authority, showing pupil enrollments by year
- `:search_type` - Search by school type, showing authorised absences
- `:search_unauth` - Search for unauthorised absences by region or local authority
- `:compare_la` - Compare two local authorities in a given year
- `:region_chart` - Analyze region performance trends from 2006-2018
- `:explore_link` - Explore links between school type, absences, and location
- `:help` - Display help information
- `:quit` - Exit the application

### Example Workflow

1. Start the application with the dataset
2. Use `:search_la` to search for specific local authorities
3. Enter a list of local authorities to analyze
4. View the results and continue with other commands

## Features

The application includes three levels of functionality:

### Basic Features (Part 1)
- Dataset reading and storage using Apache Spark
- Search by local authority for pupil enrollments
- Search by school type for authorised absences
- Search for unauthorised absences by region or local authority

### Intermediate Features (Part 2)
- Compare two local authorities in a given year
- Chart and explore the performance of regions in England from 2006-2018

### Advanced Features (Part 3)
- Explore relationships between school type, pupil absences, and location

## Output Files

Charts and visualizations are saved to the `figures/` directory.

## Project Structure

```bash
src/
├── school_absence/
│   ├── analysis/
│   │   ├── helper.py
│   │   ├── part1.py
│   │   ├── part2.py
│   │   └── part3.py
│   ├── config.py
│   ├── core/
│   │   ├── data_loader.py
│   │   └── spark_session.py
│   └── ui/
│       └── browser.py
├── school_absence_entry.py
├── README.md
├── cache/
└── figures/
```