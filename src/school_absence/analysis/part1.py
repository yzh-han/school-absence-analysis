"""
Part1Service - Service class for Part 1 requirements of School Absence Analysis.

This module contains a service class that implements the first part requirements:
- Search by local authority
- Search by school type
- Search for unauthorized absences
"""

from typing import Dict, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, isnan, count, desc, lower, row_number
from pyspark.sql.functions import sum as spark_sum, first as spark_first
from pyspark.sql.window import Window

from school_absence.analysis.helper import Helper

class Part1Service:
    """Service class for implementing Part 1 requirements."""
    
    def __init__(self, helper: Helper):
        """
        Initialize the Part1Service with a Spark DataFrame.
        
        Args:
            df: Spark DataFrame containing the school absence data
        """
        self.helper = helper
        self.df = helper.get_df()
        self.spark = helper.get_spark()
        self.df_la = helper.get_la()
        self.df_school_types = helper.get_school_types()

    def search_local_authority_enrollment(self):
        """
        Create a table of pupil enrollments with years as rows and local authorities as columns.
        Allows user to enter local authorities directly.
        
        Part 1 requirement:
        "Allow the user to search the dataset by the local authority, showing the number of 
        pupil enrolments in each local authority by time period (year)."
        """
        print("\nAvailable Local Authorities:")
        self.helper.search_local_authority_by_prefix()

        # Ask user to enter local authorities
        print("\nEnter local authority names separated by commas (e.g., 'Barnet, Luton'):")
        user_input = input("> ").strip()

        if not user_input:
            print("No local authorities entered. Returning to main menu.")
            return
        
        # Process user input
        selected_las = [la.strip() for la in user_input.split(',')]

        # Validate if selected local authorities exist in the dataset
        for la in selected_las:
            if not self.helper.valid(self.df_la, "la_name", la):
                print(f"Warning: Local authority '{la}' not found.")
                return

        # Query to get enrollment data by local authority and time period
        query = self.helper.filter(self.df, la_name=selected_las, school_type="Total")
        query = self.helper.query(
            query, "time_period", type="enrolments",pivot="la_name"
        ).orderBy("time_period")
            
        # Show the result
        print(f"\nEnrollments for local authorities - {', '.join(selected_las)}:")
        query.show(20, truncate=False)

    def search_school_type(self):
        """
        Allow users to search for authorized absences by school type and year.
        
        Part 1 requirement:
        "Allow the user to search the dataset by school type, showing the total number 
        of pupils who were given authorised absences in a specific time period (year)."
        """
        # Show available school types
        print("\nAvailable School Types:")
        self.df_school_types.show(truncate=False)
        
        # Ask user for school type
        print("\nEnter school type:")
        school_type = input("> ").strip()
        
        # Ask user for year
        print("\nEnter year from 2006-2018 (e.g., 2015):")
        year = input("> ").strip()
        
        # Basic query for total authorized absences
        query = self.helper.filter(
            self.df,
            year=year,
            geographic_level="National",
            school_type=school_type, 
        )
        # Display results
        print(f"\nAuthorized Absences of England for {school_type} Schools in {year}:")
        self.helper.query(
            query,
            "time_period",
            agg_fields={"sess_authorised": "authorised"},
            pivot="school_type"
        ).show(truncate=False)        
        
        # Extend with breakdown of specific types of authorized absences
        print("\nWould you like to see a breakdown of specific types of authorized absences? (y/n)")
        breakdown = input("> ").strip().lower()
        
        if breakdown == 'y':
            # Extended query for breakdown of specific absence types
            extended_query = self.helper.query(
                query,
                "time_period",
                type="authorized",
            ).collect()
        
            print(f"\nBreakdown of Authorized Absences for {school_type} Schools in {year}:")
            for row in extended_query:
                for key, val in row.asDict().items():
                    print(f"\t{key}: {val}")

    def search_unauthorized_absences(self):
        """
        Allow users to search for unauthorized absences by region or local authority.
        
        Part 1 requirement:
        "Allow a user to search for all unauthorized absences in a certain year, 
        broken down by either region name or local authority name."
        """
        # Ask for year
        print("\nEnter year to analyze unauthorized absences (e.g., 2015):")
        year = input("> ").strip()

        # Ask whether to group by region or local authority
        print("\nGroup by (1) Region or (2) Local Authority? Enter 1 or 2:")
        group_option = input("> ").strip()


        # Filter unauthorized absences by year, school type and group option
        dict_filter = {
            "year": year,
            "school_type": "Total"
        }

        if group_option == '1':
            dict_filter["geographic_level"] = option = "Regional"
            group_by = "region_name"
        elif group_option == '2': 
            dict_filter["geographic_level"] = option = "Local authority"
            group_by = "la_name"
        else: raise ValueError("Invalid option. Please try again.")

        query = self.helper.filter(self.df,**dict_filter)
        
        query = self.helper.query(
            query,
            group_by,
            type="unauthorized",
        ).orderBy(col("absences").desc())
        
        # Show results
        print(f"\nUnauthorized Absences by {option} in {year}:")
        self.helper.show(query, "absences")
                
