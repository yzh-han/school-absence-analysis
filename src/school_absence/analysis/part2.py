"""
Part2Service - Service class for Part 2 requirements of School Absence Analysis.

This module contains a service class that implements the intermediate requirements:
- Compare two local authorities in a given year
- Chart/explore the performance of regions in England from 2006-2018
"""
import os
from typing import Dict, List, Tuple
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, when, lit, isnan, count, desc, lower, asc, row_number, round
from pyspark.sql.functions import sum as spark_sum, first as spark_first, avg as spark_avg, round as spark_round
from pyspark.sql.window import Window

from school_absence.analysis.helper import Helper

class Part2Service:
    """Service class for implementing Part 2 requirements."""
    
    def __init__(self, helper: Helper):
        """
        Initialize the Part2Service with a Spark DataFrame.
        
        Args:
            df: Spark DataFrame containing the school absence data
            spark: SparkSession instance
        """
        self.helper = helper
        self.df = helper.get_df()
        self.spark = helper.get_spark()

        self.df_la = helper.get_la()
        self.df_school_types = helper.get_school_types()

    def compare_local_authorities(self):
        """
        Allow users to compare two local authorities in a given year.
        
        Part 2 requirement:
        "Allow a user to compare two local authorities of their choosing in a given year. 
        Justify how you will compare and present the data."
        """

        print("\n=== Compare Two Local Authorities ===")
        
        # Help user find local authorities
        print("First, let's find the local authorities you want to compare.")
        self.helper.search_local_authority_by_prefix()
        
        # Get first local authority
        print("\nEnter first local authority name:")
        la1 = input("> ").strip()
        
        # Get second local authority
        print("\nEnter second local authority name:")
        la2 = input("> ").strip()
        
        # Get year
        print("\nEnter year for comparison (e.g., 2015):")
        year = input("> ").strip()

        # Validate
        if (la1 == la2 or 
            not self.helper.valid(self.df_la, "la_name", la1) or
            not self.helper.valid(self.df_la, "la_name", la2) or
            not year.isdigit() or int(year) < 2006 or int(year) > 2018
        ): raise ValueError("Invalid input. Please try again.")
        
        la1_exists = self.df_la.filter((col("la_name") == la1)).count() > 0
        la2_exists = self.df_la.filter((col("la_name") == la2)).count() > 0

        # Get data for comparison (focused on school_type = 'Total')
        comparison_data = self.helper.filter(    
            self.df,
            year=year,
            school_type="Total",
            la_name=[la1, la2]
        )

        # Process and display comparison
        print(f"\n=== Comparison of {la1} and {la2} in {year} ===")

        # 1. Basic statistics
        print("\n1. BASIC STATISTICS")
        print("-" * 90)
        print(f"{'Metric':<30} | {la1:<20} | {la2:<20} | Difference")
        print("-" * 90)
        
        # Get the data as rows for easier access
        rows = comparison_data.collect()
        row1 = next((r for r in rows if r.la_name == la1), None)
        row2 = next((r for r in rows if r.la_name == la2), None)
        
        if not row1 or not row2:
            print("Error retrieving data for comparison.")
            return
        
        # Display basic metrics
        self.helper.print_comparison_row("Number of Schools", row1.num_schools, row2.num_schools)
        self.helper.print_comparison_row("Number of Pupils", row1.enrolments, row2.enrolments)
        self.helper.print_comparison_row("Total Sessions", row1.sess_possible, row2.sess_possible)
        self.helper.print_comparison_row("Overall Absence Rate (%)", row1.sess_overall_percent, row2.sess_overall_percent)
        self.helper.print_comparison_row("Authorised Absence Rate (%)", row1.sess_authorised_percent, row2.sess_authorised_percent)
        self.helper.print_comparison_row("Unauthorised Absence Rate (%)", row1.sess_unauthorised_percent, row2.sess_unauthorised_percent)
        self.helper.print_comparison_row("Persistent Absentees (%)", row1.enrolments_pa_10_exact_percent, row2.enrolments_pa_10_exact_percent)
        self.helper.print_comparison_instruction()

        print("\nPress Enter to continue analysis...")
        input()

        # 2. Detailed absence breakdown
        print("\n\n2. AUTHORISED ABSENCE BREAKDOWN")
        print("-" * 90)
        print(f"{'Absence Type':<30} | {la1:<20} | {la2:<20} | Difference")
        print("-" * 90)
        
        # Calculate percentages for each absence type relative to total possible sessions
        self.helper.print_comparison_row(
            "Illness (%)", 
            self.helper.calc_percentage(row1.sess_auth_illness, row1.sess_possible),
            self.helper.calc_percentage(row2.sess_auth_illness, row2.sess_possible))
        
        self.helper.print_comparison_row(
            "Medical Appointments (%)", 
            self.helper.calc_percentage(row1.sess_auth_appointments, row1.sess_possible),
            self.helper.calc_percentage(row2.sess_auth_appointments, row2.sess_possible))
        
        self.helper.print_comparison_row(
            "Religious Observance (%)", 
            self.helper.calc_percentage(row1.sess_auth_religious, row1.sess_possible),
            self.helper.calc_percentage(row2.sess_auth_religious, row2.sess_possible)
        )
        
        self.helper.print_comparison_row(
            "Study Leave (%)", 
            self.helper.calc_percentage(row1.sess_auth_study, row1.sess_possible),
            self.helper.calc_percentage(row2.sess_auth_study, row2.sess_possible)
        )
        
        self.helper.print_comparison_row("Traveller Absence (%)", 
            self.helper.calc_percentage(row1.sess_auth_traveller, row1.sess_possible),
            self.helper.calc_percentage(row2.sess_auth_traveller, row2.sess_possible)
        )
        
        self.helper.print_comparison_row("Authorised Holiday (%)", 
            self.helper.calc_percentage(row1.sess_auth_holiday, row1.sess_possible),
            self.helper.calc_percentage(row2.sess_auth_holiday, row2.sess_possible)
        )
        
        self.helper.print_comparison_row("Exclusions (%)", 
            self.helper.calc_percentage(row1.sess_auth_excluded, row1.sess_possible),
            self.helper.calc_percentage(row2.sess_auth_excluded, row2.sess_possible)
        )
        
        self.helper.print_comparison_row("Other Authorised (%)", 
            self.helper.calc_percentage(row1.sess_auth_other, row1.sess_possible),
            self.helper.calc_percentage(row2.sess_auth_other, row2.sess_possible)
        )
        self.helper.print_comparison_instruction()

        print("\nPress Enter to continue analysis...")
        input()
        
        # 3. Unauthorised absence breakdown
        print("\n\n3. UNAUTHORISED ABSENCE BREAKDOWN")
        print("-" * 90)
        print(f"{'Absence Type':<30} | {la1:<20} | {la2:<20} | Difference")
        print("-" * 90)
        
        self.helper.print_comparison_row(
            "Unauthorised Holiday (%)", 
            self.helper.calc_percentage(row1.sess_unauth_holiday, row1.sess_possible),
            self.helper.calc_percentage(row2.sess_unauth_holiday, row2.sess_possible)
        )
        
        self.helper.print_comparison_row(
            "Late Arrival (%)", 
            self.helper.calc_percentage(row1.sess_unauth_late, row1.sess_possible),
            self.helper.calc_percentage(row2.sess_unauth_late, row2.sess_possible)
        )
        
        self.helper.print_comparison_row(
            "Other Unauthorised (%)", 
            self.helper.calc_percentage(row1.sess_unauth_other, row1.sess_possible),
            self.helper.calc_percentage(row2.sess_unauth_other, row2.sess_possible)
        )
        
        self.helper.print_comparison_row(
            "No Reason Yet (%)", 
            self.helper.calc_percentage(row1.sess_unauth_noyet, row1.sess_possible),
            self.helper.calc_percentage(row2.sess_unauth_noyet, row2.sess_possible)
        )
        self.helper.print_comparison_instruction()

        print("\nPress Enter to continue analysis...")
        input()
        
        # 4. Summary comparison
        print("\n\n4. SUMMARY COMPARISON")
        print("-" * 90)
        
        # Determine which LA has better attendance
        better_attendance = la1 if row1.sess_overall_percent < row2.sess_overall_percent else la2
        
        print(f"• Overall attendance rate is better in {better_attendance}.")
        
        # Compare persistent absenteeism
        better_persistent = la1 if row1.enrolments_pa_10_exact_percent < row2.enrolments_pa_10_exact_percent else la2
        
        print(f"• Persistent absenteeism is lower in {better_persistent}.")
        
        # Compare main reasons for absence
        main_reason_la1 = self.helper.get_main_absence_reason(row1)
        main_reason_la2 = self.helper.get_main_absence_reason(row2)
        
        print(f"• Main reason for absence in {la1}: {main_reason_la1}")
        print(f"• Main reason for absence in {la2}: {main_reason_la2}")
        
        print("\n\n=== Comparison Complete ===")
        print("\nPress Enter to continue...")
        input()

    def analyze_region_performance(self):
        """
        Analyze and chart region performance over time (2006-2018).
        
        Part 2 requirement:
        "Chart/explore the performance of regions in England from 2006-2018."
        Questions to answer:
        - Are there any regions that have improved in pupil attendance over the years?
        - Are there any regions that have worsened?
        - Which is the overall best/worst region for pupil attendance?
        """
        print("\n=== Analyzing Region Performance (2006-2018) ===")
        
        # Get region data using helper filter
        df_region = self.helper.filter(
            self.df,
            geographic_level="Regional",
            school_type="Total"
        ).select(
            "region_name", 
            "time_period",
            "sess_overall_percent",
            "sess_authorised_percent", 
            "sess_unauthorised_percent"
        ).orderBy("region_name", "time_period")



        self.df_avg = df_region.groupBy("region_name").agg(
            spark_round(
                spark_avg(col("sess_overall_percent")), 4
            ).alias("avg_absence")
        ).cache()
        df_avg = self.df_avg

        
        self.df_overall = self.helper.query(
            df_region,
            "region_name",
            agg_fields={"sess_overall_percent": "overall_percent"},
            pivot="time_period"
        ).cache()
        df_overall = self.df_overall
        
    
        # Extract unique regions and time periods
        
        regions = self.helper.get_region().collect()
        regions = [regin.region_name for regin in regions]
        time_periods = df_overall.columns[1:]

        print(f"Analyzing data for {len(regions)} regions across {len(time_periods)} time periods.")
        
        # Create a DataFrame with first and last period data for each region

        self.df_comparison = df_overall.select(
            "region_name",
            spark_round(col(time_periods[0]),4).alias("first_period"),
            spark_round(col(time_periods[-1]),4).alias("last_period"),
        ).withColumn(
            "change", 
            spark_round(
                col("last_period") - col("first_period"), 4
            )
        )        .join(df_avg, "region_name").orderBy("change").cache()
        df_comparison = self.df_comparison
        
        # Display analysis
        print("\n=== Region Performance Analysis ===")

        # 1. Show overall analysis
        print("\n1. OVERALL REGION ABSENCE PERFORMANCE")
        df_overall.select(
            "region_name",
            *[spark_round(col(c), 2).alias(c[:-2]) for c in time_periods]
        ).show()
        print("\nPress Enter to continue analysis...")
        input()

        # 2. Show overall trends by region
        print("\n2. OVERALL TRENDS")
        print("-" * 85)
        print(f"{'Region':<25} | {'First Period 2006(%)':<20} | {'Last Period 2018(%)':<20} | {'Change (%)':<20}")
        print("-" * 85)
        # Get the sorted data as a list of rows
        sorted_rows = df_comparison.collect()
        # Report on each region
        for row in sorted_rows:
            self.helper.print_comparison_row(
                row.region_name,
                row.first_period,
                row.last_period,
                row.change,
                space= [25, 20, 20, 20]
            )
        print("-" * 85)
        self.helper.print_comparison_instruction()

        print("\nPress Enter to continue analysis...")
        input()
            
        # 3. Identify improved and worsened regions
        print("\n3. REGIONS WITH IMPROVED ATTENDANCE (LOWER ABSENCE RATE)")
        improved_regions = df_comparison.filter(col("change") < 0).collect()
        for row in improved_regions:
            print(f"• {row.region_name}: Absence rate decreased by {abs(row.change):.2f}% (from {row.first_period:.2f}% to {row.last_period:.2f}%)")
        if len(improved_regions) == 0:
            print("No regions have improved in attendance.")

        print("\n3. REGIONS WITH WORSENED ATTENDANCE (HIGHER ABSENCE RATE)")
        worsened_regions = df_comparison.filter(col("change") > 0).collect()
        for row in worsened_regions:
            print(f"• {row.region_name}: Increased by {row.change:.2f}% (from {row.first_absence:.2f}% to {row.last_period:.2f}%)")
        if len(worsened_regions) == 0:
            print("No regions have worsened in attendance.")
        print("\nPress Enter to continue analysis...")
        input()

        # 4. Identify best and worst regions overall
        print("\n4. BEST AND WORST REGIONS FOR PUPIL ATTENDANCE")
        
        # Find best and worst regions by average absence rate
        df_avg.orderBy("avg_absence").show()
        best_region = df_avg.orderBy("avg_absence").first()
        worst_region = df_avg.orderBy(desc("avg_absence")).first()
        print(f"• Best region overall: {best_region.region_name} (Average absence rate: {best_region.avg_absence:.2f}%)")
        print(f"• Worst region overall: {worst_region.region_name} (Average absence rate: {worst_region.avg_absence:.2f}%)")
        print("\nPress Enter to continue analysis...")
        input()

        # 5. Create visualizations
        print("\nWould you like to create visualizations for this analysis? (y/n)")
        visual_option = input("> ").strip().lower()
        
        if visual_option == 'y':
            self.helper.create_chart(
                df_overall,
                title="Region Over Absence Rate 2006-2018",
                x_label="Time Period",
                y_label="Attendance Rate (%)",
                save_path="figures/region_overall_absence.png"
            )
        print("\nAnalysis complete. ")
        print("\nPress Enter to continue...")
        input()



        

