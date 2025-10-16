"""
Part3Service - Service class for Part 3 requirements of School Absence Analysis.

This module contains a service class that implements the advanced requirement:
- Explore links between school type, pupil absences, and location
"""
import os
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, when, lit, isnan, count, desc, lower, asc, row_number, round
from pyspark.sql.functions import sum as spark_sum, first as spark_first, avg as spark_avg, round as spark_round
from pyspark.sql.functions import min as spark_min, max as spark_max

from pyspark.sql.window import Window
import matplotlib.pyplot as plt

from school_absence.analysis.helper import Helper
from school_absence.config import Config

class Part3Service:
    """Service class for implementing Part 3 requirements."""
    
    # Define the four school types to analyze
    SCHOOL_TYPES = ["Total", "Special", "State-funded secondary", "State-funded primary"]

    def __init__(self, helper: Helper):
        """
        Initialize the Part3Service with a Spark DataFrame.
        
        Args:
            df: Spark DataFrame containing the school absence data
            spark: SparkSession instance
        """
        self.helper = helper
        self.df = helper.get_df()
        self.spark = helper.get_spark()
        self.df_la = helper.get_la()
        self.df_school_types = helper.get_school_types()
        self.charts_dir = Config.figure_dir
    
    def analyze_regions_all_school_types(self):
        """Analyze absence rates by region for all school types."""
        print("\n=== Analyzing All School Types by Region ===")
        
        # Get latest year data for all school types at regional level
        region_data = self.helper.filter(
            self.df,
            year="2018",  # Using latest year for most current analysis
            geographic_level="Regional"
        ).filter(col("school_type").isin(self.SCHOOL_TYPES))
        
        
        # Aggregate absence rates by region and school type
        absence_by_region_type = region_data.groupBy("region_name", "school_type").agg(
            spark_avg("sess_overall_percent").alias("overall_absence_rate"),
            spark_avg("sess_authorised_percent").alias("authorised_absence_rate"),
            spark_avg("sess_unauthorised_percent").alias("unauthorised_absence_rate")
        )
        
        # Display results for each school type
        for school_type in self.SCHOOL_TYPES:
            print(f"\nTop 5 Regions with Highest Absence Rates for {school_type} Schools:")
            filtered_data = absence_by_region_type.filter(col("school_type") == school_type)
            filtered_data.orderBy(
                desc("overall_absence_rate")
            ).drop("school_type").limit(5).show(truncate=False)
        
        # Find the region with the highest absence rate for each school type
        highest_by_type = {}
        for school_type in self.SCHOOL_TYPES:
            highest = absence_by_region_type.filter(col("school_type") == school_type) \
                .orderBy(desc("overall_absence_rate")).first()
            if highest:
                highest_by_type[school_type] = highest
        
        

        # Calculate school type averages across all regions
        school_type_avgs = {}
        for school_type in self.SCHOOL_TYPES:
            type_data = absence_by_region_type.filter(col("school_type") == school_type)
            overall_avg = type_data.agg(spark_avg("overall_absence_rate")).collect()[0][0]
            auth_avg = type_data.agg(spark_avg("authorised_absence_rate")).collect()[0][0]
            unauth_avg = type_data.agg(spark_avg("unauthorised_absence_rate")).collect()[0][0]
            school_type_avgs[school_type] = (overall_avg, auth_avg, unauth_avg)
        
        # Calculate region ranges for each school type
        region_ranges = {}
        for school_type in self.SCHOOL_TYPES[1:]:  # Skip "Total"
            regions_data = absence_by_region_type.filter(col("school_type") == school_type).collect()
            if regions_data:
                region_rates = [(r.region_name, r.overall_absence_rate) for r in regions_data]
                region_rates.sort(key=lambda x: x[1], reverse=True)
                highest = region_rates[0]
                lowest = region_rates[-1]
                diff = highest[1] - lowest[1]
                region_ranges[school_type] = (lowest[0], lowest[1], highest[0], highest[1], diff)
        
        # Find schools types with highest variation across regions
        if region_ranges:
            most_variable_type = max(region_ranges.items(), key=lambda x: x[1][4])
        
        # Find regions that appear as highest for multiple school types
        region_counts = {}
        for school_type, highest in highest_by_type.items():
            if school_type != "Total":  # Skip Total
                region = highest.region_name
                if region not in region_counts:
                    region_counts[region] = []
                region_counts[region].append(school_type)
        
        consistent_regions = {r: types for r, types in region_counts.items() if len(types) > 1}
        
        # Create visualization
        print("\nCreating visualizations comparing all school types across regions...")
        self.create_region_comparison_charts(absence_by_region_type)
        
        print("\nPress Enter to continue analysis...")
        input()

        # +++ Comprehensive Summary Output
        print("\n" + "="*80)
        print("                   COMPREHENSIVE ANALYSIS SUMMARY")
        print("="*80)
        
        # 1. Display highest absence rate regions for each school type
        print("\n1. REGIONS WITH HIGHEST ABSENCE RATES BY SCHOOL TYPE:")
        print("-" * 70)
        for school_type in self.SCHOOL_TYPES:
            if school_type in highest_by_type:
                highest = highest_by_type[school_type]
                print(f"• {school_type}: {highest.region_name} ({highest.overall_absence_rate:.2f}%)")
                print(f"  - Authorised: {highest.authorised_absence_rate:.2f}%, Unauthorised: {highest.unauthorised_absence_rate:.2f}%")
        
        print("\nPress Enter to continue analysis...")
        input()

        # 2. Display school type comparisons
        print("\n2. COMPARISON OF SCHOOL TYPES (Average across all regions):")
        print("-" * 70)
        
        # Sort school types by average absence rate
        sorted_types = sorted([(t, v[0]) for t, v in school_type_avgs.items() if t != "Total"], 
                              key=lambda x: x[1], reverse=True)
        
        for school_type, avg in sorted_types:
            auth = school_type_avgs[school_type][1]
            unauth = school_type_avgs[school_type][2]
            print(f"• {school_type}: {avg:.2f}% (Auth: {auth:.2f}%, Unauth: {unauth:.2f}%)")
        
        # Calculate and display differences between school types
        if len(sorted_types) > 1:
            highest_type = sorted_types[0][0]
            lowest_type = sorted_types[-1][0]
            diff = sorted_types[0][1] - sorted_types[-1][1]
            print(f"• Difference: {highest_type} schools have {diff:.2f}% higher absence rates than {lowest_type} schools")
        
        print("\nPress Enter to continue analysis...")
        input()

        # 3. Display regional variation
        print("\n3. REGIONAL VARIATION BY SCHOOL TYPE:")
        print("-" * 70)
        
        for school_type, range_data in region_ranges.items():
            lowest_region, lowest_rate, highest_region, highest_rate, diff = range_data
            print(f"• {school_type}:")
            print(f"  - Highest in {highest_region}: {highest_rate:.2f}%")
            print(f"  - Lowest in {lowest_region}: {lowest_rate:.2f}%")
            print(f"  - Regional variation: {diff:.2f}% (max difference)")
        
        print("\nPress Enter to continue analysis...")
        input()

        # 4. Display consistent patterns across regions
        print("\n4. REGIONS WITH CONSISTENT PATTERNS ACROSS SCHOOL TYPES:")
        print("-" * 70)
        
        if consistent_regions:
            for region, types in consistent_regions.items():
                type_str = ", ".join(types)
                print(f"• {region}: Highest absence rates for {type_str}")
        else:
            print("No regions show consistently high absence rates across multiple school types")
        
        print("\nPress Enter to continue analysis...")
        input()

        # 5. Key findings about the relationship between school type and location
        print("\n5. KEY FINDINGS ABOUT SCHOOL TYPE AND LOCATION RELATIONSHIP:")
        print("-" * 70)
        
        # Find most variable school type
        if region_ranges:
            most_variable = most_variable_type[0]
            variation = most_variable_type[1][4]
            print(f"• {most_variable} schools show the greatest regional variation ({variation:.2f}% difference)")
        
        # Compare unauthorized absence patterns
        sec_unauth = school_type_avgs.get("State-funded secondary", (0, 0, 0))[2]
        pri_unauth = school_type_avgs.get("State-funded primary", (0, 0, 0))[2]
        if sec_unauth > pri_unauth:
            print(f"• Secondary schools have {sec_unauth - pri_unauth:.2f}% higher unauthorized absence rates than primary schools")
        
        # Special schools finding
        if "Special" in school_type_avgs:
            spec_overall = school_type_avgs["Special"][0]
            print(f"• Special schools consistently show high absence rates (avg: {spec_overall:.2f}%) across all regions")
        
        # Worst region overall if available
        if "Total" in highest_by_type:
            worst_region = highest_by_type["Total"].region_name
            print(f"• {worst_region} has the highest overall absence rates across all school types")
        
        print("\nCharts comparing all school types across regions have been saved to the 'charts' directory.")
        print("="*80)

        print("\nAnalysis complete. ")
        print("\nPress Enter to continue...")
        input()

    def create_region_comparison_charts(self, absence_data):
        os.makedirs(self.charts_dir, exist_ok=True)
        """Create charts comparing school types across regions."""
        print("\nCreating comparison visualizations...")
        
        # 1. Create a comparison chart for overall absence rates
        
        # Prepare data for charting - collect regions once
        all_regions = absence_data.select("region_name").distinct().orderBy("region_name").collect()
        region_names = [r.region_name for r in all_regions]
        
        # Create figure and axis
        plt.figure(figsize=(14, 8))
        
        # Set positions for bars and width
        x = list(range(len(region_names)))
        width = 0.2  # Narrower bars for multiple groups
        
        # Collect data for each school type
        for i, school_type in enumerate(self.SCHOOL_TYPES[1:]):  # Skip Total
            # Get data for this school type
            type_data = absence_data.filter(col("school_type") == school_type)
            
            # Map to region order
            values = []
            for region in region_names:
                row = type_data.filter(col("region_name") == region).first()
                if row:
                    values.append(row.overall_absence_rate)
                else:
                    values.append(0)
            
            # Calculate position offset for this school type
            offset = (i - 1) * width  # Center the bars
            
            # Create bars
            plt.bar([p + offset for p in x], values, width, label=school_type)
        
        # Add labels and title
        plt.xlabel('Region')
        plt.ylabel('Overall Absence Rate (%)')
        plt.title('Comparison of Absence Rates by School Type Across Regions (2018)')
        plt.xticks(x, region_names, rotation=45, ha='right')
        plt.legend()
        plt.tight_layout()
        
        # Save chart
        chart_path = os.path.join(self.charts_dir, "region_school_type_comparison.png")
        plt.savefig(chart_path)
        plt.close()
        
        print(f"Comparison chart saved to {chart_path}")
        
        # 2. Create a chart showing authorized vs unauthorized absence by school type
        
        # Calculate averages across all regions for each school type
        avg_by_type = {}
        for school_type in self.SCHOOL_TYPES:
            type_data = absence_data.filter(col("school_type") == school_type)
            avg_auth = type_data.agg(spark_avg("authorised_absence_rate")).collect()[0][0]
            avg_unauth = type_data.agg(spark_avg("unauthorised_absence_rate")).collect()[0][0]
            avg_by_type[school_type] = (avg_auth, avg_unauth)
        
        # Create figure and axis
        plt.figure(figsize=(10, 6))
        
        # Set positions for bars
        x = list(range(len(self.SCHOOL_TYPES)))
        width = 0.35
        
        # Extract values
        auth_vals = [avg_by_type[t][0] for t in self.SCHOOL_TYPES]
        unauth_vals = [avg_by_type[t][1] for t in self.SCHOOL_TYPES]
        
        # Create grouped bars
        plt.bar([i - width/2 for i in x], auth_vals, width, label='Authorised')
        plt.bar([i + width/2 for i in x], unauth_vals, width, label='Unauthorised')
        
        # Add labels and title
        plt.xlabel('School Type')
        plt.ylabel('Absence Rate (%)')
        plt.title('Average Authorised vs Unauthorised Absence by School Type (2018)')
        plt.xticks(x, self.SCHOOL_TYPES, rotation=45, ha='right')
        plt.legend()
        plt.tight_layout()
        
        # Save chart
        chart_path = os.path.join(self.charts_dir, "school_type_auth_unauth_comparison.png")
        plt.savefig(chart_path)
        plt.close()
        
        print(f"Absence type chart saved to {chart_path}")
        
        # 3. Create a heat map of absence rates by region and school type
        plt.figure(figsize=(12, 8))
        
        # Prepare data for heatmap
        heat_data = {}
        for region in region_names:
            heat_data[region] = {}
            for school_type in self.SCHOOL_TYPES[1:]:  # Skip Total
                row = absence_data.filter(
                    (col("region_name") == region) & 
                    (col("school_type") == school_type)
                ).first()
                if row:
                    heat_data[region][school_type] = row.overall_absence_rate
                else:
                    heat_data[region][school_type] = 0
        
        # Create the heatmap data matrix
        heat_matrix = []
        for region in region_names:
            row_data = []
            for school_type in self.SCHOOL_TYPES[1:]:  # Skip Total
                row_data.append(heat_data[region][school_type])
            heat_matrix.append(row_data)
        
        plt.imshow(heat_matrix, cmap='YlOrRd')
        plt.colorbar(label='Absence Rate (%)')
        
        plt.yticks(range(len(region_names)), region_names)
        plt.xticks(range(len(self.SCHOOL_TYPES[1:])), self.SCHOOL_TYPES[1:], rotation=45, ha='right')
        
        plt.title('Heatmap of Absence Rates by Region and School Type (2018)')
        plt.tight_layout()
        
        # Save heatmap
        chart_path = os.path.join(self.charts_dir, "region_school_type_heatmap.png")
        plt.savefig(chart_path)
        plt.close()
        
        print(f"Heatmap saved to {chart_path}")