import os
from functools import reduce
from typing import Dict, List
from matplotlib import pyplot as plt
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, isnan, count, desc, lower, row_number
from pyspark.sql.functions import sum as spark_sum, first as spark_first
from pyspark.sql.window import Window

class Helper:
    def __init__(self, df: DataFrame, spark: SparkSession):
        """
        Initialize the Helper class with a Spark DataFrame.
        
        Args:
            df: Spark DataFrame containing the school absence data
            spark: SparkSession instance
        """
        self.df = df
        self.spark = spark

        self.df_la = None
        self.df_school_types = None
        self.df_region = None
        self.search_types = {}

        self._initialize()
        
    
    def _initialize(self):
        enrolment_fields = {
            "enrolments": "enrolments",                         # Number of pupil enrolments
            # "enrolments_pa_10_exact": "pa",                     # Number of persistent absentees
            # "enrolments_pa_10_exact_percent": "pa_percent"      # Percentage of persistent absentees
        }

        auth_num_fields = {
            "sess_authorised": "absences",              # Number of authorised absence sessions
            "sess_auth_illness": "illness",             # Number of illness sessions
            "sess_auth_appointments": "appointments",   # Number of medical appointments sessions
            "sess_auth_religious": "religious",         # Number of religious observance sessions
            "sess_auth_study": "study",                 # Number of study leave sessions
            "sess_auth_traveller": "traveller",         # Number of traveller sessions
            "sess_auth_holiday": "holiday",             # Number of authorised holiday sessions
            "sess_auth_ext_holiday": "ext_holiday",     # Number of extended authorised holiday sessions
            "sess_auth_excluded": "excluded",           # Number of excluded sessions
            "sess_auth_other": "other",                 # Number of authorised other sessions
            "sess_auth_totalreasons": "total_reasons",  # Number of authorised reasons sessions   
            "sess_authorised_pa_10_exact": "pa"         # Number of authorised absence sessions of persistent absentees
        }

        auth_percent_fields = {
            "sess_authorised_percent": "absences_percent",      # Authorised absence rate
            "sess_authorised_percent_pa_10_exact": "pa_percent" # Authorised absence rate of persistent absentees
        }

        overall_num_fields = {
            "sess_overall": "absences",                     # Number of overall absence sessions
            "sess_overall_pa_10_exact": "pa",               # Number of overall absence sessions of persistent absentees
            "sess_overall_totalreasons": "total_reasons"    # Number of overall reasons sessions
        }

        overall_percent_fields = {
            "sess_overall_percent": "absences_percent",      # Overall absence rate
            "sess_overall_percent_pa_10_exact": "pa_percent" # Overall absence rate of persistent absentees
        }

        possible_fields = {
            "sess_possible": "possible_sessions",           # Number of sessions possible
            "sess_possible_pa_10_exact": "possible_pa"      # Number of sessions possible of persistent absentees
        }

        unauth_num_fields = {
            "sess_unauthorised": "absences",            # Number of unauthorised absence sessions
            "sess_unauth_late": "late",                 # Number of late sessions
            "sess_unauth_holiday": "holiday",           # Number of unauthorised holiday sessions
            "sess_unauth_other": "other",               # Number of unauthorised other sessions
            "sess_unauth_totalreasons": "total_reasons",# Number of unauthorised reasons sessions
            "sess_unauthorised_pa_10_exact": "pa"       # Number of unauthorised absence sessions of persistent absentees
        }
        
        unauth_percent_fields = {
            "sess_unauthorised_percent": "absences_percent",      # Unauthorised absence rate
            "sess_unauthorised_percent_pa_10_exact": "pa_percent" # Unauthorised absence rate of persistent absentees
        }

        self.search_types = {
            "enrolments": enrolment_fields,
            "authorized": auth_num_fields,
            "authorized_percent": auth_percent_fields,
            "overall": overall_num_fields,
            "overall_percent": overall_percent_fields,
            "possible_sessions": possible_fields,
            "unauthorized": unauth_num_fields,
            "unauthorized_percent": unauth_percent_fields
        }

    def get_df(self): return self.df

    def get_spark(self): return self.spark

    def get_la(self):
        if not self.df_la:
            self.df_la = self.df.select("la_name").distinct().filter(
                col("la_name").isNotNull()
            ).orderBy("la_name").cache()
        return self.df_la
    
    def get_school_types(self):
        if not self.df_school_types:
            self.df_school_types = self.df.select("school_type").distinct().filter(
                col("school_type").isNotNull()
            ).orderBy("school_type").cache()
        return self.df_school_types

    def get_region(self):
        if not self.df_region:
            self.df_region = self.df.select("region_name").distinct().filter(
                col("region_name").isNotNull()
            ).orderBy("region_name").cache()
        return self.df_region

    def valid(
        self,
        df: DataFrame,
        field: str,
        *values,
    )-> bool:
        is_valid = df.filter(col(field).isin(*values)).count() > 0
        return is_valid
        
    def show(
        self,
        df: DataFrame,
        order_by: str,
    ):
        # Show total row number
        print("\nTotal row number: ", df.count())

        window_spec = Window.orderBy(order_by)
        df = df.withColumn("row_num", row_number().over(window_spec))

        # default number of rows to show
        offset = 0
        limit = 10

        more = 'y'
        while(more == 'y'):
            # show rows
            print(f"Rows: {offset+1} to {offset+limit}")
            df.drop("row_num").show(limit, truncate=False)
            # move to next page
            offset += limit
            df = df.filter(col("row_num") > offset)

            # check if there are more rows
            if df.count() == 0: break

            print("\nShow more results? (y/n)")
            more = input("> ").strip().lower()

    def search_local_authority_by_prefix(self) -> bool:
        """
        Allow users to search for local authorities by their first few letters.
        
        Returns:
            List of selected local authority names
        """            
        # Show total count of distinct local authorities
        total_la_count = self.df_la.count()
        print(f"\nSearch Local Authorities by Prefix (Total LAs number: {total_la_count}")
        
        # Ask user for prefix input
        print("\nEnter prefixes to search for local authorities, using comma separated"
                + "(e.g., 'ca,st', empty input show first 10 LAs):")
        prefix_input = input("> ").strip().lower()
        
        if not prefix_input:
            print("No prefixes entered. Showing first 10 local authorities:")
            self.df_la.show(10, truncate=False)
            return False
            
        # Process the prefixes
        prefixes = [prefix.strip() for prefix in prefix_input.split(',')]
        
        # Find matching local authorities for each prefix
        print("\nMatching local authorities:")
        
        has_matches = False 
        for prefix in prefixes:
            matches = self.df_la.filter(
                lower(col("la_name")).like(f"{prefix}%")
            ).orderBy("la_name").collect()

            match_names = [row.la_name for row in matches]

            print(f"\nLAs starti with {prefix}: {match_names}")

            if matches:
                has_matches = True
        
        if not has_matches:
            print("No local authorities found for the given prefixes.")

        return has_matches

    def filter(
        self,
        df: DataFrame,
        year = None,
        geographic_level = None, # National, Regional,Local Authority, School
        school_type = None , # Total, Special, State-funded secondary, State-funded primary
        region_name = None,
        la_name = None,
    ) -> DataFrame:
        dict_filter = {}
        if year: dict_filter["year"] = year
        if geographic_level: dict_filter["geographic_level"] = geographic_level
        if school_type: dict_filter["school_type"] = school_type
        if region_name: dict_filter["region_name"] = region_name
        if la_name: dict_filter["la_name"] = la_name

        
        for key, values in dict_filter.items():
            if type(values) is not list: values = [values]

            if key == "year":
                conditions = [col('time_period').like(f"{value}%") for value in values]
                df = df.filter(reduce(lambda a, b: a | b, conditions))
            else: df = df.filter(col(key).isin(*values) )
        
        return df
    
    def query(
        self,
        df: DataFrame,
        *group_by: str,
        type: str = None, # enrolments, authorised, authorised_percent, overall, overall_percent, possible_sessions, unauthorised
        agg_fields: Dict[str, str] = None,
        pivot = None,
    )-> DataFrame:
        # Get fields to aggregate
        if type:
            if type in self.search_types:
                fields = self.search_types[type]
            else:
                raise ValueError("Invalid type")
        else: fields = agg_fields

        df = df.groupBy(*group_by)
        if pivot: df= df.pivot(pivot)
        df=df.agg(
            *[spark_first(field).alias(alias) for field, alias in fields.items()]
        )
        
        return df

    def print_comparison_row(self, metric, value1, value2, reverse=False, space=[30, 20, 20]):
            """Print a comparison row with formatted values."""
            try:
                # For percentage values, format with 2 decimal places
                if isinstance(value1, float) and value1 < 100:
                    fmt = "{:.2f}%"
                    diff = value1 - value2
                    diff_str = f"{diff:.2f}%"
                # For large numbers, format with commas
                else:
                    fmt = "{:,}"
                    diff = value1 - value2
                    diff_str = f"{diff:,}"
                
                # Add indicator to show which is better (lower absence is better)
                if not isinstance(value1, float) or value1 > 100: indicator = ""
                elif diff == 0: indicator = "="
                elif reverse: # if reverse is True, higher value is better
                    indicator = "▼" if diff < 0  else "▲" 
                else:       # if reverse is False, lower value is better
                    indicator = "▲" if diff < 0  else "▼" 
                
                print(f"{metric:<{space[0]}} | {fmt.format(value1):<{space[1]}} | {fmt.format(value2):<{space[1]}} | {diff_str} {indicator}")
            
            except (TypeError, ValueError):
                print(f"{metric:<{space[0]}} | {'N/A':<{space[1]}} | {'N/A':<{space[1]}} | N/A")

    def print_comparison_instruction(self, reverse=False):
        if reverse:
            print("▲ former is worse, ▼ latter is better")
        else:
            print("▲ former is better, ▼ latter is worse")

    def calc_percentage(self, value, total):
        """Calculate percentage with proper error handling."""
        try:
            if total == 0:
                return 0.0
            return (value / total) * 100
        except (TypeError, ZeroDivisionError):
            return 0.0
        
    def get_main_absence_reason(self, row):
        """Determine the main reason for absence."""
        absence_types = {
            "Illness": row.sess_auth_illness,
            "Medical Appointments": row.sess_auth_appointments,
            "Religious Observance": row.sess_auth_religious,
            "Study Leave": row.sess_auth_study,
            "Traveller Absence": row.sess_auth_traveller,
            "Authorised Holiday": row.sess_auth_holiday,
            "Exclusions": row.sess_auth_excluded,
            "Other Authorised": row.sess_auth_other,
            "Unauthorised Holiday": row.sess_unauth_holiday,
            "Late Arrival": row.sess_unauth_late,
            "Other Unauthorised": row.sess_unauth_other,
            "No Reason Yet": row.sess_unauth_noyet
        }
        
        main_reason = max(absence_types.items(), key=lambda x: x[1])
        return main_reason[0]
    
    def create_chart(
        self, 
        df: DataFrame, 
        title: str = None, 
        x_label: str =None, 
        y_label: str=None,
        save_path: str = None
    ):
        plt.figure(figsize=(12, 6))
        rows = df.collect()
        xs = df.columns[1:]
        for row in rows:
            line = row[0]
            y = [row[c] for c in xs]
            plt.plot(xs, y, label=line)
        
        plt.legend()
        if title: plt.title(title)
        if x_label: plt.xlabel(x_label)
        if y_label: plt.ylabel(y_label)

        if save_path:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            plt.savefig(save_path, bbox_inches='tight')
            print(f"Chart saved to {save_path}")
            
        plt.show()
        plt.close()


    