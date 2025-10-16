#!/usr/bin/env python3
"""
ConsoleBrowser - A console-based application for analyzing UK school absence data.

This program provides a text-based interface to analyze a dataset containing
pupil absence information in schools in England from 2006-2018.

CS5052 Practical 1: School's Out
"""

import os
import sys
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, year, desc, asc

from school_absence.analysis.part1 import Part1Service
from school_absence.analysis.part2 import Part2Service
from school_absence.analysis.part3 import Part3Service
from school_absence.analysis.helper import Helper
from school_absence.core import data_loader
from school_absence.core import spark_session
from school_absence.analysis.part1 import Part1Service

class ConsoleBrowser:
	"""Main class for the ConsoleBrowser application."""
	
	# User commands
	QUIT = ":quit"
	HELP = ":help"
	SEARCH_LA = ":search_la"  # Part 1: Search by local authority
	SEARCH_TYPE = ":search_type"  # Part 1: Search by school type
	SEARCH_UNAUTH = ":search_unauth"  # Part 1: Search unauthorized absences
	COMPARE_LA = ":compare_la"  # Part 2: Compare local authorities
	REGION_CHART = ":region_chart"  # Part 2: Chart region performance
	EXPLORE_LINK = ":explore_link"  # Part 3: Explore links between type, absence, location
	
	def __init__(self, data_path):
		self.data_path = data_path

	def initialize(self):
		"""Initialize the application."""
		self.spark = spark_session.initialize_spark()
		self.df = data_loader.load_dataset(self.spark, data_path=self.data_path)
		self.helper = Helper(self.df, self.spark)
		self.part1_service = Part1Service(self.helper)
		self.part2_service = Part2Service(self.helper)
		self.part3_service = Part3Service(self.helper)
	
	def display_help(self):
		"""Display help information."""
		help_lines = [
			"\n--* Welcome to the School Absence Analysis ConsoleBrowser. *--",
			"* This tool allows you to analyze pupil absence data from UK schools (2006-2018).",
			"* Available commands:",
			f"\t{self.SEARCH_LA}\tSearch by local authority, showing pupil enrollments by year.",
			f"\t{self.SEARCH_TYPE}\tSearch by school type, showing authorised absences.",
			f"\t{self.SEARCH_UNAUTH}\tSearch for unauthorised absences by region or local authority.",
			f"\t{self.COMPARE_LA}\tCompare two local authorities in a given year.",
			f"\t{self.REGION_CHART}\tAnalyze region performance trends from 2006-2018.",
			f"\t{self.EXPLORE_LINK}\tExplore links between school type, absences, and location.",
			f"\t{self.HELP}\t\tDisplay this help information.",
			f"\t{self.QUIT}\t\tQuit the application."
		]
		
		for line in help_lines:
			print(line)
	
	# display_services method has been removed
	
	def search_local_authority(self):
		"""
		Allow users to search for pupil enrollments by local authority and year.
		
		Part 1 requirement:
		"Allow the user to search the dataset by the local authority, showing the number of 
		pupil enrolments in each local authority by time period (year)."
		"""
		self.part1_service.search_local_authority_enrollment()
			
	def search_school_type(self):
		"""
		Allow users to search for authorized absences by school type and year.
		
		Part 1 requirement:
		"Allow the user to search the dataset by school type, showing the total number 
		of pupils who were given authorised absences in a specific time period (year)."
		"""
		self.part1_service.search_school_type()
		
	def search_unauthorized_absences(self):
		"""
		Allow users to search for unauthorized absences by region or local authority.
		
		Part 1 requirement:
		"Allow a user to search for all unauthorized absences in a certain year, 
		broken down by either region name or local authority name."
		"""
		self.part1_service.search_unauthorized_absences()
		
	def compare_local_authorities(self):
		"""
		Allow users to compare two local authorities in a given year.
		
		Part 2 requirement:
		"Allow a user to compare two local authorities of their choosing in a given year. 
		Justify how you will compare and present the data."
		"""
		self.part2_service.compare_local_authorities()
			
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
		self.part2_service.analyze_region_performance()
	
	def explore_school_type_location_link(self):
		"""
		Explore links between school type, absences, and location.
		
		Part 3 requirement:
		"Explore whether there is a link between school type, pupil absences and the location of the school.
		For example, is it more likely that schools of type X will have more pupil absences in location Y?"
		"""
		self.part3_service.analyze_regions_all_school_types()

	def exit(self):
		"""Exit the application."""
		# Clean up
		print("Shutting down ConsoleBrowser...")
		spark_session.stop_spark(self.spark)
		print("Goodbye!")

	def main_loop(self):
		user_cmd = ""
		quit_browser = False
		
		# Show help on startup
		self.display_help()
		
		while not quit_browser:
			# Display command prompt
			print("\n[", end="")
			commands = [self.SEARCH_LA, self.SEARCH_TYPE, self.SEARCH_UNAUTH, 
					  self.COMPARE_LA, self.REGION_CHART, self.EXPLORE_LINK,
					  self.HELP, self.QUIT]
			print(" | ".join(commands), end="")
			print("] ")
			
			# Get user command
			user_cmd = input("> ").strip()
			
			# Skip blank lines
			if not user_cmd:
				continue
				
			# Process command
			if user_cmd.lower() == self.QUIT:
				quit_browser = True
				
			elif user_cmd.lower() == self.HELP:
				self.display_help()
				
			elif user_cmd.lower() == self.SEARCH_LA:
				self.search_local_authority()
				
			elif user_cmd.lower() == self.SEARCH_TYPE:
				self.search_school_type()
				
			elif user_cmd.lower() == self.SEARCH_UNAUTH:
				self.search_unauthorized_absences()
				
			elif user_cmd.lower() == self.COMPARE_LA:
				self.compare_local_authorities()
				
			elif user_cmd.lower() == self.REGION_CHART:
				self.analyze_region_performance()
				
			elif user_cmd.lower() == self.EXPLORE_LINK:
				self.explore_school_type_location_link()

			else:
				print(f"Unknown command: '{user_cmd}'")

	def run(self):
		"""Initialize and run the ConsoleBrowser."""
		try:
			self.initialize()
			self.main_loop()
		except Exception as e:
			print(f"An error occurred: {str(e)}")
		finally:
			# Clean up
			self.exit()


		
