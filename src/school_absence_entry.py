"""
This is the entry point of the program. It initializes the ConsoleBrowser and runs it.
"""
import sys
import os


from school_absence.ui.browser import ConsoleBrowser


if __name__ == "__main__":
    # Get the data path from the command line arguments
    if len(sys.argv) < 2:
        data_path = None
    else:
        data_path = sys.argv[1] # Path to the dataset file
    
    # Initialize and run the ConsoleBrowser
    browser = ConsoleBrowser(data_path)
    try:
        browser.run()
    except Exception as e:
        print(f"An error occurred and exit.")
    finally:
        browser.exit()

