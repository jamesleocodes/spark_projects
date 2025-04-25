#!/usr/bin/env python3

import sys
import os
import subprocess

def print_help():
    print("Spark Analysis Runner")
    print("---------------------")
    print("Usage:")
    print("  python src/run_analysis.py [command]")
    print("\nAvailable commands:")
    print("  check   - Check if the dataset can be loaded correctly")
    print("  model   - Run the full churn prediction model")
    print("  help    - Show this help message")
    print("\nExample:")
    print("  python src/run_analysis.py check")

def main():
    if len(sys.argv) < 2:
        print("Error: Missing command")
        print_help()
        return
    
    command = sys.argv[1].lower()
    
    if command == "help":
        print_help()
    elif command == "check":
        print("Running data loading check...")
        # Get the current script's directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        subprocess.run(["python", os.path.join(current_dir, "load_churn_data.py")])
    elif command == "model":
        print("Running full churn prediction model...")
        # Get the current script's directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        subprocess.run(["python", os.path.join(current_dir, "run_churn_model.py")])
    else:
        print(f"Error: Unknown command '{command}'")
        print_help()

if __name__ == "__main__":
    main() 