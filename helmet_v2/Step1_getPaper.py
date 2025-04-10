import os
import sys
from utils.step1_getArticles import step1_getPubmed, process_multiple_instructions

def get_instruction_file_paths(file_names, base_dir, folder="DataLake"):
    """
    Given a list of instruction file names, constructs the absolute paths based on the
    project root and a specified subfolder.
    
    Args:
        file_names (List[str]): List of file names (e.g., ["bim.txt", "cem.txt"])
        base_dir (str): The project root directory.
        folder (str): Relative folder where the instruction files are stored.
        
    Returns:
        List[str]: List of absolute file paths.
    """
    file_paths = []
    instructions_folder = os.path.join(base_dir, folder)
    for name in file_names:
        file_path = os.path.join(instructions_folder, name.strip())
        file_paths.append(file_path)
    return file_paths

if __name__ == "__main__":
    # Determine the project root; this file is located in the project root.
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # Check if the user provided file names as command-line arguments.
    # For example: python Step1_getPaper.py bim.txt cem.txt
    if len(sys.argv) > 1:
        # Get file names from command-line arguments (skip the first argument which is the script name)
        instruction_file_names = sys.argv[1:]
    else:
        # Otherwise, prompt the user to input a comma–separated list of instruction file names.
        inp = input("Enter instruction file names (comma-separated, e.g., bim.txt,cem.txt): ")
        instruction_file_names = [name.strip() for name in inp.split(",") if name.strip()]
    
    # Build the absolute paths for each instruction file based on the expected folder structure.
    instruction_files = get_instruction_file_paths(instruction_file_names, BASE_DIR, folder="DataLake")
    
    # Print out the list of files to confirm what will be processed.
    print("Processing the following instruction files:")
    for f in instruction_files:
        print(f)
    
    # Process all instruction files.
    # This calls the existing function for a single file for each file in the list.
    process_multiple_instructions(instruction_files)
