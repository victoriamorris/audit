# audit
A tool to perform an audit of the FULL catalogue in Catalogue Bridge.

## Installation

From GitHub:

    git clone https://github.com/victoriamorris/audit
    cd audit

To install as a Python package:

    python setup.py install
    
To create stand-alone executable (.exe) files for individual scripts:

    python setup.py py2exe
    
Executable files will be created in the folder audit\dist, and should be copied to an executable path.

## Usage

### Running scripts

The following scripts can be run from anywhere, once the package is installed:

#### audit

Audit the FULL catalogue in Catalogue Bridge.
    
    Usage: audit [OPTIONS]

    Options:
      -i        INPUT_FOLDER - Path to folder containing input files.
      -o        OUTPUT_FOLDER - Path to folder to save output files.
      --debug   Debug mode.
      --help    Show help message and exit.
    
    INPUT_FOLDER should provide the location of folder containing the files to be analysed.
    If INPUT_FOLDER is not set, files to be audited are assumed to be present in the current folder.
    If OUTPUT_FOLDER is not set, output files are created in the current folder.
    
    Files to be audited must have named of the form full*.lex, where * is a number.
