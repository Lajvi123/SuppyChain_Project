'''
This setup file is essential part of 
packaging and distributing python projects
It's used by steup tools to define the 
configuration of the project.
'''

from setuptools import find_packages, setup
from typing import List

def get_requirements()->List[str]:
    """
    This function will return the list of requirement
    """
    require_list:List[str]=[]
    try:
        with open('requirement.txt', 'r') as file:
            #Read lines from the file 
            lines = file.readlines()
            #Process each line
            for line in lines:
                requirements = line.strip()
                #Ignore empty lines and -e. 

                if requirements and requirements!= '-e .':
                    require_list.append(requirements)

    except FileNotFoundError:
        print("File doesn't exist")

    return require_list

print(get_requirements())

setup(
    name = "SupplyChain",
    version ="0.0.1",
    author = "Lajvi Sanjay Bhavsar",
    author_email = "lb71@rice.edu",
    packages = find_packages(),
    install_requires = get_requirements()
)
