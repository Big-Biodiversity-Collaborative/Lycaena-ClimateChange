# Load functions from functions directory
# Rachel Laura
# rlaura@arizona.edu
# 2023-04-04

# Usage: source(file = "load_functions.R")

function_files <- list.files(path = "./functions", 
                             pattern = ".R$", 
                             full.names = TRUE)
for(fun_file in function_files) {
  source(file = fun_file)
}