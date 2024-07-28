#!/bin/bash

# .scripts/sample_generator.sh 100 ./test_files/sample_100.csv

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <number_of_200_blocks> <output_file>"
  exit 1
fi

# Get the parameters
num_200_blocks=$1
output_file=$2

# Write the header record
echo "100,NEM12,200506081149,UNITEDDP,NEMMCO" > "$output_file"

# Generate blocks
for i in $(seq 1 $num_200_blocks); do
    echo "200,NEM${num_200_blocks},E1E2,1,E1,N1,01009,kWh,30,20050610" >> "$output_file"
    echo "300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,A,,,20050310121004,20050310182204" >> "$output_file"
    echo "300,20050302,0,0,0,0,0,0,0,0,0,0,0,0,0.235,0.567,0.890,1.123,1.345,1.567,1.543,1.234,0.987,1.123,0.876,1.345,1.145,1.173,1.265,0.987,0.678,0.998,0.768,0.954,0.876,0.845,0.932,0.786,0.999,0.879,0.777,0.578,0.709,0.772,0.625,0.653,0.543,0.599,0.432,0.432,A,,,20050310121004,20050310182204" >> "$output_file"
    echo "300,20050303,0,0,0,0,0,0,0,0,0,0,0,0,0.261,0.310,0.678,0.934,1.211,1.134,1.423,1.370,0.988,1.207,0.890,1.320,1.130,1.913,1.180,0.950,0.746,0.635,0.956,0.887,0.560,0.700,0.788,0.668,0.543,0.738,0.802,0.490,0.598,0.809,0.520,0.670,0.570,0.600,0.289,0.321,A,,,20050310121004,20050310182204" >> "$output_file"
    echo "300,20050304,0,0,0,0,0,0,0,0,0,0,0,0,0.335,0.667,0.790,1.023,1.145,1.777,1.563,1.344,1.087,1.453,0.996,1.125,1.435,1.263,1.085,1.487,1.278,0.768,0.878,0.754,0.476,1.045,1.132,0.896,0.879,0.679,0.887,0.784,0.954,0.712,0.599,0.593,0.674,0.799,0.232,0.612,A,,,20050310121004,20050310182204" >> "$output_file"
    echo "500,O,S01009,20050310121004," >> "$output_file"
done

# Write the tail record
echo -n "900" >> "$output_file"

echo "Mock data CSV file generated: $output_file"
