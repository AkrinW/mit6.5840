#!/bin/bash

# check the number of arguments
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <test_type> <iterations>"
  exit 1
fi

test_type=$1
iterations=$2

# check the iterations is a positive integer
if ! [[ "$iterations" =~ ^[0-9]+$ ]]; then
  echo "Invalid iterations: $iterations"
  echo "iterations must be a positive integer"
  exit 1
fi


output_file="output-$test_type.txt"  # 定义结果保存的文件名
> "$output_file"  # 清空或创建 output 文件

# 迭代执行测试
for ((i=1; i<=iterations; i++))
do

  # 将完整的测试命令写入文件
  echo "time go test -failfast -run $test_type" >> "$output_file"

  echo "Running test iteration $i" >> "$output_file"
  
  # 执行完整命令并捕获输出和状态
  output=$(time go test -race -failfast -run "$test_type" 2>&1)
  status=$?

  if [[ $status -ne 0 ]]; then
    echo "Error in iteration $i:" >> "$output_file"
    echo "$output" >> "$output_file"
  else
    echo "Iteration $i succeeded." >> "$output_file"
    echo "$output" >> "$output_file"
  fi
done
