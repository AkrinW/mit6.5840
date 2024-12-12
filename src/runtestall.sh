if [ "$#" -ne 1 ]; then
  echo "Usage: $0  <iterations>"
  exit 1
fi

iterations=$1

output_file="output.txt"  # 定义结果保存的文件名
> "$output_file"  # 清空或创建 output 文件


# 迭代执行测试
for ((i=1; i<=iterations; i++))
do

  # 将完整的测试命令写入文件
  echo "time go test -failfast -race" >> "$output_file"

  echo "Running test iteration $i" >> "$output_file"
  
  # 执行完整命令并捕获输出和状态
  output=$(time go test -race -failfast 2>&1)
  status=$?

  if [[ $status -ne 0 ]]; then
    echo "Error in iteration $i:" >> "$output_file"
    echo "$output" >> "$output_file"
  else
    echo "Iteration $i succeeded." >> "$output_file"
    echo "$output" >> "$output_file"
  fi
done
