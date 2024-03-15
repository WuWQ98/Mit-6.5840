n=1

for ((i=1; i<=$n; i++)); do
    echo "Running iteration $i"
    { go test ./raft && go test ./kvraft; } 2>&1 | tee -a test_result_$n.txt
done

echo "All iterations completed. Check 'test_result_$n.txt' for details."