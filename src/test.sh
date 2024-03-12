for i in {1..100} ; do
    go test ./raft
    go test ./kvraft
done > ./test_result_100.txt