echo "go clean ..."
go clean
echo "build scheduler ..."
go build -o sched
pushd executor
echo "build executor ..."
go build -o exec
popd