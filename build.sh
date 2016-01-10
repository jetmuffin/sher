echo "build scheduler ..."
go clean
go build -o sched
pushd executor
echo "build executor ..."
go clean
go build -o exec
popd