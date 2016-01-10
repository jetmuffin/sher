go clean
go build -o sched
pushd executor
go build -o exec
popd