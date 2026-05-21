module github.com/aspectrr/beluga-ext-host

go 1.25.0

replace github.com/aspectrr/beluga => ../beluga

replace github.com/aspectrr/beluga-ext-sdk => ../beluga-ext-sdk

require (
	github.com/aspectrr/beluga v0.0.0-00010101000000-000000000000
	github.com/aspectrr/beluga-ext-sdk v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.81.1
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.9.2 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260401024825-9d38bb4040a9 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
