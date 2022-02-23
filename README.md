Redfish to grpc

This repo implements a Redfish schema to grpc translator.  It should not be
considered production ready, and the grpc APIs that it produces should not be
considered stable.

The repo aims to create a reasonable representation of Redfish schemas within
protobuf grpc, with the goal of looking as "standard" as possible to as if the
grpc models had been written by hand.

Base types are converted to their most equivalent type (string to string, number to
int64, ect).
