## Redfish to grpc

This repo implements a Redfish schema to grpc translator.  It should not be
considered production ready, and the grpc APIs that it produces should not be
considered stable.  The repo aims to create a reasonable representation of
Redfish schemas within protobuf grpc, with the goal of looking as "standard" as
possible to as if the grpc models had been written by hand.

To run:
```
pip install -r requirements.txt
python3 redfish_to_grpc.py
```

This will regenerate all the schemas, as well as run them through the protoc compiler.
Output can be seen in the grpc/proto_out directory.  For ease of reviewing, the output
from the initial run is checked into the repo.

Base types are converted to their most equivalent type (string to string, number to
int64, ect).  Enums are converted directly into protobuf enums.

For individual schemas, only the newest version of each is generated at this
time, with all properties added in the order they were added into the standard,
similar to how protobuf service additions are versioned.

To make it simpler to run, this repo includes and extracts Redfish schemas
version 2021.4, as well as the latest swordfish and odata schemas.  Missing
schemas are automatically downloaded as they are imported, as grpc requires all
schemas to be resolvable.

There are a number of things that still need answered before this can be a production
and stable conversion.
