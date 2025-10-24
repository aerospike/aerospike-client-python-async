# Aerospike Async Python Client

## Setup Instructions

### Setup local virtual environment (pyenv is recommended, but any can work)

#### Install python packages:
```commandline
pip install -r requirements.txt
```
<br>

### All-in-one command:
1. Build Rust code into a wheel that is added as a Python module into local virtual environment
2. Generate Python stubs from Rust code
3. Run tests with Pytest
```commandline
make dev-test
```
<br>

### Individual commands:
#### Build Rust code into a wheel that is added as a Python module into local virtual environment :
```commandline
make dev
```

#### Run tests:
```commandline
make test
```
<br>

### Optional - only needed if updating the Rust code
### Build the Python stubs for the Rust code:
```commandline
make stubs
```
<br>

### Known TODOs:
*   Tests:
    - Move / port more of the legacy python tests to the new client
    - Go through Java client tests and check for analogous test coverage 
*  Pipeline benchmarks: track performance between runs.
*  Decide about introducing the Bin class, or keep using Dicts (for Khosrow and Ronen when the latter is back from vacation)?
*  Move more API:
    - CDTs
*  Object serialization:
    - Test __getstate__ and __setstate__ and make sure they work. Otherwise implement them.
*  Cross-Python Client compatibility testing - esp data types
    - Write from legacy, read from new
    - Write from new, read from legacy
*  Track known remaining "core" (Rust Client) items:
    - TLS
    - AP/SC
    - MRT
    - smaller items