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



TODO:
    - Move the current python tests to the new client.
    - Do more benchmarks, track performance between runs.
    - Implement the `aerospike.Exception` class and convert rust errors to said class.
    - Decide about introducing the Bin class, or keep using Dicts (for Khosrow and Ronen when the latter is back from vacation)?
    - Move more API:
        - CDTs
        - User management
    - See if tox can be used
    - Use the CI config for github and ensure that it works
    - Test __getstate__ and __setstate__ and make sure they work. Otherwise implement them.
