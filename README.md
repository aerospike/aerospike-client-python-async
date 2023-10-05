# Aerospike Python Client

## Setup

requirements: install `maturin`, `unittest`

Run
`source .env/bin/activate`

to activate the `vitualenv`.

Use the make commands to build the plugin.

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
    - Maintain the aerospike.pyi file
    - Test __getstate__ and __setstate__ and make sure they work. Otherwise implement them.
