#!/usr/bin/env python3
"""
Post-process the generated .pyi file to add full method stubs for WritePolicy and ReadPolicy.
This is needed because pyo3_stub_gen doesn't generate stubs for classes using PyClassInitializer.
"""

import os
import sys
import re

def add_policy_stubs(pyi_file_path):
    """Add full method stubs for WritePolicy and ReadPolicy classes."""
    
    # ReadPolicy stub definition
    read_policy_stub = '''class ReadPolicy(BasePolicy):
    def __new__(cls) -> ReadPolicy: ...
    @property
    def replica(self) -> Replica: ...
    @replica.setter
    def replica(self, value: Replica) -> None: ...
    @property
    def filter_expression(self) -> typing.Optional[FilterExpression]: ...
    @filter_expression.setter
    def filter_expression(self, value: typing.Optional[FilterExpression]) -> None: ...
'''
    
    # WritePolicy stub definition
    write_policy_stub = '''class WritePolicy(BasePolicy):
    def __new__(cls) -> WritePolicy: ...
    @property
    def record_exists_action(self) -> RecordExistsAction: ...
    @record_exists_action.setter
    def record_exists_action(self, value: RecordExistsAction) -> None: ...
    @property
    def generation_policy(self) -> GenerationPolicy: ...
    @generation_policy.setter
    def generation_policy(self, value: GenerationPolicy) -> None: ...
    @property
    def commit_level(self) -> CommitLevel: ...
    @commit_level.setter
    def commit_level(self, value: CommitLevel) -> None: ...
    @property
    def generation(self) -> builtins.int: ...
    @generation.setter
    def generation(self, value: builtins.int) -> None: ...
    @property
    def expiration(self) -> Expiration: ...
    @expiration.setter
    def expiration(self, value: Expiration) -> None: ...
    @property
    def send_key(self) -> builtins.bool: ...
    @send_key.setter
    def send_key(self, value: builtins.bool) -> None: ...
    @property
    def respond_per_each_op(self) -> builtins.bool: ...
    @respond_per_each_op.setter
    def respond_per_each_op(self, value: builtins.bool) -> None: ...
    @property
    def durable_delete(self) -> builtins.bool: ...
    @durable_delete.setter
    def durable_delete(self, value: builtins.bool) -> None: ...
'''
    
    # Read the current file
    with open(pyi_file_path, 'r') as f:
        content = f.read()
    
    # Replace ReadPolicy if it's just "class ReadPolicy(BasePolicy): ..."
    read_policy_pattern = r'class ReadPolicy\(BasePolicy\):\s*\.\.\.'
    if re.search(read_policy_pattern, content):
        content = re.sub(read_policy_pattern, read_policy_stub.rstrip(), content)
        print(f"Updated ReadPolicy class stubs in {pyi_file_path}")
    else:
        print(f"ReadPolicy class not found or already updated in {pyi_file_path}")
    
    # Replace WritePolicy if it's just "class WritePolicy(BasePolicy): ..."
    write_policy_pattern = r'class WritePolicy\(BasePolicy\):\s*\.\.\.'
    if re.search(write_policy_pattern, content):
        content = re.sub(write_policy_pattern, write_policy_stub.rstrip(), content)
        print(f"Updated WritePolicy class stubs in {pyi_file_path}")
    else:
        print(f"WritePolicy class not found or already updated in {pyi_file_path}")
    
    # Write the updated content back
    with open(pyi_file_path, 'w') as f:
        f.write(content)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python add_policy_stubs.py <path_to_pyi_file>")
        sys.exit(1)
    
    pyi_file = sys.argv[1]
    if not os.path.exists(pyi_file):
        print(f"Error: File {pyi_file} does not exist")
        sys.exit(1)
    
    add_policy_stubs(pyi_file)
