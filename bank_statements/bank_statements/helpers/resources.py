from contextlib import contextmanager

import dagster as dg


@contextmanager
def yield_resource(resource_key, **kwargs):
    from bank_statements.resources.definitions import resources

    resource_config = {resource_key: {"config": kwargs}} if len(kwargs) > 0 else None

    with dg.build_resources(resources, resource_config=resource_config) as _r:
        yield _r.__getattribute__(resource_key)
