import logging
import click
from requests.exceptions import RequestException
from .commands import *
from .exceptions import ConsulException


@click.group()
@click.pass_context
def cli(ctx, **kwargs):
    """
    Consul utilities.
    """
    ctx.ensure_object(dict)


def run_command(command_cls, ctx, args):
    try:
        cmd = command_cls(ctx=ctx, args=args)
        cmd.run_and_report()
    except ConsulException as e1:
        logging.error(e1)
    except RequestException as e2:
        logging.error(e2)


@cli.command(short_help='Dump key values')
@click.option('--log-level', help='log level')
@click.option('-c', '--config-file', help='Config file path', type=click.File('r'))
@click.option('-h', '--host', help='Consul host')
@click.option('-p', '--port', help='Consul port', type=int)
@click.option('--scheme', help='Consul scheme')
@click.option('-t', '--token', help='Consul ACL token')
@click.option('-r', '--root', help='Search root for consul')
@click.option('-x', '--output-type', help='Output type, text, csv or json', type=click.Choice(['text', 'json', 'csv']))
@click.option('-o', '--output-file', help='Output file path')
@click.option('--clear-cache', help='Clear cache before search', default=False, is_flag=True)
@click.pass_context
def dump(ctx, **kwargs):
    """
    Dump consul key values.
    """
    run_command(DumpCommand, ctx, kwargs)


@cli.command(short_help='Copy key from source to target')
@click.option('--log-level', help='log level')
@click.option('-c', '--config-file', help='Config file path', type=click.File('r'))
@click.option('-h', '--host', help='Consul host')
@click.option('-p', '--port', help='Consul port', type=int)
@click.option('--scheme', help='Consul scheme')
@click.option('-t', '--token', help='Consul ACL token')
@click.option('-r', '--root', help='Search root for consul', required=True)
@click.option('-x', '--output-type', help='Output type, text, csv or json', type=click.Choice(['text', 'json', 'csv']))
@click.option('-o', '--output-file', help='Output file path')
@click.option('--clear-cache', help='Clear cache before search', default=False, is_flag=True)
@click.option('--target-root', help='Target copy root for consul', required=True)
@click.pass_context
def copy(ctx, **kwargs):
    """
    Copy consul keys from source to target.
    """
    run_command(CopyCommand, ctx, kwargs)


@cli.command(short_help='Search in the consul key values')
@click.option('--log-level', help='log level')
@click.option('-c', '--config-file', help='Config file path', type=click.File('r'))
@click.option('-h', '--host', help='Consul host')
@click.option('-p', '--port', help='Consul port', type=int)
@click.option('--scheme', help='Consul scheme')
@click.option('-t', '--token', help='Consul ACL token')
@click.option('-r', '--root', help='Search root for consul')
@click.option('-x', '--output-type', help='Output type, text, csv or json', type=click.Choice(['text', 'json', 'csv']))
@click.option('-o', '--output-file', help='Output file path')
@click.option('--clear-cache', help='Clear cache before search', default=False, is_flag=True)
@click.option('-q', '--query', help='Search query string', required=True)
@click.option('-e/ ', '--regex/--no-regex', help='Search query using regex or not', default=False)
@click.option('-f', '--fields', help='Search fields, keys or values', type=click.Choice(['keys', 'values']))
@click.option('-l', '--limit', help='Search output result limit')
@click.pass_context
def search(ctx, **kwargs):
    """
    Search in the consul key values.
    """
    run_command(SearchCommand, ctx, kwargs)


@cli.command(short_help='Diff between two consul key values.')
@click.option('--log-level', help='log level')
@click.option('-c', '--config-file', help='Config file path', type=click.File('r'))
@click.option('-h', '--host', help='Consul host')
@click.option('-p', '--port', help='Consul port', type=int)
@click.option('--scheme', help='Consul scheme')
@click.option('-t', '--token', help='Consul ACL token')
@click.option('-r', '--root', help='Search root for consul')
@click.option('-x', '--output-type', help='Output type, text, csv or json', type=click.Choice(['text', 'json', 'csv']))
@click.option('-o', '--output-file', help='Output file path')
@click.option('--clear-cache', help='Clear cache before search', default=False, is_flag=True)
@click.option('--host1', help='Consul host for group1, use --host if not specified')
@click.option('--port1', help='Consul port for group1, use --port if not specified', type=int)
@click.option('--scheme1', help='Consul scheme for group1, use --scheme if not specified')
@click.option('--token1', help='Consul ACL token for group1, use --token if not specified')
@click.option('--root1', help='Search root for consul for group1, use --root if not specified')
@click.option('--host2', help='Consul host for group2, use --host if not specified')
@click.option('--port2', help='Consul port for group2, use --port if not specified', type=int)
@click.option('--scheme2', help='Consul scheme for group2, use --scheme if not specified')
@click.option('--token2', help='Consul ACL token for group2, use --token if not specified')
@click.option('--root2', help='Search root for consul for group2, use --root if not specified')
@click.option('--with-same/--without-same', help='Output same values or not', default=False)
@click.pass_context
def diff(ctx, **kwargs):
    """
    Compare consul key values between two consul locations.
    """
    run_command(DiffCommand, ctx, kwargs)
