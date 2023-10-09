from __future__ import annotations

import contextlib
import datetime
import os
import platform
import re
import subprocess
from abc import abstractmethod
from enum import auto, StrEnum
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import ClassVar, Protocol, TypedDict, TYPE_CHECKING

import click
import fabric
from seaflowpy.seaflowfile import dayofyear_re, keep_evt_files, timestamp_from_filename
from seaflowpy.time import parse_date

if TYPE_CHECKING:
    from paramiko.sftp_client import SFTPClient


class FileSelection:
    """Class for sync file selection results"""
    def __init__(self):
        self.sync: list[str] = []
        self.nosync: list[str] = []
        self.last_file: str | None = None
        self.bad_timestamp: list[str] = []


class DirLister(Protocol):
    """Protocol ABC class for listing files in a directory"""
    @abstractmethod
    def listdir(self, path: str=".") -> list[str]:
        raise NotImplementedError


class LocalDirLister(DirLister):
    """Class to list files in a local directory"""
    def listdir(self, path: str=".") -> list[str]:
        return [str(p.name) for p in Path(path).glob("*")]


class Sync:
    inst_log_name: ClassVar[str] = "SFlog.txt"

    def __init__(
        self,
        source_root: str,
        dest_root: str,
        min_date: datetime.date | None=None,
        max_date: datetime.date | None=None
    ):
        self.source_root = source_root
        self.source_evt_sfl_root: str = str(Path(source_root) / "datafiles" / "evt")
        self.source_inst_log_root: str = str(Path(source_root) / "logs")
        self.inst_log_path: str = str(Path(self.source_inst_log_root) / self.inst_log_name)
        self.dest_root = dest_root
        self.files_from_evt_sfl_path: str = ""
        self.files_from_inst_log_path: str = ""
        self.min_date = min_date
        self.max_date = max_date

        self.source_lister: DirLister = LocalDirLister()
        self.dest_lister: DirLister = LocalDirLister()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def run(self):
        # Find all EVT and SFL files in source
        source_files = self._list_evt_sfl_files()
        source_evt_files = keep_evt_files(source_files)
        source_sfl_files = [f for f in source_files if f.endswith(".sfl")]
        print(f"found {len(source_evt_files)} EVT files on source", flush=True)
        print(f"found {len(source_sfl_files)} SFL files on source", flush=True)

        # Select files to sync
        evt_select = self._select_files_for_sync(
            source_evt_files, remove_last=True
        )
        print(f"selected {len(evt_select.sync)} EVT files to sync", flush=True)
        if evt_select.last_file:
            print(f"ignoring last EVT file {evt_select.last_file}", flush=True)
        sfl_select = self._select_files_for_sync(
            source_sfl_files, remove_last=False
        )
        print(f"selected {len(sfl_select.sync)} SFL files to sync", flush=True)

        # Make output dir
        Path(self.dest_root).mkdir(exist_ok=True, parents=True)

        # Transfer EVT and SFL
        self._rsync_evt_sfl_files(evt_select, sfl_select)

        # Transfer instrument log file
        print(f"looking for instrument log file at {self.inst_log_path}", flush=True)
        log_files = self.source_lister.listdir(self.source_inst_log_root)
        log_file = [f for f in log_files if f == self.inst_log_name]
        if log_file:
            print("found instrument log", flush=True)
            self._rsync_inst_log_file()

    def _list_evt_sfl_files(self) -> list[str]:
        """Return a sorted list of evt and sfl files on source"""
        files = []
        dayofyear_dirs = keep_dayofyear_dirs(self.source_lister.listdir(self.source_evt_sfl_root))
        for dayofyear_dir in dayofyear_dirs:
            # Get files in DOY dir
            dayofyear_dir_path = Path(self.source_evt_sfl_root) / dayofyear_dir
            dir_files = self.source_lister.listdir(str(dayofyear_dir_path))
            # Construct file path from file names relative to source evt root
            dir_files = [str(Path(dayofyear_dir) / f) for f in dir_files]
            files.extend(dir_files)
        files.sort()
        return files

    def _select_files_for_sync(self, files: list[str], remove_last: bool=False) -> FileSelection:
        """Select files that should be synced

        Files that should be synced are a product of these rules, in this order:
        * those with timestamps that can be parsed
        * if remove_last, not the latest file
        * date >= self.min_date, if set
        * date <= self.max_date, if set

        Return a 4-tuple of (files-to-sync, files-to-not-sync, files-with-bad-timestamps)
        """
        select = FileSelection()
        for f in files:
            try:
                d = { "file": f, "dt": datetime_from_filename(f) }
                passed = True
                if self.min_date and d["dt"].date() < self.min_date:
                    passed = False
                if self.max_date and d["dt"].date() > self.max_date:
                    passed = False
                if passed:
                    select.sync.append(d["file"])
                else:
                    select.nosync.append(d["file"])
            except ValueError:
                # File name could not be parsed with timestamp, skip
                select.bad_timestamp.append(f)
        select.sync.sort()
        select.nosync.sort()

        # Remove last file (may be open EVT file)
        if remove_last and len(select.sync) > 0:
            select.last_file = select.sync[-1]
            select.sync = select.sync[:-1]

        return select

    def _rsync_evt_sfl_files(self, evt_select: FileSelection, sfl_select: FileSelection):
        """Rsync transfer EVT/SFL files"""
        # Save --files-from file
        with NamedTemporaryFile(mode="w", delete=False) as fh:
            lines = sfl_select.sync + evt_select.sync
            if lines:
                fh.write("\n".join(lines) + "\n")
            self.files_from_evt_sfl_path = fh.name

        # Rsync
        args = [
            "-au", "--stats", "-v",
            "--files-from", self.files_from_evt_sfl_path,
            f"{self.source_evt_sfl_root}/", f"{self.dest_root}/"
        ]
        args = ["rsync"] + self._modify_rync_cmd(args)
        print(" ".join(args), flush=True)
        subprocess.check_call(args)

    def _rsync_inst_log_file(self):
        # Save --files-from file
        rel_path = str(Path(self.inst_log_path).relative_to(self.source_inst_log_root))
        with NamedTemporaryFile(mode="w", delete=False) as fh:
            fh.write(rel_path + "\n")
            self.files_from_inst_log_path = fh.name

        # Rsync
        args = args = [
            "-au", "--stats", "-v",
            "--files-from", self.files_from_inst_log_path,
            f"{self.source_inst_log_root}/", f"{self.dest_root}/"
        ]
        args = ["rsync"] + self._modify_rync_cmd(args)
        print(" ".join(args), flush=True)
        subprocess.check_call(args)

    def _modify_rync_cmd(self, args):
        """Override this method to add e.g. SSH connection args"""
        return args


class SSHConfig(TypedDict):
    """Config for a remote SSH connection with private key"""
    host: str
    port: int
    user: str
    pkey_file: str


class RemoteSync(Sync):
    def __init__(
        self,
        source_root: str,
        dest_root: str,
        ssh_config: SSHConfig,
        min_date: datetime.date | None=None,
        max_date: datetime.date | None=None
    ):
        super().__init__(source_root, dest_root, min_date, max_date)
        self.host = ssh_config["host"]
        self.port = ssh_config["port"]
        self.user = ssh_config["user"]
        self.pkey_file = ssh_config["pkey_file"]

        connect_kwargs = { "key_filename": self.pkey_file }
        self.conn: fabric.connection.Connection = fabric.Connection(
            self.host, port=self.port,
            user=self.user, connect_timeout=10,
            connect_kwargs=connect_kwargs
        )
        self.source_lister: DirLister = self.conn.sftp()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()

    def _modify_rync_cmd(self, args):
        """Return a list of rsync args with SSH support, without rsync itself"""
        args[-2] = f"{self.user}@{self.host}:{args[-2]}"  # source dir is always args[-2]
        ssh_arg = f"-e 'ssh -p {self.port} -i {self.pkey_file}'"
        return [ssh_arg] + args


class MD5:
    """Class to run platform md5 binary and produce normalized output"""
    exe_args: list[str]

    def __init__(self):
        if platform.system() == "Linux":
            self.exe_args = ["md5sum"]
        elif platform.system() == "Darwin":
            self.exe_args = ["md5", "-r"]

    def run(self, paths: list[str]) -> list[str]:
        """Run MD5 binary and return output as lines of 'MD5  PATH'"""
        out_b: bytes = subprocess.check_output(self.exe_args + paths)
        output = []
        for line in out_b.decode(encoding="utf-8").split("\n"):
            fields = line.split(maxsplit=1)
            output.append("  ".join(fields))
        return output


class Compression(StrEnum):
    GZ = auto()
    ZST = auto()


class Squish:
    """Compress files"""
    def __init__(self, compression: Compression):
        self.type = f"{compression}"

    def run(self, files: list[str]):
        if self.type == "gz":
            subprocess.check_call(["gzip"] + files)
        elif self.type == "zst":
            subprocess.check_call(["zstd", "--rm", "--no-progress"] + files)


def keep_dayofyear_dirs(dayofyear_dirs: list[str]) -> list[str]:
    """Filter for SeaFlow day of year directory names"""
    passed = []
    for d in dayofyear_dirs:
        if re.match(dayofyear_re, Path(d).name):
            passed.append(d)
    return passed


def datetime_from_filename(f: str | Path) -> datetime.datetime:
    return parse_date(timestamp_from_filename(f))


@contextlib.contextmanager
def chdir(path: str | Path):
    """Context manager to change working directory"""
    orig_dir = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(orig_dir)


def validate_date(ctx, param, value) -> datetime.date:
    if value:
        try:
            value = datetime.date.fromisoformat(value)
        except ValueError as e:
            raise click.BadParameter('min-timestamp could not be parsed.') from e
    return value


def command_option_group(grouped_options: list[str]):
    class CommandOptionGroupClass(click.Command):
        def invoke(self, ctx):
            found_options = []
            for option_name in grouped_options:
                if option_name in ctx.params and ctx.params[option_name] is not None:
                    found_options.append(option_name)
            if found_options:
                missing = set(grouped_options).difference(found_options)
                if missing:
                    formatted_options = [o.replace("_", "-") for o in grouped_options]
                    options_str = "--" + ", --".join(formatted_options)
                    missing_str = "--" + ", --".join(sorted(missing))
                    raise click.ClickException(
                        f"missing option(s) [{missing_str}]: {options_str} are all required if any are specified"
                    )
            super().invoke(ctx)

    return CommandOptionGroupClass

@click.group()
def cli():
    pass


@cli.command("sync", cls=command_option_group(["host", "port", "user", "pkey_file"]))
@click.option("--host", type=str)
@click.option("--port", type=int)
@click.option("--user", type=str)
@click.option("--pkey-file", type=click.Path(exists=True, dir_okay=False, readable=True))
@click.option("--source-root", type=str, required=True,
              help="Directory on source that contains datafiles/evt and logs subdirs")
@click.option("--dest-root", type=str, required=True)
@click.option("--min-date", type=str, callback=validate_date, required=False,
              help="""Min date of files to process, with format template
                      2023-10-04.""")
@click.option("--max-date", type=str, callback=validate_date, required=False,
              help="""Min timestamp of files to process, with format template
                      2023-10-04.""")
def sync_cmd(host, port, user, pkey_file, source_root, dest_root, min_date, max_date):
    """Sync SeaFlow data from source to destination"""
    if min_date:
        print(f"min-date = {min_date.isoformat()}")
    if max_date:
        print(f"max-date = {max_date.isoformat()}")

    if host:
        ssh_config: SSHConfig = {
            "host": host, "port": port,
            "user": user, "pkey_file": pkey_file
        }
        with RemoteSync(
            source_root, dest_root,
            ssh_config, min_date=min_date, max_date=max_date
        ) as sync:
            sync.run()
    else:
        sync = Sync(
            source_root, dest_root, min_date=min_date, max_date=max_date)
        sync.run()


@cli.command("prepare")
@click.option("--compression", type=click.Choice([str(c) for c in Compression], case_sensitive=False))
@click.argument(
    "root",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, writable=True, readable=True, path_type=Path),
    required=True)
def prepare_cmd(compression, root):
    """Calculate checksums and compress SeaFlow files"""
    md5 = MD5()

    with chdir(root):
        files = [str(p) for p in Path(".").glob("*/*")]     # 2023_279/2023-10-06T08-00-00+00-00 for example
        print(f"found {len(files)} files", flush=True)
        evt_files = keep_evt_files(files)
        compress_exts = [f".{c}" for c in Compression]
        evt_compressed_files = [f for f in evt_files if Path(f).suffix in compress_exts]
        if len(evt_compressed_files):
            raise click.ClickException(
                f"Error: found {len(evt_files)} uncompressed EVT files, please decompress and restart"
            )
        print(f"found {len(evt_files)} uncompressed EVT files", flush=True)
        sfl_files = [f for f in files if f.endswith(".sfl")]
        print(f"found {len(sfl_files)} SFL files", flush=True)
        log_files = []
        if Path(Sync.inst_log_name).exists():
            log_files = [Sync.inst_log_name]
        print(f"found {len(log_files)} instrument log files", flush=True)
        if evt_files or sfl_files or log_files:
            files_to_hash = evt_files + sfl_files + log_files
            md5_output = md5.run(files_to_hash)
            md5_path = Path("evt-sfl-log.md5")
            with md5_path.open(mode="w", encoding="utf-8") as fh:
                fh.write("\n".join(md5_output) + "\n")
            print(f"wrote EVT/SFL/instrument-log MD5 to {root / md5_path}", flush=True)

            if compression:
                squish = Squish(compression)
                print(f"compressing EVT files as '.{squish.type}'")
                squish.run(evt_files)

                # Recreate list of EVT files post compression
                files = [str(p) for p in Path(".").glob("*/*")]
                evt_files = keep_evt_files(files)

                # Get checksum of compressed EVT files and original SFL and instrument log files
                files_to_hash = evt_files + sfl_files + log_files
                md5_output = md5.run(files_to_hash)
                md5_path = Path(f"evt.{squish.type}-sfl-log.md5")
                with md5_path.open(mode="w", encoding="utf-8") as fh:
                    fh.write("\n".join(md5_output) + "\n")
                print(f"wrote EVT.{squish.type} MD5 to {root / md5_path}", flush=True)


if __name__ == '__main__':
    cli()
