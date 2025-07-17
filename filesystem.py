# -*- coding: utf-8 -*-

import hashlib
import logging
import os
import re
import requests
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import traceback
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path


class Filesystem():
    """Operations on files and directories"""
    shutil_ignore_patterns = shutil.ignore_patterns(  # supports globs: shutil.ignore_patterns('*.pyc', 'tmp*')
        "Thumbs.db", "*.swp", "ehthumbs.db", "ehthumbs_vista.db", "*.stackdump", "Desktop.ini", "desktop.ini",
        "$RECYCLE.BIN", "*~", ".fuse_hidden*", ".directory", ".Trash-*", ".nfs*", ".DS_Store", ".AppleDouble",
        ".LSOverride", "._*", ".DocumentRevisions-V100", ".fseventsd", ".Spotlight-V100", ".TemporaryItems",
        ".Trashes", ".VolumeIcon.icns", ".com.apple.timemachine.donotpresent", ".AppleDB", ".AppleDesktop",
        "Network Trash Folder", "Temporary Items", ".apdisk", "Dolphin check log.txt", "*dirmodified", "dds-temp",
        "*.crdownload"
    )

    def fix_permissions(target):
        # ensure that permissions are correct
        if os.path.isfile(target):
            os.chmod(target, 0o664)
        else:
            os.chmod(target, 0o777)
            for root, dirs, files in os.walk(target):
                for d in dirs:
                    os.chmod(os.path.join(root, d), 0o777)
                for f in files:
                    os.chmod(os.path.join(root, f), 0o664)

    @staticmethod
    def run_static(args,
                   cwd,
                   report=None,
                   stdout=subprocess.PIPE,
                   stderr=subprocess.PIPE,
                   shell=False,
                   timeout=600,
                   check=True,
                   stdout_level="DEBUG",
                   stderr_level="DEBUG"):
        """Convenience method for subprocess.run, with our own defaults"""

        (report if report else logging).debug("Kjører: " +
                                              (" ".join(args) if isinstance(args, list) else args))

        completedProcess = None
        try:
            completedProcess = subprocess.run(
                args, stdout=stdout, stderr=stderr, shell=shell, cwd=cwd, timeout=timeout, check=check)

        except subprocess.CalledProcessError as e:
            if report:
                report.error(traceback.format_exc(), preformatted=True)
            else:
                logging.error("exception occured", exc_info=True)
            completedProcess = e

        (report if report else logging).debug("---- stdout: ----")
        if report:
            report.add_message(stdout_level, completedProcess.stdout.decode(
                "utf-8").strip(), add_empty_line_between=True)
        else:
            logging.info(completedProcess.stdout.decode("utf-8"))
        (report if report else logging).debug("-----------------")
        (report if report else logging).debug("---- stderr: ----")
        if report:
            report.add_message(stderr_level, completedProcess.stderr.decode(
                "utf-8").strip(), add_empty_line_between=True)
        else:
            logging.info(completedProcess.stderr.decode("utf-8"))
        (report if report else logging).debug("-----------------")

        return completedProcess

    @staticmethod
    def copytree(report, src, dst):
        assert os.path.isdir(src)

        # check if ancestor directory should be ignored
        src_parts = os.path.abspath(src).split("/")
        for i in range(1, len(src_parts)):
            ignore = Filesystem.shutil_ignore_patterns(
                "/" + "/".join(src_parts[1:i]), [src_parts[i]])
            if ignore:
                return dst

        # use shutil.copytree if the target does not exist yet (no need to merge copy)
        if not os.path.exists(dst):
            try:
                return shutil.copytree(src, dst, ignore=Filesystem.shutil_ignore_patterns)
            except shutil.Error:
                short_src = os.path.sep.join(src.split(os.path.sep)[
                                             :3]) + os.path.sep + "…"
                short_dst = os.path.sep.join(dst.split(os.path.sep)[
                                             :3]) + os.path.sep + "…"
                raise Exception(
                    "An error occured while copying from {} to {}".format(short_src, short_dst))

        src_list = os.listdir(src)
        dst_list = os.listdir(dst)
        src_list.sort()
        dst_list.sort()
        ignore = Filesystem.shutil_ignore_patterns(src, src_list)

        for item in src_list:
            src_subpath = os.path.join(src, item)
            dst_subpath = os.path.join(dst, item)
            if item not in ignore:
                if os.path.isdir(src_subpath):
                    if item not in dst_list:
                        try:
                            shutil.copytree(
                                src_subpath, dst_subpath, ignore=Filesystem.shutil_ignore_patterns)
                        except shutil.Error:
                            short_src = os.path.sep.join(src.split(os.path.sep)[
                                                         :3]) + os.path.sep + "…"
                            short_dst = os.path.sep.join(dst.split(os.path.sep)[
                                                         :3]) + os.path.sep + "…"
                            raise Exception(
                                "An error occured while copying from {} to {}".format(short_src, short_dst))

                    else:
                        Filesystem.copytree(report, src_subpath, dst_subpath)
                else:
                    # Report files that have changed but where the target could not be overwritten
                    if os.path.exists(dst_subpath):
                        src_md5 = Filesystem.path_md5(
                            src_subpath, shallow=False)
                        dst_md5 = Filesystem.path_md5(
                            dst_subpath, shallow=False)
                        if src_md5 != dst_md5:
                            report.error(
                                "Klarte ikke å erstatte filen med nyere versjon: " + dst_subpath)
                    else:
                        shutil.copy(src_subpath, dst_subpath)

        # Report files and folders that could not be removed and were not supposed to be replaced
        for item in dst_list:
            dst_subpath = os.path.join(dst, item)
            if item not in src_list:
                message = "Klarte ikke å fjerne "
                if os.path.isdir(dst_subpath):
                    message += "mappe"
                else:
                    message += "fil"
                message += " som ikke skal eksistere lenger: " + dst_subpath
                report.error(message)

        return dst

    @staticmethod
    def copy(report, source, destination):
        """Copy the `source` file or directory to the `destination`"""
        assert source, "Filesystem.copy(): source must be specified"
        assert destination, "Filesystem.copy(): destination must be specified"
        assert os.path.isdir(source) or os.path.isfile(
            source), "Filesystem.copy(): source must be either a file or a directory: " + str(source)
        report.debug("Copying from '" + source + "' to '" + destination + "'")

        if os.path.isdir(source):
            files_source = os.listdir(source)
        else:
            files_source = [source]

        if os.path.isdir(source):
            try:
                if os.path.exists(destination):
                    if os.listdir(destination):
                        report.info("{} finnes i {} fra før. Eksisterende kopi blir slettet.".format(
                            os.path.basename(destination),
                            os.path.dirname(destination))
                        )
                    shutil.rmtree(destination, ignore_errors=True)
                Filesystem.copytree(report, source, destination)
            except shutil.Error as errors:
                warnings = []
                for arg in errors.args[0]:
                    src, dst, e = arg
                    if e.startswith("[Errno 95]") and "/gvfs/" in dst:
                        warnings.append(
                            "WARN: Unable to set permissions on manually mounted samba shares")
                    else:
                        warnings.append(None)
                warnings = list(set(warnings))  # distinct warnings
                for warning in warnings:
                    if warning is not None:
                        report.warn(warning)
                if None in warnings:
                    raise
        else:
            shutil.copy(source, destination)

        if len(files_source) >= 2:
            files_dir_out = os.listdir(destination)
            for file in files_source:
                if file not in files_dir_out:
                    report.warn(
                        "WARNING: Det ser ut som det mangler noen filer som ble kopiert av Filesystem.copy(): " + str(file))

        elif os.path.isfile(source) and not os.path.isfile(destination):
            report.warn(
                "WARNING: Det ser ut som det mangler noen filer som ble kopiert av Filesystem.copy(): " + str(source))

    @staticmethod
    def zip(report, directory, file):
        """Zip the contents of `dir`"""
        assert directory, "zip: directory must be specified: "+str(directory)
        assert os.path.isdir(
            directory), "zip: directory must exist and be a directory: "+directory
        assert file, "zip: file must be specified: "+str(file)
        dirpath = Path(directory)
        with zipfile.ZipFile(file, 'w') as archive:
            for f in dirpath.rglob('*'):
                relative = str(f.relative_to(dirpath))
                report.debug("zipping: " + relative)
                archive.write(str(f), relative,
                              compress_type=zipfile.ZIP_DEFLATED)

    @staticmethod
    def unzip(report, archive, target):
        """Unzip the contents of `archive`, as `dir`"""
        assert archive, "unzip: archive must be specified: "+str(archive)
        assert os.path.exists(archive), "unzip: archive must exist: "+archive
        assert target, "unzip: target must be specified: "+str(target)
        assert os.path.isdir(target) or not os.path.exists(
            target), "unzip: if target exists, it must be a directory: "+target

        if not os.path.exists(target):
            os.makedirs(target)

        if os.path.isdir(archive):
            Filesystem.copy(report, archive, target)

        else:
            with zipfile.ZipFile(archive, "r") as zip_ref:
                try:
                    zip_ref.extractall(target)
                except EOFError as e:
                    report.error(
                        "En feil oppstod ved lesing av ZIP-filen. Kanskje noen endret eller slettet den?")
                    report.debug(traceback.format_exc(), preformatted=True)
                    raise e

            Filesystem.fix_permissions(target)
